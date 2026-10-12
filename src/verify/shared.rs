//! Handles claimed by many identities. Such a handle is resolved once,
//! into its own row, and its claimants take their status from that row.
//! Two handles account for half the directory's claims; without this each
//! claimant would cost a request to the same domain.

use std::hash::{BuildHasher, Hasher};

use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use tokio_postgres::Client;

use super::check::{Check, Outcome};
use super::decide::{self, Failure, FailureChange, Rules};
use crate::db;

/// An approximate count of claims per handle: four rows of one-byte
/// counters. It never counts down, so it can over-count; a handle wrongly
/// treated as shared costs one row and nothing else.
pub struct ClaimCounter {
    rows: [Vec<u8>; 4],
    seeds: [std::hash::RandomState; 4],
    threshold: u8,
}

const WIDTH: usize = 16 * 1024 * 1024;

impl ClaimCounter {
    pub fn new(threshold: u8) -> Self {
        Self::with_width(threshold, WIDTH)
    }

    fn with_width(threshold: u8, width: usize) -> Self {
        ClaimCounter {
            rows: std::array::from_fn(|_| vec![0u8; width]),
            seeds: std::array::from_fn(|_| std::hash::RandomState::new()),
            threshold,
        }
    }

    fn slots(&self, handle: &str) -> [usize; 4] {
        std::array::from_fn(|i| {
            let mut h = self.seeds[i].build_hasher();
            h.write(handle.as_bytes());
            (h.finish() % self.rows[i].len() as u64) as usize
        })
    }

    pub fn add(&mut self, handle: &str) {
        for (i, slot) in self.slots(handle).into_iter().enumerate() {
            self.rows[i][slot] = self.rows[i][slot].saturating_add(1);
        }
    }

    /// True once more identities than the threshold have claimed the handle.
    pub fn is_shared(&self, handle: &str) -> bool {
        self.slots(handle)
            .into_iter()
            .enumerate()
            .map(|(i, slot)| self.rows[i][slot])
            .min()
            .unwrap_or(0)
            > self.threshold
    }

    /// Count every active identity's claim. One pass over `accounts`.
    pub async fn fill(&mut self, client: &Client) -> Result<u64> {
        let params: [&str; 0] = [];
        let rows = client
            .query_raw(
                "SELECT handle_claim FROM accounts WHERE status = 0 AND handle_claim IS NOT NULL",
                params,
            )
            .await
            .context("reading claims")?;
        tokio::pin!(rows);
        let mut n = 0u64;
        while let Some(row) = rows.try_next().await? {
            self.add(row.get::<_, &str>(0));
            n += 1;
        }
        Ok(n)
    }
}

/// A row exists for the handle, starting from what lodestar already knows:
/// if an account holds it as verified, the row names that account.
pub async fn ensure_row(client: &Client, handle: &str) -> Result<()> {
    client
        .execute(
            "INSERT INTO shared_handles (handle, did, outcome, checked_at) \
             SELECT $1, (SELECT did FROM accounts WHERE handle_claim = $1 AND handle_status = 0), \
                    CASE WHEN EXISTS (SELECT 1 FROM accounts WHERE handle_claim = $1 AND handle_status = 0) THEN 0 ELSE $2::int2 END, \
                    (SELECT handle_checked_at FROM accounts WHERE handle_claim = $1 AND handle_status = 0) \
             ON CONFLICT (handle) DO NOTHING",
            &[&handle, &db::HANDLE_PENDING],
        )
        .await
        .context("creating the shared handle's row")?;
    Ok(())
}

/// A claimant of a shared handle came up for a check. It needs none of its
/// own: it reads the row. A changed claim asks for the row to be refreshed
/// early, and a claimant the row already names is recorded as verified.
pub async fn claimant(
    client: &mut Client,
    did: &[u8],
    handle: &str,
    ask_refresh: bool,
) -> Result<()> {
    ensure_row(client, handle).await?;
    let tx = client.transaction().await?;
    if ask_refresh {
        tx.execute(
            "UPDATE shared_handles SET refresh_asked = true WHERE handle = $1",
            &[&handle],
        )
        .await?;
    }
    let named = tx
        .query_opt(
            "SELECT checked_at FROM shared_handles WHERE handle = $1 AND did = $2",
            &[&handle, &did],
        )
        .await?;
    if named.is_some() {
        // The row's resolution and this claim together are the two-way
        // verification, and the index has to agree with the row.
        tx.execute(
            "UPDATE accounts SET handle_status = $3 WHERE handle_claim = $1 AND handle_status = $4 AND did <> $2",
            &[&handle, &did, &db::HANDLE_MISMATCH, &db::HANDLE_VERIFIED],
        )
        .await?;
        tx.execute(
            "UPDATE accounts a SET handle_status = $3, handle_checked_at = s.checked_at FROM shared_handles s \
             WHERE a.did = $2 AND a.status = 0 AND a.handle_claim = $1 AND s.handle = $1",
            &[&handle, &did, &db::HANDLE_VERIFIED],
        )
        .await?;
    }
    // The row carries the failure state from here on.
    tx.execute(
        "DELETE FROM check_failures WHERE did = $1 AND kind = 0",
        &[&did],
    )
    .await?;
    tx.execute(
        "DELETE FROM verify_due WHERE did = $1 AND kind = 0",
        &[&did],
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// The shared handles due a resolution now: never resolved, a retry or a
/// confirming check come due, an early refresh asked for and allowed, or
/// older than the re-check interval.
pub async fn due(
    client: &Client,
    rules: &Rules,
    early_refresh_floor_secs: f64,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT handle FROM shared_handles WHERE \
                 (checked_at IS NULL AND next_attempt_at IS NULL) \
              OR next_attempt_at <= now() \
              OR (next_attempt_at IS NULL AND refresh_asked \
                  AND (early_refresh_at IS NULL OR early_refresh_at <= now() - make_interval(secs => $1::float8))) \
              OR (next_attempt_at IS NULL AND checked_at <= now() - make_interval(secs => $2::float8))",
            &[&early_refresh_floor_secs, &rules.recheck_interval.as_secs_f64()],
        )
        .await
        .context("reading shared handles")?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

fn ms(column: &str) -> String {
    format!("(extract(epoch FROM {column}) * 1000)::bigint")
}

/// Apply one resolution of a shared handle to its row, and to whichever
/// account holds the handle in the index.
pub async fn apply(
    client: &mut Client,
    rules: &Rules,
    handle: &str,
    check: &Check,
    at: i64,
    hold_demotions: bool,
) -> Result<()> {
    let tx = client.transaction().await?;
    let row = tx
        .query_opt(
            &format!(
                "SELECT did, outcome, {}, {}, {}, {} FROM shared_handles WHERE handle = $1 FOR UPDATE",
                ms("first_failure_at"),
                ms("last_attempt_at"),
                ms("next_attempt_at"),
                ms("nothing_at"),
            ),
            &[&handle],
        )
        .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let named: Option<Vec<u8>> = row.get(0);
    let outcome: i16 = row.get(1);
    let next: Option<i64> = row.get(4);
    let failure = next.map(|n| Failure {
        first_failure_at: row.get(2),
        last_attempt_at: row.get(3),
        next_attempt_at: n,
        nothing_at: row.get(5),
    });
    let at_s = at as f64 / 1000.0;
    let secs = |v: Option<i64>| v.map(|ms| ms as f64 / 1000.0);

    if let Outcome::Did(d) = &check.outcome {
        // The row names whoever the handle resolves to, claimant or not.
        let d = crate::encoding::did_bytes(d);
        tx.execute(
            "UPDATE shared_handles SET did = $2, outcome = 0, checked_at = to_timestamp($3), first_failure_at = NULL, \
                 last_attempt_at = NULL, next_attempt_at = NULL, nothing_at = NULL, early_refresh_at = now(), refresh_asked = false \
             WHERE handle = $1",
            &[&handle, &d, &at_s],
        )
        .await?;
        tx.execute(
            "UPDATE accounts SET handle_status = $3, handle_checked_at = to_timestamp($4) \
             WHERE handle_claim = $1 AND handle_status = $5 AND did <> $2",
            &[
                &handle,
                &d,
                &db::HANDLE_MISMATCH,
                &at_s,
                &db::HANDLE_VERIFIED,
            ],
        )
        .await?;
        tx.execute(
            "UPDATE accounts SET handle_status = $3, handle_checked_at = to_timestamp($4) WHERE did = $2 AND status = 0 AND handle_claim = $1",
            &[&handle, &d, &db::HANDLE_VERIFIED, &at_s],
        )
        .await?;
        tx.commit().await?;
        return Ok(());
    }

    // A row that names a DID is treated as a verified handle: one "nothing"
    // result does not unname it.
    let status = if named.is_some() {
        db::HANDLE_VERIFIED
    } else {
        outcome
    };
    let decision = decide::decide(
        rules,
        status,
        &check.outcome,
        false,
        failure,
        at,
        check.retry_after,
        hold_demotions,
    );
    if let Some(settled) = decision.status {
        // The row now names nobody, and so does the index.
        tx.execute(
            "UPDATE shared_handles SET did = NULL, outcome = $2, checked_at = to_timestamp($3), first_failure_at = NULL, \
                 last_attempt_at = NULL, next_attempt_at = NULL, nothing_at = NULL, early_refresh_at = now(), refresh_asked = false \
             WHERE handle = $1",
            &[&handle, &settled, &at_s],
        )
        .await?;
        tx.execute(
            "UPDATE accounts SET handle_status = $2, handle_checked_at = to_timestamp($3) WHERE handle_claim = $1 AND handle_status = $4",
            &[&handle, &settled, &at_s, &db::HANDLE_VERIFIED],
        )
        .await?;
    } else {
        match decision.failure {
            FailureChange::Set(f) => {
                tx.execute(
                    "UPDATE shared_handles SET first_failure_at = to_timestamp($2), last_attempt_at = to_timestamp($3), \
                         next_attempt_at = to_timestamp($4), nothing_at = to_timestamp($5), early_refresh_at = now(), refresh_asked = false \
                     WHERE handle = $1",
                    &[&handle, &secs(f.first_failure_at), &secs(f.last_attempt_at), &(f.next_attempt_at as f64 / 1000.0), &secs(f.nothing_at)],
                )
                .await?;
            }
            FailureChange::Clear => {
                tx.execute(
                    "UPDATE shared_handles SET checked_at = to_timestamp($2), first_failure_at = NULL, last_attempt_at = NULL, \
                         next_attempt_at = NULL, nothing_at = NULL, early_refresh_at = now(), refresh_asked = false WHERE handle = $1",
                    &[&handle, &at_s],
                )
                .await?;
            }
            // A confirming result that came too soon: nothing changes, and
            // the confirming check stays due when it was.
            FailureChange::Keep => {}
        }
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handle_is_shared_once_past_the_threshold() {
        let mut c = ClaimCounter::with_width(100, 4096);
        for _ in 0..100 {
            c.add("many.example");
        }
        c.add("one.example");
        assert!(!c.is_shared("many.example"));
        c.add("many.example");
        assert!(c.is_shared("many.example"));
        assert!(!c.is_shared("one.example"));
        assert!(!c.is_shared("never.example"));
    }

    #[test]
    fn counters_do_not_wrap() {
        let mut c = ClaimCounter::with_width(100, 64);
        for _ in 0..1000 {
            c.add("many.example");
        }
        assert!(c.is_shared("many.example"));
    }
}
