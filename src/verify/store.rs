//! Writing a check's result: the account's status, the verified-handle
//! index, and the failure state, in one transaction.

use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::check::{Check, Outcome};
use super::decide::{self, Failure, FailureChange, Rules};
use crate::{db, encoding};

/// The kind of check a `check_failures` row belongs to.
pub const FAILING_HANDLE: i16 = 0;

#[derive(Debug, PartialEq, Eq)]
pub enum Applied {
    /// The result was applied.
    Status {
        /// The handle's status now.
        now: i16,
        /// Its status when it was checked.
        was: i16,
        /// It had open failure state when it was checked.
        had_failure: bool,
    },
    /// The result no longer fits the account: its claim changed, it is not
    /// active, or a newer check has set its status. Nothing was written.
    Stale,
}

const EPOCH_MS: &str = "(extract(epoch FROM {}) * 1000)::bigint";

fn ms(column: &str) -> String {
    EPOCH_MS.replace("{}", column)
}

/// Apply the result of checking `handle` for the account `did`, resolved
/// at `at` (milliseconds since the epoch).
pub async fn apply_handle_result(client: &mut Client, rules: &Rules, did: &[u8], handle: &str, check: &Check, at: i64, hold_demotions: bool) -> Result<Applied> {
    let tx = client.transaction().await?;
    let row = tx
        .query_opt(
            &format!(
                "SELECT a.status, a.handle_claim, a.handle_status, {}, f.did IS NOT NULL, {}, {}, {}, {} \
                 FROM accounts a LEFT JOIN check_failures f ON f.did = a.did AND f.kind = $2 \
                 WHERE a.did = $1 FOR UPDATE OF a",
                ms("a.handle_checked_at"),
                ms("f.first_failure_at"),
                ms("f.last_attempt_at"),
                ms("f.next_attempt_at"),
                ms("f.nothing_at"),
            ),
            &[&did, &FAILING_HANDLE],
        )
        .await
        .context("loading the account")?;
    let Some(row) = row else {
        return Ok(Applied::Stale);
    };
    let account_status: i16 = row.get(0);
    let claim: Option<String> = row.get(1);
    let status: i16 = row.get(2);
    let checked_at: Option<i64> = row.get(3);
    // A result applies only to the claim that was checked, and only if it
    // was resolved after the status it would replace was set.
    if account_status != db::STATUS_ACTIVE || claim.as_deref() != Some(handle) || checked_at.is_some_and(|c| c >= at) {
        return Ok(Applied::Stale);
    }
    let failure = row.get::<_, bool>(4).then(|| Failure {
        first_failure_at: row.get(5),
        last_attempt_at: row.get(6),
        next_attempt_at: row.get::<_, Option<i64>>(7).unwrap_or(at),
        nothing_at: row.get(8),
    });
    let named_self = matches!(&check.outcome, Outcome::Did(d) if encoding::did_bytes(d) == did);
    let decision = decide::decide(rules, status, &check.outcome, named_self, failure, at, check.retry_after, hold_demotions);
    let at_s = at as f64 / 1000.0;

    if decision.status == Some(db::HANDLE_VERIFIED) {
        // One holder per handle: whoever held it no longer does. The handle
        // resolved to this account, which is a conclusive result for them.
        let displaced: Vec<Vec<u8>> = tx
            .query(
                "UPDATE accounts SET handle_status = $3, handle_checked_at = to_timestamp($4) \
                 WHERE handle_claim = $1 AND handle_status = $5 AND did <> $2 RETURNING did",
                &[&handle, &did, &db::HANDLE_MISMATCH, &at_s, &db::HANDLE_VERIFIED],
            )
            .await
            .context("moving the handle")?
            .iter()
            .map(|r| r.get(0))
            .collect();
        if !displaced.is_empty() {
            tx.execute("DELETE FROM check_failures WHERE kind = $2 AND did = ANY($1)", &[&displaced, &FAILING_HANDLE]).await?;
        }
    }
    if decision.status.is_some() || decision.checked {
        tx.execute(
            "UPDATE accounts SET handle_status = COALESCE($2, handle_status), \
                 handle_checked_at = CASE WHEN $3 THEN to_timestamp($4) ELSE handle_checked_at END \
             WHERE did = $1",
            &[&did, &decision.status, &decision.checked, &at_s],
        )
        .await
        .context("writing the status")?;
    }
    match decision.failure {
        FailureChange::Keep => {}
        FailureChange::Clear => {
            tx.execute("DELETE FROM check_failures WHERE did = $1 AND kind = $2", &[&did, &FAILING_HANDLE]).await?;
        }
        FailureChange::Set(f) => {
            let secs = |v: Option<i64>| v.map(|ms| ms as f64 / 1000.0);
            tx.execute(
                "INSERT INTO check_failures (did, kind, first_failure_at, last_attempt_at, next_attempt_at, nothing_at) \
                 VALUES ($1, $2, to_timestamp($3), to_timestamp($4), to_timestamp($5), to_timestamp($6)) \
                 ON CONFLICT (did, kind) DO UPDATE SET first_failure_at = EXCLUDED.first_failure_at, \
                     last_attempt_at = EXCLUDED.last_attempt_at, next_attempt_at = EXCLUDED.next_attempt_at, nothing_at = EXCLUDED.nothing_at",
                &[&did, &FAILING_HANDLE, &secs(f.first_failure_at), &secs(f.last_attempt_at), &(f.next_attempt_at as f64 / 1000.0), &secs(f.nothing_at)],
            )
            .await
            .context("writing the failure state")?;
        }
    }
    tx.commit().await?;
    Ok(Applied::Status { now: decision.status.unwrap_or(status), was: status, had_failure: failure.is_some() })
}
