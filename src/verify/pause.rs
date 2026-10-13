//! Running the control checks and acting on what they say: closing the
//! gate when the fault is lodestar's own, and when it opens again moving
//! every failure clock forward by the length of the pause, so lodestar's
//! own outage is never charged to a handle.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tokio_postgres::Client;

use super::check::Outcome;
use super::control::{Control, Judge, Memory, Verdict};
use super::limiter::registrable;
use super::worker::Shared;
use crate::queue::{self, Item};

/// How often the controls are checked.
pub const ROUND: Duration = Duration::from_secs(60);
const SOURCE: &str = "control-pause";

async fn load_memory(client: &Client, controls: &[Control]) -> Result<Vec<Memory>> {
    let rows = client
        .query("SELECT handle, https, (extract(epoch FROM failing_since) * 1000)::bigint, rounds, other_passed, left_out, last FROM control_state", &[])
        .await?;
    Ok(controls
        .iter()
        .map(|c| {
            rows.iter()
                .find(|r| r.get::<_, String>(0) == c.handle && r.get::<_, bool>(1) == c.https)
                .map(|r| Memory {
                    failing_since: r.get(2),
                    rounds: r.get(3),
                    other_passed: r.get(4),
                    left_out: r.get(5),
                    last: r.get(6),
                })
                .unwrap_or_default()
        })
        .collect())
}

async fn save_memory(client: &Client, controls: &[Control], memory: &[Memory]) -> Result<()> {
    for (c, m) in controls.iter().zip(memory) {
        client
            .execute(
                "INSERT INTO control_state (handle, https, failing_since, rounds, other_passed, left_out, last) \
                 VALUES ($1, $2, to_timestamp($3), $4, $5, $6, $7) \
                 ON CONFLICT (handle, https) DO UPDATE SET failing_since = EXCLUDED.failing_since, rounds = EXCLUDED.rounds, \
                     other_passed = EXCLUDED.other_passed, left_out = EXCLUDED.left_out, last = EXCLUDED.last",
                &[&c.handle, &c.https, &m.failing_since.map(|ms| ms as f64 / 1000.0), &m.rounds, &m.other_passed, &m.left_out, &m.last],
            )
            .await
            .context("saving control state")?;
    }
    Ok(())
}

/// Check one control by its own method. None if its domain has asked
/// lodestar to wait.
async fn ask(shared: &Shared, c: &Control) -> Option<bool> {
    if shared.limiter.is_held(registrable(&c.handle)) {
        return None;
    }
    // One more try in the same round, so a single lost request does not count.
    for _ in 0..2 {
        let found = shared.checker.resolve(&c.handle, !c.https, c.https).await;
        if found.outcome == Outcome::Did(c.did.clone()) {
            return Some(true);
        }
        if let Some(wait) = found.retry_after {
            shared.limiter.hold(
                registrable(&c.handle),
                wait.min(shared.rules.retry_after_ceiling),
            );
            break;
        }
    }
    Some(false)
}

/// A pause has started. Results applied in the lookback are suspect: the
/// fault began before a round caught it.
async fn pause_started(client: &Client, shared: &Shared, lookback: Duration) -> Result<()> {
    let recent = shared.gate.take_recent();
    let secs = lookback.as_secs_f64();
    // Marks set in that time are cleared.
    client
        .execute(
            "DELETE FROM check_failures WHERE kind = 0 AND first_failure_at IS NULL \
             AND nothing_at >= now() - make_interval(secs => $1::float8)",
            &[&secs],
        )
        .await?;
    client.execute("UPDATE check_failures SET nothing_at = NULL WHERE kind = 0 AND nothing_at >= now() - make_interval(secs => $1::float8)", &[&secs]).await?;
    client
        .execute(
            "UPDATE shared_handles SET nothing_at = NULL, next_attempt_at = CASE WHEN first_failure_at IS NULL THEN NULL ELSE next_attempt_at END, \
                 refresh_asked = true, early_refresh_at = NULL \
             WHERE checked_at >= now() - make_interval(secs => $1::float8) OR last_attempt_at >= now() - make_interval(secs => $1::float8) \
                OR nothing_at >= now() - make_interval(secs => $1::float8)",
            &[&secs],
        )
        .await?;
    // And each of those handles is checked again when the pause ends.
    let items: Vec<Item> = recent
        .iter()
        .map(|d| Item {
            did: d.as_slice(),
            kind: queue::KIND_HANDLE,
            listed: false,
            domain: None,
        })
        .collect();
    queue::push(client, queue::REASON_RECHECK, &items).await?;
    // The domain samples hold the same suspect results.
    shared.pauses.lock().expect("pauses lock").clear_samples();
    tracing::warn!(
        rechecks = recent.len(),
        "control checks failing: all outbound verification is paused"
    );
    Ok(())
}

/// A pause has ended. Every failure clock moves forward by its length.
async fn pause_ended(client: &mut Client, since_ms: i64, now_ms: i64) -> Result<()> {
    let secs = (now_ms - since_ms).max(0) as f64 / 1000.0;
    let tx = client.transaction().await?;
    for table in ["check_failures", "shared_handles"] {
        tx.execute(
            &format!(
                "UPDATE {table} SET first_failure_at = first_failure_at + make_interval(secs => $1::float8), \
                     last_attempt_at = last_attempt_at + make_interval(secs => $1::float8), \
                     next_attempt_at = next_attempt_at + make_interval(secs => $1::float8), \
                     nothing_at = nothing_at + make_interval(secs => $1::float8) \
                 WHERE next_attempt_at IS NOT NULL"
            ),
            &[&secs],
        )
        .await
        .context("moving failure clocks forward")?;
    }
    tx.execute(
        "UPDATE sync_state SET phase = 'clear' WHERE source = $1",
        &[&SOURCE],
    )
    .await?;
    tx.commit().await?;
    tracing::info!(
        paused_seconds = secs as i64,
        "control checks clear: verification resumes"
    );
    Ok(())
}

pub(super) async fn run(
    shared: Arc<Shared>,
    controls: Vec<Control>,
    mut client: Client,
    lookback: Duration,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let stored = client
        .query_opt("SELECT phase, (extract(epoch FROM tip_at) * 1000)::bigint FROM sync_state WHERE source = $1", &[&SOURCE])
        .await?;
    // When the pause in force began, if one is.
    let mut since: Option<i64> = stored.and_then(|r| {
        (r.get::<_, String>(0) == "paused")
            .then(|| r.get::<_, Option<i64>>(1))
            .flatten()
    });
    let mut judge = Judge::new(
        &controls,
        load_memory(&client, &controls).await?,
        since.is_some(),
    );
    let mut round = tokio::time::interval(ROUND);
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            _ = round.tick() => {}
        }
        let mut results = Vec::with_capacity(controls.len());
        for c in &controls {
            results.push(ask(&shared, c).await);
        }
        let now = chrono::Utc::now().timestamp_millis();
        let was_open = shared.gate.is_open();
        let verdict = judge.round(&results, now);
        save_memory(&client, &controls, &judge.memory).await?;
        let failed: Vec<&str> = controls
            .iter()
            .zip(&results)
            .filter(|(_, r)| **r == Some(false))
            .map(|(c, _)| c.handle.as_str())
            .collect();
        match verdict {
            Verdict::Pause => {
                shared.gate.close();
                since = Some(now);
                client
                    .execute(
                        "INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ($1, '', 0, 'paused', now()) \
                         ON CONFLICT (source) DO UPDATE SET phase = 'paused', tip_at = now()",
                        &[&SOURCE],
                    )
                    .await?;
                tracing::warn!(?failed, "control round failed");
                pause_started(&client, &shared, lookback).await?;
            }
            Verdict::Resume => {
                if let Some(s) = since.take() {
                    pause_ended(&mut client, s, now).await?;
                    // The domain pauses' holds did not run meanwhile.
                    shared.pauses.lock().expect("pauses lock").shift(now - s);
                }
                shared.gate.open();
                if !was_open {
                    tracing::info!("control checks pass: verification may run");
                }
            }
            Verdict::Stay => {
                // A start into a fault: it is a pause from now, though no
                // check has been made yet.
                if judge.paused && since.is_none() && !failed.is_empty() {
                    since = Some(now);
                    client
                        .execute(
                            "INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ($1, '', 0, 'paused', now()) \
                             ON CONFLICT (source) DO UPDATE SET phase = 'paused', tip_at = now()",
                            &[&SOURCE],
                        )
                        .await?;
                    tracing::warn!(
                        ?failed,
                        "control checks failing at start: verification has not begun"
                    );
                } else if !failed.is_empty() {
                    tracing::warn!(?failed, "a control failed");
                }
            }
        }
        let left_out: Vec<&str> = controls
            .iter()
            .zip(&judge.memory)
            .filter(|(_, m)| m.left_out)
            .map(|(c, _)| c.handle.as_str())
            .collect();
        if !left_out.is_empty() {
            tracing::warn!(
                ?left_out,
                "controls left out of the count: the list needs attention"
            );
        }
    }
    Ok(())
}
