//! Keeping the `accounts` indexes near their size. Every lap rewrites the
//! row of each handle it checks. Postgres reuses the space this frees in
//! the table, but an index keeps the size it grew to: one full sweep was
//! measured to double the primary key. So each index's bytes per entry
//! just after a rebuild is kept as its baseline, and an index that has
//! grown past [`GROWTH`] times it is rebuilt, without blocking reads or
//! writes, while no lap is running.
//!
//! Bytes per entry, not size, because the verified-handle index rightly
//! grows as handles are verified.

use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tokio_postgres::Client;

/// The indexes looked after.
const INDEXES: [&str; 2] = ["accounts_pkey", "accounts_verified_handle"];
/// How far past its baseline an index may grow before it is rebuilt.
const GROWTH: f64 = 1.5;
/// An index with fewer entries is left alone: its bytes per entry say
/// little, and rebuilding it saves nothing.
const LEAST_ENTRIES: f64 = 100_000.0;
const LOOK: Duration = Duration::from_secs(3600);

fn source(index: &str) -> String {
    format!("index:{index}")
}

/// An index's size in bytes and its entries, as Postgres last counted them.
async fn measure(client: &Client, index: &str) -> Result<(i64, f64)> {
    let row = client
        .query_one(
            // Resolved through the search path, as REINDEX resolves it.
            "SELECT pg_relation_size(oid), reltuples::float8 FROM pg_class \
             WHERE oid = $1::text::regclass",
            &[&index],
        )
        .await
        .with_context(|| format!("measuring {index}"))?;
    Ok((row.get(0), row.get(1)))
}

/// Keep an index's bytes per entry as it is now, just after a rebuild, as
/// its baseline. An index too small to judge gets none yet.
pub async fn record_baseline(client: &Client, index: &str) -> Result<()> {
    let (bytes, entries) = measure(client, index).await?;
    if entries < LEAST_ENTRIES {
        return Ok(());
    }
    let per = bytes as f64 / entries;
    client
        .execute(
            "INSERT INTO sync_state (source, origin, cursor, phase, position, started_at) \
             VALUES ($1, '', $2, 'baseline', $3, now()) \
             ON CONFLICT (source) DO UPDATE SET cursor = EXCLUDED.cursor, \
                 position = EXCLUDED.position, started_at = EXCLUDED.started_at",
            &[&source(index), &bytes, &per.to_string()],
        )
        .await
        .with_context(|| format!("keeping the baseline of {index}"))?;
    Ok(())
}

/// Rebuild an index without blocking reads or writes. A rebuild that was
/// interrupted leaves an invalid copy behind, which goes first.
async fn rebuild(client: &Client, index: &str) -> Result<()> {
    let leftovers = client
        .query(
            "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
             WHERE NOT i.indisvalid AND c.relname LIKE $1 \
               AND c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = $2::text::regclass)",
            &[&format!("{index}_ccnew%"), &index],
        )
        .await?;
    for r in leftovers {
        let name: String = r.get(0);
        client
            .batch_execute(&format!("DROP INDEX CONCURRENTLY IF EXISTS \"{name}\""))
            .await
            .with_context(|| format!("dropping the leftover {name}"))?;
    }
    client
        .batch_execute(&format!("REINDEX INDEX CONCURRENTLY {index}"))
        .await
        .with_context(|| format!("rebuilding {index}"))
}

/// Rebuild what has grown. Only while the PLC source is live and no lap is
/// running: a lap is what grows them, and the first sync rebuilds the key
/// itself when it ends.
async fn check(client: &Client) -> Result<()> {
    let busy: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM sync_state WHERE source LIKE 'lap:%' AND phase <> 'idle') \
                 OR NOT EXISTS (SELECT 1 FROM sync_state WHERE source = 'plc' AND phase = 'live')",
            &[],
        )
        .await?
        .get(0);
    if busy {
        return Ok(());
    }
    for index in INDEXES {
        let (bytes, entries) = measure(client, index).await?;
        if entries < LEAST_ENTRIES {
            continue;
        }
        let baseline: Option<f64> = client
            .query_opt(
                "SELECT position FROM sync_state WHERE source = $1",
                &[&source(index)],
            )
            .await?
            .and_then(|r| r.get::<_, Option<String>>(0))
            .and_then(|p| p.parse().ok());
        let per = bytes as f64 / entries;
        // With no baseline yet, a rebuild is what gives it one.
        if baseline.is_some_and(|b| per <= b * GROWTH) {
            continue;
        }
        tracing::info!(index, bytes, per_entry = per, baseline = ?baseline, "rebuilding an index that has grown");
        rebuild(client, index).await?;
        record_baseline(client, index).await?;
        let (after, _) = measure(client, index).await?;
        tracing::info!(index, before = bytes, after, "index rebuilt");
    }
    Ok(())
}

pub async fn run(client: Client, mut stop: watch::Receiver<bool>) -> Result<()> {
    let mut look = tokio::time::interval(LOOK);
    loop {
        tokio::select! {
            _ = stop.changed() => return Ok(()),
            _ = look.tick() => {
                // Maintenance never stops verification: a failure is
                // reported and tried again at the next look.
                if let Err(e) = check(&client).await {
                    tracing::warn!("index maintenance: {e:#}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_checks_and_the_rebuild_run_against_the_real_schema() {
        let Some(client) = crate::testdb::fresh().await else {
            return;
        };
        // Not live: nothing is looked at.
        check(&client).await.unwrap();
        client
            .execute(
                "INSERT INTO sync_state (source, origin, cursor, phase) VALUES ('plc', '', 0, 'live')",
                &[],
            )
            .await
            .unwrap();
        // Live, but the indexes are far too small to judge.
        check(&client).await.unwrap();
        for index in INDEXES {
            rebuild(&client, index).await.unwrap();
            record_baseline(&client, index).await.unwrap();
        }
        let baselines: i64 = client
            .query_one(
                "SELECT count(*) FROM sync_state WHERE source LIKE 'index:%'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(baselines, 0, "an empty index gets no baseline");
    }
}
