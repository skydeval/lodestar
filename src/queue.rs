//! `verify_due`: work for the verification role that should not wait for
//! a lap. Any role may add to it; only the verification role takes from it.

use anyhow::{Context, Result};
use tokio_postgres::GenericClient;

/// What is to be done.
pub const KIND_HANDLE: i16 = 0;
pub const KIND_DOCUMENT: i16 = 1;

/// Why, which is also its priority: the tens digit is the level, and a
/// lower number is served first.
pub const REASON_CLAIM: i16 = 10;
pub const REASON_EVENT: i16 = 31;
pub const REASON_WALK: i16 = 32;
pub const REASON_RETRY: i16 = 40;
pub const REASON_CONFIRM: i16 = 41;
pub const REASON_LAP_OVERFLOW: i16 = 42;

pub struct Item<'a> {
    pub did: &'a [u8],
    pub kind: i16,
    /// For a document fetch: the identity is known to have a repo.
    pub listed: bool,
}

/// Queue work. A row that already exists keeps the sooner reason and the
/// earlier time, and a `listed` mark is never lost.
pub async fn push(db: &impl GenericClient, reason: i16, items: &[Item<'_>]) -> Result<u64> {
    if items.is_empty() {
        return Ok(0);
    }
    let dids: Vec<&[u8]> = items.iter().map(|i| i.did).collect();
    let kinds: Vec<i16> = items.iter().map(|i| i.kind).collect();
    let listed: Vec<bool> = items.iter().map(|i| i.listed).collect();
    db.execute(
        "INSERT INTO verify_due (kind, reason, listed, did) \
         SELECT k, $4::int2, bool_or(l), d FROM UNNEST($1::bytea[], $2::int2[], $3::bool[]) AS t(d, k, l) GROUP BY d, k \
         ON CONFLICT (did, kind) DO UPDATE SET reason = LEAST(verify_due.reason, EXCLUDED.reason), \
             due_at = LEAST(verify_due.due_at, EXCLUDED.due_at), listed = verify_due.listed OR EXCLUDED.listed",
        &[&dids, &kinds, &listed, &reason],
    )
    .await
    .context("queueing work")
}

/// How many rows are queued for one reason, counted no further than `cap`.
pub async fn depth(db: &impl GenericClient, reason: i16, cap: i64) -> Result<i64> {
    let row = db
        .query_one("SELECT count(*) FROM (SELECT 1 FROM verify_due WHERE reason = $1 LIMIT $2) x", &[&reason, &cap])
        .await
        .context("measuring the queue")?;
    Ok(row.get(0))
}
