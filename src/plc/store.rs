//! Loading what a page of operations needs, and writing what it changed,
//! in one transaction with the cursor.

use std::collections::HashMap;

use anyhow::{Context, Result};
use tokio_postgres::{Client, Transaction};

use super::apply::{HistRow, Identity, RecentOp, Status};
use super::op::Op;
use crate::queue::{self, Item};
use crate::{db, encoding, handle};

pub const SOURCE: &str = "plc";

/// Interned PDS endpoints seen so far. Cleared by the caller if a page
/// fails, since it may then hold ids that were never committed.
#[derive(Default)]
pub struct PdsCache(HashMap<String, i32>);

impl PdsCache {
    pub fn clear(&mut self) {
        self.0.clear();
    }

    async fn intern(&mut self, tx: &Transaction<'_>, wanted: Vec<&str>) -> Result<()> {
        let mut missing: Vec<&str> = wanted
            .into_iter()
            .filter(|e| !self.0.contains_key(*e))
            .collect();
        missing.sort_unstable();
        missing.dedup();
        if missing.is_empty() {
            return Ok(());
        }
        tx.execute(
            "INSERT INTO pds_endpoints (endpoint) SELECT e FROM UNNEST($1::text[]) AS t(e) \
             ON CONFLICT (md5(endpoint)) DO NOTHING",
            &[&missing],
        )
        .await
        .context("interning endpoints")?;
        let rows = tx
            .query("SELECT id, endpoint FROM pds_endpoints WHERE md5(endpoint) = ANY(SELECT md5(e) FROM UNNEST($1::text[]) AS t(e))", &[&missing])
            .await?;
        for r in rows {
            self.0.insert(r.get(1), r.get(0));
        }
        Ok(())
    }

    fn id(&self, endpoint: Option<&str>) -> Result<Option<i32>> {
        match endpoint {
            None => Ok(None),
            Some(e) => self
                .0
                .get(e)
                .copied()
                .map(Some)
                .with_context(|| format!("endpoint was not interned: {e}")),
        }
    }
}

#[derive(Default)]
pub struct PageStats {
    pub new_identities: u64,
    pub nullified: u64,
}

async fn load(tx: &Transaction<'_>, dids: &[Vec<u8>]) -> Result<HashMap<Vec<u8>, Identity>> {
    let mut ids: HashMap<Vec<u8>, Identity> = HashMap::with_capacity(dids.len());
    let rows = tx
        .query(
            "SELECT a.did, a.handle_claim, p.endpoint, a.signing_key, a.status, a.created_at::text \
             FROM accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id WHERE a.did = ANY($1)",
            &[&dids],
        )
        .await
        .context("loading accounts")?;
    for r in rows {
        let status: i16 = r.get(4);
        ids.insert(
            r.get(0),
            Identity {
                exists: true,
                claim: r.get(1),
                pds: r.get(2),
                key: r.get(3),
                status: if status == db::STATUS_TOMBSTONED {
                    Status::Tombstoned
                } else {
                    Status::Active
                },
                created_at: Some(r.get(5)),
                ..Identity::default()
            },
        );
    }
    if ids.is_empty() {
        return Ok(ids);
    }
    let existing: Vec<Vec<u8>> = ids.keys().cloned().collect();

    let rows = tx
        .query(
            "SELECT did, seq, cid, (extract(epoch FROM created_at) * 1000)::bigint, hist_seq \
             FROM plc_recent_ops WHERE did = ANY($1) ORDER BY did, seq",
            &[&existing],
        )
        .await
        .context("loading recent operations")?;
    let mut with_recent: Vec<Vec<u8>> = Vec::new();
    for r in rows {
        let did: Vec<u8> = r.get(0);
        if with_recent.last() != Some(&did) {
            with_recent.push(did.clone());
        }
        let id = ids.get_mut(&did).expect("asked for by did");
        id.recent.push(RecentOp {
            seq: r.get(1),
            cid: r.get(2),
            created_ms: r.get(3),
            created_at: None,
            hist_seq: r.get(4),
            new: false,
        });
    }

    // Every row for identities a fork could touch, the last row for the rest.
    let rows = tx
        .query(
            "SELECT h.did, h.seq, h.handle_claim, p.endpoint FROM ( \
                 (SELECT DISTINCT ON (did) did, seq, handle_claim, pds_id FROM identity_history \
                  WHERE did = ANY($1) AND NOT did = ANY($2) ORDER BY did, seq DESC) \
               UNION ALL \
                 (SELECT did, seq, handle_claim, pds_id FROM identity_history WHERE did = ANY($2)) \
             ) h LEFT JOIN pds_endpoints p ON p.id = h.pds_id ORDER BY h.did, h.seq",
            &[&existing, &with_recent],
        )
        .await
        .context("loading history")?;
    for r in rows {
        let did: Vec<u8> = r.get(0);
        let id = ids.get_mut(&did).expect("asked for by did");
        id.hist.push(HistRow {
            seq: r.get(1),
            claim: r.get(2),
            pds: r.get(3),
            changed_at: None,
            new: false,
        });
    }
    Ok(ids)
}

/// Apply one page of operations and commit it with its cursor.
///
/// `queue_claims` is false during the first sync: there would be one claim
/// change per identity, and the first lap covers them.
pub async fn apply_page(
    client: &mut Client,
    pds: &mut PdsCache,
    origin: &str,
    ops: &[Op],
    window_ms: i64,
    queue_claims: bool,
) -> Result<PageStats> {
    let Some(last) = ops.last() else {
        return Ok(PageStats::default());
    };
    let mut stats = PageStats::default();
    let keyed: Vec<Vec<u8>> = ops.iter().map(|o| encoding::did_bytes(&o.did)).collect();
    let mut dids = keyed.clone();
    dids.sort_unstable();
    dids.dedup();

    let tx = client.transaction().await?;
    let mut ids = load(&tx, &dids).await?;
    for (op, did) in ops.iter().zip(&keyed) {
        let id = ids.entry(did.clone()).or_default();
        stats.nullified += id.apply(op, window_ms) as u64;
    }

    let mut endpoints: Vec<&str> = Vec::new();
    for id in ids.values().filter(|i| i.dirty) {
        endpoints.extend(id.pds.as_deref());
        endpoints.extend(
            id.hist
                .iter()
                .filter(|h| h.new)
                .filter_map(|h| h.pds.as_deref()),
        );
    }
    pds.intern(&tx, endpoints).await?;

    // New identities.
    let (mut n_did, mut n_created, mut n_pds, mut n_key, mut n_claim, mut n_status, mut n_hs) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    // Identities that already have a row.
    let (mut u_did, mut u_pds, mut u_key, mut u_claim, mut u_status, mut u_hs, mut u_reset) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let (mut h_did, mut h_seq, mut h_at, mut h_claim, mut h_pds) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut hd_did, mut hd_seq) = (Vec::new(), Vec::new());
    let (mut r_did, mut r_seq, mut r_cid, mut r_at, mut r_hist) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut rd_did, mut rd_seq) = (Vec::new(), Vec::new());

    let mut claims: Vec<Item> = Vec::new();
    for (did, id) in ids.iter().filter(|(_, i)| i.dirty) {
        let status = match id.status {
            Status::Active => db::STATUS_ACTIVE,
            Status::Tombstoned => db::STATUS_TOMBSTONED,
        };
        // A new claim has not been checked. One that can never resolve is
        // never checked.
        let handle_status = match &id.claim {
            Some(c) if !handle::never_resolves(c) => db::HANDLE_PENDING,
            _ => db::HANDLE_NONE,
        };
        if queue_claims && id.claim_changed && handle_status == db::HANDLE_PENDING {
            claims.push(Item {
                did: did.as_slice(),
                kind: queue::KIND_HANDLE,
                listed: false,
            });
        }
        let pds_id = pds.id(id.pds.as_deref())?;
        if id.exists {
            u_did.push(did);
            u_pds.push(pds_id);
            u_key.push(id.key.as_deref());
            u_claim.push(id.claim.as_deref());
            u_status.push(status);
            u_hs.push(handle_status);
            u_reset.push(id.claim_changed);
        } else {
            stats.new_identities += 1;
            n_did.push(did);
            n_created.push(
                id.created_at
                    .as_deref()
                    .context("a new identity has no creation time")?,
            );
            n_pds.push(pds_id);
            n_key.push(id.key.as_deref());
            n_claim.push(id.claim.as_deref());
            n_status.push(status);
            n_hs.push(handle_status);
        }
        for h in id.hist.iter().filter(|h| h.new) {
            h_did.push(did);
            h_seq.push(h.seq);
            h_at.push(
                h.changed_at
                    .as_deref()
                    .context("a new history row has no time")?,
            );
            h_claim.push(h.claim.as_deref());
            h_pds.push(pds.id(h.pds.as_deref())?);
        }
        for seq in &id.hist_deleted {
            hd_did.push(did);
            hd_seq.push(*seq);
        }
        for r in id.recent.iter().filter(|r| r.new) {
            r_did.push(did);
            r_seq.push(r.seq);
            r_cid.push(r.cid.as_slice());
            r_at.push(
                r.created_at
                    .as_deref()
                    .context("a new operation has no time")?,
            );
            r_hist.push(r.hist_seq);
        }
        for seq in &id.recent_deleted {
            rd_did.push(did);
            rd_seq.push(*seq);
        }
    }

    if !n_did.is_empty() {
        tx.execute(
            "INSERT INTO accounts (created_at, pds_id, method, status, handle_status, did, signing_key, handle_claim) \
             SELECT c::timestamptz, p, $8::int2, s, hs, d, k, h \
             FROM UNNEST($1::text[], $2::int4[], $3::int2[], $4::int2[], $5::bytea[], $6::bytea[], $7::text[]) \
                  AS t(c, p, s, hs, d, k, h)",
            &[&n_created, &n_pds, &n_status, &n_hs, &n_did, &n_key, &n_claim, &db::METHOD_PLC],
        )
        .await
        .context("inserting accounts")?;
    }
    if !u_did.is_empty() {
        tx.execute(
            "UPDATE accounts a SET pds_id = t.p, signing_key = t.k, handle_claim = t.h, status = t.s, \
                 handle_status = CASE WHEN t.r THEN t.hs ELSE a.handle_status END, \
                 handle_checked_at = CASE WHEN t.r THEN NULL ELSE a.handle_checked_at END \
             FROM UNNEST($1::bytea[], $2::int4[], $3::bytea[], $4::text[], $5::int2[], $6::int2[], $7::bool[]) \
                  AS t(d, p, k, h, s, hs, r) \
             WHERE a.did = t.d",
            &[&u_did, &u_pds, &u_key, &u_claim, &u_status, &u_hs, &u_reset],
        )
        .await
        .context("updating accounts")?;
    }
    // A new claim never inherits an old claim's failures.
    let reset: Vec<&Vec<u8>> = u_did
        .iter()
        .zip(&u_reset)
        .filter(|(_, r)| **r)
        .map(|(d, _)| *d)
        .collect();
    if !reset.is_empty() {
        tx.execute(
            "DELETE FROM check_failures WHERE kind = 0 AND did = ANY($1)",
            &[&reset],
        )
        .await
        .context("clearing failure state")?;
    }
    // Deletes first: a row removed by a fork can share its seq with one written after it.
    if !hd_did.is_empty() {
        tx.execute(
            "DELETE FROM identity_history h USING UNNEST($1::bytea[], $2::int4[]) AS t(d, s) WHERE h.did = t.d AND h.seq = t.s",
            &[&hd_did, &hd_seq],
        )
        .await
        .context("deleting history")?;
    }
    if !h_did.is_empty() {
        tx.execute(
            "INSERT INTO identity_history (changed_at, seq, pds_id, did, handle_claim) \
             SELECT c::timestamptz, s, p, d, h \
             FROM UNNEST($1::text[], $2::int4[], $3::int4[], $4::bytea[], $5::text[]) AS t(c, s, p, d, h)",
            &[&h_at, &h_seq, &h_pds, &h_did, &h_claim],
        )
        .await
        .context("inserting history")?;
    }
    if !rd_did.is_empty() {
        tx.execute(
            "DELETE FROM plc_recent_ops r USING UNNEST($1::bytea[], $2::int8[]) AS t(d, s) WHERE r.did = t.d AND r.seq = t.s",
            &[&rd_did, &rd_seq],
        )
        .await
        .context("deleting recent operations")?;
    }
    if !r_did.is_empty() {
        tx.execute(
            "INSERT INTO plc_recent_ops (seq, created_at, hist_seq, did, cid) \
             SELECT s, c::timestamptz, h, d, i \
             FROM UNNEST($1::int8[], $2::text[], $3::int4[], $4::bytea[], $5::bytea[]) AS t(s, c, h, d, i)",
            &[&r_seq, &r_at, &r_hist, &r_did, &r_cid],
        )
        .await
        .context("inserting recent operations")?;
    }
    // A changed claim is checked ahead of everything else.
    queue::push(&tx, queue::REASON_CLAIM, &claims).await?;
    tx.execute(
        "INSERT INTO sync_state (source, origin, cursor, phase, last_op_at) VALUES ($1, $2, $3, 'backfill', $4::text::timestamptz) \
         ON CONFLICT (source) DO UPDATE SET cursor = EXCLUDED.cursor, last_op_at = EXCLUDED.last_op_at",
        &[&SOURCE, &origin, &last.seq, &last.created_at],
    )
    .await
    .context("committing the cursor")?;
    tx.commit().await?;
    Ok(stats)
}

/// Drop operations that have left the recovery window, measured from the
/// time of the newest operation applied.
pub async fn prune_recent(client: &Client, newest_created_at: &str, window_ms: i64) -> Result<u64> {
    let n = client
        .execute(
            "DELETE FROM plc_recent_ops WHERE created_at < $1::text::timestamptz - make_interval(secs => $2::float8)",
            &[&newest_created_at, &((window_ms / 1000) as f64)],
        )
        .await
        .context("pruning the recovery window")?;
    Ok(n)
}
