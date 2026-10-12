//! The background sweep. A lap reads `accounts` in storage order, which
//! is a sequential read, and checks every handle that is due: first the
//! identities known to have a repo, then everything still due.
//!
//! One lap covers every domain but the large ones. It never waits for a
//! domain that is out of budget; what it cannot check at once goes to the
//! queue. A large domain has a walker of its own, which does wait, and is
//! paced by that domain's rate.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_postgres::Client;

use super::limiter;
use super::shared::{self, ClaimCounter};
use super::worker::{Shared, check_one};
use crate::db;
use crate::queue::{self, Item};

pub struct Opts {
    pub database_url: String,
    /// The large domain this walker is for. None is the general lap.
    pub domain: Option<String>,
    /// Every large domain. The general lap passes over their accounts.
    pub large: Vec<String>,
    /// A new lap starts no sooner than this after the last one started.
    pub interval: Duration,
    pub chunk_blocks: i64,
}

/// How often a waiting lap looks at whether it is time.
const LOOK: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Pass {
    /// Identities known to have a repo: the ones consumers ask about.
    Listed,
    All,
}

impl Pass {
    fn name(self) -> &'static str {
        match self {
            Pass::Listed => "listed",
            Pass::All => "all",
        }
    }
}

struct Lap<'a> {
    client: Client,
    shared: Arc<Shared>,
    counter: Arc<RwLock<ClaimCounter>>,
    opts: &'a Opts,
    source: String,
    /// Shared handles whose row this lap has already made sure of.
    ensured: HashSet<String>,
}

impl Lap<'_> {
    fn mine(&self, domain: &str) -> bool {
        match &self.opts.domain {
            Some(d) => d == domain,
            None => !self.opts.large.iter().any(|l| l == domain),
        }
    }

    async fn save(&self, pass: &str, block: i64) -> Result<()> {
        self.client
            .execute("UPDATE sync_state SET phase = $2, cursor = $3 WHERE source = $1", &[&self.source, &pass, &block])
            .await
            .context("saving the lap's position")?;
        Ok(())
    }

    /// Check what is due in one stretch of the table. False if asked to stop.
    async fn chunk(&mut self, pass: Pass, from: i64, to: i64, lap_started: f64, stop: &mut watch::Receiver<bool>) -> Result<bool> {
        let rows = self
            .client
            .query(
                "SELECT a.did, a.handle_claim FROM accounts a \
                 WHERE a.ctid >= $1::text::tid AND a.ctid < $2::text::tid \
                   AND a.status = 0 AND a.handle_claim IS NOT NULL AND a.handle_status <> $5 \
                   AND (a.handle_checked_at IS NULL OR a.handle_checked_at < to_timestamp($3)) \
                   AND ($4 OR a.listed) \
                   AND NOT EXISTS (SELECT 1 FROM check_failures f WHERE f.did = a.did AND f.kind = 0)",
                &[&format!("({from},0)"), &format!("({to},0)"), &lap_started, &(pass == Pass::All), &db::HANDLE_NONE],
            )
            .await
            .context("reading a stretch of accounts")?;

        let mut tasks: JoinSet<Result<()>> = JoinSet::new();
        let mut overflow: Vec<Vec<u8>> = Vec::new();
        for r in rows {
            let did: Vec<u8> = r.get(0);
            let claim: String = r.get(1);
            let domain = limiter::registrable(&claim).to_owned();
            if !self.mine(&domain) {
                continue;
            }
            if self.counter.read().expect("counter lock").is_shared(&claim) {
                // A claimant needs no check of its own. The handle's row is
                // resolved once, for all of them.
                if self.ensured.insert(claim.clone()) {
                    shared::ensure_row(&self.client, &claim).await?;
                }
                continue;
            }
            let permit = if self.opts.domain.is_some() {
                // A large domain's walker waits for its domain's rate.
                tokio::select! {
                    _ = stop.changed() => return Ok(false),
                    p = self.shared.limiter.acquire(&domain) => p,
                }
            } else {
                match self.shared.limiter.try_acquire(&domain) {
                    Some(p) => p,
                    None => {
                        overflow.push(did);
                        continue;
                    }
                }
            };
            self.shared.counts.lap_checks.fetch_add(1, Ordering::Relaxed);
            tasks.spawn(check_one(self.shared.clone(), did, claim, domain, Some(permit)));
        }
        if !overflow.is_empty() {
            let items: Vec<Item> = overflow.iter().map(|d| Item { did: d.as_slice(), kind: queue::KIND_HANDLE, listed: false }).collect();
            queue::push(&self.client, queue::REASON_LAP_OVERFLOW, &items).await?;
            self.shared.counts.lap_overflow.fetch_add(overflow.len() as u64, Ordering::Relaxed);
        }
        // The position moves on only once this stretch's results are in.
        while let Some(done) = tasks.join_next().await {
            if let Err(e) = done? {
                tracing::warn!("a lap check could not be applied: {e:#}");
            }
        }
        Ok(true)
    }

    /// One pass over the table from `block`. False if asked to stop.
    async fn pass(&mut self, pass: Pass, mut block: i64, lap_started: f64, stop: &mut watch::Receiver<bool>) -> Result<bool> {
        loop {
            // The table grows while a lap runs.
            let blocks: i64 = self
                .client
                .query_one("SELECT (pg_relation_size('accounts') / current_setting('block_size')::bigint)::bigint", &[])
                .await?
                .get(0);
            if block >= blocks {
                return Ok(true);
            }
            let to = (block + self.opts.chunk_blocks).min(blocks);
            if !self.chunk(pass, block, to, lap_started, stop).await? {
                return Ok(false);
            }
            block = to;
            self.save(pass.name(), block).await?;
            if *stop.borrow() {
                return Ok(false);
            }
        }
    }
}

pub(super) async fn run(shared: Arc<Shared>, counter: Arc<RwLock<ClaimCounter>>, opts: Opts, mut stop: watch::Receiver<bool>) -> Result<()> {
    let client = db::connect(&opts.database_url).await?;
    let source = format!("lap:{}", opts.domain.as_deref().unwrap_or("general"));
    client
        .execute("INSERT INTO sync_state (source, origin, cursor, phase) VALUES ($1, '', 0, 'idle') ON CONFLICT (source) DO NOTHING", &[&source])
        .await?;
    let mut lap = Lap { client, shared, counter, opts: &opts, source: source.clone(), ensured: HashSet::new() };

    loop {
        let row = lap
            .client
            .query_one(
                "SELECT phase, cursor, extract(epoch FROM lap_started_at)::float8, \
                        COALESCE(lap_started_at <= now() - make_interval(secs => $2::float8), true), \
                        EXISTS (SELECT 1 FROM sync_state WHERE source = 'plc' AND phase = 'live'), \
                        EXISTS (SELECT 1 FROM sync_state WHERE source = 'relay-walk' AND tip_at IS NOT NULL) \
                 FROM sync_state WHERE source = $1",
                &[&source, &opts.interval.as_secs_f64()],
            )
            .await?;
        let phase: String = row.get(0);
        let block: i64 = row.get(1);
        let started: Option<f64> = row.get(2);
        let time_for_a_new_lap: bool = row.get(3);
        // Handle checks wait for the PLC source to reach the tip, and the
        // sweep's order depends on the marks the first walk sets.
        let ready: bool = row.get::<_, bool>(4) && row.get::<_, bool>(5);

        let resume = match (phase.as_str(), started) {
            ("listed", Some(s)) => Some((Pass::Listed, block, s)),
            ("all", Some(s)) => Some((Pass::All, block, s)),
            _ if ready && time_for_a_new_lap => {
                let s: f64 = lap
                    .client
                    .query_one(
                        "UPDATE sync_state SET lap_started_at = now(), phase = 'listed', cursor = 0 WHERE source = $1 \
                         RETURNING extract(epoch FROM lap_started_at)::float8",
                        &[&source],
                    )
                    .await?
                    .get(0);
                tracing::info!(lap = %source, "lap starting");
                lap.ensured.clear();
                Some((Pass::Listed, 0, s))
            }
            _ => None,
        };

        if let Some((first, block, lap_started)) = resume {
            let t = std::time::Instant::now();
            if first == Pass::Listed {
                if !lap.pass(Pass::Listed, block, lap_started, &mut stop).await? {
                    break;
                }
                lap.save(Pass::All.name(), 0).await?;
            }
            let from = if first == Pass::All { block } else { 0 };
            if !lap.pass(Pass::All, from, lap_started, &mut stop).await? {
                break;
            }
            lap.save("idle", 0).await?;
            tracing::info!(lap = %source, seconds = t.elapsed().as_secs(), "lap finished");
            continue;
        }
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(LOOK) => {}
        }
    }
    Ok(())
}
