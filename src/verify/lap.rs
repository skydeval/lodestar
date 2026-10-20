//! The background sweep. A lap checks every handle that is due: first the
//! identities known to have a repo, read from `listed` in key order and
//! looked up in `accounts`, then everything still due, read from
//! `accounts` in storage order, which is a sequential read.
//!
//! One lap covers every domain but the large ones. It never waits for a
//! domain that is out of budget; what it cannot check at once goes to the
//! queue. A large domain has a walker of its own, which does wait, and is
//! paced by that domain's rate.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_postgres::Client;

use super::domains::{Admit, Kind};
use super::limiter;
use super::shared::{self, ClaimCounter};
use super::worker::{How, Shared, check_one};
use crate::db;
use crate::queue::{self, Item};

pub struct Opts {
    pub database_url: String,
    /// The large domain this walker is for. None is the general lap, which
    /// passes over every large domain's accounts (`Shared::large`).
    pub domain: Option<String>,
    /// A new lap starts no sooner than this after the last one started.
    pub interval: Duration,
    pub chunk_blocks: i64,
}

/// How often a waiting lap looks at whether it is time.
const LOOK: Duration = Duration::from_secs(30);
/// Identities read from `listed` at a time in the first pass.
const LISTED_PAGE: i64 = 2000;
/// Pages whose checks may be under way at once. A page's slowest check
/// (a timeout, a provider's stall) then holds back only its own page's
/// position, not the next pages' checks; the pace is the domain's rate.
const PIPELINE: usize = 8;

/// Where a lap is, saved once every check before it has finished.
enum Mark {
    Listed(Vec<u8>),
    Block(i64),
}

/// A page's checks, and where the lap is once they have all finished.
type Page = (JoinSet<Result<()>>, Mark);

/// Wait for the rest of a page's checks.
async fn finish(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    while let Some(done) = tasks.join_next().await {
        if let Err(e) = done? {
            tracing::warn!("a lap check could not be applied: {e:#}");
        }
    }
    Ok(())
}

/// Collect a page's checks that have finished, without waiting. True once
/// all have.
fn finished(tasks: &mut JoinSet<Result<()>>) -> Result<bool> {
    while let Some(done) = tasks.try_join_next() {
        if let Err(e) = done? {
            tracing::warn!("a lap check could not be applied: {e:#}");
        }
    }
    Ok(tasks.is_empty())
}

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
            None => !self
                .shared
                .large
                .read()
                .expect("large lock")
                .iter()
                .any(|l| l == domain),
        }
    }

    async fn save(&self, pass: &str, block: i64) -> Result<()> {
        self.client
            .execute(
                "UPDATE sync_state SET phase = $2, cursor = $3 WHERE source = $1",
                &[&self.source, &pass, &block],
            )
            .await
            .context("saving the lap's position")?;
        Ok(())
    }

    /// Save where the lap is.
    async fn save_mark(&self, mark: &Mark) -> Result<()> {
        match mark {
            Mark::Listed(key) => self
                .client
                .execute(
                    "UPDATE sync_state SET phase = 'listed', position = $2 WHERE source = $1",
                    &[&self.source, &hex(key)],
                )
                .await
                .map(|_| ())
                .context("saving the lap's position"),
            Mark::Block(block) => self.save(Pass::All.name(), *block).await,
        }
    }

    /// Save the position of each page at the front whose checks have all
    /// finished, waiting for the oldest while more than `keep` are under
    /// way.
    async fn settle(&self, pages: &mut VecDeque<Page>, keep: usize) -> Result<()> {
        loop {
            let over = pages.len() > keep;
            let Some((tasks, _)) = pages.front_mut() else {
                break;
            };
            if over {
                finish(tasks).await?;
            } else if !finished(tasks)? {
                break;
            }
            let (_, mark) = pages.pop_front().expect("a page at the front");
            self.save_mark(&mark).await?;
        }
        Ok(())
    }

    /// Start the checks of the due accounts read. None if asked to stop.
    async fn check(
        &mut self,
        rows: Vec<tokio_postgres::Row>,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<Option<JoinSet<Result<()>>>> {
        let mut tasks: JoinSet<Result<()>> = JoinSet::new();
        let mut overflow: Vec<(Vec<u8>, String)> = Vec::new();
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
            let walker = self.opts.domain.is_some();
            // A paused domain: the general lap leaves the account to the
            // queue and moves on; a large domain's own walker waits where
            // it is, and its lap does not end.
            let how = loop {
                let admit = self.shared.pauses.lock().expect("pauses lock").admit(
                    &domain,
                    Kind::First,
                    chrono::Utc::now().timestamp_millis(),
                );
                match admit {
                    Admit::Go => {
                        break Some(How {
                            kind: Kind::First,
                            probe: false,
                        });
                    }
                    Admit::Probe => {
                        break Some(How {
                            kind: Kind::First,
                            probe: true,
                        });
                    }
                    Admit::Wait if !walker => break None,
                    Admit::Wait => {
                        tokio::select! {
                            _ = stop.changed() => return Ok(None),
                            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                        }
                    }
                }
            };
            let Some(how) = how else {
                overflow.push((did, domain));
                continue;
            };
            let permit = if walker {
                // A large domain's walker waits for its domain's rate.
                tokio::select! {
                    _ = stop.changed() => return Ok(None),
                    p = self.shared.limiter.acquire(&domain) => p,
                }
            } else {
                match self.shared.limiter.try_acquire(&domain) {
                    Some(p) => p,
                    None => {
                        overflow.push((did, domain));
                        continue;
                    }
                }
            };
            self.shared
                .counts
                .lap_checks
                .fetch_add(1, Ordering::Relaxed);
            tasks.spawn(check_one(
                self.shared.clone(),
                did,
                claim,
                domain,
                Some(permit),
                how,
            ));
        }
        if !overflow.is_empty() {
            let items: Vec<Item> = overflow
                .iter()
                .map(|(did, domain)| Item {
                    did: did.as_slice(),
                    kind: queue::KIND_HANDLE,
                    listed: false,
                    domain: Some(domain.as_str()),
                })
                .collect();
            queue::push(&self.client, queue::REASON_LAP_OVERFLOW, &items).await?;
            self.shared
                .counts
                .lap_overflow
                .fetch_add(overflow.len() as u64, Ordering::Relaxed);
        }
        // The caller moves the position on once this page's results are in.
        Ok(Some(tasks))
    }

    /// The first pass: `listed` in key order, from after `after`. False
    /// if asked to stop.
    async fn listed_pass(
        &mut self,
        mut after: Vec<u8>,
        lap_started: f64,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        let mut pages: VecDeque<Page> = VecDeque::new();
        loop {
            tokio::select! {
                _ = stop.changed() => return Ok(false),
                _ = self.shared.gate.wait_open() => {}
            }
            let (rows, last) = due_in_listed(&self.client, &after, lap_started).await?;
            let Some(last) = last else {
                self.settle(&mut pages, 0).await?;
                return Ok(true);
            };
            let Some(tasks) = self.check(rows, stop).await? else {
                return Ok(false);
            };
            pages.push_back((tasks, Mark::Listed(last.clone())));
            self.settle(&mut pages, PIPELINE).await?;
            after = last;
            if *stop.borrow() {
                return Ok(false);
            }
        }
    }

    /// The second pass: the whole table in storage order, from `block`.
    /// False if asked to stop.
    async fn pass(
        &mut self,
        mut block: i64,
        lap_started: f64,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        let mut pages: VecDeque<Page> = VecDeque::new();
        loop {
            // The table grows while a lap runs.
            let blocks: i64 = self
                .client
                .query_one("SELECT (pg_relation_size('accounts') / current_setting('block_size')::bigint)::bigint", &[])
                .await?
                .get(0);
            if block >= blocks {
                self.settle(&mut pages, 0).await?;
                return Ok(true);
            }
            // While verification is paused a lap waits where it is.
            tokio::select! {
                _ = stop.changed() => return Ok(false),
                _ = self.shared.gate.wait_open() => {}
            }
            let to = (block + self.opts.chunk_blocks).min(blocks);
            let rows = due_in_blocks(&self.client, block, to, lap_started).await?;
            let Some(tasks) = self.check(rows, stop).await? else {
                return Ok(false);
            };
            pages.push_back((tasks, Mark::Block(to)));
            self.settle(&mut pages, PIPELINE).await?;
            block = to;
            if *stop.borrow() {
                return Ok(false);
            }
        }
    }
}

/// What is due in one stretch of the table.
async fn due_in_blocks(
    client: &Client,
    from: i64,
    to: i64,
    lap_started: f64,
) -> Result<Vec<tokio_postgres::Row>> {
    client
        .query(
            "SELECT a.did, a.handle_claim FROM accounts a \
             WHERE a.ctid >= $1::text::tid AND a.ctid < $2::text::tid \
               AND a.status = 0 AND a.handle_claim IS NOT NULL AND a.handle_status <> $4 \
               AND (a.handle_checked_at IS NULL OR a.handle_checked_at < to_timestamp($3)) \
               AND NOT EXISTS (SELECT 1 FROM check_failures f WHERE f.did = a.did AND f.kind = 0)",
            &[
                &format!("({from},0)"),
                &format!("({to},0)"),
                &lap_started,
                &db::HANDLE_NONE,
            ],
        )
        .await
        .context("reading a stretch of accounts")
}

/// The next page of `listed` after `after`, and what of it is due. The
/// page's last key is returned whether or not anything on it is due;
/// None once `listed` is read to the end.
async fn due_in_listed(
    client: &Client,
    after: &[u8],
    lap_started: f64,
) -> Result<(Vec<tokio_postgres::Row>, Option<Vec<u8>>)> {
    let keys: Vec<Vec<u8>> = client
        .query(
            "SELECT did FROM listed WHERE did > $1 ORDER BY did LIMIT $2",
            &[&after, &LISTED_PAGE],
        )
        .await
        .context("reading listed")?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let Some(last) = keys.last().cloned() else {
        return Ok((Vec::new(), None));
    };
    let rows = client
        .query(
            "SELECT a.did, a.handle_claim FROM accounts a \
             WHERE a.did = ANY($1) \
               AND a.status = 0 AND a.handle_claim IS NOT NULL AND a.handle_status <> $3 \
               AND (a.handle_checked_at IS NULL OR a.handle_checked_at < to_timestamp($2)) \
               AND NOT EXISTS (SELECT 1 FROM check_failures f WHERE f.did = a.did AND f.kind = 0)",
            &[&keys, &lap_started, &db::HANDLE_NONE],
        )
        .await
        .context("reading the listed accounts")?;
    Ok((rows, Some(last)))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Bytes from `hex`. Anything that is not hex reads as empty: the pass
/// starts again from the beginning, which only repeats work.
fn unhex(text: &str) -> Vec<u8> {
    if !text.len().is_multiple_of(2) {
        return Vec::new();
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .unwrap_or_default()
}

fn self_ready(lap: &Lap<'_>) -> bool {
    lap.shared.claims_ready.load(Ordering::Acquire)
}

pub(super) async fn run(
    shared: Arc<Shared>,
    counter: Arc<RwLock<ClaimCounter>>,
    opts: Opts,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let client = db::connect(&opts.database_url).await?;
    let source = format!("lap:{}", opts.domain.as_deref().unwrap_or("general"));
    client
        .execute("INSERT INTO sync_state (source, origin, cursor, phase) VALUES ($1, '', 0, 'idle') ON CONFLICT (source) DO NOTHING", &[&source])
        .await?;
    let mut lap = Lap {
        client,
        shared,
        counter,
        opts: &opts,
        source: source.clone(),
        ensured: HashSet::new(),
    };

    loop {
        let row = lap
            .client
            .query_one(
                "SELECT phase, cursor, extract(epoch FROM lap_started_at)::float8, position, \
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
        let after = row
            .get::<_, Option<String>>(3)
            .map(|p| unhex(&p))
            .unwrap_or_default();
        let time_for_a_new_lap: bool = row.get(4);
        // Handle checks wait for the PLC source to reach the tip, and the
        // sweep's order depends on the marks the first walk sets.
        // And no handle is checked before the claims have been counted,
        // which is what finds the shared handles.
        let ready: bool = row.get::<_, bool>(5) && row.get::<_, bool>(6) && self_ready(&lap);

        let resume = match (phase.as_str(), started) {
            ("listed", Some(s)) => Some((Pass::Listed, block, s)),
            ("all", Some(s)) => Some((Pass::All, block, s)),
            _ if ready && time_for_a_new_lap => {
                let s: f64 = lap
                    .client
                    .query_one(
                        "UPDATE sync_state SET lap_started_at = now(), phase = 'listed', cursor = 0, position = NULL WHERE source = $1 \
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
                let after = if phase == "listed" { after } else { Vec::new() };
                if !lap.listed_pass(after, lap_started, &mut stop).await? {
                    break;
                }
                lap.save(Pass::All.name(), 0).await?;
            }
            let from = if first == Pass::All { block } else { 0 };
            if !lap.pass(from, lap_started, &mut stop).await? {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn both_passes_find_what_is_due() {
        let Some(client) = crate::testdb::fresh().await else {
            return;
        };
        let a = crate::encoding::did_bytes("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
        let b = crate::encoding::did_bytes("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
        for did in [&a, &b] {
            client
                .execute(
                    "INSERT INTO accounts (created_at, method, status, handle_status, did, handle_claim) VALUES (now(), 0, 0, 4, $1, 'x.example.com')",
                    &[did],
                )
                .await
                .unwrap();
        }
        client
            .execute("INSERT INTO listed (did) VALUES ($1)", &[&a])
            .await
            .unwrap();
        let now = chrono::Utc::now().timestamp() as f64;
        let (rows, last) = due_in_listed(&client, &[], now).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(last.as_deref(), Some(a.as_slice()));
        let (rows, last) = due_in_listed(&client, &a, now).await.unwrap();
        assert!(rows.is_empty() && last.is_none());
        let rows = due_in_blocks(&client, 0, 1000, now).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn a_position_reads_back() {
        let did = vec![0u8, 1, 0xab, 0xff, 0x10];
        assert_eq!(unhex(&hex(&did)), did);
        assert_eq!(unhex(""), Vec::<u8>::new());
        assert_eq!(unhex("zz"), Vec::<u8>::new());
        assert_eq!(unhex("abc"), Vec::<u8>::new());
    }
}
