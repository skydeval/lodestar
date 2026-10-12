//! The verification worker: takes work from `verify_due` in priority
//! order, checks each handle under the limiter, and applies the result.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;
use tokio_postgres::Client;

use super::check::{Checker, Outcome};
use super::control::{Control, Gate};
use super::decide::Rules;
use super::lap;
use super::limiter::{self, Limiter};
use super::pause;
use super::shared::{self, ClaimCounter};
use super::store::{self, Applied};
use crate::queue::{self, KIND_HANDLE};
use crate::{db, handle};

pub struct Opts {
    pub database_url: String,
    pub user_agent: String,
    /// `system`, or a resolver's address.
    pub resolver: String,
    /// Database connections used to apply results.
    pub connections: usize,
    /// A handle claimed by more identities than this is resolved once,
    /// for all of them.
    pub shared_threshold: u8,
    /// A shared handle is refreshed early no more often than this.
    pub early_refresh_floor: Duration,
    /// Domains with so many handles that each gets a walker of its own.
    pub large_domains: Vec<String>,
    /// How much of `accounts` a lap reads at a time, in 8 kB blocks.
    pub lap_chunk_blocks: i64,
    /// The control checks. Empty runs without them, which only the
    /// operator can ask for.
    pub controls: Vec<Control>,
    /// When a control pause starts, results this recent are checked again.
    pub lookback: Duration,
}

/// How many queued items are read at a time, and how many reads one pass
/// makes before it stops looking for something it can start.
const PAGE: i64 = 2000;
const PAGES: usize = 10;
/// How long the worker waits when there is nothing it can start.
const IDLE: Duration = Duration::from_millis(250);
/// How often failing checks are looked at for retries that have come due.
const RETRY_LOOK: Duration = Duration::from_secs(30);
/// How often shared handles are looked at.
const SHARED_LOOK: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(super) struct Counts {
    verified: AtomicU64,
    mismatch: AtomicU64,
    nothing: AtomicU64,
    failing: AtomicU64,
    stale: AtomicU64,
    shared_claimants: AtomicU64,
    shared_resolutions: AtomicU64,
    pub(super) lap_checks: AtomicU64,
    pub(super) lap_overflow: AtomicU64,
}

pub(super) struct Shared {
    pub(super) checker: Checker,
    /// Closed while the control checks say the fault is lodestar's own.
    pub(super) gate: Gate,
    pub(super) limiter: Limiter,
    pub(super) rules: Rules,
    clients: Vec<Mutex<Client>>,
    next: AtomicUsize,
    pub(super) counts: Counts,
}

impl Shared {
    fn db(&self) -> &Mutex<Client> {
        &self.clients[self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len()]
    }

    /// Resolve under the limiter. A caller that already holds a slot for
    /// the domain passes it in.
    ///
    /// None if a control pause started while the check was in flight: its
    /// result is suspect and is dropped.
    async fn resolve(&self, claim: &str, domain: &str, held: Option<limiter::Permit>) -> Option<super::check::Check> {
        self.gate.wait_open().await;
        let epoch = self.gate.epoch();
        let found = {
            let _permit = match held {
                Some(p) => p,
                None => self.limiter.acquire(domain).await,
            };
            self.checker.check(claim).await
        };
        if let Some(wait) = found.retry_after {
            self.limiter.hold(domain, wait.min(self.rules.retry_after_ceiling));
        }
        (self.gate.epoch() == epoch).then_some(found)
    }
}

/// Check one account's handle and apply what is found. Leaves the queue
/// row in place if the claim changed meanwhile, so the new claim is checked.
pub(super) async fn check_one(shared: Arc<Shared>, did: Vec<u8>, claim: String, domain: String, held: Option<limiter::Permit>) -> Result<()> {
    let from_lap = held.is_some();
    let Some(found) = shared.resolve(&claim, &domain, held).await else {
        // Dropped. A queued check keeps its row; a lap's check has none,
        // so it is queued to be made again.
        if from_lap {
            let client = shared.db().lock().await;
            queue::push(&*client, queue::REASON_RECHECK, &[queue::Item { did: &did, kind: KIND_HANDLE, listed: false }]).await?;
        }
        return Ok(());
    };
    let at = chrono::Utc::now().timestamp_millis();
    let mut client = shared.db().lock().await;
    let applied = store::apply_handle_result(&mut client, &shared.rules, &did, &claim, &found, at, false).await?;
    let done = match applied {
        Applied::Status(_) => {
            let counter = match found.outcome {
                Outcome::Did(_) if applied == Applied::Status(db::HANDLE_VERIFIED) => &shared.counts.verified,
                Outcome::Did(_) => &shared.counts.mismatch,
                Outcome::Nothing => &shared.counts.nothing,
                Outcome::Inconclusive(_) => &shared.counts.failing,
            };
            counter.fetch_add(1, Ordering::Relaxed);
            shared.gate.note(&did);
            true
        }
        Applied::Stale => {
            shared.counts.stale.fetch_add(1, Ordering::Relaxed);
            // Stale because the claim changed: the row stays and the new
            // claim is checked. Stale for any other reason: nothing to do.
            let now: Option<String> =
                client.query_opt("SELECT handle_claim FROM accounts WHERE did = $1 AND status = 0", &[&did]).await?.and_then(|r| r.get(0));
            now.as_deref() == Some(claim.as_str()) || now.is_none()
        }
    };
    if done {
        client.execute("DELETE FROM verify_due WHERE did = $1 AND kind = $2", &[&did, &KIND_HANDLE]).await.context("clearing the queue row")?;
    }
    Ok(())
}

/// Resolve a shared handle once and apply it to its row.
async fn check_shared(shared: Arc<Shared>, handle: String, domain: String) -> Result<()> {
    let Some(found) = shared.resolve(&handle, &domain, None).await else {
        return Ok(());
    };
    let at = chrono::Utc::now().timestamp_millis();
    let mut client = shared.db().lock().await;
    shared::apply(&mut client, &shared.rules, &handle, &found, at, false).await?;
    shared.counts.shared_resolutions.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Queue the retries and confirming checks that have come due.
async fn queue_retries(client: &Client) -> Result<u64> {
    client
        .execute(
            "INSERT INTO verify_due (kind, reason, due_at, did) \
             SELECT $1::int2, CASE WHEN nothing_at IS NOT NULL THEN $3::int2 ELSE $2::int2 END, next_attempt_at, did \
             FROM check_failures WHERE kind = 0 AND next_attempt_at <= now() \
             ON CONFLICT (did, kind) DO UPDATE SET reason = LEAST(verify_due.reason, EXCLUDED.reason), \
                 due_at = LEAST(verify_due.due_at, EXCLUDED.due_at)",
            &[&KIND_HANDLE, &queue::REASON_RETRY, &queue::REASON_CONFIRM],
        )
        .await
        .context("queueing retries")
}

/// What is running: by DID for an account's check, by handle for a shared
/// handle's.
#[derive(Clone, PartialEq, Eq, Hash)]
enum Key {
    Account(Vec<u8>),
    Handle(String),
}

struct Running {
    tasks: JoinSet<(Key, String, Result<()>)>,
    keys: HashSet<Key>,
    /// Checks started or waiting, per domain. A domain that is slow or out
    /// of budget is not allowed to fill the worker with waiting checks.
    per_domain: HashMap<String, usize>,
}

impl Running {
    fn finished(&mut self, key: Key, domain: String, result: Result<()>) {
        self.keys.remove(&key);
        if let Some(n) = self.per_domain.get_mut(&domain) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.per_domain.remove(&domain);
            }
        }
        if let Err(e) = result {
            tracing::warn!("a check could not be applied: {e:#}");
        }
    }

    /// True if the domain has room for another check to wait on it.
    fn admit(&mut self, key: &Key, domain: &str, cap: usize) -> bool {
        let n = self.per_domain.entry(domain.to_owned()).or_insert(0);
        if *n >= cap * 2 {
            return false;
        }
        *n += 1;
        self.keys.insert(key.clone());
        true
    }
}

pub async fn run(opts: Opts, mut stop: watch::Receiver<bool>) -> Result<()> {
    let checker = Checker::new(&opts.resolver, &opts.user_agent, &super::check::Limits::default())?;
    let limits = limiter::Limits::default();
    let caps: HashMap<String, usize> = limits.overrides.iter().map(|(d, l)| (d.clone(), l.in_flight)).collect();
    let default_cap = limits.default.in_flight;
    let cap_of = |domain: &str| caps.get(domain).copied().unwrap_or(default_cap);
    let mut clients = Vec::new();
    for _ in 0..opts.connections.max(1) {
        clients.push(Mutex::new(db::connect(&opts.database_url).await?));
    }
    let gate = Gate::new(opts.lookback);
    if opts.controls.is_empty() {
        // Only when the operator has said to run without them.
        tracing::warn!("running with no control checks: lodestar cannot tell its own outage from the network's");
        gate.open();
    }
    let shared = Arc::new(Shared { checker, gate, limiter: Limiter::new(limits), rules: Rules::default(), clients, next: AtomicUsize::new(0), counts: Counts::default() });
    let mut control = db::connect(&opts.database_url).await?;

    // Which handles are shared has to be known before anything is checked.
    let counter = Arc::new(std::sync::RwLock::new(ClaimCounter::new(opts.shared_threshold)));
    let t = Instant::now();
    let claims = {
        let mut filling = ClaimCounter::new(opts.shared_threshold);
        let n = filling.fill(&control).await?;
        *counter.write().expect("counter lock") = filling;
        n
    };
    tracing::info!(claims, seconds = t.elapsed().as_secs(), "verification worker started; claims counted");

    let mut controls: JoinSet<Result<()>> = JoinSet::new();
    if !opts.controls.is_empty() {
        let client = db::connect(&opts.database_url).await?;
        controls.spawn(pause::run(shared.clone(), opts.controls.clone(), client, opts.lookback, stop.clone()));
    }

    // The background sweep: one lap for everything but the large domains,
    // and a walker for each of those.
    let mut laps: JoinSet<Result<()>> = JoinSet::new();
    let large: Vec<String> = opts.large_domains.clone();
    for domain in std::iter::once(None).chain(large.iter().cloned().map(Some)) {
        let o = lap::Opts {
            database_url: opts.database_url.clone(),
            domain,
            large: large.clone(),
            interval: shared.rules.recheck_interval,
            chunk_blocks: opts.lap_chunk_blocks,
        };
        laps.spawn(lap::run(shared.clone(), counter.clone(), o, stop.clone()));
    }

    let mut running = Running { tasks: JoinSet::new(), keys: HashSet::new(), per_domain: HashMap::new() };
    let mut retry_look = tokio::time::interval(RETRY_LOOK);
    let mut shared_look = tokio::time::interval(SHARED_LOOK);
    let mut report = tokio::time::interval(Duration::from_secs(60));
    report.tick().await;

    loop {
        while let Some(joined) = running.tasks.try_join_next() {
            let (key, domain, result) = joined?;
            running.finished(key, domain, result);
        }

        // One pass over the queue, in priority order. It reads on past
        // items it cannot start, so a full domain at the head of the queue
        // does not hide the rest.
        let mut started = 0usize;
        let mut after: Option<(i16, f64, Vec<u8>)> = None;
        // While verification is paused nothing is started.
        let pages = if shared.gate.is_open() { PAGES } else { 0 };
        for _ in 0..pages {
            let (reason, due, did) = after.clone().unwrap_or((0, 0.0, Vec::new()));
            let rows = control
                .query(
                    "SELECT v.did, v.reason, extract(epoch FROM v.due_at)::float8, a.handle_claim, a.status \
                     FROM verify_due v LEFT JOIN accounts a ON a.did = v.did \
                     WHERE v.kind = $1 AND v.due_at <= now() AND (v.reason, extract(epoch FROM v.due_at)::float8, v.did) > ($3, $4, $5) \
                     ORDER BY v.reason, extract(epoch FROM v.due_at)::float8, v.did LIMIT $2",
                    &[&KIND_HANDLE, &PAGE, &reason, &due, &did],
                )
                .await
                .context("reading the queue")?;
            let full_page = rows.len() as i64 == PAGE;
            let mut dead: Vec<Vec<u8>> = Vec::new();
            for r in &rows {
                let did: Vec<u8> = r.get(0);
                let reason: i16 = r.get(1);
                after = Some((reason, r.get(2), did.clone()));
                let key = Key::Account(did.clone());
                if running.keys.contains(&key) {
                    continue;
                }
                let claim: Option<String> = r.get(3);
                let status: Option<i16> = r.get(4);
                // Nothing to check: the account is gone, not active, or
                // claims no handle that can resolve.
                let Some(claim) = claim.filter(|c| status == Some(db::STATUS_ACTIVE) && !handle::never_resolves(c)) else {
                    dead.push(did);
                    continue;
                };
                if reason == queue::REASON_CLAIM {
                    counter.write().expect("counter lock").add(&claim);
                }
                if counter.read().expect("counter lock").is_shared(&claim) {
                    // No check of its own: it reads the handle's row.
                    shared::claimant(&mut control, &did, &claim, reason < queue::REASON_EVENT).await?;
                    shared.counts.shared_claimants.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let domain = limiter::registrable(&claim).to_owned();
                if !running.admit(&key, &domain, cap_of(&domain)) {
                    continue;
                }
                started += 1;
                let shared = shared.clone();
                running.tasks.spawn(async move {
                    let result = check_one(shared, did, claim, domain.clone(), None).await;
                    (key, domain, result)
                });
            }
            if !dead.is_empty() {
                control.execute("DELETE FROM verify_due WHERE kind = $2 AND did = ANY($1)", &[&dead, &KIND_HANDLE]).await?;
            }
            if !full_page {
                break;
            }
        }

        tokio::select! {
            _ = stop.changed() => break,
            Some(ended) = laps.join_next(), if !laps.is_empty() => {
                // A lap only ends when asked to stop, or on an error.
                ended??;
            }
            _ = retry_look.tick() => {
                queue_retries(&control).await?;
            }
            Some(ended) = controls.join_next(), if !controls.is_empty() => {
                ended??;
            }
            _ = shared_look.tick(), if shared.gate.is_open() => {
                for handle in shared::due(&control, &shared.rules, opts.early_refresh_floor.as_secs_f64()).await? {
                    let key = Key::Handle(handle.clone());
                    if running.keys.contains(&key) {
                        continue;
                    }
                    let domain = limiter::registrable(&handle).to_owned();
                    if !running.admit(&key, &domain, cap_of(&domain)) {
                        continue;
                    }
                    let shared = shared.clone();
                    running.tasks.spawn(async move {
                        let result = check_shared(shared, handle, domain.clone()).await;
                        (key, domain, result)
                    });
                }
            }
            _ = report.tick() => {
                let c = &shared.counts;
                tracing::info!(
                    verified = c.verified.load(Ordering::Relaxed),
                    mismatch = c.mismatch.load(Ordering::Relaxed),
                    nothing = c.nothing.load(Ordering::Relaxed),
                    failing = c.failing.load(Ordering::Relaxed),
                    stale = c.stale.load(Ordering::Relaxed),
                    shared_claimants = c.shared_claimants.load(Ordering::Relaxed),
                    shared_resolutions = c.shared_resolutions.load(Ordering::Relaxed),
                    lap_checks = c.lap_checks.load(Ordering::Relaxed),
                    lap_overflow = c.lap_overflow.load(Ordering::Relaxed),
                    in_flight = running.keys.len(),
                    "verification"
                );
            }
            // A finished check frees room: look again at once.
            Some(joined) = running.tasks.join_next(), if !running.tasks.is_empty() => {
                let (key, domain, result) = joined?;
                running.finished(key, domain, result);
            }
            _ = tokio::time::sleep(IDLE), if started == 0 => {}
        }
    }
    tracing::info!(waiting = running.tasks.len(), "verification worker stopping");
    // Checks in flight are short; let them finish and be applied.
    let _ = tokio::time::timeout(Duration::from_secs(30), async { while running.tasks.join_next().await.is_some() {} }).await;
    Ok(())
}
