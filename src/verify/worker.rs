//! The verification worker: takes work from `verify_due` in priority
//! order, checks each handle under the limiter, and applies the result.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;
use tokio_postgres::Client;

use super::check::{Checker, Outcome};
use super::control::{Control, Gate};
use super::decide::Rules;
use super::domains::{Admit, Answer, Change, Kind, Pauses, State};
use super::lap;
use super::limiter::{self, Limiter};
use super::pause;
use super::requests;
use super::shared::{self, ClaimCounter};
use super::store::{self, Applied};
use super::web;
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
    /// Rates and caps: a profile's, with the operator's overrides.
    pub limits: limiter::Limits,
}

/// How many queued items are read at a time, and how many reads one pass
/// makes before it stops looking for something it can start.
const PAGE: i64 = 2000;
const PAGES: usize = 10;
/// How long the worker waits when there is nothing it can start.
const IDLE: Duration = Duration::from_millis(250);
/// The longest rest between passes over the queue. Changed claims and
/// consumers' requests wait at most this long for a pass.
const MAX_REST: Duration = Duration::from_secs(3);
/// The longest a pass over the queue goes on reading further pages.
const PASS_BUDGET: Duration = Duration::from_millis(500);
/// How long the queue's rows for a domain are passed over once it is found
/// to have no room for another waiting check.
const FULL_FOR: Duration = Duration::from_secs(1);
/// And once it is found to be paused. A paused domain is looked at again
/// this often, which is when a probe of it can be started.
const PAUSED_FOR: Duration = Duration::from_secs(30);
/// How often failing checks are looked at for retries that have come due.
const RETRY_LOOK: Duration = Duration::from_secs(30);
/// How often the API role's live requests are looked for.
const REQUEST_LOOK: Duration = Duration::from_millis(100);
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
    documents: AtomicU64,
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
    /// The per-domain pauses.
    pub(super) pauses: std::sync::Mutex<Pauses>,
    /// Set once claims have been counted with the PLC source at the tip.
    /// Until then no handle is checked.
    pub(super) claims_ready: AtomicBool,
}

impl Shared {
    pub(super) fn db(&self) -> &Mutex<Client> {
        &self.clients[self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len()]
    }

    /// Resolve under the limiter. A caller that already holds a slot for
    /// the domain passes it in.
    ///
    /// None if a control pause started while the check was in flight: its
    /// result is suspect and is dropped.
    pub(super) async fn resolve(
        &self,
        claim: &str,
        domain: &str,
        held: Option<limiter::Permit>,
    ) -> Option<super::check::Check> {
        self.gate.wait_open().await;
        let epoch = self.gate.epoch();
        let found = {
            let _permit = match held {
                Some(p) => p,
                None => self.limiter.acquire(domain).await,
            };
            self.checker.check(claim).await
        };
        self.limiter.answered(domain, how_it_answered(&found));
        if let Some(wait) = found.retry_after {
            self.limiter
                .hold(domain, wait.min(self.rules.retry_after_ceiling));
        }
        (self.gate.epoch() == epoch).then_some(found)
    }
}

/// A page of the queue in the order work is taken, after a position and
/// past the domains with no room, with what is needed from `accounts`.
const QUEUE_PAGE: &str = "SELECT q.did, q.reason, q.due_at::text, a.handle_claim, a.status, q.domain \
     FROM (SELECT v.did, v.reason, v.due_at, v.domain FROM verify_due v \
           WHERE v.kind = $1 AND v.due_at <= now() AND (v.reason, v.due_at, v.did) > ($3, $4::text::timestamptz, $5) \
             AND (v.domain IS NULL OR v.domain <> ALL($6)) \
           ORDER BY v.reason, v.due_at, v.did LIMIT $2) q \
     LEFT JOIN accounts a ON a.did = q.did \
     ORDER BY q.reason, q.due_at, q.did";

/// An answer slower than this counts as strain on the domain.
const SLOW: Duration = Duration::from_secs(2);

/// How a check went, for the domain's adapting rate. A refusal says
/// nothing about how much the domain can take; a wait asked for, a
/// failure or a slow answer is strain.
fn how_it_answered(found: &super::check::Check) -> limiter::Answer {
    use super::check::Outcome;
    if found.retry_after.is_some() {
        return limiter::Answer::Strained;
    }
    match found.outcome {
        Outcome::Inconclusive("refused") => limiter::Answer::Neither,
        Outcome::Inconclusive(_) => limiter::Answer::Strained,
        Outcome::Did(_) | Outcome::Nothing if found.took > SLOW => limiter::Answer::Strained,
        Outcome::Did(_) | Outcome::Nothing => limiter::Answer::Clean,
    }
}

/// How a check came to be made.
#[derive(Clone, Copy)]
pub(super) struct How {
    pub kind: Kind,
    /// It is its domain's probe during a pause.
    pub probe: bool,
}

/// Store and report what the per-domain pauses did.
pub(super) async fn save_changes(client: &Client, changes: &[Change]) -> Result<()> {
    for c in changes {
        let source = format!("pause{}:{}", c.pause, c.domain);
        match c.state {
            State::Clear => {
                tracing::info!(domain = %c.domain, pause = c.pause, "domain pause ended");
                client
                    .execute("DELETE FROM sync_state WHERE source = $1", &[&source])
                    .await?;
            }
            State::Paused | State::Released => {
                let phase = if c.state == State::Paused {
                    "paused"
                } else {
                    "released"
                };
                tracing::warn!(domain = %c.domain, pause = c.pause, state = phase, "domain pause");
                client
                    .execute(
                        "INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ($1, '', 0, $2, to_timestamp($3)) \
                         ON CONFLICT (source) DO UPDATE SET phase = EXCLUDED.phase, tip_at = EXCLUDED.tip_at",
                        &[&source, &phase, &(c.since as f64 / 1000.0)],
                    )
                    .await?;
                if c.pause == 2 && c.state == State::Released {
                    settle_failing_domain(client, &c.domain).await?;
                }
            }
        }
    }
    Ok(())
}

/// A domain has failed for a whole hold without one conclusive answer.
/// Its queued handles are settled as unreachable now, not left waiting:
/// a domain that keeps failing re-enters its pause within minutes and
/// would check them a few at a time for months. A verified handle is
/// left to its own grace period. The next lap checks them all again.
async fn settle_failing_domain(client: &Client, domain: &str) -> Result<()> {
    let settled = client
        .execute(
            "WITH settled AS ( \
                 UPDATE accounts a SET handle_status = $2, handle_checked_at = now() \
                 FROM verify_due v \
                 WHERE v.kind = 0 AND v.domain = $1 AND a.did = v.did AND a.handle_status <> $3 \
                 RETURNING a.did), \
             cleared AS (DELETE FROM check_failures f USING settled s WHERE f.did = s.did AND f.kind = 0) \
             DELETE FROM verify_due v USING settled s WHERE v.did = s.did AND v.kind = 0",
            &[&domain, &db::HANDLE_UNREACHABLE, &db::HANDLE_VERIFIED],
        )
        .await
        .context("settling a failing domain's handles")?;
    if settled > 0 {
        tracing::warn!(
            domain,
            settled,
            "handles of a failing domain settled as unreachable"
        );
    }
    Ok(())
}

/// Check one account's handle and apply what is found. Leaves the queue
/// row in place if the claim changed meanwhile, so the new claim is checked.
pub(super) async fn check_one(
    shared: Arc<Shared>,
    did: Vec<u8>,
    claim: String,
    domain: String,
    held: Option<limiter::Permit>,
    how: How,
) -> Result<()> {
    let from_lap = held.is_some();
    let Some(found) = shared.resolve(&claim, &domain, held).await else {
        // Dropped. A queued check keeps its row; a lap's check has none,
        // so it is queued to be made again.
        if from_lap {
            let client = shared.db().lock().await;
            queue::push(
                &*client,
                queue::REASON_RECHECK,
                &[queue::Item {
                    did: &did,
                    kind: KIND_HANDLE,
                    listed: false,
                    domain: None,
                }],
            )
            .await?;
        }
        return Ok(());
    };
    let at = chrono::Utc::now().timestamp_millis();
    let answer = match found.outcome {
        Outcome::Did(_) => Answer::Did,
        Outcome::Nothing => Answer::Nothing,
        Outcome::Inconclusive(_) => Answer::Failed,
    };
    let (failing, hold_demotions) = {
        let pauses = shared.pauses.lock().expect("pauses lock");
        (pauses.failing(&domain), pauses.hold_demotions(&domain))
    };
    let mut client = shared.db().lock().await;
    if how.probe && failing && answer == Answer::Failed {
        // A failing probe changes nothing: no row is opened, moved or settled.
        shared
            .pauses
            .lock()
            .expect("pauses lock")
            .record(&domain, how.kind, false, answer, true, at);
        if from_lap {
            queue::push(
                &*client,
                queue::REASON_LAP_OVERFLOW,
                &[queue::Item {
                    did: &did,
                    kind: KIND_HANDLE,
                    listed: false,
                    domain: None,
                }],
            )
            .await?;
        }
        return Ok(());
    }
    let applied = store::apply_handle_result(
        &mut client,
        &shared.rules,
        &did,
        &claim,
        &found,
        at,
        hold_demotions,
    )
    .await?;
    let done = match applied {
        Applied::Status {
            now,
            was,
            had_failure,
        } => {
            let counter = match answer {
                Answer::Did if now == db::HANDLE_VERIFIED => &shared.counts.verified,
                Answer::Did => &shared.counts.mismatch,
                Answer::Nothing => &shared.counts.nothing,
                Answer::Failed => &shared.counts.failing,
            };
            counter.fetch_add(1, Ordering::Relaxed);
            shared.gate.note(&did);
            // A handle already known to be failing is not a first attempt,
            // however its check came to be made.
            let kind = if how.kind == Kind::First && (had_failure || was == db::HANDLE_UNREACHABLE)
            {
                Kind::Retry
            } else {
                how.kind
            };
            let changes = shared.pauses.lock().expect("pauses lock").record(
                &domain,
                kind,
                was == db::HANDLE_VERIFIED,
                answer,
                how.probe,
                at,
            );
            save_changes(&client, &changes).await?;
            true
        }
        Applied::Stale => {
            shared.counts.stale.fetch_add(1, Ordering::Relaxed);
            // Stale because the claim changed: the row stays and the new
            // claim is checked. Stale for any other reason: nothing to do.
            let now: Option<String> = client
                .query_opt(
                    "SELECT handle_claim FROM accounts WHERE did = $1 AND status = 0",
                    &[&did],
                )
                .await?
                .and_then(|r| r.get(0));
            now.as_deref() == Some(claim.as_str()) || now.is_none()
        }
    };
    if done {
        client
            .execute(
                "DELETE FROM verify_due WHERE did = $1 AND kind = $2",
                &[&did, &KIND_HANDLE],
            )
            .await
            .context("clearing the queue row")?;
    }
    Ok(())
}

/// Fetch one did:web document and store what it says.
async fn fetch_document(shared: Arc<Shared>, did: Vec<u8>, listed: bool) -> Result<()> {
    let text = crate::encoding::did_text(&did).unwrap_or_default();
    let Some(host) = web::host(&text) else {
        // Not a did:web lodestar asks for: rejected without a request.
        let client = shared.db().lock().await;
        client
            .execute(
                "DELETE FROM verify_due WHERE did = $1 AND kind = $2",
                &[&did, &queue::KIND_DOCUMENT],
            )
            .await?;
        return Ok(());
    };
    let domain = limiter::registrable(host).to_owned();
    shared.gate.wait_open().await;
    let epoch = shared.gate.epoch();
    let (body, retry_after) = {
        let _permit = shared.limiter.acquire(&domain).await;
        shared.checker.fetch_document(host).await
    };
    if let Some(wait) = retry_after {
        shared
            .limiter
            .hold(&domain, wait.min(shared.rules.retry_after_ceiling));
    }
    if shared.gate.epoch() != epoch {
        // A control pause started meanwhile: the entry stays for later.
        return Ok(());
    }
    let document = body.and_then(|b| web::read(&text, &b));
    let mut client = shared.db().lock().await;
    web::apply(
        &mut client,
        &web::Rules::default(),
        &did,
        document.as_ref(),
        listed,
    )
    .await?;
    shared.counts.documents.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Queue the document fetches that have come due: retries of failing
/// documents, and the regular refresh of each known one.
async fn queue_documents(client: &Client, refresh_secs: f64) -> Result<()> {
    client
        .execute(
            "INSERT INTO verify_due (kind, reason, due_at, did) \
             SELECT 1::int2, $1::int2, next_attempt_at, did FROM check_failures WHERE kind = 1 AND next_attempt_at <= now() \
             ON CONFLICT (did, kind) DO NOTHING",
            &[&queue::REASON_RETRY],
        )
        .await
        .context("queueing document retries")?;
    client
        .execute(
            "INSERT INTO verify_due (kind, reason, did) \
             SELECT 1::int2, $1::int2, w.did FROM web_documents w \
             WHERE w.last_fetch_at <= now() - make_interval(secs => $2::float8) \
               AND NOT EXISTS (SELECT 1 FROM check_failures f WHERE f.did = w.did AND f.kind = 1) \
             ON CONFLICT (did, kind) DO NOTHING",
            &[&queue::REASON_REFRESH, &refresh_secs],
        )
        .await
        .context("queueing document refreshes")?;
    Ok(())
}

/// Resolve a shared handle once and apply it to its row.
async fn check_shared(shared: Arc<Shared>, handle: String, domain: String) -> Result<()> {
    let Some(found) = shared.resolve(&handle, &domain, None).await else {
        return Ok(());
    };
    let at = chrono::Utc::now().timestamp_millis();
    let hold_demotions = shared
        .pauses
        .lock()
        .expect("pauses lock")
        .hold_demotions(&domain);
    let mut client = shared.db().lock().await;
    shared::apply(
        &mut client,
        &shared.rules,
        &handle,
        &found,
        at,
        hold_demotions,
    )
    .await?;
    shared
        .counts
        .shared_resolutions
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Count every identity's claim, once, when the PLC source is at the tip.
/// Does nothing before then, or once it has been done.
async fn count_claims_at_the_tip(
    client: &Client,
    shared: &Shared,
    counter: &std::sync::RwLock<ClaimCounter>,
    threshold: u8,
) -> Result<()> {
    if shared.claims_ready.load(Ordering::Acquire) {
        return Ok(());
    }
    let live = client
        .query_opt(
            "SELECT 1 FROM sync_state WHERE source = 'plc' AND phase = 'live'",
            &[],
        )
        .await?
        .is_some();
    if !live {
        return Ok(());
    }
    let t = Instant::now();
    let mut filling = ClaimCounter::new(threshold);
    let claims = filling.fill(client).await?;
    *counter.write().expect("counter lock") = filling;
    shared.claims_ready.store(true, Ordering::Release);
    tracing::info!(
        claims,
        seconds = t.elapsed().as_secs(),
        "claims counted; handle checks may run"
    );
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
    /// A live request from the API role.
    Request(i64),
    Account(Vec<u8>),
    Document(Vec<u8>),
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

    fn has_room(&self, domain: &str, cap: usize) -> bool {
        self.per_domain.get(domain).copied().unwrap_or(0) < cap * 2
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
    let checker = Checker::new(
        &opts.resolver,
        &opts.user_agent,
        &super::check::Limits::default(),
    )?;
    let limits = opts.limits.clone();
    let caps: HashMap<String, usize> = limits
        .overrides
        .iter()
        .map(|(d, l)| (d.clone(), l.in_flight))
        .collect();
    let default_cap = limits.default.in_flight;
    let cap_of = |domain: &str| caps.get(domain).copied().unwrap_or(default_cap);
    let mut clients = Vec::new();
    for _ in 0..opts.connections.max(1) {
        clients.push(Mutex::new(db::connect(&opts.database_url).await?));
    }
    let gate = Gate::new(opts.lookback);
    if opts.controls.is_empty() {
        // Only when the operator has said to run without them.
        tracing::warn!(
            "running with no control checks: lodestar cannot tell its own outage from the network's"
        );
        gate.open();
    }
    let mut control = db::connect(&opts.database_url).await?;
    // Domain pauses in force when lodestar stopped are still in force.
    let mut pauses = Pauses::default();
    let now_ms = chrono::Utc::now().timestamp_millis();
    for r in control.query("SELECT source, phase, (extract(epoch FROM tip_at) * 1000)::bigint FROM sync_state WHERE source LIKE 'pause_:%'", &[]).await? {
        let source: String = r.get(0);
        let state = if r.get::<_, String>(1) == "paused" { State::Paused } else { State::Released };
        if let Some((kind, domain)) = source.split_once(':') {
            pauses.restore(domain, if kind == "pause2" { 2 } else { 3 }, state, r.get::<_, Option<i64>>(2).unwrap_or(now_ms), now_ms);
        }
    }
    let shared = Arc::new(Shared {
        pauses: std::sync::Mutex::new(pauses),
        checker,
        gate,
        limiter: Limiter::new(limits),
        rules: Rules::default(),
        clients,
        next: AtomicUsize::new(0),
        counts: Counts::default(),
        claims_ready: AtomicBool::new(false),
    });
    // Which handles are shared has to be known before any handle is
    // checked. The count is taken once the PLC source is at the tip: a
    // count taken during the first sync would miss most of the directory,
    // and the claims that sync applies are not queued.
    let counter = Arc::new(std::sync::RwLock::new(ClaimCounter::new(
        opts.shared_threshold,
    )));
    tracing::info!("verification worker started");
    count_claims_at_the_tip(&control, &shared, &counter, opts.shared_threshold).await?;

    let mut controls: JoinSet<Result<()>> = JoinSet::new();
    if !opts.controls.is_empty() {
        let client = db::connect(&opts.database_url).await?;
        controls.spawn(pause::run(
            shared.clone(),
            opts.controls.clone(),
            client,
            opts.lookback,
            stop.clone(),
        ));
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

    let mut running = Running {
        tasks: JoinSet::new(),
        keys: HashSet::new(),
        per_domain: HashMap::new(),
    };
    let mut retry_look = tokio::time::interval(RETRY_LOOK);
    let mut shared_look = tokio::time::interval(SHARED_LOOK);
    let mut request_look = tokio::time::interval(REQUEST_LOOK);
    let mut report = tokio::time::interval(Duration::from_secs(60));
    report.tick().await;
    let mut last_pass: Option<std::time::Instant> = None;
    let mut last_started = 1usize;
    let mut last_took = Duration::ZERO;
    // Domains whose queued rows are not worth reading for now, and until
    // when: one with no room for another waiting check, or one that is
    // paused. Without this a domain with a great many rows queued fills
    // every pass, and the domains behind it are never reached.
    let mut passed_over: HashMap<String, std::time::Instant> = HashMap::new();

    loop {
        while let Some(joined) = running.tasks.try_join_next() {
            let (key, domain, result) = joined?;
            running.finished(key, domain, result);
        }

        // One pass over the queue, in priority order. It reads on past
        // items it cannot start, so a full domain at the head of the queue
        // does not hide the rest.
        let mut started = 0usize;
        let pass_began = std::time::Instant::now();
        let mut read = 0usize;
        let mut after: Option<(i16, String, Vec<u8>)> = None;
        // While verification is paused nothing is started, and no handle
        // is checked before the claims have been counted.
        // Nor more often than this: with a long queue, checks finish all
        // the time, and a pass for each would read the queue without rest.
        // A pass that started nothing is followed by a longer rest.
        // After a slow pass, rest a few times as long: the read stays a
        // small share of the database's time however long the queue is.
        let mut rest = (last_took * 3).clamp(IDLE, MAX_REST);
        if last_started == 0 {
            rest = rest.max(IDLE * 4);
        }
        let rested = last_pass.is_none_or(|t| t.elapsed() >= rest);
        let pages =
            if rested && shared.gate.is_open() && shared.claims_ready.load(Ordering::Acquire) {
                last_pass = Some(pass_began);
                PAGES
            } else {
                0
            };
        for _ in 0..pages {
            let (reason, due, did) = after.clone().unwrap_or((0, "epoch".to_owned(), Vec::new()));
            passed_over.retain(|_, until| *until > pass_began);
            let skip: Vec<&str> = passed_over.keys().map(String::as_str).collect();
            // The page is taken from the queue alone, along its index, and
            // only its rows are looked up in `accounts`. Joined first, a
            // long queue has every page read all of `accounts`.
            let rows = control
                .query(
                    QUEUE_PAGE,
                    &[&KIND_HANDLE, &PAGE, &reason, &due, &did, &skip],
                )
                .await
                .context("reading the queue")?;
            let full_page = rows.len() as i64 == PAGE;
            read += rows.len();
            let mut dead: Vec<Vec<u8>> = Vec::new();
            // Rows whose domain the queue does not have, or has wrong.
            let mut learned: (Vec<Vec<u8>>, Vec<String>) = (Vec::new(), Vec::new());
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
                let Some(claim) = claim
                    .filter(|c| status == Some(db::STATUS_ACTIVE) && !handle::never_resolves(c))
                else {
                    dead.push(did);
                    continue;
                };
                if reason == queue::REASON_CLAIM {
                    counter.write().expect("counter lock").add(&claim);
                }
                if counter.read().expect("counter lock").is_shared(&claim) {
                    // No check of its own: it reads the handle's row.
                    // A changed claim or a consumer's refresh asks for the
                    // row to be refreshed early. A plain lookup does not.
                    let ask =
                        reason == queue::REASON_CLAIM || reason == queue::REASON_REFRESH_ASKED;
                    shared::claimant(&mut control, &did, &claim, ask).await?;
                    shared
                        .counts
                        .shared_claimants
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let domain = limiter::registrable(&claim).to_owned();
                if r.get::<_, Option<&str>>(5) != Some(domain.as_str()) {
                    learned.0.push(did.clone());
                    learned.1.push(domain.clone());
                }
                if !running.has_room(&domain, cap_of(&domain)) {
                    passed_over.insert(domain, pass_began + FULL_FOR);
                    continue;
                }
                let kind = match reason {
                    queue::REASON_RETRY => Kind::Retry,
                    queue::REASON_CONFIRM => Kind::Confirm,
                    _ => Kind::First,
                };
                let how = match shared.pauses.lock().expect("pauses lock").admit(
                    &domain,
                    kind,
                    chrono::Utc::now().timestamp_millis(),
                ) {
                    // The domain is paused: the entry waits where it is.
                    Admit::Wait => {
                        passed_over.insert(domain, pass_began + PAUSED_FOR);
                        continue;
                    }
                    Admit::Probe => How { kind, probe: true },
                    Admit::Go => How { kind, probe: false },
                };
                running.admit(&key, &domain, cap_of(&domain));
                started += 1;
                let shared = shared.clone();
                running.tasks.spawn(async move {
                    let result = check_one(shared, did, claim, domain.clone(), None, how).await;
                    (key, domain, result)
                });
            }
            if !dead.is_empty() {
                control
                    .execute(
                        "DELETE FROM verify_due WHERE kind = $2 AND did = ANY($1)",
                        &[&dead, &KIND_HANDLE],
                    )
                    .await?;
            }
            if !learned.0.is_empty() {
                control
                    .execute(
                        "UPDATE verify_due v SET domain = t.domain FROM UNNEST($1::bytea[], $2::text[]) AS t(did, domain) \
                         WHERE v.did = t.did AND v.kind = $3",
                        &[&learned.0, &learned.1, &KIND_HANDLE],
                    )
                    .await?;
            }
            // A pass is kept short: requests from consumers and changed
            // claims wait on it. What it did not reach, the next pass
            // reaches sooner, since it reads past what this one learned.
            if !full_page || pass_began.elapsed() >= PASS_BUDGET {
                break;
            }
        }
        if pages != 0 {
            last_started = started;
            last_took = pass_began.elapsed();
            tracing::debug!(
                read,
                started,
                ms = pass_began.elapsed().as_millis() as u64,
                in_flight = running.keys.len(),
                "queue pass"
            );
        }

        tokio::select! {
            _ = stop.changed() => break,
            Some(ended) = laps.join_next(), if !laps.is_empty() => {
                // A lap only ends when asked to stop, or on an error.
                ended??;
            }
            _ = retry_look.tick() => {
                queue_retries(&control).await?;
                count_claims_at_the_tip(&control, &shared, &counter, opts.shared_threshold).await?;
                queue_documents(&control, web::Rules::default().refresh_secs).await?;
                // Let the domain pauses' holds run out.
                let changes = shared.pauses.lock().expect("pauses lock").tick(chrono::Utc::now().timestamp_millis());
                save_changes(&control, &changes).await?;
            }
            _ = request_look.tick() => {
                // A consumer is waiting on these: they go ahead of the queue.
                for request in requests::take(&control).await? {
                    let key = Key::Request(request.id);
                    running.keys.insert(key.clone());
                    let shared = shared.clone();
                    running.tasks.spawn(async move {
                        let result = requests::serve(shared, request).await;
                        (key, String::new(), result)
                    });
                }
            }
            Some(ended) = controls.join_next(), if !controls.is_empty() => {
                ended??;
            }
            _ = shared_look.tick(), if shared.gate.is_open() => {
                // did:web documents waiting to be fetched.
                let rows = control
                    .query(
                        "SELECT did, listed FROM verify_due WHERE kind = $1 AND due_at <= now() ORDER BY reason, due_at LIMIT 200",
                        &[&queue::KIND_DOCUMENT],
                    )
                    .await?;
                for r in rows {
                    let did: Vec<u8> = r.get(0);
                    let listed: bool = r.get(1);
                    let key = Key::Document(did.clone());
                    if running.keys.contains(&key) {
                        continue;
                    }
                    let domain = crate::encoding::did_text(&did).as_deref().and_then(web::host).map(|h| limiter::registrable(h).to_owned()).unwrap_or_default();
                    if !running.admit(&key, &domain, cap_of(&domain)) {
                        continue;
                    }
                    let shared = shared.clone();
                    running.tasks.spawn(async move {
                        let result = fetch_document(shared, did, listed).await;
                        (key, domain, result)
                    });
                }
                for handle in shared::due(&control, &shared.rules, opts.early_refresh_floor.as_secs_f64()).await? {
                    let key = Key::Handle(handle.clone());
                    if running.keys.contains(&key) {
                        continue;
                    }
                    let domain = limiter::registrable(&handle).to_owned();
                    if shared.pauses.lock().expect("pauses lock").failing(&domain) || !running.admit(&key, &domain, cap_of(&domain)) {
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
                let (failing_domains, nothing_domains) = shared.pauses.lock().expect("pauses lock").paused();
                let status = serde_json::json!({
                    "paused_by_controls": !shared.gate.is_open(),
                    "checks": {
                        "verified": c.verified.load(Ordering::Relaxed),
                        "mismatch": c.mismatch.load(Ordering::Relaxed),
                        "nothing": c.nothing.load(Ordering::Relaxed),
                        "failing": c.failing.load(Ordering::Relaxed),
                        "stale": c.stale.load(Ordering::Relaxed),
                    },
                    "shared_claimants": c.shared_claimants.load(Ordering::Relaxed),
                    "shared_resolutions": c.shared_resolutions.load(Ordering::Relaxed),
                    "lap_checks": c.lap_checks.load(Ordering::Relaxed),
                    "lap_overflow": c.lap_overflow.load(Ordering::Relaxed),
                    "documents": c.documents.load(Ordering::Relaxed),
                    "in_flight": running.keys.len(),
                    "domains_paused_failing": failing_domains,
                    "domains_paused_resolving_to_nothing": nothing_domains,
                    "domains_at_their_in_flight_cap": shared.limiter.full_domains(),
                    "domains_climbed": shared.limiter.climbed(20),
                });
                control
                    .execute(
                        "INSERT INTO role_status (role, status) VALUES ('verify', $1) \
                         ON CONFLICT (role) DO UPDATE SET status = EXCLUDED.status, updated_at = now()",
                        &[&status.to_string()],
                    )
                    .await?;
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
                    documents = c.documents.load(Ordering::Relaxed),
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
    tracing::info!(
        waiting = running.tasks.len(),
        "verification worker stopping"
    );
    // Checks in flight are short; let them finish and be applied.
    let _ = tokio::time::timeout(Duration::from_secs(30), async {
        while running.tasks.join_next().await.is_some() {}
    })
    .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding;

    async fn account(client: &Client, did: &str, claim: &str, status: i16) {
        client
            .execute(
                "INSERT INTO accounts (created_at, method, status, handle_status, did, handle_claim) VALUES (now(), 0, 0, $1, $2, $3)",
                &[&status, &encoding::did_bytes(did), &claim],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn the_queue_is_read_in_order_past_full_domains() {
        let Some(client) = crate::testdb::fresh().await else {
            return;
        };
        account(
            &client,
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
            "a.full.example",
            db::HANDLE_PENDING,
        )
        .await;
        account(
            &client,
            "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
            "b.open.example",
            db::HANDLE_PENDING,
        )
        .await;
        let a = encoding::did_bytes("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
        let b = encoding::did_bytes("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
        let items = [
            queue::Item {
                did: &a,
                kind: KIND_HANDLE,
                listed: false,
                domain: Some("full.example"),
            },
            queue::Item {
                did: &b,
                kind: KIND_HANDLE,
                listed: false,
                domain: Some("open.example"),
            },
        ];
        queue::push(&client, queue::REASON_LAP_OVERFLOW, &items)
            .await
            .unwrap();
        let skip: Vec<&str> = vec!["full.example"];
        let rows = client
            .query(
                QUEUE_PAGE,
                &[
                    &KIND_HANDLE,
                    &PAGE,
                    &0i16,
                    &"epoch",
                    &Vec::<u8>::new(),
                    &skip,
                ],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get::<_, Option<String>>(3).as_deref(),
            Some("b.open.example")
        );
        // A changed claim drops the domain the row had.
        queue::push(
            &client,
            queue::REASON_CLAIM,
            &[queue::Item {
                did: &a,
                kind: KIND_HANDLE,
                listed: false,
                domain: None,
            }],
        )
        .await
        .unwrap();
        let rows = client
            .query(
                QUEUE_PAGE,
                &[
                    &KIND_HANDLE,
                    &PAGE,
                    &0i16,
                    &"epoch",
                    &Vec::<u8>::new(),
                    &skip,
                ],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i16>(1), queue::REASON_CLAIM);
    }

    #[tokio::test]
    async fn a_failing_domain_is_settled_except_verified_handles() {
        let Some(client) = crate::testdb::fresh().await else {
            return;
        };
        account(
            &client,
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
            "a.dead.example",
            db::HANDLE_PENDING,
        )
        .await;
        account(
            &client,
            "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
            "b.dead.example",
            db::HANDLE_VERIFIED,
        )
        .await;
        let a = encoding::did_bytes("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
        let b = encoding::did_bytes("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
        let items = [
            queue::Item {
                did: &a,
                kind: KIND_HANDLE,
                listed: false,
                domain: Some("dead.example"),
            },
            queue::Item {
                did: &b,
                kind: KIND_HANDLE,
                listed: false,
                domain: Some("dead.example"),
            },
        ];
        queue::push(&client, queue::REASON_LAP_OVERFLOW, &items)
            .await
            .unwrap();
        settle_failing_domain(&client, "dead.example")
            .await
            .unwrap();
        let rows = client
            .query(
                "SELECT handle_claim, handle_status FROM accounts ORDER BY handle_claim",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows[0].get::<_, i16>(1), db::HANDLE_UNREACHABLE);
        assert_eq!(rows[1].get::<_, i16>(1), db::HANDLE_VERIFIED);
        let left: i64 = client
            .query_one("SELECT count(*) FROM verify_due", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(left, 1);
    }
}
