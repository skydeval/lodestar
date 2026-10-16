//! The API role: reads over what the other roles have stored. HTTP and
//! JSON, for consumers on a private network. It makes no outbound request.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_postgres::{Client, Row};

use crate::verify::requests;
use crate::{db, encoding, handle, queue};

mod status;
pub use status::Opts as StatusOpts;

pub struct Opts {
    pub database_url: String,
    pub listen: String,
    pub connections: usize,
    /// The PLC source is fresh while its last tip confirmation is no older
    /// than this.
    pub fresh_seconds: i64,
    pub max_batch: usize,
    /// How long a request waits on a live resolution.
    pub live_deadline: Duration,
    /// Most checks consumers may have waiting, per kind of request.
    pub queue_cap: i64,
    /// The status page, on an address of its own. None leaves it off.
    pub status: Option<status::Opts>,
}

struct App {
    clients: Vec<Client>,
    next: AtomicUsize,
    fresh_seconds: i64,
    max_batch: usize,
    /// How long a request waits on a live resolution.
    live_deadline: Duration,
    /// Most checks consumers may have waiting, per kind of request.
    queue_cap: i64,
    /// Recent misses, so a repeated request does not repeat the work.
    misses: Mutex<HashMap<String, (Instant, StatusCode, &'static str)>>,
}

/// How long a miss is remembered, and how many are.
const MISS_TTL: Duration = Duration::from_secs(60);
const MISS_CAP: usize = 10_000;

impl App {
    fn db(&self) -> &Client {
        &self.clients[self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len()]
    }

    fn remembered(&self, key: &str) -> Option<(StatusCode, &'static str)> {
        let misses = self.misses.lock().expect("miss cache lock");
        misses
            .get(key)
            .filter(|(at, _, _)| at.elapsed() < MISS_TTL)
            .map(|(_, code, word)| (*code, *word))
    }

    fn remember(&self, key: &str, code: StatusCode, word: &'static str) {
        let mut misses = self.misses.lock().expect("miss cache lock");
        if misses.len() >= MISS_CAP {
            misses.retain(|_, (at, _, _)| at.elapsed() < MISS_TTL);
            if misses.len() >= MISS_CAP {
                misses.clear();
            }
        }
        misses.insert(key.to_owned(), (Instant::now(), code, word));
    }

    /// Ask the verification role for a live resolution and wait for it, up
    /// to the deadline. None if no answer came in time; the work still
    /// completes and is stored, so a repeat of the request finds it.
    async fn ask(&self, kind: i16, subject: &str) -> Result<Option<(String, Option<Vec<u8>>)>> {
        let db = self.db();
        let id: i64 = db
            .query_one(
                "INSERT INTO resolve_requests (kind, subject, deadline) VALUES ($1, $2, now() + make_interval(secs => $3::float8)) RETURNING id",
                &[&kind, &subject, &self.live_deadline.as_secs_f64()],
            )
            .await
            .context("asking for a live resolution")?
            .get(0);
        let until = Instant::now() + self.live_deadline;
        let mut answer = None;
        while Instant::now() < until {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if let Some(r) = db
                .query_opt(
                    "SELECT outcome, did FROM resolve_requests WHERE id = $1 AND state = 2",
                    &[&id],
                )
                .await?
            {
                answer = Some((r.get::<_, Option<String>>(0).unwrap_or_default(), r.get(1)));
                break;
            }
        }
        db.execute("DELETE FROM resolve_requests WHERE id = $1", &[&id])
            .await?;
        Ok(answer)
    }

    /// Queue a handle check for a consumer, unless that many are already
    /// waiting. True if it was queued.
    async fn queue_check(&self, did: &[u8], reason: i16) -> Result<bool> {
        let db = self.db();
        if queue::depth(db, reason, self.queue_cap).await? >= self.queue_cap {
            return Ok(false);
        }
        queue::push(
            db,
            reason,
            &[queue::Item {
                did,
                kind: queue::KIND_HANDLE,
                listed: false,
                domain: None,
            }],
        )
        .await?;
        Ok(true)
    }
}

/// An answer that is not the account: the status code and the word a
/// consumer switches on.
struct Miss(StatusCode, &'static str, Option<Value>);

impl IntoResponse for Miss {
    fn into_response(self) -> Response {
        let mut body = json!({ "error": self.1 });
        if let Some(source) = self.2 {
            body["source"] = source;
        }
        (self.0, Json(body)).into_response()
    }
}

impl From<anyhow::Error> for Miss {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("request failed: {e:#}");
        Miss(StatusCode::INTERNAL_SERVER_ERROR, "internal", None)
    }
}

impl From<tokio_postgres::Error> for Miss {
    fn from(e: tokio_postgres::Error) -> Self {
        anyhow::Error::from(e).into()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Method {
    Plc,
    Web,
}

/// A DID lodestar indexes, or None if its syntax or method is not one it
/// takes. A `did:web` is hostname only: no path.
fn parse_did(did: &str) -> Option<Method> {
    if let Some(id) = did.strip_prefix("did:plc:") {
        let ok = id.len() == 24
            && id
                .bytes()
                .all(|c| c.is_ascii_lowercase() || (b'2'..=b'7').contains(&c));
        return ok.then_some(Method::Plc);
    }
    if let Some(host) = did.strip_prefix("did:web:") {
        let (name, port) = match host.split_once("%3A") {
            Some((n, p)) => (n, Some(p)),
            None => (host, None),
        };
        let port_ok = port
            .is_none_or(|p| !p.is_empty() && p.len() <= 5 && p.bytes().all(|c| c.is_ascii_digit()));
        let name_ok = name == "localhost" || handle::normalize(name).as_deref() == Some(name);
        return (port_ok && name_ok).then_some(Method::Web);
    }
    None
}

const TS: &str = r#"'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'"#;

fn handle_status(code: i16) -> &'static str {
    match code {
        0 => "verified",
        1 => "mismatch",
        2 => "unresolved",
        3 => "unreachable",
        4 => "pending",
        _ => "none",
    }
}

fn account_status(code: i16) -> &'static str {
    match code {
        db::STATUS_ACTIVE => "active",
        db::STATUS_TOMBSTONED => "tombstoned",
        _ => "unresolvable",
    }
}

/// The key as a Multikey multibase string, whatever form the source used.
fn multikey(stored: Option<Vec<u8>>) -> Option<String> {
    let text = encoding::key_text(&stored?)?;
    Some(
        text.strip_prefix("did:key:")
            .map(str::to_owned)
            .unwrap_or(text),
    )
}

/// The PLC source's position, carried on every account answer so a
/// consumer can judge freshness.
async fn source(app: &App) -> Result<(Value, bool)> {
    let row = app
        .db()
        .query_opt(
            &format!(
                "SELECT phase, cursor, to_char(tip_at AT TIME ZONE 'UTC', {TS}), \
                        to_char(last_op_at AT TIME ZONE 'UTC', {TS}), \
                        COALESCE(tip_at > now() - make_interval(secs => $1::float8), false) \
                 FROM sync_state WHERE source = 'plc'"
            ),
            &[&(app.fresh_seconds as f64)],
        )
        .await
        .context("reading the PLC source's state")?;
    Ok(match row {
        None => (
            json!({ "plc": { "phase": "backfill", "cursor": 0, "tip_confirmed_at": null, "fresh": false } }),
            false,
        ),
        Some(r) => {
            let phase: String = r.get(0);
            let fresh: bool = r.get(4);
            let live = phase == "live" && fresh;
            (
                json!({ "plc": {
                    "phase": phase,
                    "cursor": r.get::<_, i64>(1),
                    "tip_confirmed_at": r.get::<_, Option<String>>(2),
                    "last_operation_created_at": r.get::<_, Option<String>>(3),
                    "fresh": fresh,
                } }),
                live,
            )
        }
    })
}

/// The columns of an account answer. An active claimant of a shared handle
/// takes its handle status and check time from the handle's row.
fn account_columns() -> String {
    format!(
        "a.did, a.method, a.status, p.endpoint, a.signing_key, a.handle_claim, \
         CASE WHEN s.handle IS NULL OR a.status <> 0 THEN a.handle_status \
              WHEN s.did = a.did THEN 0::int2 WHEN s.did IS NOT NULL THEN 1::int2 ELSE s.outcome END, \
         to_char(CASE WHEN s.handle IS NULL OR a.status <> 0 THEN a.handle_checked_at ELSE s.checked_at END AT TIME ZONE 'UTC', {TS}), \
         to_char(a.created_at AT TIME ZONE 'UTC', {TS}), s.handle IS NOT NULL"
    )
}

/// True for an account whose handle has a check of its own to make and has
/// never had one: a consumer asking about it moves it ahead of the sweep.
fn wants_check(r: &Row) -> bool {
    r.get::<_, i16>(2) == db::STATUS_ACTIVE
        && r.get::<_, i16>(6) == db::HANDLE_PENDING
        && !r.get::<_, bool>(9)
}

/// The tables an account answer reads.
const ACCOUNT_FROM: &str = "accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id LEFT JOIN shared_handles s ON s.handle = a.handle_claim";

fn account_json(r: &Row) -> Value {
    let method: i16 = r.get(1);
    let claim: Option<String> = r.get(5);
    let hs: i16 = r.get(6);
    let mut v = json!({
        "did": encoding::did_text(&r.get::<_, Vec<u8>>(0)),
        "status": account_status(r.get(2)),
        "pds": r.get::<_, Option<String>>(3),
        "signing_key": multikey(r.get(4)),
        "handle_status": handle_status(hs),
        "handle_checked_at": r.get::<_, Option<String>>(7),
        "created_at": r.get::<_, String>(8),
        "created_at_kind": if method == db::METHOD_PLC { "plc_genesis" } else { "first_seen" },
    });
    // Only a verified handle is the account's handle. Anything else is a
    // claim and is labelled as one.
    if hs == 0 {
        v["handle"] = json!(claim);
    }
    if let Some(c) = claim {
        v["claimed_handle"] = json!(c);
    }
    v
}

/// What to answer for a DID lodestar has no row for.
fn absent(method: Method, live: bool) -> (StatusCode, &'static str) {
    match method {
        // Never fetched from the directory: either it does not exist or
        // lodestar has not caught up to it.
        Method::Plc if live => (StatusCode::NOT_FOUND, "not_found"),
        Method::Plc => (StatusCode::SERVICE_UNAVAILABLE, "syncing"),
        // Resolving an unseen did:web is the verification role's work.
        Method::Web => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
    }
}

async fn get_account(
    State(app): State<Arc<App>>,
    Path(did): Path<String>,
) -> Result<Json<Value>, Miss> {
    let method = parse_did(&did).ok_or(Miss(StatusCode::BAD_REQUEST, "invalid_did", None))?;
    let (src, live) = source(&app).await?;
    let row = app
        .db()
        .query_opt(
            &format!(
                "SELECT {} FROM {ACCOUNT_FROM} WHERE a.did = $1",
                account_columns()
            ),
            &[&encoding::did_bytes(&did)],
        )
        .await?;
    let row = match row {
        Some(r) => Some(r),
        // A did:web lodestar has not seen is resolved now, by the
        // verification role, and stored if it is an account.
        None if method == Method::Web => {
            if let Some((code, word)) = app.remembered(&did) {
                return Err(Miss(code, word, Some(src)));
            }
            match app.ask(requests::ASK_DOCUMENT, &did).await? {
                Some((outcome, _)) if outcome == requests::STORED => {
                    app.db()
                        .query_opt(
                            &format!(
                                "SELECT {} FROM {ACCOUNT_FROM} WHERE a.did = $1",
                                account_columns()
                            ),
                            &[&encoding::did_bytes(&did)],
                        )
                        .await?
                }
                Some((outcome, _)) if outcome == requests::NOT_AN_ACCOUNT => {
                    app.remember(&did, StatusCode::NOT_FOUND, "not_an_account");
                    return Err(Miss(StatusCode::NOT_FOUND, "not_an_account", Some(src)));
                }
                _ => {
                    app.remember(&did, StatusCode::SERVICE_UNAVAILABLE, "unavailable");
                    return Err(Miss(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "unavailable",
                        Some(src),
                    ));
                }
            }
        }
        None => None,
    };
    match row {
        Some(r) => {
            if wants_check(&r) {
                app.queue_check(&encoding::did_bytes(&did), queue::REASON_LOOKUP)
                    .await?;
            }
            let mut v = account_json(&r);
            v["source"] = src;
            Ok(Json(v))
        }
        None => {
            let (code, word) = absent(method, live);
            Err(Miss(code, word, Some(src)))
        }
    }
}

/// A consumer's signal that what it holds looks stale. A did:web's
/// document is fetched again now; any account's handle is checked again
/// ahead of the sweep.
async fn post_refresh(
    State(app): State<Arc<App>>,
    Path(did): Path<String>,
) -> Result<Json<Value>, Miss> {
    let method = parse_did(&did).ok_or(Miss(StatusCode::BAD_REQUEST, "invalid_did", None))?;
    let key = encoding::did_bytes(&did);
    let (src, live) = source(&app).await?;
    let mut document = None;
    if method == Method::Web {
        document = Some(match app.ask(requests::ASK_DOCUMENT, &did).await? {
            Some((outcome, _)) => outcome,
            None => requests::UNAVAILABLE.to_owned(),
        });
    }
    let row = app
        .db()
        .query_opt(
            &format!(
                "SELECT {} FROM {ACCOUNT_FROM} WHERE a.did = $1",
                account_columns()
            ),
            &[&key],
        )
        .await?;
    let Some(r) = row else {
        let (code, word) = absent(method, live);
        return Err(Miss(code, word, Some(src)));
    };
    let queued = r.get::<_, i16>(2) == db::STATUS_ACTIVE
        && r.get::<_, Option<String>>(5).is_some()
        && app.queue_check(&key, queue::REASON_REFRESH_ASKED).await?;
    let mut v = account_json(&r);
    v["source"] = src;
    v["refresh"] = json!({ "check_queued": queued, "document": document });
    Ok(Json(v))
}

#[derive(Deserialize)]
struct Batch {
    dids: Vec<String>,
}

async fn post_accounts(
    State(app): State<Arc<App>>,
    Json(batch): Json<Batch>,
) -> Result<Json<Value>, Miss> {
    if batch.dids.len() > app.max_batch {
        return Err(Miss(StatusCode::BAD_REQUEST, "batch_too_large", None));
    }
    let (src, live) = source(&app).await?;
    let mut results = serde_json::Map::with_capacity(batch.dids.len());
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(batch.dids.len());
    for did in &batch.dids {
        match parse_did(did) {
            Some(_) => keys.push(encoding::did_bytes(did)),
            None => {
                results.insert(did.clone(), json!({ "error": "invalid_did" }));
            }
        }
    }
    let rows = app
        .db()
        .query(
            &format!(
                "SELECT {} FROM {ACCOUNT_FROM} WHERE a.did = ANY($1)",
                account_columns()
            ),
            &[&keys],
        )
        .await?;
    let mut wanting: Vec<Vec<u8>> = Vec::new();
    for r in &rows {
        if wants_check(r) {
            wanting.push(r.get(0));
        }
        let v = account_json(r);
        if let Some(did) = v["did"].as_str().map(str::to_owned) {
            results.insert(did, v);
        }
    }
    for did in &batch.dids {
        if results.contains_key(did) {
            continue;
        }
        // A batch never waits on a live fetch: an unseen did:web is unknown.
        let word = match parse_did(did) {
            Some(Method::Plc) => absent(Method::Plc, live).1,
            _ => "unknown",
        };
        results.insert(did.clone(), json!({ "error": word }));
    }
    // Never-checked handles in a batch go ahead of the sweep, up to a bound.
    let db = app.db();
    let room = app.queue_cap - queue::depth(db, queue::REASON_BATCH, app.queue_cap).await?;
    wanting.truncate(room.max(0) as usize);
    let items: Vec<queue::Item> = wanting
        .iter()
        .map(|d| queue::Item {
            did: d.as_slice(),
            kind: queue::KIND_HANDLE,
            listed: false,
            domain: None,
        })
        .collect();
    queue::push(db, queue::REASON_BATCH, &items).await?;
    Ok(Json(json!({ "accounts": results, "source": src })))
}

#[derive(Deserialize)]
struct Page {
    limit: Option<i64>,
    /// The `next_cursor` of the page before.
    cursor: Option<i32>,
}

async fn get_history(
    State(app): State<Arc<App>>,
    Path(did): Path<String>,
    Query(page): Query<Page>,
) -> Result<Json<Value>, Miss> {
    let method = parse_did(&did).ok_or(Miss(StatusCode::BAD_REQUEST, "invalid_did", None))?;
    let key = encoding::did_bytes(&did);
    let limit = page.limit.unwrap_or(50).clamp(1, 200);
    let (src, live) = source(&app).await?;
    let account = app
        .db()
        .query_opt(
            &format!(
                "SELECT a.status, a.handle_claim, p.endpoint, to_char(a.created_at AT TIME ZONE 'UTC', {TS}) \
                 FROM accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id WHERE a.did = $1"
            ),
            &[&key],
        )
        .await?;
    let Some(account) = account else {
        let (code, word) = absent(method, live);
        return Err(Miss(code, word, Some(src)));
    };
    let rows = app
        .db()
        .query(
            &format!(
                "SELECT h.seq, to_char(h.changed_at AT TIME ZONE 'UTC', {TS}), h.handle_claim, p.endpoint \
                 FROM identity_history h LEFT JOIN pds_endpoints p ON p.id = h.pds_id \
                 WHERE h.did = $1 AND h.seq < $2 ORDER BY h.seq DESC LIMIT $3"
            ),
            &[&key, &page.cursor.unwrap_or(i32::MAX), &(limit + 1)],
        )
        .await?;
    let more = rows.len() as i64 > limit;
    let mut entries: Vec<Value> = rows
        .iter()
        .take(limit as usize)
        .map(|r| json!({ "changed_at": r.get::<_, String>(1), "handle_claim": r.get::<_, Option<String>>(2), "pds": r.get::<_, Option<String>>(3) }))
        .collect();
    let next_cursor = if more {
        rows.get(limit as usize - 1).map(|r| r.get::<_, i32>(0))
    } else {
        None
    };
    // An identity that has never changed has no rows: its history is its
    // current state, since it was created.
    let status: i16 = account.get(0);
    if entries.is_empty() && page.cursor.is_none() && status == db::STATUS_ACTIVE {
        entries.push(json!({
            "changed_at": account.get::<_, String>(3),
            "handle_claim": account.get::<_, Option<String>>(1),
            "pds": account.get::<_, Option<String>>(2),
        }));
    }
    Ok(Json(json!({
        "did": did,
        "status": account_status(status),
        "entries": entries,
        "next_cursor": next_cursor,
        "note": "handle_claim values are claims, not verified handles",
        "source": src,
    })))
}

async fn get_handle(
    State(app): State<Arc<App>>,
    Path(raw): Path<String>,
) -> Result<Json<Value>, Miss> {
    let handle =
        handle::normalize(&raw).ok_or(Miss(StatusCode::BAD_REQUEST, "invalid_handle", None))?;
    if handle::never_resolves(&handle) {
        return Err(Miss(StatusCode::NOT_FOUND, "not_found", None));
    }
    let row = app
        .db()
        .query_opt(
            &format!(
                "SELECT did, to_char(handle_checked_at AT TIME ZONE 'UTC', {TS}) FROM accounts \
                 WHERE handle_claim = $1 AND handle_status = 0"
            ),
            &[&handle],
        )
        .await?;
    match row {
        Some(r) => Ok(Json(json!({
            "handle": handle,
            "did": encoding::did_text(&r.get::<_, Vec<u8>>(0)),
            "checked_at": r.get::<_, Option<String>>(1),
        }))),
        // Not in the index: lodestar resolves the handle itself, through
        // the verification role, and waits for it.
        None => {
            if let Some((code, word)) = app.remembered(&handle) {
                return Err(Miss(code, word, None));
            }
            let (src, live) = source(&app).await?;
            // Until the first sync reaches the tip no handle is checked,
            // so there is nothing to wait for.
            if src.pointer("/plc/phase").and_then(Value::as_str) != Some("live") {
                return Err(Miss(StatusCode::SERVICE_UNAVAILABLE, "syncing", Some(src)));
            }
            let (code, word) = match app.ask(requests::ASK_HANDLE, &handle).await? {
                Some((outcome, Some(did))) if outcome == requests::FOUND => {
                    return Ok(Json(json!({
                        "handle": handle,
                        "did": encoding::did_text(&did),
                        "checked_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    })));
                }
                // It resolves to an identity lodestar does not have, or has
                // with another claim. If the PLC source is behind, lodestar
                // may simply not have caught up.
                Some((outcome, _))
                    if (outcome == requests::UNKNOWN_PLC || outcome == requests::CLAIM_DIFFERS)
                        && !live =>
                {
                    return Err(Miss(StatusCode::SERVICE_UNAVAILABLE, "syncing", Some(src)));
                }
                Some((outcome, _)) if outcome == requests::UNAVAILABLE => {
                    (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
                }
                Some(_) => (StatusCode::NOT_FOUND, "not_found"),
                None => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            };
            app.remember(&handle, code, word);
            Err(Miss(code, word, None))
        }
    }
}

async fn get_health(State(app): State<Arc<App>>) -> Result<Json<Value>, Miss> {
    let (src, _) = source(&app).await?;
    let rows = app
        .db()
        .query(
            "SELECT c.relname, pg_table_size(c.oid), pg_indexes_size(c.oid), c.reltuples::bigint \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY c.relname",
            &[],
        )
        .await?;
    let mut storage = serde_json::Map::new();
    for r in rows {
        storage.insert(
            r.get(0),
            json!({ "table_bytes": r.get::<_, i64>(1), "index_bytes": r.get::<_, i64>(2), "rows_estimate": r.get::<_, i64>(3) }),
        );
    }
    let verification = verification(&app).await?;
    Ok(Json(
        json!({ "version": env!("CARGO_PKG_VERSION"), "sources": src, "verification": verification, "storage": storage }),
    ))
}

/// What is known about verification: the worker's own report, the queue,
/// the laps, the pauses and the control checks.
async fn verification(app: &App) -> Result<Value> {
    let db = app.db();
    let worker = match db
        .query_opt(&format!("SELECT status, to_char(updated_at AT TIME ZONE 'UTC', {TS}) FROM role_status WHERE role = 'verify'"), &[])
        .await?
    {
        Some(r) => {
            let mut v: Value = serde_json::from_str(&r.get::<_, String>(0)).unwrap_or(Value::Null);
            if v.is_object() {
                v["reported_at"] = json!(r.get::<_, String>(1));
            }
            v
        }
        None => Value::Null,
    };
    let mut queue = serde_json::Map::new();
    for r in db
        .query(
            "SELECT kind, reason, count(*) FROM verify_due GROUP BY 1, 2 ORDER BY 1, 2",
            &[],
        )
        .await?
    {
        let what = if r.get::<_, i16>(0) == queue::KIND_HANDLE {
            "handle"
        } else {
            "document"
        };
        queue.insert(
            format!("{what}:{}", r.get::<_, i16>(1)),
            json!(r.get::<_, i64>(2)),
        );
    }
    let mut laps = serde_json::Map::new();
    let mut pauses = Vec::new();
    for r in db
        .query(
            &format!(
                "SELECT source, phase, cursor, to_char(lap_started_at AT TIME ZONE 'UTC', {TS}), to_char(tip_at AT TIME ZONE 'UTC', {TS}) \
                 FROM sync_state WHERE source LIKE 'lap:%' OR source LIKE 'pause%' OR source = 'control-pause' ORDER BY source"
            ),
            &[],
        )
        .await?
    {
        let source: String = r.get(0);
        let phase: String = r.get(1);
        match source.strip_prefix("lap:") {
            Some(name) => {
                laps.insert(name.to_owned(), json!({ "pass": phase, "block": r.get::<_, i64>(2), "started_at": r.get::<_, Option<String>>(3) }));
            }
            None => pauses.push(json!({ "pause": source, "state": phase, "since": r.get::<_, Option<String>>(4) })),
        }
    }
    let controls: Vec<Value> = db
        .query(&format!("SELECT handle, https, last, left_out, to_char(failing_since AT TIME ZONE 'UTC', {TS}) FROM control_state ORDER BY https, handle"), &[])
        .await?
        .iter()
        .map(|r| {
            json!({
                "handle": r.get::<_, String>(0),
                "method": if r.get::<_, bool>(1) { "https" } else { "dns" },
                "passed_last_round": r.get::<_, Option<bool>>(2),
                "left_out": r.get::<_, bool>(3),
                "failing_since": r.get::<_, Option<String>>(4),
            })
        })
        .collect();
    let counts = db
        .query_one("SELECT (SELECT count(*) FROM shared_handles), (SELECT count(*) FROM check_failures), (SELECT count(*) FROM check_failures WHERE nothing_at IS NOT NULL)", &[])
        .await?;
    Ok(json!({
        "worker": worker,
        "queue": queue,
        "laps": laps,
        "pauses": pauses,
        "controls": controls,
        "shared_handles": counts.get::<_, i64>(0),
        "checks_failing": counts.get::<_, i64>(1),
        "awaiting_confirmation": counts.get::<_, i64>(2),
    }))
}

/// The same figures in Prometheus text format.
async fn get_metrics(State(app): State<Arc<App>>) -> Result<String, Miss> {
    use std::fmt::Write as _;
    let db = app.db();
    let mut out = String::new();
    for r in db
        .query(
            "SELECT source, cursor, COALESCE(extract(epoch FROM now() - tip_at), -1)::float8, (phase = 'live')::int \
             FROM sync_state WHERE source = 'plc' OR source LIKE 'jetstream:%'",
            &[],
        )
        .await?
    {
        let source: String = r.get(0);
        let _ = writeln!(out, "lodestar_source_cursor{{source=\"{source}\"}} {}", r.get::<_, i64>(1));
        let _ = writeln!(out, "lodestar_source_live{{source=\"{source}\"}} {}", r.get::<_, i32>(3));
        let _ = writeln!(out, "lodestar_source_tip_age_seconds{{source=\"{source}\"}} {:.0}", r.get::<_, f64>(2));
    }
    for r in db
        .query(
            "SELECT c.relname, pg_table_size(c.oid), pg_indexes_size(c.oid), c.reltuples::bigint \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r'",
            &[],
        )
        .await?
    {
        let table: String = r.get(0);
        let _ = writeln!(out, "lodestar_table_bytes{{table=\"{table}\"}} {}", r.get::<_, i64>(1));
        let _ = writeln!(out, "lodestar_index_bytes{{table=\"{table}\"}} {}", r.get::<_, i64>(2));
        let _ = writeln!(out, "lodestar_table_rows_estimate{{table=\"{table}\"}} {}", r.get::<_, i64>(3));
    }
    for r in db
        .query(
            "SELECT kind, reason, count(*) FROM verify_due GROUP BY 1, 2",
            &[],
        )
        .await?
    {
        let _ = writeln!(
            out,
            "lodestar_verify_due{{kind=\"{}\",reason=\"{}\"}} {}",
            r.get::<_, i16>(0),
            r.get::<_, i16>(1),
            r.get::<_, i64>(2)
        );
    }
    let counts = db
        .query_one(
            "SELECT (SELECT count(*) FROM shared_handles), (SELECT count(*) FROM check_failures)",
            &[],
        )
        .await?;
    let _ = writeln!(out, "lodestar_shared_handles {}", counts.get::<_, i64>(0));
    let _ = writeln!(out, "lodestar_checks_failing {}", counts.get::<_, i64>(1));
    if let Some(r) = db
        .query_opt("SELECT status FROM role_status WHERE role = 'verify'", &[])
        .await?
    {
        let v: Value = serde_json::from_str(&r.get::<_, String>(0)).unwrap_or(Value::Null);
        let _ = writeln!(
            out,
            "lodestar_verification_paused {}",
            i32::from(v["paused_by_controls"].as_bool().unwrap_or(false))
        );
        if let Some(checks) = v["checks"].as_object() {
            for (result, n) in checks {
                let _ = writeln!(
                    out,
                    "lodestar_checks_total{{result=\"{result}\"}} {}",
                    n.as_u64().unwrap_or(0)
                );
            }
        }
        let _ = writeln!(
            out,
            "lodestar_checks_in_flight {}",
            v["in_flight"].as_u64().unwrap_or(0)
        );
        let _ = writeln!(
            out,
            "lodestar_domains_paused {}",
            v["domains_paused_failing"].as_array().map_or(0, Vec::len)
                + v["domains_paused_resolving_to_nothing"]
                    .as_array()
                    .map_or(0, Vec::len)
        );
    }
    Ok(out)
}

pub async fn run(opts: Opts) -> Result<()> {
    let mut clients = Vec::with_capacity(opts.connections.max(1));
    for _ in 0..opts.connections.max(1) {
        clients.push(db::connect(&opts.database_url).await?);
    }
    let app = Arc::new(App {
        clients,
        next: AtomicUsize::new(0),
        fresh_seconds: opts.fresh_seconds,
        max_batch: opts.max_batch,
        live_deadline: opts.live_deadline,
        queue_cap: opts.queue_cap,
        misses: Mutex::new(HashMap::new()),
    });
    if let Some(status_opts) = opts.status {
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = status::run(app, status_opts).await {
                tracing::error!("the status page stopped: {e:#}");
            }
        });
    }
    let router = Router::new()
        .route("/account/{did}", get(get_account))
        .route("/account/{did}/history", get(get_history))
        .route("/account/{did}/refresh", post(post_refresh))
        .route("/accounts", post(post_accounts))
        .route("/handle/{handle}", get(get_handle))
        .route("/ops/health", get(get_health))
        .route("/ops/metrics", get(get_metrics))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(&opts.listen)
        .await
        .with_context(|| format!("listening on {}", opts.listen))?;
    tracing::info!(listen = %opts.listen, "API role started");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let mut stop = crate::ingest::stop_signal();
            let _ = stop.changed().await;
        })
        .await
        .context("serving")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dids_are_validated() {
        assert_eq!(
            parse_did("did:plc:ewvi7nxzyoun6zhxrhs64oiz"),
            Some(Method::Plc)
        );
        assert_eq!(parse_did("did:web:example.com"), Some(Method::Web));
        assert_eq!(parse_did("did:web:example.com%3A8080"), Some(Method::Web));
        assert_eq!(parse_did("did:web:localhost%3A3000"), Some(Method::Web));
        for bad in [
            "did:plc:short",
            "did:plc:EWVI7NXZYOUN6ZHXRHS64OIZ",
            "did:plc:ewvi7nxzyoun6zhxrhs64oi1",
            "did:web:example.com:path",
            "did:web:Example.com",
            "did:web:example.com%3A",
            "did:web:",
            "did:key:zQ3sh",
            "plc:ewvi7nxzyoun6zhxrhs64oiz",
        ] {
            assert_eq!(parse_did(bad), None, "{bad}");
        }
    }

    #[test]
    fn keys_are_multikey_strings() {
        let stored =
            encoding::key_bytes("did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF");
        assert_eq!(
            multikey(Some(stored)).as_deref(),
            Some("zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF")
        );
        assert_eq!(
            multikey(Some(encoding::key_bytes("odd key"))).as_deref(),
            Some("odd key")
        );
        assert_eq!(multikey(None), None);
    }

    #[test]
    fn absent_dids_depend_on_the_source() {
        assert_eq!(absent(Method::Plc, true).1, "not_found");
        assert_eq!(absent(Method::Plc, false).1, "syncing");
        assert_eq!(absent(Method::Web, true).1, "unavailable");
    }
}
