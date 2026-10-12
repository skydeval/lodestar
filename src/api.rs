//! The API role: reads over what the other roles have stored. HTTP and
//! JSON, for consumers on a private network. It makes no outbound request.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_postgres::{Client, Row};

use crate::{db, encoding, handle};

pub struct Opts {
    pub database_url: String,
    pub listen: String,
    pub connections: usize,
    /// The PLC source is fresh while its last tip confirmation is no older
    /// than this.
    pub fresh_seconds: i64,
    pub max_batch: usize,
}

struct App {
    clients: Vec<Client>,
    next: AtomicUsize,
    fresh_seconds: i64,
    max_batch: usize,
}

impl App {
    fn db(&self) -> &Client {
        &self.clients[self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len()]
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
        let ok = id.len() == 24 && id.bytes().all(|c| c.is_ascii_lowercase() || (b'2'..=b'7').contains(&c));
        return ok.then_some(Method::Plc);
    }
    if let Some(host) = did.strip_prefix("did:web:") {
        let (name, port) = match host.split_once("%3A") {
            Some((n, p)) => (n, Some(p)),
            None => (host, None),
        };
        let port_ok = port.is_none_or(|p| !p.is_empty() && p.len() <= 5 && p.bytes().all(|c| c.is_ascii_digit()));
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
    Some(text.strip_prefix("did:key:").map(str::to_owned).unwrap_or(text))
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
        None => (json!({ "plc": { "phase": "backfill", "cursor": 0, "tip_confirmed_at": null, "fresh": false } }), false),
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

fn account_columns() -> String {
    format!(
        "a.did, a.method, a.status, p.endpoint, a.signing_key, a.handle_claim, a.handle_status, \
         to_char(a.handle_checked_at AT TIME ZONE 'UTC', {TS}), to_char(a.created_at AT TIME ZONE 'UTC', {TS})"
    )
}

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

async fn get_account(State(app): State<Arc<App>>, Path(did): Path<String>) -> Result<Json<Value>, Miss> {
    let method = parse_did(&did).ok_or(Miss(StatusCode::BAD_REQUEST, "invalid_did", None))?;
    let (src, live) = source(&app).await?;
    let row = app
        .db()
        .query_opt(
            &format!("SELECT {} FROM accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id WHERE a.did = $1", account_columns()),
            &[&encoding::did_bytes(&did)],
        )
        .await?;
    match row {
        Some(r) => {
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

#[derive(Deserialize)]
struct Batch {
    dids: Vec<String>,
}

async fn post_accounts(State(app): State<Arc<App>>, Json(batch): Json<Batch>) -> Result<Json<Value>, Miss> {
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
            &format!("SELECT {} FROM accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id WHERE a.did = ANY($1)", account_columns()),
            &[&keys],
        )
        .await?;
    for r in &rows {
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
    Ok(Json(json!({ "accounts": results, "source": src })))
}

#[derive(Deserialize)]
struct Page {
    limit: Option<i64>,
    /// The `next_cursor` of the page before.
    cursor: Option<i32>,
}

async fn get_history(State(app): State<Arc<App>>, Path(did): Path<String>, Query(page): Query<Page>) -> Result<Json<Value>, Miss> {
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
    let next_cursor = if more { rows.get(limit as usize - 1).map(|r| r.get::<_, i32>(0)) } else { None };
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

async fn get_handle(State(app): State<Arc<App>>, Path(raw): Path<String>) -> Result<Json<Value>, Miss> {
    let handle = handle::normalize(&raw).ok_or(Miss(StatusCode::BAD_REQUEST, "invalid_handle", None))?;
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
        // Resolving a handle that is not in the index is the verification
        // role's work.
        None => Err(Miss(StatusCode::SERVICE_UNAVAILABLE, "unavailable", None)),
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
    Ok(Json(json!({ "version": env!("CARGO_PKG_VERSION"), "sources": src, "storage": storage })))
}

pub async fn run(opts: Opts) -> Result<()> {
    let mut clients = Vec::with_capacity(opts.connections.max(1));
    for _ in 0..opts.connections.max(1) {
        clients.push(db::connect(&opts.database_url).await?);
    }
    let app = Arc::new(App { clients, next: AtomicUsize::new(0), fresh_seconds: opts.fresh_seconds, max_batch: opts.max_batch });
    let router = Router::new()
        .route("/account/{did}", get(get_account))
        .route("/account/{did}/history", get(get_history))
        .route("/accounts", post(post_accounts))
        .route("/handle/{handle}", get(get_handle))
        .route("/ops/health", get(get_health))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(&opts.listen).await.with_context(|| format!("listening on {}", opts.listen))?;
    tracing::info!(listen = %opts.listen, "API role started");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
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
        assert_eq!(parse_did("did:plc:ewvi7nxzyoun6zhxrhs64oiz"), Some(Method::Plc));
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
        let stored = encoding::key_bytes("did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF");
        assert_eq!(multikey(Some(stored)).as_deref(), Some("zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF"));
        assert_eq!(multikey(Some(encoding::key_bytes("odd key"))).as_deref(), Some("odd key"));
        assert_eq!(multikey(None), None);
    }

    #[test]
    fn absent_dids_depend_on_the_source() {
        assert_eq!(absent(Method::Plc, true).1, "not_found");
        assert_eq!(absent(Method::Plc, false).1, "syncing");
        assert_eq!(absent(Method::Web, true).1, "unavailable");
    }
}
