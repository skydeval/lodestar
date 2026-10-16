//! The status page: a read-only page and the aggregate figures it shows,
//! served on an address of its own, apart from the API. It serves nothing
//! else, and names nothing an operator may not want public: the control
//! handles appear only as a count. Off unless an address is given.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use serde_json::{Value, json};
use tokio::sync::RwLock;

use super::{App, TS, source, verification};

const PAGE: &str = include_str!("status.html");

pub struct Opts {
    pub database_url: String,
    pub listen: String,
    /// How often the totals over `accounts` are counted again. Each count
    /// reads the whole table.
    pub totals_every: Duration,
}

struct Status {
    app: Arc<App>,
    /// The last totals and when they were counted.
    totals: RwLock<Value>,
}

/// Count handle statuses over all of `accounts`, gently: one process, no
/// parallel workers.
async fn count_totals(client: &mut tokio_postgres::Client) -> Result<Value> {
    let tx = client.transaction().await?;
    tx.batch_execute("SET LOCAL max_parallel_workers_per_gather = 0")
        .await?;
    let rows = tx
        .query(
            "SELECT handle_status, count(*) FROM accounts WHERE status = 0 GROUP BY 1",
            &[],
        )
        .await
        .context("counting handle statuses")?;
    let identities: i64 = tx
        .query_one("SELECT count(*) FROM accounts", &[])
        .await?
        .get(0);
    let listed: i64 = tx
        .query_one("SELECT count(*) FROM listed", &[])
        .await?
        .get(0);
    let at: String = tx
        .query_one(
            &format!("SELECT to_char(now() AT TIME ZONE 'UTC', {TS})"),
            &[],
        )
        .await?
        .get(0);
    tx.commit().await?;
    let mut by_status = serde_json::Map::new();
    for r in rows {
        let name = match r.get::<_, i16>(0) {
            0 => "verified",
            1 => "mismatch",
            2 => "unresolved",
            3 => "unreachable",
            4 => "pending",
            _ => "none",
        };
        by_status.insert(name.to_owned(), json!(r.get::<_, i64>(1)));
    }
    Ok(json!({
        "counted_at": at,
        "identities": identities,
        "listed": listed,
        "active_by_handle_status": by_status,
    }))
}

/// The figures the page shows. Built from what `/ops/health` reports,
/// with the control handles reduced to counts.
async fn figures(status: &Status) -> Result<Value> {
    let app = &status.app;
    let (src, live) = source(app).await?;
    let mut v = verification(app).await?;
    let controls = v["controls"].as_array().cloned().unwrap_or_default();
    let counted = controls
        .iter()
        .filter(|c| c["left_out"] != json!(true))
        .count();
    let passing = controls
        .iter()
        .filter(|c| c["left_out"] != json!(true) && c["passed_last_round"] == json!(true))
        .count();
    v["controls"] = json!({ "counted": counted, "passing": passing });
    let db = app.db();
    let streams: Vec<Value> = db
        .query(
            &format!(
                "SELECT source, phase, to_char(tip_at AT TIME ZONE 'UTC', {TS}), \
                        COALESCE(tip_at > now() - interval '5 minutes', false) \
                 FROM sync_state WHERE source LIKE 'jetstream:%' OR source = 'relay-walk' ORDER BY source"
            ),
            &[],
        )
        .await?
        .iter()
        .map(|r| {
            json!({
                "source": r.get::<_, String>(0),
                "phase": r.get::<_, String>(1),
                "at": r.get::<_, Option<String>>(2),
                "current": r.get::<_, bool>(3),
            })
        })
        .collect();
    let database_bytes: i64 = db
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await?
        .get(0);
    let now: String = db
        .query_one(
            &format!("SELECT to_char(now() AT TIME ZONE 'UTC', {TS})"),
            &[],
        )
        .await?
        .get(0);
    Ok(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "now": now,
        "plc": src["plc"],
        "plc_live": live,
        "streams": streams,
        "verification": v,
        "totals": *status.totals.read().await,
        "database_bytes": database_bytes,
    }))
}

async fn page() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        PAGE,
    )
        .into_response()
}

async fn data(State(status): State<Arc<Status>>) -> Response {
    match figures(&status).await {
        Ok(v) => (
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            v.to_string(),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!("status figures: {e:#}");
            (StatusCode::SERVICE_UNAVAILABLE, "unavailable").into_response()
        }
    }
}

pub async fn run(app: Arc<App>, opts: Opts) -> Result<()> {
    let status = Arc::new(Status {
        app,
        totals: RwLock::new(Value::Null),
    });
    // The totals are counted in the background, never for a request.
    let counting = status.clone();
    let (url, every) = (opts.database_url.clone(), opts.totals_every);
    tokio::spawn(async move {
        loop {
            let counted = async {
                let mut client = crate::db::connect(&url).await?;
                count_totals(&mut client).await
            };
            match counted.await {
                Ok(t) => *counting.totals.write().await = t,
                Err(e) => tracing::warn!("counting the status totals: {e:#}"),
            }
            tokio::time::sleep(every).await;
        }
    });
    let router = Router::new()
        .route("/status", get(|| async { Redirect::permanent("/status/") }))
        .route("/status/", get(page))
        .route("/status/data.json", get(data))
        .with_state(status);
    let listener = tokio::net::TcpListener::bind(&opts.listen)
        .await
        .with_context(|| format!("listening on {}", opts.listen))?;
    tracing::info!(listen = %opts.listen, "status page started");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let mut stop = crate::ingest::stop_signal();
            let _ = stop.changed().await;
        })
        .await
        .context("serving the status page")?;
    Ok(())
}
