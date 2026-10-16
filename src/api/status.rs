//! The status page: a read-only page and the aggregate figures it shows,
//! served on an address of its own, apart from the API. It serves nothing
//! else, and names nothing an operator may not want public: the control
//! handles and the domains appear only as counts. Off unless an address
//! is given.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock};

use super::{App, TS, source, verification};

const PAGE: &str = include_str!("status.html");

/// How long one reading of the figures is served to every request, so
/// the page's load on the database does not grow with its viewers.
const FIGURES_FOR: Duration = Duration::from_secs(30);

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
    /// The last figures served, as sent, and when they were read.
    figures: Mutex<Option<(Instant, String)>>,
}

/// Count handle statuses over all of `accounts`, gently: one process, no
/// parallel workers.
async fn count_totals(client: &mut tokio_postgres::Client) -> Result<Value> {
    let tx = client.transaction().await?;
    tx.batch_execute("SET LOCAL max_parallel_workers_per_gather = 0")
        .await?;
    // An account claiming a shared handle is never checked on its own; the
    // handle is resolved once for all its claimants, so the account's own
    // status says nothing. Such accounts are counted apart, all but the one
    // the handle resolves to.
    let rows = tx
        .query(
            "SELECT a.handle_status, s.handle IS NOT NULL, count(*) \
             FROM accounts a LEFT JOIN shared_handles s \
               ON s.handle = a.handle_claim AND s.did IS DISTINCT FROM a.did \
             WHERE a.status = 0 GROUP BY 1, 2",
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
        let name = if r.get::<_, bool>(1) {
            "shared"
        } else {
            match r.get::<_, i16>(0) {
                0 => "verified",
                1 => "mismatch",
                2 => "unresolved",
                3 => "unreachable",
                4 => "pending",
                _ => "none",
            }
        };
        let n = r.get::<_, i64>(2) + by_status.get(name).and_then(Value::as_i64).unwrap_or(0);
        by_status.insert(name.to_owned(), json!(n));
    }
    Ok(json!({
        "counted_at": at,
        "identities": identities,
        "listed": listed,
        "active_by_handle_status": by_status,
    }))
}

/// What `/ops/health` reports about verification, with every name taken
/// out: the control handles and each list of domains become counts.
fn public_verification(mut v: Value) -> Value {
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
    let pauses = v["pauses"].as_array().cloned().unwrap_or_default();
    let paused = |kind: &str| {
        pauses
            .iter()
            .filter(|p| {
                p["state"] == json!("paused")
                    && p["pause"].as_str().is_some_and(|s| s.starts_with(kind))
            })
            .count()
    };
    v["pauses"] = json!({
        "not_answering": paused("pause2:"),
        "answers_look_wrong": paused("pause3:"),
    });
    if let Some(worker) = v["worker"].as_object_mut() {
        for (key, value) in worker.iter_mut() {
            if key.starts_with("domains_")
                && let Some(n) = value.as_array().map(Vec::len)
            {
                *value = json!(n);
            }
        }
    }
    v
}

/// The figures the page shows. Built from what `/ops/health` reports,
/// with the names taken out.
async fn figures(status: &Status) -> Result<Value> {
    let app = &status.app;
    let (src, live) = source(app).await?;
    let v = public_verification(verification(app).await?);
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
    // One request at a time reads the database; the rest wait for it and
    // are given what it read.
    let mut cached = status.figures.lock().await;
    let body = match cached.as_ref() {
        Some((at, body)) if at.elapsed() < FIGURES_FOR => body.clone(),
        _ => match figures(&status).await {
            Ok(v) => {
                let body = v.to_string();
                *cached = Some((Instant::now(), body.clone()));
                body
            }
            Err(e) => {
                tracing::warn!("status figures: {e:#}");
                return (StatusCode::SERVICE_UNAVAILABLE, "unavailable").into_response();
            }
        },
    };
    drop(cached);
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

pub async fn run(app: Arc<App>, opts: Opts) -> Result<()> {
    let status = Arc::new(Status {
        app,
        totals: RwLock::new(Value::Null),
        figures: Mutex::new(None),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_names_reach_the_page() {
        let v = public_verification(json!({
            "controls": [
                {"handle": "a.example", "left_out": false, "passed_last_round": true},
                {"handle": "b.example", "left_out": false, "passed_last_round": false},
                {"handle": "c.example", "left_out": true, "passed_last_round": false},
            ],
            "pauses": [
                {"pause": "pause2:d.example", "state": "paused"},
                {"pause": "pause2:e.example", "state": "released"},
                {"pause": "pause3:f.example", "state": "paused"},
            ],
            "worker": {
                "domains_climbed": [["g.example", 2.0], ["h.example", 1.5]],
                "domains_paused_failing": ["d.example"],
                "in_flight": 3,
            },
        }));
        assert_eq!(v["controls"], json!({"counted": 2, "passing": 1}));
        assert_eq!(
            v["pauses"],
            json!({"not_answering": 1, "answers_look_wrong": 1})
        );
        assert_eq!(v["worker"]["domains_climbed"], json!(2));
        assert_eq!(v["worker"]["domains_paused_failing"], json!(1));
        assert_eq!(v["worker"]["in_flight"], json!(3));
        assert!(!v.to_string().contains(".example"));
    }

    #[tokio::test]
    async fn shared_claimants_are_counted_apart() {
        let Some(mut client) = crate::testdb::fresh().await else {
            return;
        };
        // 1 and 2 claim a shared handle that resolves to 3; 3 holds it.
        // 4 is pending on a handle of its own, 5 is verified, 6 is
        // tombstoned and claims the shared handle.
        client
            .batch_execute(
                "INSERT INTO accounts (created_at, method, status, handle_status, did, handle_claim) VALUES \
                 (now(), 0, 0, 4, '\\x01', 'wave.example'), \
                 (now(), 0, 0, 4, '\\x02', 'wave.example'), \
                 (now(), 0, 0, 0, '\\x03', 'wave.example'), \
                 (now(), 0, 0, 4, '\\x04', 'own.example'), \
                 (now(), 0, 0, 0, '\\x05', 'five.example'), \
                 (now(), 0, 1, 4, '\\x06', 'wave.example'); \
                 INSERT INTO shared_handles (outcome, handle, did) VALUES (0, 'wave.example', '\\x03');",
            )
            .await
            .unwrap();
        let t = count_totals(&mut client).await.unwrap();
        assert_eq!(
            t["active_by_handle_status"],
            json!({"shared": 2, "pending": 1, "verified": 2})
        );
        assert_eq!(t["identities"], json!(6));
    }
}
