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

/// How often the rows of `live_counts` are folded into one per kind.
const FOLD_EVERY: Duration = Duration::from_secs(60);

pub struct Opts {
    pub database_url: String,
    pub listen: String,
    /// How often the live totals are checked against a full count. Each
    /// count reads the whole of `accounts`.
    pub totals_every: Duration,
}

struct Status {
    app: Arc<App>,
    /// When the live totals were last recounted, once they have been since
    /// the role started. Until then they may be partial and are not shown.
    recounted_at: RwLock<Option<String>>,
    /// The last figures served, as sent, and when they were read.
    figures: Mutex<Option<(Instant, String)>>,
}

/// One total: (kind, status, handle status, shared) and its count.
type Counts = std::collections::BTreeMap<(i16, Option<i16>, Option<i16>, Option<bool>), i64>;

async fn sum_counts(db: &impl tokio_postgres::GenericClient) -> Result<Counts> {
    let rows = db
        .query(
            "SELECT kind, status, handle_status, shared, sum(n)::bigint FROM live_counts \
             GROUP BY 1, 2, 3, 4 HAVING sum(n) <> 0",
            &[],
        )
        .await
        .context("reading the live totals")?;
    Ok(rows
        .iter()
        .map(|r| ((r.get(0), r.get(1), r.get(2), r.get(3)), r.get(4)))
        .collect())
}

/// Fold the rows the triggers have added into one per total.
async fn fold(db: &tokio_postgres::Client) -> Result<()> {
    db.execute(
        "WITH d AS (DELETE FROM live_counts RETURNING *) \
         INSERT INTO live_counts (n, kind, status, handle_status, shared) \
         SELECT sum(n), kind, status, handle_status, shared FROM d \
         GROUP BY 2, 3, 4, 5 HAVING sum(n) <> 0",
        &[],
    )
    .await
    .context("folding the live totals")?;
    Ok(())
}

/// Count everything the live totals hold from the tables themselves, gently
/// (one process, no parallel workers), and put the count in their place.
/// All from one snapshot: what is written meanwhile stays as it is, on top.
/// Returns what the live totals had wrong, which is nothing unless a handle
/// was newly found shared.
async fn recount(client: &mut tokio_postgres::Client) -> Result<Counts> {
    let tx = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await?;
    tx.batch_execute("SET LOCAL max_parallel_workers_per_gather = 0")
        .await?;
    let live = sum_counts(&tx).await?;
    // An account claiming a shared handle is never checked on its own; the
    // handle is resolved once for all its claimants, so the account's own
    // status says nothing. Such accounts are counted apart, all but the one
    // the handle resolves to.
    let mut counted: Counts = tx
        .query(
            "SELECT a.status, a.handle_status, s.handle IS NOT NULL, count(*) \
             FROM accounts a LEFT JOIN shared_handles s \
               ON s.handle = a.handle_claim AND s.did IS DISTINCT FROM a.did \
             GROUP BY 1, 2, 3",
            &[],
        )
        .await
        .context("counting accounts")?
        .iter()
        .map(|r| {
            (
                (0, Some(r.get(0)), Some(r.get(1)), Some(r.get(2))),
                r.get(3),
            )
        })
        .collect();
    let listed: i64 = tx
        .query_one("SELECT count(*) FROM listed", &[])
        .await?
        .get(0);
    if listed > 0 {
        counted.insert((1, None, None, None), listed);
    }
    tx.execute("DELETE FROM live_counts", &[]).await?;
    for (&(kind, status, handle_status, shared), n) in &counted {
        tx.execute(
            "INSERT INTO live_counts (n, kind, status, handle_status, shared) VALUES ($1, $2, $3, $4, $5)",
            &[n, &kind, &status, &handle_status, &shared],
        )
        .await?;
    }
    tx.commit().await?;
    let mut wrong = Counts::new();
    for key in live.keys().chain(counted.keys()) {
        let d = counted.get(key).unwrap_or(&0) - live.get(key).unwrap_or(&0);
        if d != 0 {
            wrong.insert(*key, d);
        }
    }
    Ok(wrong)
}

/// The totals as the page shows them.
fn totals(counts: &Counts, recounted_at: &str) -> Value {
    let mut identities = 0;
    let mut listed = 0;
    let mut by_status = serde_json::Map::new();
    for (&(kind, status, handle_status, shared), &n) in counts {
        if kind == 1 {
            listed += n;
            continue;
        }
        identities += n;
        if status != Some(crate::db::STATUS_ACTIVE) {
            continue;
        }
        let name = if shared == Some(true) {
            "shared"
        } else {
            match handle_status {
                Some(0) => "verified",
                Some(1) => "mismatch",
                Some(2) => "unresolved",
                Some(3) => "unreachable",
                Some(4) => "pending",
                _ => "none",
            }
        };
        let n = n + by_status.get(name).and_then(Value::as_i64).unwrap_or(0);
        by_status.insert(name.to_owned(), json!(n));
    }
    json!({
        "recounted_at": recounted_at,
        "identities": identities,
        "listed": listed,
        "active_by_handle_status": by_status,
    })
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
    let live_totals = match status.recounted_at.read().await.clone() {
        Some(at) => totals(&sum_counts(db).await?, &at),
        None => Value::Null,
    };
    Ok(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "now": now,
        "plc": src["plc"],
        "plc_live": live,
        "streams": streams,
        "verification": v,
        "totals": live_totals,
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
        recounted_at: RwLock::new(None),
        figures: Mutex::new(None),
    });
    // Folding and recounting happen in the background, one at a time,
    // never for a request.
    let counting = status.clone();
    let (url, every) = (opts.database_url.clone(), opts.totals_every);
    tokio::spawn(async move {
        let mut next_recount = Instant::now();
        let mut kept: Option<tokio_postgres::Client> = None;
        loop {
            let done = async {
                if kept.as_ref().is_none_or(|c| c.is_closed()) {
                    kept = Some(crate::db::connect(&url).await?);
                }
                let client = kept.as_mut().expect("connected above");
                if Instant::now() >= next_recount {
                    let wrong = recount(client).await?;
                    if !wrong.is_empty() {
                        tracing::info!(?wrong, "recount corrected the live totals");
                    }
                    let at: String = client
                        .query_one(
                            &format!("SELECT to_char(now() AT TIME ZONE 'UTC', {TS})"),
                            &[],
                        )
                        .await?
                        .get(0);
                    *counting.recounted_at.write().await = Some(at);
                    next_recount = Instant::now() + every;
                } else {
                    fold(client).await?;
                }
                anyhow::Ok(())
            };
            if let Err(e) = done.await {
                tracing::warn!("keeping the status totals: {e:#}");
            }
            tokio::time::sleep(FOLD_EVERY).await;
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

    async fn live(client: &tokio_postgres::Client) -> Value {
        let mut t = totals(&sum_counts(client).await.unwrap(), "");
        t.as_object_mut().unwrap().remove("recounted_at");
        t
    }

    // 1 and 2 claim a shared handle that resolves to 3; 3 holds it. 4 is
    // pending on a handle of its own, 5 is verified, 6 is tombstoned and
    // claims the shared handle.
    const ACCOUNTS: &str = "INSERT INTO accounts (created_at, method, status, handle_status, did, handle_claim) VALUES \
         (now(), 0, 0, 4, '\\x01', 'wave.example'), \
         (now(), 0, 0, 4, '\\x02', 'wave.example'), \
         (now(), 0, 0, 0, '\\x03', 'wave.example'), \
         (now(), 0, 0, 4, '\\x04', 'own.example'), \
         (now(), 0, 0, 0, '\\x05', 'five.example'), \
         (now(), 0, 1, 4, '\\x06', 'wave.example'); \
         INSERT INTO listed (did) VALUES ('\\x01'), ('\\x04');";
    const SHARED: &str =
        "INSERT INTO shared_handles (outcome, handle, did) VALUES (0, 'wave.example', '\\x03');";

    #[tokio::test]
    async fn the_live_totals_follow_every_change() {
        let Some(mut client) = crate::testdb::fresh().await else {
            return;
        };
        client.batch_execute(SHARED).await.unwrap();
        client.batch_execute(ACCOUNTS).await.unwrap();
        assert_eq!(
            live(&client).await,
            json!({"identities": 6, "listed": 2,
                   "active_by_handle_status": {"shared": 2, "pending": 1, "verified": 2}})
        );
        // 4 is verified, 5 tombstoned, and the shared handle moves from 3
        // to 1 the way a resolution moves it.
        client
            .batch_execute(
                "UPDATE accounts SET handle_status = 0 WHERE did = '\\x04'; \
                 UPDATE accounts SET status = 1 WHERE did = '\\x05'; \
                 BEGIN; \
                 UPDATE shared_handles SET did = '\\x01' WHERE handle = 'wave.example'; \
                 UPDATE accounts SET handle_status = 1 WHERE handle_claim = 'wave.example' AND handle_status = 0 AND did <> '\\x01'; \
                 UPDATE accounts SET handle_status = 0 WHERE did = '\\x01'; \
                 COMMIT; \
                 INSERT INTO listed (did) VALUES ('\\x05');",
            )
            .await
            .unwrap();
        let after = json!({"identities": 6, "listed": 3,
                           "active_by_handle_status": {"shared": 2, "verified": 2}});
        assert_eq!(live(&client).await, after);
        fold(&client).await.unwrap();
        assert_eq!(live(&client).await, after);
        assert_eq!(recount(&mut client).await.unwrap(), Counts::new());
        assert_eq!(live(&client).await, after);
    }

    #[tokio::test]
    async fn a_handle_found_shared_later_is_put_right_by_the_recount() {
        let Some(mut client) = crate::testdb::fresh().await else {
            return;
        };
        client.batch_execute(ACCOUNTS).await.unwrap();
        client.batch_execute(SHARED).await.unwrap();
        assert!(!recount(&mut client).await.unwrap().is_empty());
        assert_eq!(
            live(&client).await,
            json!({"identities": 6, "listed": 2,
                   "active_by_handle_status": {"shared": 2, "pending": 1, "verified": 2}})
        );
    }
}
