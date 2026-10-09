//! Jetstream live subscription and relay listings.

use std::io::Write;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_postgres::Client;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Error as WsError;

const TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS recon_js_events (
    seq         bigint PRIMARY KEY,
    kind        text NOT NULL,
    did         text NOT NULL,
    event_time  timestamptz,
    received_at timestamptz NOT NULL DEFAULT now(),
    handle      text,
    active      boolean,
    status      text,
    bytes       integer NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_didweb (
    did        text NOT NULL,
    source     text NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    detail     text,
    PRIMARY KEY (did, source)
);
CREATE TABLE IF NOT EXISTS recon_hosts (
    hostname      text PRIMARY KEY,
    account_count bigint,
    status        text,
    seq           bigint
);
CREATE TABLE IF NOT EXISTS recon_walk_status (
    source text NOT NULL,
    active boolean,
    status text,
    method text NOT NULL,
    n      bigint NOT NULL,
    UNIQUE NULLS NOT DISTINCT (source, active, status, method)
);
"#;

pub async fn init(client: &Client) -> Result<()> {
    client.batch_execute(TABLES).await.context("creating net tables")?;
    Ok(())
}

fn ws_url(base: &str, kinds: &[String], cursor: Option<&str>) -> String {
    let base = base.trim_end_matches('/');
    let mut url = format!("{base}/xrpc/network.bsky.jetstream.subscribeEvents?");
    let mut first = true;
    for k in kinds {
        if !first {
            url.push('&');
        }
        first = false;
        url.push_str("kinds=");
        url.push_str(k);
    }
    if let Some(c) = cursor {
        if !first {
            url.push('&');
        }
        url.push_str("cursor=");
        url.push_str(c);
    }
    url
}

async fn connect(
    url: &str,
    user_agent: &str,
) -> std::result::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    WsError,
> {
    let mut req = url.into_client_request()?;
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("xrpc.v1.json"));
    if let Ok(v) = HeaderValue::from_str(user_agent) {
        req.headers_mut().insert("User-Agent", v);
    }
    let (ws, _resp) = tokio_tungstenite::connect_async(req).await?;
    Ok(ws)
}

fn describe_refusal(e: &WsError) -> String {
    match e {
        WsError::Http(resp) => {
            let body = resp
                .body()
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            format!("http {} body {}", resp.status(), body)
        }
        other => format!("error {other}"),
    }
}

/// Try one connection with the given cursor and report what the server does.
pub async fn jetstream_probe(base: &str, user_agent: &str, kinds: &[String], cursor: &str) -> Result<()> {
    let url = ws_url(base, kinds, Some(cursor));
    let started = Instant::now();
    match connect(&url, user_agent).await {
        Err(e) => {
            println!("{}", json!({"cursor": cursor, "connected": false, "detail": describe_refusal(&e)}));
        }
        Ok(mut ws) => {
            let mut first: Option<Value> = None;
            let mut raw: Vec<String> = Vec::new();
            let mut n = 0u64;
            let deadline = tokio::time::sleep(Duration::from_secs(8));
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    _ = &mut deadline => break,
                    m = ws.next() => match m {
                        Some(Ok(msg)) => {
                            if let Ok(t) = msg.to_text() {
                                if raw.len() < 2 && !t.is_empty() {
                                    raw.push(t.chars().take(700).collect());
                                }
                                if let Ok(v) = serde_json::from_str::<Value>(t) {
                                    n += 1;
                                    if first.is_none() { first = Some(v); }
                                }
                            }
                        }
                        Some(Err(e)) => { println!("{}", json!({"cursor": cursor, "stream_error": e.to_string()})); break; }
                        None => break,
                    }
                }
            }
            let p = first.as_ref().and_then(|v| v.get("payload")).cloned().unwrap_or(Value::Null);
            println!(
                "{}",
                json!({
                    "cursor": cursor, "connected": true, "messages_in_8s": n,
                    "connect_ms": started.elapsed().as_millis() as u64,
                    "first_seq": p.get("seq"), "first_time": p.get("time"),
                    "first_type": first.as_ref().and_then(|v| v.get("$type")).cloned(),
                    "first_payload_type": p.get("$type"),
                })
            );
            for r in raw {
                println!("{r}");
            }
        }
    }
    Ok(())
}

pub struct LiveOpts {
    pub base: String,
    pub user_agent: String,
    pub kinds: Vec<String>,
    pub seconds: u64,
    pub metrics_path: String,
    pub sample_path: String,
}

/// Subscribe and record every event for a fixed time.
pub async fn jetstream_live(client: &Client, o: LiveOpts) -> Result<()> {
    init(client).await?;
    let mut metrics = std::fs::OpenOptions::new().create(true).append(true).open(&o.metrics_path)?;
    let mut sample = std::fs::OpenOptions::new().create(true).append(true).open(&o.sample_path)?;
    let mut sampled = 0u32;
    let started = Instant::now();
    let total = Duration::from_secs(o.seconds);
    let mut cursor: Option<String> = None;
    let (mut events, mut bytes, mut reconnects) = (0u64, 0u64, 0u64);
    let (mut win_events, mut win_bytes) = (0u64, 0u64);
    let mut win_started = Instant::now();
    let mut by_kind: std::collections::BTreeMap<String, u64> = Default::default();

    while started.elapsed() < total {
        let url = ws_url(&o.base, &o.kinds, cursor.as_deref());
        let mut ws = match connect(&url, &o.user_agent).await {
            Ok(ws) => ws,
            Err(e) => {
                writeln!(metrics, "{}", json!({"wall": chrono::Utc::now().to_rfc3339(), "connect_failed": describe_refusal(&e)}))?;
                tokio::time::sleep(Duration::from_secs(10)).await;
                reconnects += 1;
                continue;
            }
        };
        loop {
            let left = total.saturating_sub(started.elapsed());
            if left.is_zero() {
                break;
            }
            let m = match tokio::time::timeout(left.min(Duration::from_secs(120)), ws.next()).await {
                Err(_) => {
                    if started.elapsed() >= total { break; }
                    writeln!(metrics, "{}", json!({"wall": chrono::Utc::now().to_rfc3339(), "note": "no message for 120s, reconnecting"}))?;
                    break;
                }
                Ok(None) => break,
                Ok(Some(Err(e))) => {
                    writeln!(metrics, "{}", json!({"wall": chrono::Utc::now().to_rfc3339(), "stream_error": e.to_string()}))?;
                    break;
                }
                Ok(Some(Ok(m))) => m,
            };
            let Ok(text) = m.to_text() else { continue };
            if text.is_empty() {
                continue;
            }
            let len = text.len();
            let Ok(v) = serde_json::from_str::<Value>(text) else { continue };
            let p = v.get("payload").unwrap_or(&v);
            let ty = p.get("$type").and_then(Value::as_str).unwrap_or("");
            let kind = ty.rsplit('#').next().unwrap_or("").to_owned();
            let Some(seq) = p.get("seq").and_then(Value::as_i64) else {
                if sampled < 40 {
                    writeln!(sample, "{text}")?;
                    sampled += 1;
                }
                continue;
            };
            if sampled < 40 {
                writeln!(sample, "{text}")?;
                sampled += 1;
            }
            let did = p.get("did").and_then(Value::as_str).unwrap_or("").to_owned();
            let time = p.get("time").and_then(Value::as_str).map(str::to_owned);
            let inner = p.get(kind.as_str()).unwrap_or(p);
            let handle = inner.get("handle").and_then(Value::as_str).map(str::to_owned);
            let active = inner.get("active").and_then(Value::as_bool);
            let status = inner.get("status").and_then(Value::as_str).map(str::to_owned);
            client
                .execute(
                    "INSERT INTO recon_js_events (seq, kind, did, event_time, handle, active, status, bytes) \
                     VALUES ($1, $2, $3, $4::text::timestamptz, $5, $6, $7, $8) ON CONFLICT (seq) DO NOTHING",
                    &[&seq, &kind, &did, &time, &handle, &active, &status, &(len as i32)],
                )
                .await?;
            if did.starts_with("did:web:") {
                client
                    .execute(
                        "INSERT INTO recon_didweb (did, source, detail) VALUES ($1, 'jetstream-live', $2) ON CONFLICT DO NOTHING",
                        &[&did, &kind],
                    )
                    .await?;
            }
            cursor = Some(seq.to_string());
            events += 1;
            bytes += len as u64;
            win_events += 1;
            win_bytes += len as u64;
            *by_kind.entry(kind).or_insert(0) += 1;
            if win_started.elapsed() >= Duration::from_secs(300) {
                writeln!(
                    metrics,
                    "{}",
                    json!({"wall": chrono::Utc::now().to_rfc3339(), "elapsed_s": started.elapsed().as_secs(),
                           "events": events, "bytes": bytes, "win_s": win_started.elapsed().as_secs(),
                           "win_events": win_events, "win_bytes": win_bytes, "by_kind": by_kind, "reconnects": reconnects})
                )?;
                metrics.flush()?;
                win_events = 0;
                win_bytes = 0;
                win_started = Instant::now();
            }
        }
        reconnects += 1;
    }
    writeln!(
        metrics,
        "{}",
        json!({"wall": chrono::Utc::now().to_rfc3339(), "final": true, "elapsed_s": started.elapsed().as_secs(),
               "events": events, "bytes": bytes, "by_kind": by_kind, "reconnects": reconnects})
    )?;
    Ok(())
}

fn http(user_agent: &str) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(user_agent.to_owned())
        .timeout(Duration::from_secs(60))
        .build()?)
}

async fn get_json(h: &reqwest::Client, url: &str) -> Result<(Value, usize)> {
    let mut backoff = Duration::from_secs(2);
    for _ in 0..12 {
        match h.get(url).send().await {
            Ok(r) => {
                let status = r.status();
                if status.as_u16() == 429 || status.is_server_error() {
                    let wait = r
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(Duration::from_secs)
                        .unwrap_or(backoff);
                    eprintln!("status {status} for {url}; waiting {wait:?}");
                    tokio::time::sleep(wait).await;
                    backoff = (backoff * 2).min(Duration::from_secs(300));
                    continue;
                }
                if !status.is_success() {
                    bail!("status {status} for {url}");
                }
                let body = r.text().await?;
                let v = serde_json::from_str(&body).context("response is not JSON")?;
                return Ok((v, body.len()));
            }
            Err(e) => {
                eprintln!("error for {url}: {e}; retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(300));
            }
        }
    }
    bail!("gave up on {url}")
}

/// Page the relay's host roster.
pub async fn list_hosts(client: &Client, relay: &str, user_agent: &str) -> Result<()> {
    init(client).await?;
    let h = http(user_agent)?;
    let relay = relay.trim_end_matches('/');
    let mut cursor: Option<String> = None;
    let (mut pages, mut hosts) = (0u64, 0u64);
    loop {
        let mut url = format!("{relay}/xrpc/com.atproto.sync.listHosts?limit=1000");
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(c);
        }
        let (v, _) = get_json(&h, &url).await?;
        pages += 1;
        let list = v.get("hosts").and_then(Value::as_array).cloned().unwrap_or_default();
        for x in &list {
            let Some(name) = x.get("hostname").and_then(Value::as_str) else { continue };
            let count = x.get("accountCount").and_then(Value::as_i64);
            let status = x.get("status").and_then(Value::as_str);
            let seq = x.get("seq").and_then(Value::as_i64);
            client
                .execute(
                    "INSERT INTO recon_hosts (hostname, account_count, status, seq) VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (hostname) DO UPDATE SET account_count = EXCLUDED.account_count, status = EXCLUDED.status, seq = EXCLUDED.seq",
                    &[&name, &count, &status, &seq],
                )
                .await?;
            hosts += 1;
        }
        cursor = v.get("cursor").and_then(Value::as_str).map(str::to_owned);
        if cursor.is_none() || list.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("{}", json!({"pages": pages, "hosts": hosts}));
    Ok(())
}

pub struct WalkOpts {
    pub base: String,
    pub source: String,
    pub user_agent: String,
    pub min_interval: Duration,
    pub metrics_path: String,
    pub max_pages: u64,
}

/// Walk a `listRepos` listing, keeping did:web DIDs and status counts.
pub async fn walk(client: &Client, o: WalkOpts) -> Result<()> {
    init(client).await?;
    let h = http(&o.user_agent)?;
    let base = o.base.trim_end_matches('/').to_owned();
    let row = client
        .query_opt("SELECT origin, cursor, phase FROM sync_state WHERE source = $1", &[&o.source])
        .await?;
    let mut cursor: Option<String> = None;
    if let Some(r) = row {
        let origin: String = r.get(0);
        if origin != base {
            bail!("walk cursor belongs to {origin}");
        }
        let phase: String = r.get(2);
        if phase == "done" {
            println!("walk already complete");
            return Ok(());
        }
        // The listing cursor is opaque text; it is kept in `phase` as `at:<cursor>`.
        cursor = phase.strip_prefix("at:").map(str::to_owned);
    }
    let mut metrics = std::fs::OpenOptions::new().create(true).append(true).open(&o.metrics_path)?;
    let started = Instant::now();
    let (mut pages, mut repos, mut web, mut bytes) = (0u64, 0u64, 0u64, 0u64);
    loop {
        let t0 = Instant::now();
        let mut url = format!("{base}/xrpc/com.atproto.sync.listRepos?limit=1000");
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(c);
        }
        let (v, len) = get_json(&h, &url).await?;
        pages += 1;
        bytes += len as u64;
        let list = v.get("repos").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut counts: std::collections::HashMap<(Option<bool>, Option<String>, &'static str), i64> = Default::default();
        for x in &list {
            let Some(did) = x.get("did").and_then(Value::as_str) else { continue };
            repos += 1;
            let active = x.get("active").and_then(Value::as_bool);
            let status = x.get("status").and_then(Value::as_str).map(str::to_owned);
            let method = if did.starts_with("did:plc:") {
                "plc"
            } else if did.starts_with("did:web:") {
                "web"
            } else {
                "other"
            };
            if method != "plc" {
                web += 1;
                let detail = json!({"active": active, "status": status}).to_string();
                client
                    .execute(
                        "INSERT INTO recon_didweb (did, source, detail) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        &[&did, &o.source, &detail],
                    )
                    .await?;
            }
            *counts.entry((active, status, method)).or_insert(0) += 1;
        }
        for ((active, status, method), n) in counts {
            client
                .execute(
                    "INSERT INTO recon_walk_status (source, active, status, method, n) VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (source, active, status, method) DO UPDATE SET n = recon_walk_status.n + EXCLUDED.n",
                    &[&o.source, &active, &status, &method, &n],
                )
                .await?;
        }
        cursor = v.get("cursor").and_then(Value::as_str).map(str::to_owned);
        let done = cursor.is_none() || list.is_empty();
        let phase = if done { "done".to_owned() } else { format!("at:{}", cursor.clone().unwrap_or_default()) };
        client
            .execute(
                "INSERT INTO sync_state (source, origin, cursor, phase) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (source) DO UPDATE SET cursor = sync_state.cursor + EXCLUDED.cursor, phase = EXCLUDED.phase",
                &[&o.source, &base, &(list.len() as i64), &phase],
            )
            .await?;
        if pages % 200 == 0 || done {
            writeln!(
                metrics,
                "{}",
                json!({"wall": chrono::Utc::now().to_rfc3339(), "elapsed_s": started.elapsed().as_secs(), "pages": pages,
                       "repos": repos, "non_plc": web, "bytes": bytes, "done": done})
            )?;
            metrics.flush()?;
        }
        if done || (o.max_pages != 0 && pages >= o.max_pages) {
            break;
        }
        let spent = t0.elapsed();
        if spent < o.min_interval {
            tokio::time::sleep(o.min_interval - spent).await;
        }
    }
    println!("{}", json!({"pages": pages, "repos": repos, "non_plc": web, "bytes": bytes}));
    Ok(())
}
