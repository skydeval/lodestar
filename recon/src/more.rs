//! did:web document fetches, audit-log comparison, and event-to-check timing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::DateTime;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tokio_postgres::Client;

use crate::{handle, net, plc, verify};

const TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS recon_didweb_docs (
    did         text PRIMARY KEY,
    outcome     text NOT NULL,
    http_status integer,
    id_ok       boolean,
    has_pds     boolean,
    pds         text,
    key_type    text,
    handle      text,
    services    text,
    ms          integer NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_audit_compare (
    did     text PRIMARY KEY,
    kind    text NOT NULL,
    ops     integer NOT NULL,
    nullified integer NOT NULL,
    state_match boolean NOT NULL,
    history_match boolean NOT NULL,
    detail  text
);
CREATE TABLE IF NOT EXISTS recon_event_lag (
    seq         bigint PRIMARY KEY,
    did         text NOT NULL,
    handle      text,
    handle_from text,
    stored_handle text,
    event_time  timestamptz,
    received_at timestamptz NOT NULL,
    first_result text,
    first_ms    integer,
    first_dns   text,
    first_https text,
    after_60s   text,
    after_600s  text
);
"#;

pub async fn init(client: &Client) -> Result<()> {
    client.batch_execute(TABLES).await.context("creating more tables")?;
    Ok(())
}

/// A did:web the design accepts: hostname only, no port, no path (§3.3).
fn didweb_host(did: &str) -> Option<String> {
    let host = did.strip_prefix("did:web:")?;
    if host.contains(':') || host.contains('%') || host.contains('/') {
        return None;
    }
    let h = handle::normalize(host)?;
    if handle::never_resolves(&h) {
        return None;
    }
    Some(h)
}

/// Fetch the document of every did:web DID recon has seen.
pub async fn didweb_fetch(client: Arc<Client>, user_agent: &str) -> Result<()> {
    init(&client).await?;
    let resolver = verify::build_resolver("system")?;
    let http = verify::build_http(resolver, user_agent)?;
    let rows = client
        .query(
            "SELECT DISTINCT d.did FROM recon_didweb d \
             WHERE NOT EXISTS (SELECT 1 FROM recon_didweb_docs x WHERE x.did = d.did) ORDER BY 1",
            &[],
        )
        .await?;
    let total = rows.len();
    let sem = Arc::new(Semaphore::new(8));
    let mut tasks = Vec::new();
    for r in rows {
        let did: String = r.get(0);
        let (sem, http, client) = (sem.clone(), http.clone(), client.clone());
        tasks.push(tokio::spawn(async move {
            let _p = sem.acquire().await.expect("open");
            let t = Instant::now();
            let mut outcome = String::new();
            let (mut status, mut id_ok, mut has_pds) = (None::<i32>, None::<bool>, None::<bool>);
            let (mut pds, mut key_type, mut hclaim, mut services) = (None::<String>, None::<String>, None::<String>, None::<String>);
            match didweb_host(&did) {
                None => outcome.push_str("rejected_syntax"),
                Some(host) => {
                    let url = format!("https://{host}/.well-known/did.json");
                    match http.get(&url).send().await {
                        Err(e) => {
                            outcome = if e.is_timeout() { "timeout".into() } else if e.is_connect() { "connect".into() } else { "error".into() };
                        }
                        Ok(resp) => {
                            status = Some(resp.status().as_u16() as i32);
                            if !resp.status().is_success() {
                                outcome = "http_status".into();
                            } else {
                                match resp.bytes().await {
                                    Err(_) => outcome = "body_error".into(),
                                    Ok(b) if b.len() > 2 * 1024 * 1024 => outcome = "too_large".into(),
                                    Ok(b) => match serde_json::from_slice::<Value>(&b) {
                                        Err(_) => outcome = "not_json".into(),
                                        Ok(doc) => {
                                            outcome = "ok".into();
                                            id_ok = Some(doc.get("id").and_then(Value::as_str) == Some(did.as_str()));
                                            let svcs = doc.get("service").and_then(Value::as_array).cloned().unwrap_or_default();
                                            let mut ids = Vec::new();
                                            let mut found = false;
                                            for s in &svcs {
                                                let id = s.get("id").and_then(Value::as_str).unwrap_or("");
                                                ids.push(id.rsplit('#').next().unwrap_or("").to_owned());
                                                if !found
                                                    && id.ends_with("#atproto_pds")
                                                    && s.get("type").and_then(Value::as_str) == Some("AtprotoPersonalDataServer")
                                                {
                                                    found = true;
                                                    pds = s.get("serviceEndpoint").and_then(Value::as_str).map(str::to_owned);
                                                }
                                            }
                                            has_pds = Some(found);
                                            services = Some(ids.join(","));
                                            if let Some(vms) = doc.get("verificationMethod").and_then(Value::as_array) {
                                                for vm in vms {
                                                    let id = vm.get("id").and_then(Value::as_str).unwrap_or("");
                                                    if id.ends_with("#atproto") {
                                                        key_type = vm.get("type").and_then(Value::as_str).map(str::to_owned);
                                                        break;
                                                    }
                                                }
                                            }
                                            if let Some(aka) = doc.get("alsoKnownAs").and_then(Value::as_array) {
                                                for e in aka {
                                                    if let Some(h) = e.as_str().and_then(|s| s.strip_prefix("at://")).and_then(handle::normalize) {
                                                        hclaim = Some(h);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            }
            let ms = t.elapsed().as_millis() as i32;
            let res = client
                .execute(
                    "INSERT INTO recon_didweb_docs (did, outcome, http_status, id_ok, has_pds, pds, key_type, handle, services, ms) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING",
                    &[&did, &outcome, &status, &id_ok, &has_pds, &pds, &key_type, &hclaim, &services, &ms],
                )
                .await;
            if let Err(e) = res {
                eprintln!("recording {did}: {e}");
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    println!("{}", json!({"fetched": total}));
    Ok(())
}

/// Compare lodestar-style state and history with the directory's audit log.
pub async fn audit_compare(client: &Client, plc_url: &str, user_agent: &str, random: i64, forks: i64) -> Result<()> {
    init(client).await?;
    let http = reqwest::Client::builder().user_agent(user_agent.to_owned()).timeout(Duration::from_secs(30)).build()?;
    let mut targets: Vec<(String, &'static str)> = Vec::new();
    for r in client
        .query(
            "SELECT did FROM (SELECT did, count(*) AS n FROM recon_forks GROUP BY did) t \
             ORDER BY (n > 20) DESC, md5(did) LIMIT $1",
            &[&forks],
        )
        .await?
    {
        targets.push((r.get(0), "fork"));
    }
    for r in client
        .query(
            "SELECT did FROM accounts TABLESAMPLE BERNOULLI (0.01) WHERE method = 0 ORDER BY md5(did) LIMIT $1",
            &[&random],
        )
        .await?
    {
        targets.push((r.get(0), "random"));
    }
    let (mut ok, mut bad) = (0, 0);
    for (did, kind) in targets {
        let t0 = Instant::now();
        let url = format!("{}/{}/log/audit", plc_url.trim_end_matches('/'), did);
        let resp = http.get(&url).send().await;
        let body = match resp {
            Ok(r) if r.status().is_success() => r.text().await.unwrap_or_default(),
            Ok(r) => {
                eprintln!("{did}: status {}", r.status());
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            Err(e) => {
                eprintln!("{did}: {e}");
                continue;
            }
        };
        let entries: Vec<Value> = serde_json::from_str(&body).unwrap_or_default();
        let mut expect: Vec<(i64, Option<String>, Option<String>)> = Vec::new();
        let (mut cur_h, mut cur_p, mut cur_k, mut tomb) = (None::<String>, None::<String>, None::<String>, false);
        let mut nullified = 0;
        for e in &entries {
            if e.get("nullified").and_then(Value::as_bool) == Some(true) {
                nullified += 1;
                continue;
            }
            let Some(op) = e.get("operation") else { continue };
            let ms = e
                .get("createdAt")
                .and_then(Value::as_str)
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.timestamp_millis())
                .unwrap_or(0);
            let (is_tomb, h, p, k) = plc::derive_public(op);
            if is_tomb {
                tomb = true;
                cur_h = None;
                cur_p = None;
                cur_k = None;
                continue;
            }
            tomb = false;
            cur_h = h;
            cur_p = p;
            cur_k = k;
            let changed = match expect.last() {
                None => true,
                Some(l) => l.1 != cur_h || l.2 != cur_p,
            };
            if changed {
                expect.push((ms, cur_h.clone(), cur_p.clone()));
            }
        }
        let have: Vec<(i64, Option<String>, Option<String>)> = client
            .query(
                "SELECT (extract(epoch FROM changed_at) * 1000)::bigint, handle_claim, pds_endpoint \
                 FROM identity_history WHERE did = $1 ORDER BY seq",
                &[&did],
            )
            .await?
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
        let acct = client
            .query_opt("SELECT handle_claim, pds_endpoint, signing_key, status FROM accounts WHERE did = $1", &[&did])
            .await?;
        let state_match = match &acct {
            None => false,
            Some(r) => {
                let (h, p, k, s): (Option<String>, Option<String>, Option<String>, i16) = (r.get(0), r.get(1), r.get(2), r.get(3));
                h == cur_h && p == cur_p && k == cur_k && (s == 1) == tomb
            }
        };
        let history_match = have == expect;
        let detail = if state_match && history_match {
            None
        } else {
            Some(json!({"expect_rows": expect.len(), "have_rows": have.len()}).to_string())
        };
        if state_match && history_match {
            ok += 1;
        } else {
            bad += 1;
        }
        client
            .execute(
                "INSERT INTO recon_audit_compare (did, kind, ops, nullified, state_match, history_match, detail) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (did) DO UPDATE SET kind = EXCLUDED.kind, ops = EXCLUDED.ops, \
                 nullified = EXCLUDED.nullified, state_match = EXCLUDED.state_match, history_match = EXCLUDED.history_match, detail = EXCLUDED.detail",
                &[&did, &kind, &(entries.len() as i32), &nullified, &state_match, &history_match, &detail],
            )
            .await?;
        let spent = t0.elapsed();
        if spent < Duration::from_millis(300) {
            tokio::time::sleep(Duration::from_millis(300) - spent).await;
        }
    }
    println!("{}", json!({"match": ok, "differ": bad}));
    Ok(())
}

/// For live identity events, check the handle at once and again later.
pub async fn event_lag(
    client: Arc<Client>,
    base: &str,
    plc_url: &str,
    user_agent: &str,
    seconds: u64,
    min_gap_ms: u64,
) -> Result<()> {
    init(&client).await?;
    let resolver = verify::build_resolver("system")?;
    let http = verify::build_http(resolver.clone(), user_agent)?;
    let plain = reqwest::Client::builder().user_agent(user_agent.to_owned()).timeout(Duration::from_secs(15)).build()?;
    let plc_url = plc_url.trim_end_matches('/').to_owned();
    let kinds = vec!["identity".to_owned()];
    let started = Instant::now();
    let total = Duration::from_secs(seconds);
    let mut last_taken = Instant::now() - Duration::from_secs(60);
    let (mut seen, mut taken) = (0u64, 0u64);
    let mut tasks = Vec::new();
    while started.elapsed() < total {
        let mut ws = match net::connect_events(base, user_agent, &kinds, None).await {
            Ok(ws) => ws,
            Err(e) => {
                eprintln!("connect failed: {e}");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };
        loop {
            let left = total.saturating_sub(started.elapsed());
            if left.is_zero() {
                break;
            }
            let m = match tokio::time::timeout(left.min(Duration::from_secs(120)), ws.next()).await {
                Ok(Some(Ok(m))) => m,
                _ => break,
            };
            let Ok(text) = m.to_text() else { continue };
            let Ok(v) = serde_json::from_str::<Value>(text) else { continue };
            let p = v.get("payload").unwrap_or(&v);
            let Some(seq) = p.get("seq").and_then(Value::as_i64) else { continue };
            seen += 1;
            if last_taken.elapsed() < Duration::from_millis(min_gap_ms) {
                continue;
            }
            last_taken = Instant::now();
            taken += 1;
            let did = p.get("did").and_then(Value::as_str).unwrap_or("").to_owned();
            let ev_handle = p
                .pointer("/identity/handle")
                .and_then(Value::as_str)
                .and_then(handle::normalize);
            let time = p.get("time").and_then(Value::as_str).map(str::to_owned);
            let (client, resolver, http, plain, plc_url) =
                (client.clone(), resolver.clone(), http.clone(), plain.clone(), plc_url.clone());
            tasks.push(tokio::spawn(async move {
                // The claim lodestar would hold comes from the directory, which
                // it follows within seconds. Read it from the directory now.
                let stored: Option<String> = client
                    .query_opt("SELECT handle_claim FROM accounts WHERE did = $1", &[&did])
                    .await
                    .ok()
                    .flatten()
                    .and_then(|r| r.get::<_, Option<String>>(0));
                let mut from = "event";
                let mut h = ev_handle;
                if did.starts_with("did:plc:") {
                    let url = format!("{plc_url}/{did}");
                    if let Ok(resp) = plain.get(&url).send().await {
                        if let Ok(doc) = resp.json::<Value>().await {
                            let claim = doc.get("alsoKnownAs").and_then(Value::as_array).and_then(|a| {
                                a.iter().find_map(|e| e.as_str().and_then(|s| s.strip_prefix("at://")).and_then(handle::normalize))
                            });
                            if claim.is_some() {
                                h = claim;
                                from = "directory";
                            }
                        }
                    }
                }
                if h.is_none() {
                    h = stored.clone();
                    from = "accounts";
                }
                let _ = client
                    .execute(
                        "INSERT INTO recon_event_lag (seq, did, handle, handle_from, stored_handle, event_time, received_at) \
                         VALUES ($1,$2,$3,$4,$5,$6::text::timestamptz, now()) ON CONFLICT DO NOTHING",
                        &[&seq, &did, &h, &from, &stored, &time],
                    )
                    .await;
                let Some(h) = h else { return };
                if handle::never_resolves(&h) {
                    return;
                }
                let c = verify::check(&resolver, &http, &h).await;
                let r1 = verify::result_for(&c, &did);
                let _ = client
                    .execute(
                        "UPDATE recon_event_lag SET first_result = $2, first_ms = $3, first_dns = $4, first_https = $5 WHERE seq = $1",
                        &[&seq, &r1, &c.total_ms, &c.dns, &c.https],
                    )
                    .await;
                if r1 == "verified" {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
                let c2 = verify::check(&resolver, &http, &h).await;
                let r2 = verify::result_for(&c2, &did);
                let _ = client.execute("UPDATE recon_event_lag SET after_60s = $2 WHERE seq = $1", &[&seq, &r2]).await;
                if r2 == "verified" {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(540)).await;
                let c3 = verify::check(&resolver, &http, &h).await;
                let r3 = verify::result_for(&c3, &did);
                let _ = client.execute("UPDATE recon_event_lag SET after_600s = $2 WHERE seq = $1", &[&seq, &r3]).await;
            }));
        }
    }
    for t in tasks {
        let _ = t.await;
    }
    println!("{}", json!({"identity_events_seen": seen, "checked": taken}));
    Ok(())
}
