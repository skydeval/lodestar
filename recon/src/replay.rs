//! Jetstream v2 Network Replay: plan sizing, then download and decode of
//! identity and account events. The API key is read from the environment and
//! is never written to any output.

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio_postgres::Client;

use crate::jss;

const TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS recon_replay_dids (
    did       text PRIMARY KEY,
    first_seq bigint NOT NULL,
    last_seq  bigint NOT NULL,
    identity  integer NOT NULL,
    account   integer NOT NULL,
    other     integer NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS recon_replay_units (
    segment     text NOT NULL,
    block       integer NOT NULL,
    bytes       bigint NOT NULL,
    rows        bigint NOT NULL,
    identity    bigint NOT NULL,
    account     bigint NOT NULL,
    other       bigint NOT NULL,
    with_handle bigint NOT NULL,
    ms          integer NOT NULL,
    PRIMARY KEY (segment, block)
);
"#;

pub struct Api {
    base: String,
    http: reqwest::Client,
    key: String,
}

impl Api {
    pub fn new(base: &str, user_agent: &str) -> Result<Api> {
        let key = std::env::var("RECON_JETSTREAM_API_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty())
            .context("RECON_JETSTREAM_API_KEY is not set in the recon env file")?;
        let http = reqwest::Client::builder()
            .user_agent(user_agent.to_owned())
            .timeout(Duration::from_secs(600))
            .build()?;
        Ok(Api { base: base.trim_end_matches('/').to_owned(), http, key: key.trim().to_owned() })
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let req = req.bearer_auth(&self.key);
        let mut backoff = Duration::from_secs(2);
        for _ in 0..20 {
            let Some(r) = req.try_clone() else { bail!("request cannot be retried") };
            match r.send().await {
                Ok(resp) => {
                    let s = resp.status().as_u16();
                    if s == 429 || s == 503 {
                        let wait = resp
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .map(Duration::from_secs)
                            .unwrap_or(backoff);
                        eprintln!("status {s}; waiting {wait:?}");
                        tokio::time::sleep(wait).await;
                        backoff = (backoff * 2).min(Duration::from_secs(600));
                        continue;
                    }
                    if s == 401 {
                        bail!("the API key was rejected (401)");
                    }
                    return Ok(resp);
                }
                Err(e) => {
                    // reqwest errors can include the URL but never headers.
                    eprintln!("request error: {e}; retrying in {backoff:?}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(600));
                }
            }
        }
        bail!("gave up after repeated failures")
    }

    async fn plan_page(&self, kinds: &[String], after: u64, before: Option<u64>) -> Result<Value> {
        // An empty kinds list means an unfiltered plan: omit the field.
        let mut body = if kinds.is_empty() { json!({"afterSeq": after}) } else { json!({"kinds": kinds, "afterSeq": after}) };
        if let Some(b) = before {
            body["beforeSeq"] = json!(b);
        }
        let url = format!("{}/xrpc/network.bsky.jetstream.planSnapshot", self.base);
        let resp = self.send(self.http.post(&url).json(&body)).await?;
        if !resp.status().is_success() {
            bail!("planSnapshot status {} body {}", resp.status(), resp.text().await.unwrap_or_default());
        }
        Ok(resp.json().await?)
    }

    async fn list_segments(&self) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut url = format!("{}/xrpc/network.bsky.jetstream.listSegments?limit=1000", self.base);
            if let Some(c) = &cursor {
                url.push_str("&cursor=");
                url.push_str(c);
            }
            let resp = self.send(self.http.get(&url)).await?;
            if !resp.status().is_success() {
                bail!("listSegments status {}", resp.status());
            }
            let v: Value = resp.json().await?;
            let list = v.get("segments").and_then(Value::as_array).cloned().unwrap_or_default();
            let n = list.len();
            out.extend(list);
            cursor = v.get("cursor").and_then(Value::as_str).map(str::to_owned);
            if cursor.is_none() || n == 0 {
                break;
            }
        }
        Ok(out)
    }

    async fn range(&self, name: &str, from: u64, to_inclusive: u64) -> Result<Vec<u8>> {
        let url = format!("{}/xrpc/network.bsky.jetstream.getSegment?name={}", self.base, name);
        let resp = self
            .send(self.http.get(&url).header("Range", format!("bytes={from}-{to_inclusive}")))
            .await?;
        if resp.status().as_u16() != 206 {
            bail!("getSegment range status {}", resp.status());
        }
        Ok(resp.bytes().await?.to_vec())
    }

    async fn block(&self, name: &str, index: u64) -> Result<Vec<u8>> {
        let url = format!("{}/xrpc/network.bsky.jetstream.getBlock?segment={}&blockIndex={}", self.base, name, index);
        let resp = self.send(self.http.get(&url)).await?;
        if !resp.status().is_success() {
            bail!("getBlock status {}", resp.status());
        }
        Ok(resp.bytes().await?.to_vec())
    }
}

/// Page the whole plan and write each page to `out`. Returns the units.
async fn full_plan(api: &Api, kinds: &[String], after: u64, out: &mut std::fs::File) -> Result<(u64, Vec<Value>)> {
    let mut units = Vec::new();
    let first = api.plan_page(kinds, after, None).await?;
    writeln!(out, "{first}")?;
    let tip = first.get("sealedTipSeq").and_then(Value::as_u64).context("no sealedTipSeq")?;
    let mut through = first.get("plannedThroughSeq").and_then(Value::as_u64).context("no plannedThroughSeq")?;
    units.extend(first.get("segments").and_then(Value::as_array).cloned().unwrap_or_default());
    while through < tip {
        let page = api.plan_page(kinds, through, Some(tip)).await?;
        writeln!(out, "{page}")?;
        let next = page.get("plannedThroughSeq").and_then(Value::as_u64).context("no plannedThroughSeq")?;
        if next <= through {
            bail!("plan did not advance past {through}");
        }
        through = next;
        units.extend(page.get("segments").and_then(Value::as_array).cloned().unwrap_or_default());
    }
    Ok((tip, units))
}

/// Size a replay without downloading any event data.
pub async fn plan(base: &str, user_agent: &str, kinds: Vec<String>, after: u64, out_path: &str, exact_blocks: bool) -> Result<()> {
    let api = Api::new(base, user_agent)?;
    let mut out = std::fs::OpenOptions::new().create(true).append(true).open(out_path)?;
    let (tip, units) = full_plan(&api, &kinds, after, &mut out).await?;
    let listing = api.list_segments().await?;
    let mut size: HashMap<String, (u64, u64)> = HashMap::new();
    let (mut archive_bytes, mut archive_events) = (0u64, 0u64);
    for s in &listing {
        let name = s.get("name").and_then(Value::as_str).unwrap_or("").to_owned();
        let b = s.get("sizeBytes").and_then(Value::as_u64).unwrap_or(0);
        let e = s.get("eventCount").and_then(Value::as_u64).unwrap_or(0);
        archive_bytes += b;
        archive_events += e;
        size.insert(name, (b, e));
    }
    let (mut whole, mut whole_bytes, mut partial, mut partial_blocks, mut partial_seg_bytes) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut exact_block_bytes, mut exact_total_blocks, mut probe_bytes) = (0u64, 0u64, 0u64);
    for u in &units {
        let name = u.get("name").and_then(Value::as_str).unwrap_or("");
        let seg_bytes = size.get(name).map(|x| x.0).unwrap_or(0);
        if u.get("mode").and_then(Value::as_str) == Some("segment") {
            whole += 1;
            whole_bytes += seg_bytes;
            continue;
        }
        partial += 1;
        partial_seg_bytes += seg_bytes;
        let ranges = u.get("blocks").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut wanted: Vec<(u64, u64)> = Vec::new();
        for r in &ranges {
            let (a, b) = (r.get("first").and_then(Value::as_u64).unwrap_or(0), r.get("last").and_then(Value::as_u64).unwrap_or(0));
            partial_blocks += b - a + 1;
            wanted.push((a, b));
        }
        if exact_blocks {
            let head = api.range(name, 0, jss::HEADER_LEN as u64 - 1).await?;
            let h = jss::parse_header(&head)?;
            let idx_len = h.block_count as u64 * jss::BLOCK_ENTRY_LEN as u64;
            let idx = api.range(name, h.block_index_offset, h.block_index_offset + idx_len - 1).await?;
            probe_bytes += head.len() as u64 + idx.len() as u64;
            let entries = jss::parse_block_index(&idx, h.block_count)?;
            exact_total_blocks += entries.len() as u64;
            for (a, b) in wanted {
                for i in a..=b {
                    if let Some(e) = entries.get(i as usize) {
                        exact_block_bytes += e.compressed_size as u64;
                    }
                }
            }
        }
    }
    let summary = json!({
        "wall": chrono::Utc::now().to_rfc3339(),
        "kinds": kinds, "after_seq": after, "sealed_tip_seq": tip,
        "archive_segments": listing.len(), "archive_bytes": archive_bytes, "archive_events": archive_events,
        "plan_units": units.len(),
        "whole_segments": whole, "whole_segment_bytes": whole_bytes,
        "block_mode_segments": partial, "block_mode_blocks": partial_blocks, "block_mode_segment_bytes_if_whole": partial_seg_bytes,
        "block_mode_exact": exact_blocks, "block_mode_block_bytes": exact_block_bytes,
        "block_mode_total_blocks_in_those_segments": exact_total_blocks, "bytes_spent_probing": probe_bytes,
        "download_bytes_estimate": whole_bytes + if exact_blocks { exact_block_bytes } else { 0 },
    });
    writeln!(out, "{summary}")?;
    println!("{summary}");
    Ok(())
}

#[derive(Default)]
struct Agg {
    first: u64,
    last: u64,
    identity: i32,
    account: i32,
    other: i32,
}

/// Download and decode the planned units, recording every DID seen on an
/// identity or account row. Stops when `max_bytes` have been downloaded.
pub async fn run(client: &Client, base: &str, user_agent: &str, kinds: Vec<String>, after: u64, out_path: &str, max_bytes: u64) -> Result<()> {
    client.batch_execute(TABLES).await?;
    let api = Api::new(base, user_agent)?;
    let mut out = std::fs::OpenOptions::new().create(true).append(true).open(out_path)?;
    let (tip, units) = full_plan(&api, &kinds, after, &mut out).await?;
    let started = Instant::now();
    let mut spent = 0u64;
    let all_rows = kinds.is_empty();
    client
        .batch_execute("ALTER TABLE recon_replay_dids ADD COLUMN IF NOT EXISTS other integer NOT NULL DEFAULT 0")
        .await?;
    let mut dids: HashMap<String, Agg> = HashMap::new();
    let mut done_units = 0u64;

    'units: for u in &units {
        let name = u.get("name").and_then(Value::as_str).unwrap_or("").to_owned();
        let whole = u.get("mode").and_then(Value::as_str) == Some("segment");
        let mut frames: Vec<(i32, Vec<u8>)> = Vec::new();
        let t0 = Instant::now();
        let already: i64 = client
            .query_one("SELECT count(*) FROM recon_replay_units WHERE segment = $1", &[&name])
            .await?
            .get(0);
        if already > 0 {
            continue;
        }
        if whole {
            let head = api.range(&name, 0, jss::HEADER_LEN as u64 - 1).await?;
            let h = jss::parse_header(&head)?;
            let idx_len = h.block_count as u64 * jss::BLOCK_ENTRY_LEN as u64;
            let idx = api.range(&name, h.block_index_offset, h.block_index_offset + idx_len - 1).await?;
            spent += head.len() as u64 + idx.len() as u64;
            let entries = jss::parse_block_index(&idx, h.block_count)?;
            // One ranged read for the whole block region.
            let body = api.range(&name, jss::HEADER_LEN as u64, h.footer_offset - 1).await?;
            spent += body.len() as u64;
            for (i, e) in entries.iter().enumerate() {
                let start = (e.offset as usize - jss::HEADER_LEN) + 8;
                let end = start + e.compressed_size as usize;
                if end > body.len() {
                    bail!("segment {name} changed during download");
                }
                frames.push((i as i32, body[start..end].to_vec()));
            }
        } else {
            for r in u.get("blocks").and_then(Value::as_array).cloned().unwrap_or_default() {
                let (a, b) = (r.get("first").and_then(Value::as_u64).unwrap_or(0), r.get("last").and_then(Value::as_u64).unwrap_or(0));
                for i in a..=b {
                    let f = api.block(&name, i).await?;
                    spent += f.len() as u64;
                    frames.push((i as i32, f));
                }
            }
        }
        for (i, frame) in &frames {
            let body = jss::decompress(frame)?;
            let (mut rows, mut ident, mut acct, mut other, mut with_handle) = (0i64, 0i64, 0i64, 0i64, 0i64);
            jss::for_each_row(&body, |r| {
                rows += 1;
                if r.seq <= after || r.seq > tip {
                    return;
                }
                let is_ident = r.kind == jss::KIND_IDENTITY;
                let is_acct = r.kind == jss::KIND_ACCOUNT;
                if !is_ident && !is_acct {
                    other += 1;
                    // An unfiltered run also collects DIDs from every other row.
                    if !all_rows || r.did.is_empty() {
                        return;
                    }
                } else if is_ident {
                    ident += 1;
                    if let Ok(m) = jss::parse_marker(r.payload) {
                        if m.handle.is_some() {
                            with_handle += 1;
                        }
                    }
                } else {
                    acct += 1;
                }
                let did = String::from_utf8_lossy(r.did).into_owned();
                let a = dids.entry(did).or_default();
                if a.first == 0 {
                    a.first = r.seq;
                }
                a.last = r.seq;
                if is_ident {
                    a.identity += 1;
                } else if is_acct {
                    a.account += 1;
                } else {
                    a.other += 1;
                }
            })?;
            client
                .execute(
                    "INSERT INTO recon_replay_units (segment, block, bytes, rows, identity, account, other, with_handle, ms) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING",
                    &[&name, i, &(frame.len() as i64), &rows, &ident, &acct, &other, &with_handle, &(t0.elapsed().as_millis() as i32)],
                )
                .await?;
        }
        done_units += 1;
        if dids.len() > 500_000 {
            flush(client, &mut dids).await?;
        }
        if max_bytes != 0 && spent >= max_bytes {
            eprintln!("byte budget reached after {done_units} units");
            break 'units;
        }
    }
    flush(client, &mut dids).await?;
    let summary = json!({
        "wall": chrono::Utc::now().to_rfc3339(), "sealed_tip_seq": tip, "units_planned": units.len(),
        "units_done": done_units, "bytes_downloaded": spent, "elapsed_s": started.elapsed().as_secs(),
    });
    writeln!(out, "{summary}")?;
    println!("{summary}");
    Ok(())
}

async fn flush(client: &Client, dids: &mut HashMap<String, Agg>) -> Result<()> {
    let all: Vec<(String, Agg)> = dids.drain().collect();
    for chunk in all.chunks(20_000) {
        let d: Vec<&str> = chunk.iter().map(|x| x.0.as_str()).collect();
        let f: Vec<i64> = chunk.iter().map(|x| x.1.first as i64).collect();
        let l: Vec<i64> = chunk.iter().map(|x| x.1.last as i64).collect();
        let i: Vec<i32> = chunk.iter().map(|x| x.1.identity).collect();
        let a: Vec<i32> = chunk.iter().map(|x| x.1.account).collect();
        let o: Vec<i32> = chunk.iter().map(|x| x.1.other).collect();
        client
            .execute(
                "INSERT INTO recon_replay_dids (did, first_seq, last_seq, identity, account, other) \
                 SELECT a, b, c, d, e, f FROM UNNEST($1::text[], $2::int8[], $3::int8[], $4::int4[], $5::int4[], $6::int4[]) AS t(a, b, c, d, e, f) \
                 ON CONFLICT (did) DO UPDATE SET first_seq = LEAST(recon_replay_dids.first_seq, EXCLUDED.first_seq), \
                     last_seq = GREATEST(recon_replay_dids.last_seq, EXCLUDED.last_seq), \
                     identity = recon_replay_dids.identity + EXCLUDED.identity, account = recon_replay_dids.account + EXCLUDED.account, \
                     other = recon_replay_dids.other + EXCLUDED.other",
                &[&d, &f, &l, &i, &a, &o],
            )
            .await?;
    }
    Ok(())
}
