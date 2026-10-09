//! PLC backfill: page the sequenced `/export` and apply each operation the
//! way the design describes (§3.1, §3.1.1), measuring as it goes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::DateTime;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_postgres::Client;

use crate::handle;

const SOURCE: &str = "plc";
const STATUS_ACTIVE: i16 = 0;
const STATUS_TOMBSTONED: i16 = 1;
const HANDLE_PENDING: i16 = 3;
const HANDLE_NONE: i16 = 4;

pub struct Opts {
    pub plc_url: String,
    pub user_agent: String,
    pub count: u32,
    /// Minimum gap between requests.
    pub min_interval: Duration,
    /// Recovery window plus margin, in hours.
    pub window_hours: i64,
    pub metrics_path: String,
    pub metrics_every: u64,
    /// Stop after this many pages (0 = run to the tip).
    pub max_pages: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Operation,
    LegacyCreate,
    Tombstone,
    Other,
}

struct Line {
    did: String,
    cid: String,
    seq: i64,
    created_at: String,
    created_ms: i64,
    prev: Option<String>,
    kind: Kind,
    handle: Option<String>,
    pds: Option<String>,
    key: Option<String>,
}

struct Page {
    lines: Vec<Line>,
    bytes: usize,
    net_ms: u128,
    notes: Vec<String>,
}

#[derive(Default)]
struct Counters(BTreeMap<&'static str, i64>);

impl Counters {
    fn add(&mut self, k: &'static str, n: i64) {
        *self.0.entry(k).or_insert(0) += n;
    }
}

fn derive(op: &Value, c: &mut Counters) -> (Kind, Option<String>, Option<String>, Option<String>) {
    let ty = op.get("type").and_then(Value::as_str).unwrap_or("");
    match ty {
        "plc_tombstone" => {
            c.add("op.tombstone", 1);
            (Kind::Tombstone, None, None, None)
        }
        "create" => {
            c.add("op.legacy_create", 1);
            let h = op
                .get("handle")
                .and_then(Value::as_str)
                .and_then(handle::normalize);
            if h.is_none() {
                c.add("claim.none", 1);
            }
            let pds = op.get("service").and_then(Value::as_str).map(str::to_owned);
            let key = op.get("signingKey").and_then(Value::as_str).map(str::to_owned);
            (Kind::LegacyCreate, h, pds, key)
        }
        "plc_operation" => {
            c.add("op.plc_operation", 1);
            let mut h = None;
            let mut at_entries = 0;
            if let Some(aka) = op.get("alsoKnownAs").and_then(Value::as_array) {
                c.add("aka.entries", aka.len() as i64);
                for e in aka {
                    if let Some(rest) = e.as_str().and_then(|s| s.strip_prefix("at://")) {
                        at_entries += 1;
                        if h.is_none() {
                            match handle::normalize(rest) {
                                Some(v) => h = Some(v),
                                None => c.add("claim.at_entry_invalid", 1),
                            }
                        }
                    }
                }
            }
            if at_entries > 1 {
                c.add("aka.multiple_at_entries", 1);
            }
            if h.is_none() {
                c.add("claim.none", 1);
            }
            let svc = op.pointer("/services/atproto_pds");
            let mut pds = None;
            match svc {
                None => c.add("pds.absent", 1),
                Some(s) => {
                    let ty_ok = s.get("type").and_then(Value::as_str)
                        == Some("AtprotoPersonalDataServer");
                    if !ty_ok {
                        c.add("pds.wrong_type", 1);
                    } else {
                        pds = s.get("endpoint").and_then(Value::as_str).map(str::to_owned);
                        if pds.is_none() {
                            c.add("pds.no_endpoint", 1);
                        }
                    }
                }
            }
            let key = op
                .pointer("/verificationMethods/atproto")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if key.is_none() {
                c.add("key.absent", 1);
            }
            (Kind::Operation, h, pds, key)
        }
        _ => {
            c.add("op.other", 1);
            (Kind::Other, None, None, None)
        }
    }
}

fn parse_page(body: &str, c: &mut Counters) -> Result<Vec<Line>> {
    let mut out = Vec::with_capacity(1000);
    for l in body.lines() {
        if l.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(l).context("export line is not JSON")?;
        let did = v.get("did").and_then(Value::as_str).ok_or_else(|| anyhow!("no did"))?;
        let cid = v.get("cid").and_then(Value::as_str).ok_or_else(|| anyhow!("no cid"))?;
        let seq = v.get("seq").and_then(Value::as_i64).ok_or_else(|| anyhow!("no seq"))?;
        let created_at = v
            .get("createdAt")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("no createdAt"))?;
        let created_ms = DateTime::parse_from_rfc3339(created_at)
            .with_context(|| format!("bad createdAt {created_at}"))?
            .timestamp_millis();
        let op = v.get("operation").ok_or_else(|| anyhow!("no operation"))?;
        if v.get("type").and_then(Value::as_str) != Some("sequenced_op") {
            c.add("line.type_not_sequenced_op", 1);
        }
        if v.get("nullified").is_some() {
            c.add("line.has_nullified_field", 1);
        }
        let prev = op.get("prev").and_then(Value::as_str).map(str::to_owned);
        let (kind, handle, pds, key) = derive(op, c);
        out.push(Line {
            did: did.to_owned(),
            cid: cid.to_owned(),
            seq,
            created_at: created_at.to_owned(),
            created_ms,
            prev,
            kind,
            handle,
            pds,
            key,
        });
    }
    Ok(out)
}

async fn fetch_loop(opts: &Opts, mut after: i64, tx: mpsc::Sender<Page>) -> Result<()> {
    let http = reqwest::Client::builder()
        .user_agent(opts.user_agent.clone())
        .timeout(Duration::from_secs(60))
        .build()?;
    let mut backoff = Duration::from_secs(2);
    let mut seen_headers: HashSet<String> = HashSet::new();
    let mut pages: u64 = 0;
    loop {
        let started = Instant::now();
        let url = format!("{}/export?after={}&count={}", opts.plc_url, after, opts.count);
        let mut notes = Vec::new();
        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("fetch error after={after}: {e}; retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(300));
                continue;
            }
        };
        let status = resp.status();
        for (k, v) in resp.headers() {
            let name = k.as_str().to_ascii_lowercase();
            if (name.contains("ratelimit") || name == "retry-after") && seen_headers.insert(name.clone()) {
                notes.push(format!("header {name}: {}", v.to_str().unwrap_or("?")));
            }
        }
        if status.as_u16() == 429 || status.is_server_error() {
            let wait = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(backoff);
            eprintln!("status {status} after={after}; waiting {wait:?}");
            let page = Page { lines: Vec::new(), bytes: 0, net_ms: 0, notes: vec![format!("status {status} wait {}s", wait.as_secs())] };
            if tx.send(page).await.is_err() {
                return Ok(());
            }
            tokio::time::sleep(wait).await;
            backoff = (backoff * 2).min(Duration::from_secs(300));
            continue;
        }
        if !status.is_success() {
            bail!("unexpected status {status} for {url}");
        }
        let body = match resp.text().await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("body error after={after}: {e}; retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(300));
                continue;
            }
        };
        backoff = Duration::from_secs(2);
        let net_ms = started.elapsed().as_millis();
        let mut c = Counters::default();
        let lines = parse_page(&body, &mut c)?;
        for (k, n) in c.0 {
            notes.push(format!("count {k} {n}"));
        }
        let n = lines.len();
        if let Some(last) = lines.last() {
            after = last.seq;
        }
        let page = Page { lines, bytes: body.len(), net_ms, notes };
        if tx.send(page).await.is_err() {
            return Ok(());
        }
        pages += 1;
        if n < opts.count as usize {
            // Short page: the tip. An empty marker page tells the applier.
            let _ = tx
                .send(Page { lines: Vec::new(), bytes: 0, net_ms: 0, notes: vec!["tip".into()] })
                .await;
            return Ok(());
        }
        if opts.max_pages != 0 && pages >= opts.max_pages {
            return Ok(());
        }
        let spent = started.elapsed();
        if spent < opts.min_interval {
            tokio::time::sleep(opts.min_interval - spent).await;
        }
    }
}

#[derive(Clone)]
struct Hist {
    seq: i32,
    handle: Option<String>,
    pds: Option<String>,
    changed_at: Option<String>,
    new: bool,
}

struct Rec {
    seq: i64,
    cid: String,
    created_ms: i64,
    created_at: Option<String>,
    hist_seq: Option<i32>,
    new: bool,
}

#[derive(Default)]
struct St {
    exists: bool,
    dirty: bool,
    handle: Option<String>,
    pds: Option<String>,
    key: Option<String>,
    status: i16,
    created_at: Option<String>,
    updated_at: Option<String>,
    hist: Vec<Hist>,
    hist_deleted: Vec<i32>,
    next_hist_seq: i32,
    recent: Vec<Rec>,
    recent_deleted: Vec<i64>,
}

struct ForkRec {
    did: String,
    seq: i64,
    created_at: String,
    prev: Option<String>,
    head_cid: String,
    prev_in_window: bool,
    nullified: Vec<i64>,
    hist_removed: i32,
}

#[derive(Default)]
struct Totals {
    pages: u64,
    ops: i64,
    new_accounts: i64,
    hist_inserted: i64,
    hist_deleted: i64,
    recent_live: i64,
    recent_peak: i64,
    recent_peak_at: String,
    forks: i64,
    nullified_ops: i64,
    bytes: u64,
    net_ms: u128,
    db_ms: u128,
    first_seq: i64,
    last_seq: i64,
    last_created_at: String,
    waits: u64,
}

async fn apply_page(
    client: &mut Client,
    origin: &str,
    lines: &[Line],
    window_ms: i64,
    t: &mut Totals,
) -> Result<()> {
    let mut dids: Vec<String> = lines.iter().map(|l| l.did.clone()).collect();
    dids.sort();
    dids.dedup();

    let tx = client.transaction().await?;
    let mut st: HashMap<String, St> = HashMap::with_capacity(dids.len());

    let rows = tx
        .query(
            "SELECT did, handle_claim, pds_endpoint, signing_key, status, created_at::text, updated_at::text \
             FROM accounts WHERE did = ANY($1)",
            &[&dids],
        )
        .await?;
    for r in rows {
        let did: String = r.get(0);
        st.insert(
            did,
            St {
                exists: true,
                handle: r.get(1),
                pds: r.get(2),
                key: r.get(3),
                status: r.get(4),
                created_at: Some(r.get(5)),
                updated_at: Some(r.get(6)),
                ..St::default()
            },
        );
    }
    let existing: Vec<String> = st.keys().cloned().collect();

    if !existing.is_empty() {
        let rows = tx
            .query(
                "SELECT did, seq, cid, (extract(epoch FROM created_at) * 1000)::bigint, hist_seq \
                 FROM plc_recent_ops WHERE did = ANY($1) ORDER BY did, seq",
                &[&existing],
            )
            .await?;
        let mut with_recent: Vec<String> = Vec::new();
        for r in rows {
            let did: String = r.get(0);
            let s = st.get_mut(&did).expect("row for a did we asked about");
            if s.recent.is_empty() {
                with_recent.push(did.clone());
            }
            s.recent.push(Rec {
                seq: r.get(1),
                cid: r.get(2),
                created_ms: r.get(3),
                created_at: None,
                hist_seq: r.get(4),
                new: false,
            });
        }

        // Last history row for every existing account.
        let rows = tx
            .query(
                "SELECT DISTINCT ON (did) did, seq, handle_claim, pds_endpoint \
                 FROM identity_history WHERE did = ANY($1) ORDER BY did, seq DESC",
                &[&existing],
            )
            .await?;
        for r in rows {
            let did: String = r.get(0);
            let s = st.get_mut(&did).expect("row for a did we asked about");
            let seq: i32 = r.get(1);
            s.next_hist_seq = seq + 1;
            s.hist = vec![Hist { seq, handle: r.get(2), pds: r.get(3), changed_at: None, new: false }];
        }

        // Every history row for accounts with operations in the window, so a
        // fork can find the last surviving row.
        if !with_recent.is_empty() {
            let rows = tx
                .query(
                    "SELECT did, seq, handle_claim, pds_endpoint FROM identity_history \
                     WHERE did = ANY($1) ORDER BY did, seq",
                    &[&with_recent],
                )
                .await?;
            for d in &with_recent {
                st.get_mut(d).expect("present").hist.clear();
            }
            for r in rows {
                let did: String = r.get(0);
                let s = st.get_mut(&did).expect("present");
                s.hist.push(Hist { seq: r.get(1), handle: r.get(2), pds: r.get(3), changed_at: None, new: false });
            }
        }
    }

    let mut forks: Vec<ForkRec> = Vec::new();
    let mut daily: BTreeMap<String, (i64, i64)> = BTreeMap::new();

    for l in lines {
        let day = l.created_at.get(0..10).unwrap_or("1970-01-01").to_owned();
        let e = daily.entry(day).or_insert((0, 0));
        e.0 += 1;
        if l.prev.is_none() {
            e.1 += 1;
        }

        let s = st.entry(l.did.clone()).or_default();

        // Operations older than the window cannot be nullified any more.
        let mut i = 0;
        while i < s.recent.len() && l.created_ms - s.recent[i].created_ms > window_ms {
            i += 1;
        }
        if i > 0 {
            s.recent.drain(0..i);
        }

        if let Some(head) = s.recent.last() {
            if l.prev.as_deref() != Some(head.cid.as_str()) {
                let head_cid = head.cid.clone();
                let pos = s.recent.iter().position(|r| Some(r.cid.as_str()) == l.prev.as_deref());
                let cut = pos.map(|p| p + 1).unwrap_or(0);
                let gone: Vec<Rec> = s.recent.split_off(cut);
                let mut removed = 0;
                let mut seqs = Vec::new();
                for r in gone {
                    seqs.push(r.seq);
                    if let Some(h) = r.hist_seq {
                        if let Some(ix) = s.hist.iter().position(|x| x.seq == h) {
                            let row = s.hist.remove(ix);
                            if !row.new {
                                s.hist_deleted.push(row.seq);
                            }
                            removed += 1;
                        }
                    }
                    if !r.new {
                        s.recent_deleted.push(r.seq);
                    }
                }
                t.nullified_ops += seqs.len() as i64;
                forks.push(ForkRec {
                    did: l.did.clone(),
                    seq: l.seq,
                    created_at: l.created_at.clone(),
                    prev: l.prev.clone(),
                    head_cid,
                    prev_in_window: pos.is_some(),
                    nullified: seqs,
                    hist_removed: removed,
                });
            }
        }

        let mut hist_seq = None;
        match l.kind {
            Kind::Tombstone => {
                s.status = STATUS_TOMBSTONED;
                s.handle = None;
                s.pds = None;
                s.key = None;
            }
            _ => {
                s.status = STATUS_ACTIVE;
                s.handle = l.handle.clone();
                s.pds = l.pds.clone();
                s.key = l.key.clone();
                let changed = match s.hist.last() {
                    None => true,
                    Some(h) => h.handle != s.handle || h.pds != s.pds,
                };
                if changed {
                    let seq = s.next_hist_seq;
                    s.next_hist_seq += 1;
                    s.hist.push(Hist {
                        seq,
                        handle: s.handle.clone(),
                        pds: s.pds.clone(),
                        changed_at: Some(l.created_at.clone()),
                        new: true,
                    });
                    hist_seq = Some(seq);
                }
            }
        }
        if s.created_at.is_none() {
            s.created_at = Some(l.created_at.clone());
        }
        s.updated_at = Some(l.created_at.clone());
        s.dirty = true;
        s.recent.push(Rec {
            seq: l.seq,
            cid: l.cid.clone(),
            created_ms: l.created_ms,
            created_at: Some(l.created_at.clone()),
            hist_seq,
            new: true,
        });
    }

    // ---- writes
    let (mut a_did, mut a_pds, mut a_key, mut a_handle) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut a_hs, mut a_status, mut a_created, mut a_updated) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut h_did, mut h_seq, mut h_at, mut h_handle, mut h_pds) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut hd_did, mut hd_seq) = (Vec::new(), Vec::new());
    let (mut r_did, mut r_seq, mut r_cid, mut r_at, mut r_hist) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut rd_did, mut rd_seq) = (Vec::new(), Vec::new());

    for (did, s) in &st {
        if !s.dirty {
            continue;
        }
        if !s.exists {
            t.new_accounts += 1;
        }
        a_did.push(did.clone());
        a_pds.push(s.pds.clone());
        a_key.push(s.key.clone());
        a_handle.push(s.handle.clone());
        a_hs.push(if s.handle.is_some() { HANDLE_PENDING } else { HANDLE_NONE });
        a_status.push(s.status);
        a_created.push(s.created_at.clone().unwrap_or_default());
        a_updated.push(s.updated_at.clone().unwrap_or_default());
        for h in &s.hist {
            if h.new {
                h_did.push(did.clone());
                h_seq.push(h.seq);
                h_at.push(h.changed_at.clone().unwrap_or_default());
                h_handle.push(h.handle.clone());
                h_pds.push(h.pds.clone());
            }
        }
        for d in &s.hist_deleted {
            hd_did.push(did.clone());
            hd_seq.push(*d);
        }
        for r in &s.recent {
            if r.new {
                r_did.push(did.clone());
                r_seq.push(r.seq);
                r_cid.push(r.cid.clone());
                r_at.push(r.created_at.clone().unwrap_or_default());
                r_hist.push(r.hist_seq);
            }
        }
        for d in &s.recent_deleted {
            rd_did.push(did.clone());
            rd_seq.push(*d);
        }
    }

    tx.execute(
        "INSERT INTO accounts (did, method, pds_endpoint, signing_key, handle_claim, handle_status, status, created_at, updated_at) \
         SELECT d, 0, p, k, h, hs, s, c::timestamptz, u::timestamptz \
         FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::int2[], $6::int2[], $7::text[], $8::text[]) \
              AS t(d, p, k, h, hs, s, c, u) \
         ON CONFLICT (did) DO UPDATE SET pds_endpoint = EXCLUDED.pds_endpoint, signing_key = EXCLUDED.signing_key, \
             handle_claim = EXCLUDED.handle_claim, handle_status = EXCLUDED.handle_status, status = EXCLUDED.status, \
             updated_at = EXCLUDED.updated_at",
        &[&a_did, &a_pds, &a_key, &a_handle, &a_hs, &a_status, &a_created, &a_updated],
    )
    .await
    .context("upserting accounts")?;

    if !hd_did.is_empty() {
        let n = tx
            .execute(
                "DELETE FROM identity_history h USING UNNEST($1::text[], $2::int4[]) AS t(d, s) \
                 WHERE h.did = t.d AND h.seq = t.s",
                &[&hd_did, &hd_seq],
            )
            .await?;
        t.hist_deleted += n as i64;
    }
    if !h_did.is_empty() {
        let n = tx
            .execute(
                "INSERT INTO identity_history (did, seq, changed_at, handle_claim, pds_endpoint) \
                 SELECT d, s, c::timestamptz, h, p \
                 FROM UNNEST($1::text[], $2::int4[], $3::text[], $4::text[], $5::text[]) AS t(d, s, c, h, p)",
                &[&h_did, &h_seq, &h_at, &h_handle, &h_pds],
            )
            .await
            .context("inserting history")?;
        t.hist_inserted += n as i64;
    }
    if !rd_did.is_empty() {
        let n = tx
            .execute(
                "DELETE FROM plc_recent_ops r USING UNNEST($1::text[], $2::int8[]) AS t(d, s) \
                 WHERE r.did = t.d AND r.seq = t.s",
                &[&rd_did, &rd_seq],
            )
            .await?;
        t.recent_live -= n as i64;
    }
    let n = tx
        .execute(
            "INSERT INTO plc_recent_ops (did, seq, cid, created_at, hist_seq) \
             SELECT d, s, c, a::timestamptz, h \
             FROM UNNEST($1::text[], $2::int8[], $3::text[], $4::text[], $5::int4[]) AS t(d, s, c, a, h)",
            &[&r_did, &r_seq, &r_cid, &r_at, &r_hist],
        )
        .await
        .context("inserting recent ops")?;
    t.recent_live += n as i64;

    for f in &forks {
        let seqs = f.nullified.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(",");
        tx.execute(
            "INSERT INTO recon_forks (did, seq, created_at, prev, head_cid, prev_in_window, nullified_seqs, hist_rows_removed) \
             VALUES ($1, $2, $3::text::timestamptz, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
            &[&f.did, &f.seq, &f.created_at, &f.prev, &f.head_cid, &f.prev_in_window, &seqs, &f.hist_removed],
        )
        .await
        .context("recording fork")?;
    }
    t.forks += forks.len() as i64;

    let days: Vec<String> = daily.keys().cloned().collect();
    let d_ops: Vec<i64> = daily.values().map(|v| v.0).collect();
    let d_creates: Vec<i64> = daily.values().map(|v| v.1).collect();
    tx.execute(
        "INSERT INTO recon_plc_daily (day, ops, creates) \
         SELECT d::date, o, c FROM UNNEST($1::text[], $2::int8[], $3::int8[]) AS t(d, o, c) \
         ON CONFLICT (day) DO UPDATE SET ops = recon_plc_daily.ops + EXCLUDED.ops, \
             creates = recon_plc_daily.creates + EXCLUDED.creates",
        &[&days, &d_ops, &d_creates],
    )
    .await?;

    let last = lines.last().expect("non-empty page");
    tx.execute(
        "INSERT INTO sync_state (source, origin, cursor, phase) VALUES ($1, $2, $3, 'backfill') \
         ON CONFLICT (source) DO UPDATE SET cursor = EXCLUDED.cursor",
        &[&SOURCE, &origin, &last.seq],
    )
    .await?;
    tx.commit().await?;

    if t.recent_live > t.recent_peak {
        t.recent_peak = t.recent_live;
        t.recent_peak_at = last.created_at.clone();
    }
    Ok(())
}

async fn flush_counters(client: &Client, pending: &mut BTreeMap<String, i64>) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let names: Vec<String> = pending.keys().cloned().collect();
    let ns: Vec<i64> = pending.values().copied().collect();
    client
        .execute(
            "INSERT INTO recon_counters (name, n) SELECT a, b FROM UNNEST($1::text[], $2::int8[]) AS t(a, b) \
             ON CONFLICT (name) DO UPDATE SET n = recon_counters.n + EXCLUDED.n",
            &[&names, &ns],
        )
        .await?;
    pending.clear();
    Ok(())
}

pub async fn backfill(client: &mut Client, opts: Opts) -> Result<()> {
    let row = client
        .query_opt("SELECT origin, cursor FROM sync_state WHERE source = $1", &[&SOURCE])
        .await?;
    let after: i64 = match row {
        Some(r) => {
            let origin: String = r.get(0);
            if origin != opts.plc_url {
                bail!("cursor belongs to {origin}, refusing to run against {}", opts.plc_url);
            }
            r.get(1)
        }
        None => 0,
    };
    let mut t = Totals::default();
    t.recent_live = client
        .query_one("SELECT count(*) FROM plc_recent_ops", &[])
        .await?
        .get(0);
    t.recent_peak = t.recent_live;
    eprintln!("starting after seq {after}; recent ops in table {}", t.recent_live);

    let window_ms = opts.window_hours * 3_600_000;
    let (tx, mut rx) = mpsc::channel::<Page>(8);
    let origin = opts.plc_url.clone();
    let metrics_path = opts.metrics_path.clone();
    let metrics_every = opts.metrics_every.max(1);
    let fetcher = tokio::spawn(async move { fetch_loop(&opts, after, tx).await });

    let started = Instant::now();
    let mut pending_counters: BTreeMap<String, i64> = BTreeMap::new();
    let mut metrics = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&metrics_path)
        .with_context(|| format!("opening {metrics_path}"))?;
    let mut at_tip = false;
    let mut window: (u64, u128, u128, u64) = (0, 0, 0, 0); // pages, net_ms, db_ms, bytes

    while let Some(page) = rx.recv().await {
        for n in &page.notes {
            if n == "tip" {
                at_tip = true;
            } else if let Some(rest) = n.strip_prefix("count ") {
                let mut it = rest.split(' ');
                if let (Some(k), Some(v)) = (it.next(), it.next()) {
                    *pending_counters.entry(k.to_owned()).or_insert(0) += v.parse::<i64>().unwrap_or(0);
                }
            } else {
                if n.starts_with("status ") {
                    t.waits += 1;
                }
                writeln!(metrics, "{}", json!({"note": n, "at_seq": t.last_seq, "wall": chrono::Utc::now().to_rfc3339()}))?;
            }
        }
        if page.lines.is_empty() {
            continue;
        }
        let db_started = Instant::now();
        apply_page(client, &origin, &page.lines, window_ms, &mut t).await?;
        let db_ms = db_started.elapsed().as_millis();

        if t.first_seq == 0 {
            t.first_seq = page.lines[0].seq;
        }
        let last = page.lines.last().expect("non-empty");
        t.last_seq = last.seq;
        t.last_created_at = last.created_at.clone();
        t.pages += 1;
        t.ops += page.lines.len() as i64;
        t.bytes += page.bytes as u64;
        t.net_ms += page.net_ms;
        t.db_ms += db_ms;
        window.0 += 1;
        window.1 += page.net_ms;
        window.2 += db_ms;
        window.3 += page.bytes as u64;

        if t.pages % 200 == 0 {
            // Prune the recovery window by operation time.
            let n = client
                .execute(
                    "DELETE FROM plc_recent_ops WHERE created_at < $1::text::timestamptz - make_interval(secs => $2::float8)",
                    &[&t.last_created_at, &((window_ms / 1000) as f64)],
                )
                .await?;
            t.recent_live -= n as i64;
            flush_counters(client, &mut pending_counters).await?;
        }
        if t.pages % metrics_every == 0 {
            let line = json!({
                "wall": chrono::Utc::now().to_rfc3339(),
                "elapsed_s": started.elapsed().as_secs(),
                "pages": t.pages,
                "ops": t.ops,
                "first_seq": t.first_seq,
                "cursor": t.last_seq,
                "op_time": t.last_created_at,
                "new_accounts": t.new_accounts,
                "hist_inserted": t.hist_inserted,
                "hist_deleted": t.hist_deleted,
                "recent_live": t.recent_live,
                "recent_peak": t.recent_peak,
                "recent_peak_at": t.recent_peak_at,
                "forks": t.forks,
                "nullified_ops": t.nullified_ops,
                "bytes": t.bytes,
                "waits": t.waits,
                "win_pages": window.0,
                "win_net_ms_avg": window.1 / window.0.max(1) as u128,
                "win_db_ms_avg": window.2 / window.0.max(1) as u128,
                "win_bytes_avg": window.3 / window.0.max(1),
            });
            writeln!(metrics, "{line}")?;
            metrics.flush()?;
            window = (0, 0, 0, 0);
        }
    }

    flush_counters(client, &mut pending_counters).await?;
    fetcher.await??;
    if at_tip {
        client
            .execute(
                "UPDATE sync_state SET phase = 'live', tip_at = now() WHERE source = $1",
                &[&SOURCE],
            )
            .await?;
    }
    let line = json!({
        "wall": chrono::Utc::now().to_rfc3339(),
        "final": true,
        "at_tip": at_tip,
        "elapsed_s": started.elapsed().as_secs(),
        "pages": t.pages,
        "ops": t.ops,
        "first_seq": t.first_seq,
        "cursor": t.last_seq,
        "op_time": t.last_created_at,
        "new_accounts": t.new_accounts,
        "hist_inserted": t.hist_inserted,
        "hist_deleted": t.hist_deleted,
        "recent_live": t.recent_live,
        "recent_peak": t.recent_peak,
        "recent_peak_at": t.recent_peak_at,
        "forks": t.forks,
        "nullified_ops": t.nullified_ops,
        "bytes": t.bytes,
        "net_ms_total": t.net_ms as u64,
        "db_ms_total": t.db_ms as u64,
        "waits": t.waits,
    });
    writeln!(metrics, "{line}")?;
    eprintln!("{line}");
    Ok(())
}
