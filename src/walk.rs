//! The relay walk: page a relay's `listRepos` to find `did:web` accounts
//! and to mark which identities have a repo. It runs once the PLC source
//! has reached the tip, and again on an interval or when asked.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::sync::watch;
use tokio_postgres::Client;

use crate::encoding;
use crate::queue::{self, Item};

pub const SOURCE: &str = "relay-walk";

#[derive(Clone)]
pub struct Opts {
    /// The relay, as `https://host`.
    pub url: String,
    pub user_agent: String,
    /// Least time between two requests.
    pub min_interval: Duration,
    /// A finished walk is run again after this long.
    pub every: Duration,
    /// Stop a walk after this many pages. 0 walks to the end.
    pub max_pages: u64,
}

const BACKOFF_START: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(300);
/// How often a waiting walk looks at whether it is time.
const LOOK: Duration = Duration::from_secs(30);

/// One page of the listing: its DIDs and the cursor for the next page.
async fn fetch(http: &reqwest::Client, opts: &Opts, position: Option<&str>) -> Result<(Vec<String>, Option<String>), String> {
    let mut url = format!("{}/xrpc/com.atproto.sync.listRepos?limit=1000", opts.url);
    if let Some(p) = position {
        url.push_str("&cursor=");
        url.push_str(p);
    }
    let resp = http.get(&url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("status {}", resp.status()));
    }
    let body = resp.text().await.map_err(|e| e.to_string())?;
    let v: Value = serde_json::from_str(&body).map_err(|e| format!("the listing is not JSON: {e}"))?;
    let dids = v
        .get("repos")
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(|r| r.get("did").and_then(Value::as_str)).map(str::to_owned).collect())
        .unwrap_or_default();
    Ok((dids, v.get("cursor").and_then(Value::as_str).map(str::to_owned)))
}

#[derive(Default)]
struct Tally {
    pages: u64,
    repos: u64,
    marked: u64,
    web_found: u64,
}

/// Mark the identities a page lists and queue its unseen `did:web`
/// accounts, in one transaction with the walk's position.
async fn apply(client: &mut Client, opts: &Opts, dids: &[String], next: Option<&str>, tally: &mut Tally) -> Result<()> {
    let keys: Vec<Vec<u8>> = dids.iter().filter(|d| d.starts_with("did:plc:") || d.starts_with("did:web:")).map(|d| encoding::did_bytes(d)).collect();
    let web: Vec<Vec<u8>> = dids.iter().filter(|d| d.starts_with("did:web:")).map(|d| encoding::did_bytes(d)).collect();
    let tx = client.transaction().await?;
    // A mark for an identity with no row creates nothing.
    tally.marked += tx.execute("UPDATE accounts SET listed = true WHERE did = ANY($1) AND NOT listed", &[&keys]).await.context("marking listed")?;
    if !web.is_empty() {
        let known: HashSet<Vec<u8>> =
            tx.query("SELECT did FROM accounts WHERE did = ANY($1)", &[&web]).await?.iter().map(|r| r.get(0)).collect();
        // A did:web with no row yet: its fetch carries the mark, so the row
        // is listed from the moment it is created.
        let unseen: Vec<Item> =
            web.iter().filter(|d| !known.contains(*d)).map(|d| Item { did: d.as_slice(), kind: queue::KIND_DOCUMENT, listed: true }).collect();
        tally.web_found += queue::push(&tx, queue::REASON_WALK, &unseen).await?;
    }
    let committed = match next {
        Some(position) => {
            tx.execute(
                "INSERT INTO sync_state (source, origin, cursor, phase, position) VALUES ($1, $2, 0, 'walking', $3) \
                 ON CONFLICT (source) DO UPDATE SET origin = EXCLUDED.origin, phase = 'walking', position = EXCLUDED.position",
                &[&SOURCE, &opts.url, &position],
            )
            .await
        }
        None => {
            tx.execute(
                "INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ($1, $2, 0, 'done', now()) \
                 ON CONFLICT (source) DO UPDATE SET origin = EXCLUDED.origin, phase = 'done', position = NULL, tip_at = now()",
                &[&SOURCE, &opts.url],
            )
            .await
        }
    };
    committed.context("committing the walk's position")?;
    tx.commit().await?;
    tally.pages += 1;
    tally.repos += dids.len() as u64;
    Ok(())
}

/// One walk, from wherever the last one stopped. False if asked to stop.
async fn walk(client: &mut Client, http: &reqwest::Client, opts: &Opts, mut position: Option<String>, stop: &mut watch::Receiver<bool>) -> Result<bool> {
    tracing::info!(resuming = position.is_some(), "relay walk starting");
    let started = Instant::now();
    let mut tally = Tally::default();
    let mut backoff = BACKOFF_START;
    loop {
        let t0 = Instant::now();
        let (dids, next) = match fetch(http, opts, position.as_deref()).await {
            Ok(page) => page,
            Err(why) => {
                tracing::warn!("relay listing failed: {why}; trying again in {backoff:?}");
                tokio::select! {
                    _ = stop.changed() => return Ok(false),
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(BACKOFF_MAX);
                continue;
            }
        };
        backoff = BACKOFF_START;
        // The listing ends with a page that has no cursor, or with an empty page.
        let next = if dids.is_empty() { None } else { next };
        let last = next.is_none() || (opts.max_pages != 0 && tally.pages + 1 >= opts.max_pages);
        apply(client, opts, &dids, if last { None } else { next.as_deref() }, &mut tally).await?;
        if tally.pages % 1000 == 0 || last {
            tracing::info!(
                pages = tally.pages,
                repos = tally.repos,
                marked = tally.marked,
                did_web_queued = tally.web_found,
                elapsed_s = started.elapsed().as_secs(),
                "relay walk"
            );
        }
        if last {
            return Ok(true);
        }
        position = next;
        tokio::select! {
            _ = stop.changed() => return Ok(false),
            _ = tokio::time::sleep(opts.min_interval.saturating_sub(t0.elapsed())) => {}
        }
    }
}

pub async fn run(mut client: Client, opts: Opts, mut stop: watch::Receiver<bool>) -> Result<()> {
    let http = reqwest::Client::builder().user_agent(opts.user_agent.clone()).timeout(Duration::from_secs(60)).build()?;
    loop {
        // The first walk waits for the PLC source: before then it would
        // list millions of identities that have no row yet.
        let plc_live = client.query_opt("SELECT 1 FROM sync_state WHERE source = 'plc' AND phase = 'live'", &[]).await?.is_some();
        let row = client
            .query_opt(
                "SELECT origin, phase, position, COALESCE(tip_at < now() - make_interval(secs => $2::float8), true) \
                 FROM sync_state WHERE source = $1",
                &[&SOURCE, &(opts.every.as_secs() as f64)],
            )
            .await?;
        let start: Option<Option<String>> = match row {
            None => Some(None),
            Some(r) => {
                let origin: String = r.get(0);
                let phase: String = r.get(1);
                if origin != opts.url {
                    bail!("the walk's position belongs to {origin}; refusing to run against {}", opts.url);
                }
                match phase.as_str() {
                    "walking" => Some(r.get(2)),
                    "done" if !r.get::<_, bool>(3) => None,
                    // Finished long enough ago, or asked for.
                    _ => Some(None),
                }
            }
        };
        if let (true, Some(position)) = (plc_live, start) {
            if !walk(&mut client, &http, &opts, position, &mut stop).await? {
                break;
            }
            continue;
        }
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(LOOK) => {}
        }
    }
    tracing::info!("relay walk stopped");
    Ok(())
}
