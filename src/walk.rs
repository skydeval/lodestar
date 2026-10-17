//! The relay walk: page a relay's `listRepos` to find `did:web` accounts
//! and to mark which identities have a repo. It runs once the PLC source
//! has reached the tip, and again on an interval or when asked.
//!
//! The marks are rows in `listed`, not a column of `accounts`. A walk
//! does not look anything up as it goes: each page is appended to
//! `walk_seen`, and when the listing ends what was seen is merged into
//! `listed` in key order, in one pass. Looking each page's thousand DIDs
//! up as they arrive is a thousand scattered reads, which a host that
//! cannot hold the table in memory spends most of the walk waiting on.

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
    /// The relays a walk may use, each as `https://host`. A walk times
    /// them when it starts and uses the one that answers soonest.
    pub urls: Vec<String>,
    /// The relay this walk is using.
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
async fn fetch(
    http: &reqwest::Client,
    opts: &Opts,
    position: Option<&str>,
) -> Result<(Vec<String>, Option<String>), String> {
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
    let v: Value =
        serde_json::from_str(&body).map_err(|e| format!("the listing is not JSON: {e}"))?;
    let dids = v
        .get("repos")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|r| r.get("did").and_then(Value::as_str))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    Ok((
        dids,
        v.get("cursor").and_then(Value::as_str).map(str::to_owned),
    ))
}

#[derive(Default)]
struct Tally {
    pages: u64,
    repos: u64,
    web_found: u64,
}

/// Record the identities a page lists and queue its unseen `did:web`
/// accounts, in one transaction with the walk's position. After the last
/// page the walk moves on to merging.
async fn apply(
    client: &mut Client,
    opts: &Opts,
    dids: &[String],
    next: Option<&str>,
    tally: &mut Tally,
) -> Result<()> {
    let keys: Vec<Vec<u8>> = dids
        .iter()
        .filter(|d| d.starts_with("did:plc:") || d.starts_with("did:web:"))
        .map(|d| encoding::did_bytes(d))
        .collect();
    let web: Vec<Vec<u8>> = dids
        .iter()
        .filter(|d| d.starts_with("did:web:"))
        .map(|d| encoding::did_bytes(d))
        .collect();
    let tx = client.transaction().await?;
    tx.execute(
        "INSERT INTO walk_seen (did) SELECT unnest($1::bytea[])",
        &[&keys],
    )
    .await
    .context("recording a page of the listing")?;
    if !web.is_empty() {
        let known: HashSet<Vec<u8>> = tx
            .query("SELECT did FROM accounts WHERE did = ANY($1)", &[&web])
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        // A did:web with no row yet: its fetch carries the mark, so the row
        // is listed from the moment it is created.
        let unseen: Vec<Item> = web
            .iter()
            .filter(|d| !known.contains(*d))
            .map(|d| Item {
                did: d.as_slice(),
                kind: queue::KIND_DOCUMENT,
                listed: true,
                domain: None,
            })
            .collect();
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
                "INSERT INTO sync_state (source, origin, cursor, phase) VALUES ($1, $2, 0, 'merging') \
                 ON CONFLICT (source) DO UPDATE SET origin = EXCLUDED.origin, phase = 'merging', position = NULL",
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
async fn walk(
    client: &mut Client,
    http: &reqwest::Client,
    opts: &Opts,
    mut position: Option<String>,
    stop: &mut watch::Receiver<bool>,
) -> Result<bool> {
    if position.is_none() {
        client.batch_execute("TRUNCATE walk_seen").await?;
    }
    tracing::info!(resuming = position.is_some(), relay = %opts.url, "relay walk starting");
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
        apply(
            client,
            opts,
            &dids,
            if last { None } else { next.as_deref() },
            &mut tally,
        )
        .await?;
        if tally.pages % 1000 == 0 || last {
            tracing::info!(
                pages = tally.pages,
                repos = tally.repos,
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

/// The relay that answers a page of the listing soonest from here. Two
/// relays that serve the same listing can differ by more than a factor of
/// two, which over twenty thousand pages is hours.
async fn nearest(http: &reqwest::Client, opts: &Opts) -> String {
    let mut best: Option<(Duration, &String)> = None;
    for url in &opts.urls {
        let probe = Opts {
            url: url.clone(),
            ..opts.clone()
        };
        // The first request pays for the connection; the second is timed.
        let _ = fetch(http, &probe, None).await;
        let t0 = Instant::now();
        match fetch(http, &probe, None).await {
            Ok(_) => {
                let took = t0.elapsed();
                tracing::info!(relay = %url, ms = took.as_millis() as u64, "relay timed");
                if best.is_none_or(|(b, _)| took < b) {
                    best = Some((took, url));
                }
            }
            Err(why) => tracing::warn!(relay = %url, "relay did not answer: {why}"),
        }
    }
    // If none answered, the walk's own retries deal with it.
    best.map(|(_, url)| url.clone())
        .unwrap_or_else(|| opts.urls[0].clone())
}

/// Merge what the walk saw into `listed`, and finish the walk. Both sides
/// are read in key order, so nothing is looked up a row at a time. Safe to
/// run again if it was interrupted.
async fn merge(client: &mut Client) -> Result<()> {
    let started = Instant::now();
    let tx = client.transaction().await?;
    // A row-at-a-time plan here would be the scattered reads the walk
    // exists to avoid.
    tx.batch_execute("SET LOCAL enable_nestloop = off").await?;
    let marked = tx
        .execute(
            // A Jetstream account event can list one of these while the
            // merge runs; that one is already marked.
            "INSERT INTO listed (did) \
             SELECT s.did FROM (SELECT DISTINCT did FROM walk_seen) s \
             WHERE NOT EXISTS (SELECT 1 FROM listed l WHERE l.did = s.did) \
             ORDER BY s.did \
             ON CONFLICT (did) DO NOTHING",
            &[],
        )
        .await
        .context("merging the listing")?;
    tx.batch_execute("TRUNCATE walk_seen").await?;
    tx.execute(
        "UPDATE sync_state SET phase = 'done', position = NULL, tip_at = now() WHERE source = $1",
        &[&SOURCE],
    )
    .await?;
    tx.commit().await?;
    tracing::info!(
        marked,
        elapsed_s = started.elapsed().as_secs(),
        "relay walk merged"
    );
    Ok(())
}

pub async fn run(mut client: Client, opts: Opts, mut stop: watch::Receiver<bool>) -> Result<()> {
    if opts.urls.is_empty() {
        bail!("no relay is configured for the walk");
    }
    let http = reqwest::Client::builder()
        .user_agent(opts.user_agent.clone())
        .timeout(Duration::from_secs(60))
        .build()?;
    loop {
        // The first walk waits for the PLC source: before then it would
        // list millions of identities that have no row yet.
        let plc_live = client
            .query_opt(
                "SELECT 1 FROM sync_state WHERE source = 'plc' AND phase = 'live'",
                &[],
            )
            .await?
            .is_some();
        let row = client
            .query_opt(
                "SELECT origin, phase, position, COALESCE(tip_at < now() - make_interval(secs => $2::float8), true) \
                 FROM sync_state WHERE source = $1",
                &[&SOURCE, &(opts.every.as_secs() as f64)],
            )
            .await?;
        let mut merging = false;
        // A walk under way stays with the relay it began on: a position
        // means nothing to another relay.
        let mut began_on: Option<String> = None;
        let start: Option<Option<String>> = match row {
            None => Some(None),
            Some(r) => {
                let origin: String = r.get(0);
                let phase: String = r.get(1);
                match phase.as_str() {
                    "walking" => {
                        if !opts.urls.contains(&origin) {
                            bail!(
                                "the walk's position belongs to {origin}, which is not among the relays configured"
                            );
                        }
                        began_on = Some(origin);
                        Some(r.get(2))
                    }
                    "merging" => {
                        merging = true;
                        None
                    }
                    "done" if !r.get::<_, bool>(3) => None,
                    // Finished long enough ago, or asked for.
                    _ => Some(None),
                }
            }
        };
        if merging {
            merge(&mut client).await?;
            continue;
        }
        if let (true, Some(position)) = (plc_live, start) {
            let url = match began_on.filter(|_| position.is_some()) {
                Some(url) => url,
                None => nearest(&http, &opts).await,
            };
            let opts = Opts {
                url,
                ..opts.clone()
            };
            if !walk(&mut client, &http, &opts, position, &mut stop).await? {
                break;
            }
            merge(&mut client).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_walk_records_pages_then_merges_them() {
        let Some(mut client) = crate::testdb::fresh().await else {
            return;
        };
        let opts = Opts {
            urls: vec!["https://relay.example".into()],
            url: "https://relay.example".into(),
            user_agent: String::new(),
            min_interval: Duration::ZERO,
            every: Duration::from_secs(1),
            max_pages: 0,
        };
        let mut tally = Tally::default();
        let page1 = vec![
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "did:web:example.com".to_owned(),
        ];
        let page2 = vec![
            "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            "not a did".to_owned(),
        ];
        apply(&mut client, &opts, &page1, Some("next"), &mut tally)
            .await
            .unwrap();
        let phase: String = client
            .query_one("SELECT phase FROM sync_state WHERE source = $1", &[&SOURCE])
            .await
            .unwrap()
            .get(0);
        assert_eq!(phase, "walking");
        apply(&mut client, &opts, &page2, None, &mut tally)
            .await
            .unwrap();
        assert_eq!(tally.web_found, 1);
        merge(&mut client).await.unwrap();
        let listed: i64 = client
            .query_one("SELECT count(*) FROM listed", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(listed, 3);
        let seen: i64 = client
            .query_one("SELECT count(*) FROM walk_seen", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(seen, 0);
        let phase: String = client
            .query_one("SELECT phase FROM sync_state WHERE source = $1", &[&SOURCE])
            .await
            .unwrap()
            .get(0);
        assert_eq!(phase, "done");
        // A second walk's merge adds only what is new.
        apply(&mut client, &opts, &page1, None, &mut tally)
            .await
            .unwrap();
        merge(&mut client).await.unwrap();
        let listed: i64 = client
            .query_one("SELECT count(*) FROM listed", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(listed, 3);
    }
}
