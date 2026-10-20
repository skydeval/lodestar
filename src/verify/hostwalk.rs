//! The per-host walk: the relay's list of hosts, then each host's own
//! `listRepos`. It finds identities with a repo, and `did:web` accounts,
//! that the relay's own listing lacks: a host the relay has stopped
//! crawling still lists its repos itself.
//!
//! Every request goes to a host lodestar does not control, so the walk
//! runs here, in the verification role, behind the address guard and under
//! each domain's rate, and believes a host only so far. It marks only
//! identities lodestar already holds, queues at most [`WEB_PER_HOST`]
//! unseen `did:web` documents for one host, and reads at most
//! [`PAGES_PER_HOST`] pages of one host's listing.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::sync::watch;
use tokio_postgres::Client;

use super::check::valid_did;
use super::limiter::{self, Answer};
use super::worker::Shared;
use crate::queue::{self, Item};
use crate::{encoding, handle};

pub const SOURCE: &str = "host-walk";

#[derive(Clone)]
pub struct Opts {
    /// The relays whose list of hosts may be read, each as `https://host`.
    /// The first that answers is used.
    pub relays: Vec<String>,
    /// A finished walk is run again after this long.
    pub every: Duration,
}

/// Most pages read from one host's listing: a million repos.
const PAGES_PER_HOST: u32 = 1000;
/// Most unseen `did:web` documents one host's listing may queue a walk.
const WEB_PER_HOST: usize = 1000;
/// Largest listing page read, in bytes. A page of a thousand repos is
/// about 150 kB.
const PAGE_CAP: usize = 4 * 1024 * 1024;
/// How often a waiting walk looks at whether it is time.
const LOOK: Duration = Duration::from_secs(300);

/// The hosts a relay lists as active, as valid hostnames, sorted and
/// without repeats.
fn read_hosts(body: &[u8]) -> Result<(Vec<String>, Option<String>), &'static str> {
    let v: Value = serde_json::from_slice(body).map_err(|_| "not JSON")?;
    let hosts = v
        .get("hosts")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter(|h| {
                    h.get("status")
                        .and_then(Value::as_str)
                        .is_none_or(|s| s == "active")
                })
                .filter_map(|h| h.get("hostname").and_then(Value::as_str))
                .filter_map(|name| handle::normalize(name).filter(|n| n == name))
                .collect()
        })
        .unwrap_or_default();
    let cursor = v.get("cursor").and_then(Value::as_str).map(str::to_owned);
    Ok((hosts, cursor))
}

/// The DIDs on a page of a host's listing, and the cursor for the next.
fn read_repos(body: &[u8]) -> Result<(Vec<String>, Option<String>), &'static str> {
    let v: Value = serde_json::from_slice(body).map_err(|_| "not JSON")?;
    let dids = v
        .get("repos")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|r| r.get("did").and_then(Value::as_str))
                .filter(|d| valid_did(d))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let cursor = v.get("cursor").and_then(Value::as_str).map(str::to_owned);
    Ok((dids, cursor))
}

/// Every active host a relay lists. None if no relay answered.
async fn all_hosts(shared: &Shared, opts: &Opts) -> Option<Vec<String>> {
    for relay in &opts.relays {
        let mut hosts: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        let complete = loop {
            let mut url = match reqwest::Url::parse(&format!(
                "{relay}/xrpc/com.atproto.sync.listHosts?limit=1000"
            )) {
                Ok(u) => u,
                Err(_) => break false,
            };
            if let Some(c) = &cursor {
                url.query_pairs_mut().append_pair("cursor", c);
            }
            let page = match shared.checker.get_listing(url, PAGE_CAP).await {
                Ok(body) => read_hosts(&body),
                Err(why) => Err(why),
            };
            match page {
                Ok((page, next)) => {
                    let empty = page.is_empty();
                    hosts.extend(page);
                    match next.filter(|n| !empty && Some(n) != cursor.as_ref()) {
                        Some(n) => cursor = Some(n),
                        None => break true,
                    }
                }
                Err(why) => {
                    tracing::warn!(%relay, "the relay's list of hosts: {why}");
                    break false;
                }
            }
        };
        if complete {
            hosts.sort();
            hosts.dedup();
            return Some(hosts);
        }
    }
    None
}

#[derive(Default)]
struct Tally {
    hosts: u64,
    failed: u64,
    pages: u64,
    repos: u64,
    marked: u64,
    web_queued: u64,
}

/// Mark the identities on a page that lodestar holds, and queue unseen
/// `did:web` accounts for a document fetch, up to what `web_left` allows.
/// Returns how many were marked and queued.
async fn apply_page(client: &Client, dids: &[String], web_left: &mut usize) -> Result<(u64, u64)> {
    let keys: Vec<Vec<u8>> = dids.iter().map(|d| encoding::did_bytes(d)).collect();
    // Only identities lodestar already holds: a host can list anything.
    let marked = client
        .execute(
            "INSERT INTO listed (did) SELECT a.did FROM accounts a WHERE a.did = ANY($1) \
             ON CONFLICT (did) DO NOTHING",
            &[&keys],
        )
        .await
        .context("marking a host's repos")?;
    let web: Vec<Vec<u8>> = dids
        .iter()
        .filter(|d| d.starts_with("did:web:"))
        .map(|d| encoding::did_bytes(d))
        .collect();
    let mut queued = 0;
    if !web.is_empty() && *web_left > 0 {
        let known: HashSet<Vec<u8>> = client
            .query("SELECT did FROM accounts WHERE did = ANY($1)", &[&web])
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let unseen: Vec<Item> = web
            .iter()
            .filter(|d| !known.contains(*d))
            .take(*web_left)
            .map(|d| Item {
                did: d.as_slice(),
                kind: queue::KIND_DOCUMENT,
                listed: true,
                domain: None,
            })
            .collect();
        *web_left -= unseen.len();
        queued = queue::push(client, queue::REASON_WALK, &unseen).await?;
    }
    Ok((marked, queued))
}

/// Walk one host's listing to its end, or to [`PAGES_PER_HOST`]. False if
/// the host failed before it ended.
async fn walk_host(
    shared: &Shared,
    client: &Client,
    host: &str,
    tally: &mut Tally,
) -> Result<bool> {
    let domain = limiter::registrable(host).to_owned();
    let mut cursor: Option<String> = None;
    let mut web_left = WEB_PER_HOST;
    for _ in 0..PAGES_PER_HOST {
        let mut url = match reqwest::Url::parse(&format!(
            "https://{host}/xrpc/com.atproto.sync.listRepos?limit=1000"
        )) {
            Ok(u) => u,
            Err(_) => return Ok(false),
        };
        if let Some(c) = &cursor {
            url.query_pairs_mut().append_pair("cursor", c);
        }
        shared.gate.wait_open().await;
        let started = Instant::now();
        let body = {
            let _permit = shared.limiter.acquire(&domain).await;
            shared.checker.get_listing(url, PAGE_CAP).await
        };
        let page = body.and_then(|b| read_repos(&b));
        let strained = page.is_err() || started.elapsed() > Duration::from_secs(2);
        shared.limiter.answered(
            &domain,
            if strained {
                Answer::Strained
            } else {
                Answer::Clean
            },
        );
        let Ok((dids, next)) = page else {
            return Ok(false);
        };
        let (marked, queued) = apply_page(client, &dids, &mut web_left).await?;
        tally.pages += 1;
        tally.repos += dids.len() as u64;
        tally.marked += marked;
        tally.web_queued += queued;
        match next.filter(|n| !dids.is_empty() && Some(n) != cursor.as_ref()) {
            Some(n) => cursor = Some(n),
            None => return Ok(true),
        }
    }
    Ok(true)
}

/// One walk over every host, after `after`, the last host a stopped walk
/// finished. False if asked to stop.
async fn walk(
    shared: &Shared,
    client: &Client,
    opts: &Opts,
    after: Option<String>,
    stop: &mut watch::Receiver<bool>,
) -> Result<bool> {
    let Some(hosts) = all_hosts(shared, opts).await else {
        tracing::warn!("no relay listed its hosts; the per-host walk waits");
        return Ok(true);
    };
    tracing::info!(
        hosts = hosts.len(),
        resuming = after.is_some(),
        "per-host walk starting"
    );
    let started = Instant::now();
    let mut tally = Tally::default();
    for host in hosts
        .iter()
        .filter(|h| after.as_ref().is_none_or(|a| *h > a))
    {
        if *stop.borrow() {
            return Ok(false);
        }
        let finished = tokio::select! {
            _ = stop.changed() => return Ok(false),
            r = walk_host(shared, client, host, &mut tally) => r?,
        };
        tally.hosts += 1;
        if !finished {
            tally.failed += 1;
        }
        client
            .execute(
                "INSERT INTO sync_state (source, origin, cursor, phase, position) VALUES ($1, '', 0, 'walking', $2) \
                 ON CONFLICT (source) DO UPDATE SET phase = 'walking', position = EXCLUDED.position",
                &[&SOURCE, host],
            )
            .await
            .context("saving the per-host walk's position")?;
        if tally.hosts % 500 == 0 {
            tracing::info!(
                hosts = tally.hosts,
                failed = tally.failed,
                repos = tally.repos,
                marked = tally.marked,
                did_web_queued = tally.web_queued,
                "per-host walk"
            );
        }
    }
    client
        .execute(
            "INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ($1, '', 0, 'done', now()) \
             ON CONFLICT (source) DO UPDATE SET phase = 'done', position = NULL, tip_at = now()",
            &[&SOURCE],
        )
        .await?;
    tracing::info!(
        hosts = tally.hosts,
        failed = tally.failed,
        pages = tally.pages,
        repos = tally.repos,
        marked = tally.marked,
        did_web_queued = tally.web_queued,
        elapsed_s = started.elapsed().as_secs(),
        "per-host walk done"
    );
    Ok(true)
}

/// Walk every host once the relay walk has run, and again every `every`.
pub(super) async fn run(
    shared: Arc<Shared>,
    client: Client,
    opts: Opts,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let row = client
            .query_one(
                "SELECT (SELECT phase FROM sync_state WHERE source = $1), \
                        (SELECT position FROM sync_state WHERE source = $1), \
                        COALESCE((SELECT tip_at < now() - make_interval(secs => $2::float8) FROM sync_state WHERE source = $1), true), \
                        EXISTS (SELECT 1 FROM sync_state WHERE source = 'relay-walk' AND tip_at IS NOT NULL)",
                &[&SOURCE, &opts.every.as_secs_f64()],
            )
            .await?;
        let phase: Option<String> = row.get(0);
        let position: Option<String> = row.get(1);
        let due: bool = row.get(2);
        let relay_walked: bool = row.get(3);
        // After the relay's walk: it marks most identities, and this one
        // adds what it missed.
        if relay_walked && (phase.as_deref() == Some("walking") || due) {
            let after = position.filter(|_| phase.as_deref() == Some("walking"));
            if !walk(&shared, &client, &opts, after, &mut stop).await? {
                break;
            }
            continue;
        }
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(LOOK) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_are_active_valid_names() {
        let body = br#"{"cursor":"42","hosts":[
            {"hostname":"pds.example.com","status":"active"},
            {"hostname":"old.example.com","status":"offline"},
            {"hostname":"bare.example.net"},
            {"hostname":"localhost:2583","status":"active"},
            {"hostname":"Upper.Example.com","status":"active"},
            {"hostname":"x","status":"active"}]}"#;
        let (hosts, cursor) = read_hosts(body).unwrap();
        assert_eq!(hosts, ["pds.example.com", "bare.example.net"]);
        assert_eq!(cursor.as_deref(), Some("42"));
        assert!(read_hosts(b"not json").is_err());
    }

    #[test]
    fn repos_keep_only_dids() {
        let body = br#"{"cursor":"c2","repos":[
            {"did":"did:plc:ewvi7nxzyoun6zhxrhs64oiz","active":true},
            {"did":"did:web:example.com"},
            {"did":"not a did"},
            {"head":"no did"}]}"#;
        let (dids, cursor) = read_repos(body).unwrap();
        assert_eq!(
            dids,
            ["did:plc:ewvi7nxzyoun6zhxrhs64oiz", "did:web:example.com"]
        );
        assert_eq!(cursor.as_deref(), Some("c2"));
    }

    #[tokio::test]
    async fn a_page_marks_only_known_identities_and_caps_unseen_documents() {
        let Some(client) = crate::testdb::fresh().await else {
            return;
        };
        let known = "did:plc:ewvi7nxzyoun6zhxrhs64oiz";
        client
            .execute(
                "INSERT INTO accounts (created_at, method, status, handle_status, did) VALUES (now(), 0, 0, 5, $1)",
                &[&encoding::did_bytes(known)],
            )
            .await
            .unwrap();
        let dids: Vec<String> = [
            known,
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
            "did:web:one.example",
            "did:web:two.example",
            "did:web:three.example",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let mut web_left = 2;
        let (marked, queued) = apply_page(&client, &dids, &mut web_left).await.unwrap();
        assert_eq!(marked, 1, "only the identity lodestar holds");
        assert_eq!(queued, 2, "unseen did:web documents, up to the cap");
        assert_eq!(web_left, 0);
        let listed: i64 = client
            .query_one("SELECT count(*) FROM listed", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(listed, 1);
        // Again: nothing new is marked, and the cap is spent.
        let (marked, queued) = apply_page(&client, &dids, &mut web_left).await.unwrap();
        assert_eq!((marked, queued), (0, 0));
    }
}
