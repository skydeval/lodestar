//! Paging the directory's export from the committed cursor to the tip, and
//! then following it by polling.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use tokio_postgres::Client;

use super::op::{self, Op};
use super::store::{self, PdsCache, SOURCE};

pub struct Opts {
    pub plc_url: String,
    pub user_agent: String,
    /// Operations a page. The directory allows at most 1,000.
    pub count: u32,
    /// Least time between two requests.
    pub min_interval: Duration,
    /// How often the export is asked for more once the tip is reached.
    pub poll_interval: Duration,
    /// The recovery window with its margin, in hours.
    pub window_hours: i64,
    /// Stop after this many pages. 0 runs until stopped.
    pub max_pages: u64,
}

struct Page {
    ops: Vec<Op>,
    /// Shorter than asked for: the tip of the directory.
    short: bool,
}

const BACKOFF_START: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(300);

/// Fetch pages one after another and hand them on. Pages already fetched
/// wait in the channel, so the next is on its way while one is applied.
async fn fetch_loop(opts: &Opts, mut after: i64, tx: mpsc::Sender<Page>) -> Result<()> {
    let http = reqwest::Client::builder().user_agent(opts.user_agent.clone()).timeout(Duration::from_secs(60)).build()?;
    let mut backoff = BACKOFF_START;
    loop {
        let started = Instant::now();
        let url = format!("{}/export?after={after}&count={}", opts.plc_url, opts.count);
        let body = match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => resp.text().await.map_err(|e| e.to_string()),
            Ok(resp) => {
                let asked = resp.headers().get("retry-after").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
                if let Some(secs) = asked {
                    backoff = backoff.max(Duration::from_secs(secs).min(BACKOFF_MAX));
                }
                Err(format!("status {}", resp.status()))
            }
            Err(e) => Err(e.to_string()),
        };
        let body = match body {
            Ok(b) => b,
            Err(why) => {
                tracing::warn!(after, "export request failed: {why}; trying again in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
                continue;
            }
        };
        backoff = BACKOFF_START;

        let mut ops = Vec::with_capacity(opts.count as usize);
        for line in body.lines().filter(|l| !l.is_empty()) {
            let op = op::parse_line(line).with_context(|| format!("export page after {after}"))?;
            if op.seq <= after {
                bail!("export page after {after} holds seq {}: the directory's order is not what lodestar relies on", op.seq);
            }
            after = op.seq;
            ops.push(op);
        }
        let short = ops.len() < opts.count as usize;
        if tx.send(Page { ops, short }).await.is_err() {
            return Ok(());
        }
        let wait = if short { opts.poll_interval } else { opts.min_interval };
        tokio::time::sleep(wait.saturating_sub(started.elapsed())).await;
    }
}

/// What has to happen once, when the first sync reaches the tip. Filling
/// the tables row by row leaves the primary key about a third larger than
/// one built in a single pass, and the recovery-window table at the size
/// of its peak.
async fn finish_first_sync(client: &Client) -> Result<()> {
    tracing::info!("first sync reached the tip; rebuilding the accounts key");
    client.batch_execute("REINDEX INDEX CONCURRENTLY accounts_pkey").await.context("rebuilding the accounts key")?;
    client.batch_execute("VACUUM FULL plc_recent_ops").await.context("rebuilding the recovery-window table")?;
    client.batch_execute("ANALYZE").await.context("analyzing")?;
    Ok(())
}

pub async fn run(client: &mut Client, opts: Opts) -> Result<()> {
    let row = client.query_opt("SELECT origin, cursor, phase FROM sync_state WHERE source = $1", &[&SOURCE]).await?;
    let (after, mut live) = match row {
        Some(r) => {
            let origin: String = r.get(0);
            if origin != opts.plc_url {
                bail!("the stored cursor belongs to {origin}; refusing to run against {}", opts.plc_url);
            }
            let phase: String = r.get(2);
            (r.get::<_, i64>(1), phase == "live")
        }
        None => (0, false),
    };
    tracing::info!(after, live, "starting the PLC source");

    let window_ms = opts.window_hours * 3_600_000;
    let origin = opts.plc_url.clone();
    let max_pages = opts.max_pages;
    let (tx, mut rx) = mpsc::channel::<Page>(4);
    let fetcher = tokio::spawn(async move { fetch_loop(&opts, after, tx).await });

    let mut pds = PdsCache::default();
    let started = Instant::now();
    let (mut pages, mut ops_applied, mut new_identities, mut nullified) = (0u64, 0u64, 0u64, 0u64);
    let mut newest: Option<String> = None;
    let mut last_prune = Instant::now();
    let mut stop = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        let page = tokio::select! {
            _ = &mut stop => {
                tracing::info!("stopping the PLC source");
                break;
            }
            page = rx.recv() => match page {
                Some(p) => p,
                None => break,
            },
        };
        if let Some(last) = page.ops.last() {
            let stats = match store::apply_page(client, &mut pds, &origin, &page.ops, window_ms).await {
                Ok(s) => s,
                Err(e) => {
                    pds.clear();
                    return Err(e.context(format!("applying the page ending at seq {}", last.seq)));
                }
            };
            pages += 1;
            ops_applied += page.ops.len() as u64;
            new_identities += stats.new_identities;
            nullified += stats.nullified;
            newest = Some(last.created_at.clone());
            if pages % 500 == 0 {
                tracing::info!(
                    pages,
                    ops = ops_applied,
                    new_identities,
                    nullified,
                    cursor = last.seq,
                    op_time = %last.created_at,
                    elapsed_s = started.elapsed().as_secs(),
                    "PLC sync"
                );
            }
        }
        // Prune by operation time, so it works the same during the first sync.
        if let Some(at) = &newest {
            if pages % 200 == 0 && !page.ops.is_empty() || (page.short && last_prune.elapsed() > Duration::from_secs(600)) {
                store::prune_recent(client, at, window_ms).await?;
                last_prune = Instant::now();
            }
        }
        if page.short {
            if !live {
                finish_first_sync(client).await?;
                live = true;
            }
            client
                .execute(
                    "INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ($1, $2, 0, 'live', now()) \
                     ON CONFLICT (source) DO UPDATE SET phase = 'live', tip_at = now()",
                    &[&SOURCE, &origin],
                )
                .await
                .context("recording the tip")?;
        }
        if max_pages != 0 && pages >= max_pages {
            break;
        }
    }
    drop(rx);
    fetcher.abort();
    match fetcher.await {
        Ok(result) => result?,
        Err(e) if e.is_cancelled() => {}
        Err(e) => return Err(e.into()),
    }
    tracing::info!(pages, ops = ops_applied, new_identities, nullified, elapsed_s = started.elapsed().as_secs(), "PLC source stopped");
    Ok(())
}
