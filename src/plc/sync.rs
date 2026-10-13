//! Following the directory: page the export from the committed cursor to
//! the tip, then take the stream from there, with the paged export as the
//! fallback and as an independent check that the tip is really the tip.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio_postgres::Client;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use super::op::{self, Op};
use super::store::{self, PdsCache, SOURCE};

#[derive(Clone)]
pub struct Opts {
    pub plc_url: String,
    pub user_agent: String,
    /// Operations a page. The directory allows at most 1,000.
    pub count: u32,
    /// Least time between two export requests while paging.
    pub min_interval: Duration,
    /// How often the export is asked for more when there is no stream.
    pub poll_interval: Duration,
    /// How often the export is asked to confirm the tip while streaming.
    pub tip_check: Duration,
    /// Take the stream once at the tip. Without it the export is polled.
    pub stream: bool,
    /// The recovery window with its margin, in hours.
    pub window_hours: i64,
    /// Stop after this many pages. 0 runs until stopped.
    pub max_pages: u64,
}

const BACKOFF_START: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Operations from the stream are applied in batches no larger and no
/// older than this.
const STREAM_BATCH: usize = 500;
const STREAM_BATCH_AGE: Duration = Duration::from_millis(250);
/// While the stream is connected and healthy the tip time is refreshed
/// this often.
const TIP_REFRESH: Duration = Duration::from_secs(15);

enum FetchError {
    /// Worth another try, after the wait the directory asked for if any.
    Transient(String, Option<Duration>),
    /// The export is not what lodestar relies on. Stop.
    Fatal(anyhow::Error),
}

/// One export request: the operations after `after`.
async fn fetch(http: &reqwest::Client, opts: &Opts, after: i64) -> Result<Vec<Op>, FetchError> {
    let url = format!("{}/export?after={after}&count={}", opts.plc_url, opts.count);
    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(|e| FetchError::Transient(e.to_string(), None))?;
    if !resp.status().is_success() {
        let asked = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        return Err(FetchError::Transient(
            format!("status {}", resp.status()),
            asked.map(Duration::from_secs),
        ));
    }
    let body = resp
        .text()
        .await
        .map_err(|e| FetchError::Transient(e.to_string(), None))?;
    let mut ops = Vec::with_capacity(opts.count as usize);
    let mut last = after;
    for line in body.lines().filter(|l| !l.is_empty()) {
        let op = op::parse_line(line)
            .with_context(|| format!("export page after {after}"))
            .map_err(FetchError::Fatal)?;
        if op.seq <= last {
            return Err(FetchError::Fatal(anyhow::anyhow!(
                "export page after {after} holds seq {} after {last}: not the order lodestar relies on",
                op.seq
            )));
        }
        last = op.seq;
        ops.push(op);
    }
    Ok(ops)
}

/// Fetch pages one after another, up to and including the first short one,
/// and hand them on. Pages already fetched wait in the channel, so the
/// next is on its way while one is applied.
async fn page_loop(
    http: reqwest::Client,
    opts: Opts,
    mut after: i64,
    tx: mpsc::Sender<Vec<Op>>,
) -> Result<()> {
    let mut backoff = BACKOFF_START;
    loop {
        let started = Instant::now();
        let ops = match fetch(&http, &opts, after).await {
            Ok(ops) => ops,
            Err(FetchError::Fatal(e)) => return Err(e),
            Err(FetchError::Transient(why, asked)) => {
                let wait = asked.map_or(backoff, |a| a.max(backoff)).min(BACKOFF_MAX);
                tracing::warn!(
                    after,
                    "export request failed: {why}; trying again in {wait:?}"
                );
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
                continue;
            }
        };
        backoff = BACKOFF_START;
        if let Some(last) = ops.last() {
            after = last.seq;
        }
        let short = ops.len() < opts.count as usize;
        if tx.send(ops).await.is_err() || short {
            return Ok(());
        }
        tokio::time::sleep(opts.min_interval.saturating_sub(started.elapsed())).await;
    }
}

enum Paged {
    Tip,
    Stop,
}

enum Streamed {
    Stop,
    /// Go back to the paged export, then try the stream again.
    Repage(String),
    /// The stream could not be opened. Poll in its place.
    Unavailable(String),
}

struct Source<'a> {
    client: &'a mut Client,
    http: reqwest::Client,
    opts: Opts,
    stop: watch::Receiver<bool>,
    pds: PdsCache,
    window_ms: i64,
    cursor: i64,
    live: bool,
    /// `createdAt` of the newest operation applied in this run.
    newest: Option<String>,
    last_prune: Instant,
    started: Instant,
    pages: u64,
    ops: u64,
    new_identities: u64,
    nullified: u64,
}

impl Source<'_> {
    async fn apply(&mut self, ops: &[Op]) -> Result<()> {
        let Some(last) = ops.last() else {
            return Ok(());
        };
        let stats = match store::apply_page(
            self.client,
            &mut self.pds,
            &self.opts.plc_url,
            ops,
            self.window_ms,
            self.live,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                self.pds.clear();
                return Err(e.context(format!("applying operations up to seq {}", last.seq)));
            }
        };
        self.cursor = last.seq;
        self.pages += 1;
        self.ops += ops.len() as u64;
        self.new_identities += stats.new_identities;
        self.nullified += stats.nullified;
        self.newest = Some(last.created_at.clone());
        // Prune by operation time, so it works the same during the first sync.
        if self.pages.is_multiple_of(200) || self.last_prune.elapsed() > Duration::from_secs(600) {
            store::prune_recent(self.client, &last.created_at, self.window_ms).await?;
            self.last_prune = Instant::now();
        }
        Ok(())
    }

    async fn record_tip(&mut self) -> Result<()> {
        self.client
            .execute(
                "INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ($1, $2, 0, 'live', now()) \
                 ON CONFLICT (source) DO UPDATE SET phase = 'live', tip_at = now()",
                &[&SOURCE, &self.opts.plc_url],
            )
            .await
            .context("recording the tip")?;
        Ok(())
    }

    /// What has to happen once, when the first sync reaches the tip.
    /// Filling the tables row by row leaves the primary key about a third
    /// larger than one built in a single pass, and the recovery-window
    /// table at the size of its peak.
    async fn finish_first_sync(&mut self) -> Result<()> {
        tracing::info!("first sync reached the tip; rebuilding the accounts key");
        self.client
            .batch_execute("REINDEX INDEX CONCURRENTLY accounts_pkey")
            .await
            .context("rebuilding the accounts key")?;
        self.client
            .batch_execute("VACUUM FULL plc_recent_ops")
            .await
            .context("rebuilding the recovery-window table")?;
        self.client
            .batch_execute("ANALYZE")
            .await
            .context("analyzing")?;
        Ok(())
    }

    fn progress(&self, what: &str) {
        tracing::info!(
            pages = self.pages,
            ops = self.ops,
            new_identities = self.new_identities,
            nullified = self.nullified,
            cursor = self.cursor,
            op_time = self.newest.as_deref().unwrap_or(""),
            elapsed_s = self.started.elapsed().as_secs(),
            "{what}"
        );
    }

    async fn page_to_tip(&mut self) -> Result<Paged> {
        let (tx, mut rx) = mpsc::channel::<Vec<Op>>(4);
        let pager = tokio::spawn(page_loop(
            self.http.clone(),
            self.opts.clone(),
            self.cursor,
            tx,
        ));
        let mut outcome = Paged::Tip;
        loop {
            let ops = tokio::select! {
                _ = self.stop.changed() => {
                    outcome = Paged::Stop;
                    break;
                }
                ops = rx.recv() => match ops {
                    Some(ops) => ops,
                    None => break,
                },
            };
            let short = ops.len() < self.opts.count as usize;
            self.apply(&ops).await?;
            if !ops.is_empty() && self.pages.is_multiple_of(500) {
                self.progress("PLC sync");
            }
            if short {
                if !self.live {
                    self.progress("PLC sync at the tip");
                    self.finish_first_sync().await?;
                    self.live = true;
                }
                self.record_tip().await?;
                break;
            }
            if self.opts.max_pages != 0 && self.pages >= self.opts.max_pages {
                outcome = Paged::Stop;
                break;
            }
        }
        drop(rx);
        pager.abort();
        match pager.await {
            Ok(result) => result?,
            Err(e) if e.is_cancelled() => {}
            Err(e) => return Err(e.into()),
        }
        Ok(outcome)
    }

    fn stream_url(&self) -> String {
        let base = match self.opts.plc_url.strip_prefix("http") {
            Some(rest) => format!("ws{rest}"),
            None => self.opts.plc_url.clone(),
        };
        format!("{base}/export/stream?cursor={}", self.cursor)
    }

    async fn stream(&mut self) -> Result<Streamed> {
        let url = self.stream_url();
        let mut request = url
            .as_str()
            .into_client_request()
            .context("building the stream request")?;
        if let Ok(v) = HeaderValue::from_str(&self.opts.user_agent) {
            request.headers_mut().insert("User-Agent", v);
        }
        let mut ws = match tokio::time::timeout(
            Duration::from_secs(20),
            tokio_tungstenite::connect_async(request),
        )
        .await
        {
            Ok(Ok((ws, _))) => ws,
            Ok(Err(e)) => return Ok(Streamed::Unavailable(e.to_string())),
            Err(_) => return Ok(Streamed::Unavailable("timed out connecting".into())),
        };
        tracing::info!(cursor = self.cursor, "PLC stream open");

        let mut buf: Vec<Op> = Vec::new();
        // The highest seq taken so far, applied or waiting in `buf`.
        let mut seen = self.cursor;
        let mut flush_at: Option<tokio::time::Instant> = None;
        // False while the export cannot confirm what the stream says.
        let mut healthy = true;
        let now = tokio::time::Instant::now();
        let mut refresh = tokio::time::interval_at(now + TIP_REFRESH, TIP_REFRESH);
        let mut check = tokio::time::interval_at(now + self.opts.tip_check, self.opts.tip_check);

        let end = loop {
            tokio::select! {
                _ = self.stop.changed() => break Streamed::Stop,
                msg = ws.next() => match msg {
                    None => break Streamed::Repage("the stream ended".into()),
                    Some(Err(e)) => break Streamed::Repage(format!("stream error: {e}")),
                    Some(Ok(Message::Text(text))) => {
                        let op = op::parse_line(text.as_str()).context("stream message")?;
                        if op.seq > seen {
                            seen = op.seq;
                            buf.push(op);
                            flush_at.get_or_insert(tokio::time::Instant::now() + STREAM_BATCH_AGE);
                        }
                        if buf.len() >= STREAM_BATCH {
                            self.apply(&std::mem::take(&mut buf)).await?;
                            flush_at = None;
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                        if reason.contains("FutureCursor") {
                            bail!("the directory says cursor {} is in its future; the cursor is kept as it is", self.cursor);
                        }
                        break Streamed::Repage(format!("the directory closed the stream: {reason}"));
                    }
                    Some(Ok(_)) => {}
                },
                _ = async { tokio::time::sleep_until(flush_at.unwrap_or(now)).await }, if flush_at.is_some() => {
                    self.apply(&std::mem::take(&mut buf)).await?;
                    flush_at = None;
                }
                _ = refresh.tick() => {
                    if healthy {
                        self.record_tip().await?;
                    }
                }
                _ = check.tick() => {
                    self.apply(&std::mem::take(&mut buf)).await?;
                    flush_at = None;
                    match fetch(&self.http, &self.opts, self.cursor).await {
                        Err(FetchError::Fatal(e)) => return Err(e),
                        Err(FetchError::Transient(why, _)) => {
                            healthy = false;
                            tracing::warn!("could not confirm the tip from the export: {why}");
                        }
                        Ok(ops) if ops.len() >= self.opts.count as usize => {
                            self.apply(&ops).await?;
                            break Streamed::Repage("the stream is a full page behind the export".into());
                        }
                        Ok(ops) => {
                            // A few here are ordinary: they are on their way down the stream.
                            self.apply(&ops).await?;
                            seen = seen.max(self.cursor);
                            healthy = true;
                            self.record_tip().await?;
                        }
                    }
                }
            }
        };
        self.apply(&buf).await?;
        Ok(end)
    }

    /// Wait out the polling interval. True if asked to stop meanwhile.
    async fn idle(&mut self, wait: Duration) -> bool {
        tokio::select! {
            _ = self.stop.changed() => true,
            _ = tokio::time::sleep(wait) => false,
        }
    }
}

pub async fn run(mut owned: Client, opts: Opts, stop: watch::Receiver<bool>) -> Result<()> {
    let client = &mut owned;
    let row = client
        .query_opt(
            "SELECT origin, cursor, phase FROM sync_state WHERE source = $1",
            &[&SOURCE],
        )
        .await?;
    let (cursor, live) = match row {
        Some(r) => {
            let origin: String = r.get(0);
            if origin != opts.plc_url {
                bail!(
                    "the stored cursor belongs to {origin}; refusing to run against {}",
                    opts.plc_url
                );
            }
            let phase: String = r.get(2);
            (r.get::<_, i64>(1), phase == "live")
        }
        None => (0, false),
    };
    tracing::info!(cursor, live, "starting the PLC source");

    let mut source = Source {
        client,
        http: reqwest::Client::builder()
            .user_agent(opts.user_agent.clone())
            .timeout(Duration::from_secs(60))
            .build()?,
        window_ms: opts.window_hours * 3_600_000,
        opts,
        stop,
        pds: PdsCache::default(),
        cursor,
        live,
        newest: None,
        last_prune: Instant::now(),
        started: Instant::now(),
        pages: 0,
        ops: 0,
        new_identities: 0,
        nullified: 0,
    };

    loop {
        match source.page_to_tip().await? {
            Paged::Stop => break,
            Paged::Tip => {}
        }
        let poll = source.opts.poll_interval;
        if !source.opts.stream {
            if source.idle(poll).await {
                break;
            }
            continue;
        }
        match source.stream().await? {
            Streamed::Stop => break,
            Streamed::Repage(why) => {
                tracing::info!("back to the paged export: {why}");
                if source.idle(Duration::from_secs(1)).await {
                    break;
                }
            }
            Streamed::Unavailable(why) => {
                tracing::warn!("the PLC stream is unavailable ({why}); polling the export");
                if source.idle(poll).await {
                    break;
                }
            }
        }
    }
    source.progress("PLC source stopped");
    Ok(())
}
