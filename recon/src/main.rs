//! Throwaway recon tooling. See README.md.

mod compact;
mod db;
mod handle;
mod jss;
mod more;
mod net;
mod plc;
mod replay;
mod verify;

use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "lodestar-recon")]
struct Cli {
    /// Postgres connection string for the recon database.
    #[arg(long, env = "RECON_DATABASE_URL")]
    database_url: String,
    /// Sent on every outbound request. Names the operator.
    #[arg(long, env = "RECON_USER_AGENT")]
    user_agent: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create the plain schema and recon bookkeeping tables.
    InitDb,
    /// Page the PLC export from the committed cursor to the tip.
    PlcBackfill {
        #[arg(long, default_value = "https://plc.directory")]
        plc_url: String,
        #[arg(long, default_value_t = 1000)]
        count: u32,
        /// Minimum milliseconds between requests.
        #[arg(long, default_value_t = 250)]
        min_interval_ms: u64,
        /// Recovery window plus margin, in hours.
        #[arg(long, default_value_t = 84)]
        window_hours: i64,
        #[arg(long)]
        metrics: String,
        #[arg(long, default_value_t = 500)]
        metrics_every: u64,
        /// Stop after this many pages. 0 runs to the tip.
        #[arg(long, default_value_t = 0)]
        max_pages: u64,
    },
    /// Connect to Jetstream once with a cursor and report what happens.
    JetstreamProbe {
        #[arg(long, default_value = "wss://jetstream.us-east.bsky.network")]
        base: String,
        #[arg(long, value_delimiter = ',', default_value = "identity,account")]
        kinds: Vec<String>,
        #[arg(long)]
        cursor: String,
    },
    /// Subscribe to Jetstream for a fixed time and record every event.
    JetstreamLive {
        #[arg(long, default_value = "wss://jetstream.us-east.bsky.network")]
        base: String,
        #[arg(long, value_delimiter = ',', default_value = "identity,account")]
        kinds: Vec<String>,
        #[arg(long)]
        seconds: u64,
        #[arg(long)]
        metrics: String,
        #[arg(long)]
        sample: String,
    },
    /// Summarise a local archive segment or bare block frame.
    JssInspect {
        #[arg(long)]
        path: String,
    },
    /// Size a Jetstream replay without downloading event data. Needs the API key.
    ReplayPlan {
        #[arg(long, default_value = "https://jetstream.us-east.bsky.network")]
        base: String,
        #[arg(long, value_delimiter = ',', default_value = "identity,account")]
        kinds: Vec<String>,
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long)]
        out: String,
        /// Plan with no kinds filter at all (every event kind).
        #[arg(long)]
        all_kinds: bool,
        /// Read each block-mode segment's index to get exact block sizes.
        #[arg(long)]
        exact_blocks: bool,
    },
    /// Ask the planner about individual DIDs and confirm with a few blocks.
    ReplayProbe {
        #[arg(long, default_value = "https://jetstream.us-east.bsky.network")]
        base: String,
        /// Also probe this many randomly chosen active did:plc accounts.
        #[arg(long, default_value_t = 100)]
        plc_sample: i64,
        /// Fetch at most this many planned blocks per DID per plan.
        #[arg(long, default_value_t = 2)]
        blocks_per_plan: u64,
        /// Stop after downloading this many bytes.
        #[arg(long, default_value_t = 50_000_000)]
        max_bytes: u64,
    },
    /// Download and decode a planned replay. Needs the API key and approval.
    ReplayRun {
        #[arg(long, default_value = "https://jetstream.us-east.bsky.network")]
        base: String,
        #[arg(long, value_delimiter = ',', default_value = "identity,account")]
        kinds: Vec<String>,
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long)]
        out: String,
        /// Run with no kinds filter and collect DIDs from every row.
        #[arg(long)]
        all_kinds: bool,
        /// Stop after downloading this many bytes. 0 means no limit.
        #[arg(long)]
        max_bytes: u64,
    },
    /// Build compact-encoded copies of accounts and history (design §4.5).
    Compact,
    /// Fetch the document of every did:web DID recon has seen.
    DidwebFetch,
    /// Compare stored state and history with the directory's audit log.
    AuditCompare {
        #[arg(long, default_value = "https://plc.directory")]
        plc_url: String,
        #[arg(long, default_value_t = 200)]
        random: i64,
        #[arg(long, default_value_t = 100)]
        forks: i64,
    },
    /// For live identity events, check the handle at once and again later.
    EventLag {
        #[arg(long, default_value = "wss://jetstream.us-east.bsky.network")]
        base: String,
        #[arg(long, default_value = "https://plc.directory")]
        plc_url: String,
        #[arg(long)]
        seconds: u64,
        /// Take at most one event per this many milliseconds.
        #[arg(long, default_value_t = 1000)]
        min_gap_ms: u64,
    },
    /// Count handle claims per registrable domain from the loaded accounts.
    DomainCounts,
    /// Draw a sample of handle claims, capped per registrable domain.
    SampleBuild {
        #[arg(long)]
        run: String,
        /// Percent of accounts to draw before capping.
        #[arg(long)]
        percent: f64,
        #[arg(long, default_value_t = 50)]
        per_domain_cap: i64,
        #[arg(long, default_value_t = 20000)]
        target: i64,
    },
    /// Check every member of a sample once.
    SampleRun {
        #[arg(long)]
        run: String,
        #[arg(long)]
        round: String,
        /// "system", or the address of a DNS server.
        #[arg(long, default_value = "system")]
        resolver: String,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        /// Minimum milliseconds between checks under one registrable domain.
        #[arg(long, default_value_t = 1000)]
        per_domain_ms: u64,
        /// Re-check only members whose result in this earlier round matched --only-results.
        #[arg(long)]
        only_round: Option<String>,
        #[arg(long, value_delimiter = ',')]
        only_results: Vec<String>,
    },
    /// Page a relay's host roster into recon_hosts.
    ListHosts {
        #[arg(long, default_value = "https://relay1.us-east.bsky.network")]
        relay: String,
    },
    /// Walk a listRepos listing, keeping did:web DIDs and status counts.
    Walk {
        #[arg(long, default_value = "https://relay1.us-east.bsky.network")]
        base: String,
        /// Name for this walk in sync_state and recon_didweb.
        #[arg(long, default_value = "relay-walk")]
        source: String,
        #[arg(long, default_value_t = 250)]
        min_interval_ms: u64,
        #[arg(long)]
        metrics: String,
        #[arg(long, default_value_t = 0)]
        max_pages: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cli = Cli::parse();
    let mut client = db::connect(&cli.database_url).await?;
    match cli.cmd {
        Cmd::InitDb => {
            db::init(&client).await?;
            net::init(&client).await?;
            verify::init(&client).await?;
            more::init(&client).await?;
        }
        Cmd::PlcBackfill { plc_url, count, min_interval_ms, window_hours, metrics, metrics_every, max_pages } => {
            let opts = plc::Opts {
                plc_url: plc_url.trim_end_matches('/').to_owned(),
                user_agent: cli.user_agent,
                count,
                min_interval: Duration::from_millis(min_interval_ms),
                window_hours,
                metrics_path: metrics,
                metrics_every,
                max_pages,
            };
            plc::backfill(&mut client, opts).await?;
        }
        Cmd::JetstreamProbe { base, kinds, cursor } => {
            net::jetstream_probe(&base, &cli.user_agent, &kinds, &cursor).await?;
        }
        Cmd::JetstreamLive { base, kinds, seconds, metrics, sample } => {
            let o = net::LiveOpts { base, user_agent: cli.user_agent, kinds, seconds, metrics_path: metrics, sample_path: sample };
            net::jetstream_live(&client, o).await?;
        }
        Cmd::JssInspect { path } => jss::inspect(&path)?,
        Cmd::ReplayPlan { base, kinds, after, out, all_kinds, exact_blocks } => {
            let kinds = if all_kinds { Vec::new() } else { kinds };
            replay::plan(&base, &cli.user_agent, kinds, after, &out, exact_blocks).await?;
        }
        Cmd::ReplayProbe { base, plc_sample, blocks_per_plan, max_bytes } => {
            replay::probe_dids(&client, &base, &cli.user_agent, plc_sample, blocks_per_plan, max_bytes).await?;
        }
        Cmd::ReplayRun { base, kinds, after, out, all_kinds, max_bytes } => {
            let kinds = if all_kinds { Vec::new() } else { kinds };
            replay::run(&client, &base, &cli.user_agent, kinds, after, &out, max_bytes).await?;
        }
        Cmd::Compact => {
            let writer = db::connect(&cli.database_url).await?;
            compact::build(&client, &writer).await?;
        }
        Cmd::DidwebFetch => more::didweb_fetch(std::sync::Arc::new(client), &cli.user_agent).await?,
        Cmd::AuditCompare { plc_url, random, forks } => {
            more::audit_compare(&client, &plc_url, &cli.user_agent, random, forks).await?;
        }
        Cmd::EventLag { base, plc_url, seconds, min_gap_ms } => {
            more::event_lag(std::sync::Arc::new(client), &base, &plc_url, &cli.user_agent, seconds, min_gap_ms).await?;
        }
        Cmd::DomainCounts => verify::domain_counts(&client).await?,
        Cmd::SampleBuild { run, percent, per_domain_cap, target } => {
            verify::sample_build(&client, &run, percent, per_domain_cap, target).await?;
        }
        Cmd::SampleRun { run, round, resolver, concurrency, per_domain_ms, only_round, only_results } => {
            let o = verify::RunOpts {
                run,
                round,
                resolver,
                user_agent: cli.user_agent,
                concurrency,
                per_domain_ms,
                only_round,
                only_results,
            };
            verify::sample_run(std::sync::Arc::new(client), o).await?;
        }
        Cmd::ListHosts { relay } => net::list_hosts(&client, &relay, &cli.user_agent).await?,
        Cmd::Walk { base, source, min_interval_ms, metrics, max_pages } => {
            let o = net::WalkOpts {
                base,
                source,
                user_agent: cli.user_agent,
                min_interval: Duration::from_millis(min_interval_ms),
                metrics_path: metrics,
                max_pages,
            };
            net::walk(&client, o).await?;
        }
    }
    Ok(())
}
