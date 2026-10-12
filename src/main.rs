//! Lodestar: an identity index for the ATProto network.

mod api;
mod db;
mod encoding;
mod handle;
mod ingest;
mod jetstream;
mod plc;
mod queue;
mod verify;
mod walk;

use std::time::Duration;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "lodestar", version)]
struct Cli {
    /// Postgres connection string.
    #[arg(long, env = "LODESTAR_DATABASE_URL")]
    database_url: String,
    /// Sent on every outbound request. Should name the operator.
    #[arg(long, env = "LODESTAR_USER_AGENT")]
    user_agent: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args)]
struct IngestArgs {
    #[arg(
        long,
        env = "LODESTAR_PLC_URL",
        default_value = "https://plc.directory"
    )]
    plc_url: String,
    /// Operations a page, at most 1,000.
    #[arg(long, default_value_t = 1000)]
    plc_page_size: u32,
    /// Least time between export requests during the first sync.
    #[arg(long, default_value_t = 250)]
    plc_min_interval_ms: u64,
    /// How often the export is asked for more when there is no stream.
    #[arg(long, default_value_t = 5)]
    plc_poll_seconds: u64,
    /// How often the export is asked to confirm the tip while streaming.
    #[arg(long, default_value_t = 120)]
    plc_tip_check_seconds: u64,
    /// Poll the export at the tip instead of taking the stream.
    #[arg(long)]
    plc_no_stream: bool,
    /// The recovery window with its margin, in hours.
    #[arg(long, default_value_t = 84)]
    plc_window_hours: i64,
    /// Stop after this many pages. 0 runs until stopped.
    #[arg(long, default_value_t = 0)]
    plc_max_pages: u64,

    /// The Jetstream instance.
    #[arg(
        long,
        env = "LODESTAR_JETSTREAM_URL",
        default_value = "wss://jetstream.us-east.bsky.network"
    )]
    jetstream_url: String,
    /// Reconnect when the subscription has been silent this long.
    #[arg(long, default_value_t = 300)]
    jetstream_quiet_seconds: u64,
    /// Most events that may wait to be acted on. More are dropped.
    #[arg(long, default_value_t = 10_000)]
    jetstream_event_cap: i64,
    /// Run without the Jetstream source.
    #[arg(long)]
    no_jetstream: bool,

    /// The relay whose listing is walked.
    #[arg(
        long,
        env = "LODESTAR_RELAY_URL",
        default_value = "https://relay1.us-east.bsky.network"
    )]
    relay_url: String,
    /// Least time between listing requests.
    #[arg(long, default_value_t = 250)]
    walk_min_interval_ms: u64,
    /// A finished walk is run again after this many days.
    #[arg(long, default_value_t = 7)]
    walk_every_days: u64,
    /// Stop a walk after this many pages. 0 walks to the end.
    #[arg(long, default_value_t = 0)]
    walk_max_pages: u64,
    /// Run without the relay walk.
    #[arg(long)]
    no_walk: bool,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create the tables. Safe to run again.
    InitDb,
    /// The ingest role: follow the PLC directory and Jetstream, and walk
    /// the relay's listing.
    Ingest(Box<IngestArgs>),
    /// Resolve handles once and print what each resolves to. Uses no
    /// database.
    CheckHandle {
        handles: Vec<String>,
        /// `system`, or a resolver's address.
        #[arg(long, default_value = "system")]
        resolver: String,
    },
    /// Check the handles of the named accounts now and store the result.
    CheckAccount {
        dids: Vec<String>,
        #[arg(long, default_value = "system")]
        resolver: String,
    },
    /// The verification role: check handles. The only role that makes
    /// requests to arbitrary hosts.
    Verify {
        /// `system`, or a resolver's address.
        #[arg(long, env = "LODESTAR_RESOLVER", default_value = "system")]
        resolver: String,
        /// Database connections used to apply results.
        #[arg(long, default_value_t = 8)]
        connections: usize,
        /// A handle claimed by more identities than this is resolved
        /// once, for all of them. At most 254.
        #[arg(long, default_value_t = 100)]
        shared_threshold: u8,
        /// A shared handle is refreshed early no more often than this.
        #[arg(long, default_value_t = 60)]
        shared_refresh_minutes: u64,
        /// Domains with so many handles that each gets its own walker.
        #[arg(long, value_delimiter = ',', default_value = "bsky.social")]
        large_domains: Vec<String>,
        /// How much of the accounts table a lap reads at a time, in blocks.
        #[arg(long, default_value_t = 500)]
        lap_chunk_blocks: i64,
        /// A control check, as dns:handle=did or https:handle=did. At
        /// least two of each method are needed, each on its own domain.
        #[arg(long = "control", env = "LODESTAR_CONTROLS", value_delimiter = ',')]
        controls: Vec<String>,
        /// Run with no control checks. Lodestar then cannot tell its own
        /// outage from the network's.
        #[arg(long)]
        unsafe_no_controls: bool,
        /// When a control pause starts, results this recent are checked again.
        #[arg(long, default_value_t = 600)]
        lookback_seconds: u64,
        /// The starting values for rates and caps: vps or hobbyist.
        #[arg(long, env = "LODESTAR_PROFILE", default_value = "vps", value_parser = ["vps", "hobbyist"])]
        profile: String,
        /// A domain's rate and in-flight cap, as domain=rate:in_flight.
        #[arg(long = "rate", env = "LODESTAR_RATES", value_delimiter = ',')]
        rates: Vec<String>,
    },
    /// The API role: serve lookups.
    Api {
        #[arg(long, env = "LODESTAR_LISTEN", default_value = "127.0.0.1:8080")]
        listen: String,
        /// Database connections to hold.
        #[arg(long, default_value_t = 4)]
        connections: usize,
        /// The PLC source counts as fresh while its last tip confirmation
        /// is no older than this.
        #[arg(long, default_value_t = 300)]
        plc_fresh_seconds: i64,
        /// Most DIDs in one batch lookup.
        #[arg(long, default_value_t = 1000)]
        max_batch: usize,
        /// How long a request waits on a live resolution, in seconds.
        #[arg(long, default_value_t = 5)]
        live_deadline_seconds: u64,
        /// Most checks consumers may have waiting, per kind of request.
        #[arg(long, default_value_t = 10_000)]
        consumer_queue_cap: i64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cli = Cli::parse();
    if let Cmd::CheckHandle { handles, resolver } = &cli.cmd {
        let checker = verify::check::Checker::new(
            resolver,
            &cli.user_agent,
            &verify::check::Limits::default(),
        )?;
        for h in handles {
            match handle::normalize(h) {
                None => println!("{h}: not a valid handle"),
                Some(h) if handle::never_resolves(&h) => println!("{h}: never resolved"),
                Some(h) => {
                    let c = checker.check(&h).await;
                    println!(
                        "{h}: {:?} by_dns={} retry_after={:?} took={:?}",
                        c.outcome, c.by_dns, c.retry_after, c.took
                    );
                }
            }
        }
        return Ok(());
    }
    let client = db::connect(&cli.database_url).await?;
    db::init(&client).await?;
    match cli.cmd {
        Cmd::InitDb => {}
        Cmd::CheckHandle { .. } => unreachable!("handled before the database connection"),
        Cmd::CheckAccount { dids, resolver } => {
            let checker = verify::check::Checker::new(
                &resolver,
                &cli.user_agent,
                &verify::check::Limits::default(),
            )?;
            let mut client = client;
            verify::check_accounts(&mut client, &checker, &dids).await?;
        }
        Cmd::Ingest(a) => {
            let opts = ingest::Opts {
                database_url: cli.database_url,
                plc: plc::sync::Opts {
                    plc_url: a.plc_url.trim_end_matches('/').to_owned(),
                    user_agent: cli.user_agent.clone(),
                    count: a.plc_page_size.clamp(1, 1000),
                    min_interval: Duration::from_millis(a.plc_min_interval_ms),
                    poll_interval: Duration::from_secs(a.plc_poll_seconds),
                    tip_check: Duration::from_secs(a.plc_tip_check_seconds.max(1)),
                    stream: !a.plc_no_stream,
                    window_hours: a.plc_window_hours,
                    max_pages: a.plc_max_pages,
                },
                jetstream: (!a.no_jetstream).then(|| jetstream::Opts {
                    url: a.jetstream_url.trim_end_matches('/').to_owned(),
                    user_agent: cli.user_agent.clone(),
                    quiet: Duration::from_secs(a.jetstream_quiet_seconds.max(60)),
                    event_cap: a.jetstream_event_cap,
                }),
                walk: (!a.no_walk).then(|| walk::Opts {
                    url: a.relay_url.trim_end_matches('/').to_owned(),
                    user_agent: cli.user_agent.clone(),
                    min_interval: Duration::from_millis(a.walk_min_interval_ms),
                    every: Duration::from_secs(a.walk_every_days.max(1) * 86_400),
                    max_pages: a.walk_max_pages,
                }),
            };
            drop(client);
            ingest::run(opts).await?;
        }
        Cmd::Verify {
            resolver,
            connections,
            shared_threshold,
            shared_refresh_minutes,
            large_domains,
            lap_chunk_blocks,
            controls,
            unsafe_no_controls,
            lookback_seconds,
            profile,
            rates,
        } => {
            // An empty setting in the environment is no setting.
            let rates: Vec<String> = rates.into_iter().filter(|r| !r.trim().is_empty()).collect();
            let controls: Vec<String> = controls
                .into_iter()
                .filter(|c| !c.trim().is_empty())
                .collect();
            let limits = verify::limiter::Limits::profile(&profile).with_overrides(&rates)?;
            let controls = controls
                .iter()
                .map(|c| verify::control::parse(c))
                .collect::<Result<Vec<_>>>()?;
            if unsafe_no_controls {
                anyhow::ensure!(
                    controls.is_empty(),
                    "--unsafe-no-controls and --control cannot both be given"
                );
            } else {
                verify::control::validate(&controls)?;
            }
            drop(client);
            let opts = verify::worker::Opts {
                database_url: cli.database_url,
                user_agent: cli.user_agent,
                resolver,
                connections,
                shared_threshold: shared_threshold.min(254),
                early_refresh_floor: Duration::from_secs(shared_refresh_minutes * 60),
                large_domains,
                lap_chunk_blocks: lap_chunk_blocks.max(1),
                controls,
                lookback: Duration::from_secs(lookback_seconds),
                limits,
            };
            verify::worker::run(opts, ingest::stop_signal()).await?;
        }
        Cmd::Api {
            listen,
            connections,
            plc_fresh_seconds,
            max_batch,
            live_deadline_seconds,
            consumer_queue_cap,
        } => {
            drop(client);
            let opts = api::Opts {
                database_url: cli.database_url,
                listen,
                connections,
                fresh_seconds: plc_fresh_seconds,
                max_batch,
                live_deadline: Duration::from_secs(live_deadline_seconds.max(1)),
                queue_cap: consumer_queue_cap,
            };
            api::run(opts).await?;
        }
    }
    Ok(())
}
