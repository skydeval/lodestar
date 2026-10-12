//! Lodestar: an identity index for the ATProto network.

mod db;
mod encoding;
mod handle;
mod plc;

use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};

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

#[derive(Subcommand)]
enum Cmd {
    /// Create the tables. Safe to run again.
    InitDb,
    /// The ingest role: follow the PLC directory.
    Ingest {
        #[arg(long, env = "LODESTAR_PLC_URL", default_value = "https://plc.directory")]
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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cli = Cli::parse();
    let mut client = db::connect(&cli.database_url).await?;
    match cli.cmd {
        Cmd::InitDb => db::init(&client).await?,
        Cmd::Ingest {
            plc_url,
            plc_page_size,
            plc_min_interval_ms,
            plc_poll_seconds,
            plc_tip_check_seconds,
            plc_no_stream,
            plc_window_hours,
            plc_max_pages,
        } => {
            db::init(&client).await?;
            let opts = plc::sync::Opts {
                plc_url: plc_url.trim_end_matches('/').to_owned(),
                user_agent: cli.user_agent,
                count: plc_page_size.clamp(1, 1000),
                min_interval: Duration::from_millis(plc_min_interval_ms),
                poll_interval: Duration::from_secs(plc_poll_seconds),
                tip_check: Duration::from_secs(plc_tip_check_seconds.max(1)),
                stream: !plc_no_stream,
                window_hours: plc_window_hours,
                max_pages: plc_max_pages,
            };
            plc::sync::run(&mut client, opts).await?;
        }
    }
    Ok(())
}
