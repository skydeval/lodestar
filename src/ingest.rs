//! The ingest role: the PLC source, the Jetstream source and the relay
//! walk, each on its own database connection, stopping together.

use anyhow::Result;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::{db, jetstream, plc, walk};

pub struct Opts {
    pub database_url: String,
    pub plc: plc::sync::Opts,
    pub jetstream: Option<jetstream::Opts>,
    pub walk: Option<walk::Opts>,
}

/// A receiver that changes once, when the process is asked to stop.
pub fn stop_signal() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                match term.as_mut() {
                    Some(t) => {
                        t.recv().await;
                    }
                    None => std::future::pending().await,
                }
            } => {}
            // Every receiver is gone: nothing is left to stop.
            _ = tx.closed() => return,
        }
        tracing::info!("stopping");
        let _ = tx.send(true);
        // Keep the sender, so a receiver's `changed` does not fire again on its drop.
        tx.closed().await;
    });
    rx
}

pub async fn run(opts: Opts) -> Result<()> {
    let stop = stop_signal();
    let (halt, halted) = watch::channel(false);
    // A source that fails stops the others, so the role restarts whole.
    let either = |mut stop: watch::Receiver<bool>, mut halted: watch::Receiver<bool>| {
        let (tx, rx) = watch::channel(false);
        tokio::spawn(async move {
            tokio::select! {
                _ = stop.changed() => {}
                _ = halted.changed() => {}
            }
            let _ = tx.send(true);
            tx.closed().await;
        });
        rx
    };

    let mut tasks: JoinSet<(&'static str, Result<()>)> = JoinSet::new();
    let client = db::connect(&opts.database_url).await?;
    let s = either(stop.clone(), halted.clone());
    tasks.spawn(async move { ("PLC source", plc::sync::run(client, opts.plc, s).await) });
    if let Some(o) = opts.jetstream {
        let client = db::connect(&opts.database_url).await?;
        let s = either(stop.clone(), halted.clone());
        tasks.spawn(async move { ("Jetstream source", jetstream::run(client, o, s).await) });
    }
    if let Some(o) = opts.walk {
        let client = db::connect(&opts.database_url).await?;
        let s = either(stop.clone(), halted.clone());
        tasks.spawn(async move { ("relay walk", walk::run(client, o, s).await) });
    }

    let mut failed = None;
    while let Some(joined) = tasks.join_next().await {
        let (name, result) = joined?;
        match result {
            Ok(()) => tracing::info!("{name} finished"),
            Err(e) => {
                tracing::error!("{name} failed: {e:#}");
                failed.get_or_insert(e.context(name));
            }
        }
        // One source ending, for any reason, ends the role.
        let _ = halt.send(true);
    }
    match failed {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
