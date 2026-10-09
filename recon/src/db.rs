//! Connection and the plain §4 schema, plus recon bookkeeping tables.

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};

pub async fn connect(url: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(url, NoTls)
        .await
        .context("connecting to postgres")?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("postgres connection ended: {e}");
        }
    });
    Ok(client)
}

/// handle_status: 0 verified, 1 mismatch, 2 unresolved, 3 pending, 4 none.
/// status: 0 active, 1 tombstoned, 2 unresolvable.
/// method: 0 plc, 1 web.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS accounts (
    did                  text PRIMARY KEY,
    method               smallint NOT NULL,
    pds_endpoint         text,
    signing_key          text,
    handle_claim         text,
    handle_status        smallint NOT NULL,
    handle_checked_at    timestamptz,
    handle_failing_since timestamptz,
    status               smallint NOT NULL,
    created_at           timestamptz NOT NULL,
    updated_at           timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS identity_history (
    did          text NOT NULL,
    seq          integer NOT NULL,
    changed_at   timestamptz NOT NULL,
    handle_claim text,
    pds_endpoint text,
    PRIMARY KEY (did, seq)
);

CREATE TABLE IF NOT EXISTS plc_recent_ops (
    did        text NOT NULL,
    seq        bigint NOT NULL,
    cid        text NOT NULL,
    created_at timestamptz NOT NULL,
    hist_seq   integer,
    PRIMARY KEY (did, seq)
);
CREATE INDEX IF NOT EXISTS plc_recent_ops_created_at ON plc_recent_ops (created_at);

CREATE TABLE IF NOT EXISTS sync_state (
    source     text PRIMARY KEY,
    origin     text NOT NULL,
    cursor     bigint NOT NULL,
    phase      text NOT NULL,
    tip_at     timestamptz
);

-- Recon bookkeeping. Not part of the design's model.
CREATE TABLE IF NOT EXISTS recon_forks (
    did             text NOT NULL,
    seq             bigint NOT NULL,
    created_at      timestamptz NOT NULL,
    prev            text,
    head_cid        text NOT NULL,
    prev_in_window  boolean NOT NULL,
    nullified_seqs  text NOT NULL,
    hist_rows_removed integer NOT NULL,
    PRIMARY KEY (did, seq)
);

CREATE TABLE IF NOT EXISTS recon_plc_daily (
    day      date PRIMARY KEY,
    ops      bigint NOT NULL,
    creates  bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS recon_counters (
    name text PRIMARY KEY,
    n    bigint NOT NULL
);
"#;

pub async fn init(client: &Client) -> Result<()> {
    client.batch_execute(SCHEMA).await.context("creating schema")?;
    Ok(())
}
