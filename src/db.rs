//! Connection and schema.

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};

pub const METHOD_PLC: i16 = 0;

pub const STATUS_ACTIVE: i16 = 0;
pub const STATUS_TOMBSTONED: i16 = 1;

pub const HANDLE_PENDING: i16 = 4;
pub const HANDLE_NONE: i16 = 5;

pub async fn connect(url: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.context("connecting to postgres")?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("postgres connection ended: {e}");
        }
    });
    Ok(client)
}

/// method: 0 plc, 1 web.
/// status: 0 active, 1 tombstoned, 2 unresolvable.
/// handle_status: 0 verified, 1 mismatch, 2 unresolved, 3 unreachable,
/// 4 pending, 5 none.
///
/// Columns are ordered widest alignment first, so rows carry no padding.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pds_endpoints (
    id       integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    endpoint text NOT NULL
);
-- Endpoints are whatever an operation carried, of any length.
CREATE UNIQUE INDEX IF NOT EXISTS pds_endpoints_endpoint ON pds_endpoints (md5(endpoint));

CREATE TABLE IF NOT EXISTS accounts (
    created_at        timestamptz NOT NULL,
    handle_checked_at timestamptz,
    pds_id            integer,
    method            smallint NOT NULL,
    status            smallint NOT NULL,
    handle_status     smallint NOT NULL,
    listed            boolean NOT NULL DEFAULT false,
    did               bytea PRIMARY KEY,
    signing_key       bytea,
    handle_claim      text
);
-- The verified handle to DID lookup: one holder per handle.
CREATE UNIQUE INDEX IF NOT EXISTS accounts_verified_handle ON accounts (handle_claim) WHERE handle_status = 0;

CREATE TABLE IF NOT EXISTS identity_history (
    changed_at   timestamptz NOT NULL,
    seq          integer NOT NULL,
    pds_id       integer,
    did          bytea NOT NULL,
    handle_claim text,
    PRIMARY KEY (did, seq)
);

CREATE TABLE IF NOT EXISTS plc_recent_ops (
    seq        bigint NOT NULL,
    created_at timestamptz NOT NULL,
    hist_seq   integer,
    did        bytea NOT NULL,
    cid        bytea NOT NULL,
    PRIMARY KEY (did, seq)
);

CREATE TABLE IF NOT EXISTS sync_state (
    source text PRIMARY KEY,
    origin text NOT NULL,
    cursor bigint NOT NULL,
    phase  text NOT NULL,
    tip_at timestamptz
);
"#;

pub async fn init(client: &Client) -> Result<()> {
    client.batch_execute(SCHEMA).await.context("creating schema")?;
    Ok(())
}
