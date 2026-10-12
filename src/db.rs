//! Connection and schema.

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};

pub const METHOD_PLC: i16 = 0;
pub const METHOD_WEB: i16 = 1;

pub const STATUS_ACTIVE: i16 = 0;
pub const STATUS_TOMBSTONED: i16 = 1;
pub const STATUS_UNRESOLVABLE: i16 = 2;

pub const HANDLE_VERIFIED: i16 = 0;
pub const HANDLE_MISMATCH: i16 = 1;
pub const HANDLE_UNRESOLVED: i16 = 2;
pub const HANDLE_UNREACHABLE: i16 = 3;
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
-- `createdAt` of the last operation applied. Context for consumers; how
-- far behind lodestar is comes from tip_at.
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS last_op_at timestamptz;
-- When a lap or a large domain's walker last started. A handle is due
-- when its last check is older than that.
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS lap_started_at timestamptz;
-- Where a listing walk has got to. A listing's cursor is opaque text.
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS position text;

-- Work for the verification role that should not wait for a lap: a handle
-- to check or a did:web document to fetch. One row per DID and kind of
-- work. `reason` orders the work; lower is sooner.
CREATE TABLE IF NOT EXISTS verify_due (
    due_at timestamptz NOT NULL DEFAULT now(),
    kind   smallint NOT NULL,
    reason smallint NOT NULL,
    listed boolean NOT NULL DEFAULT false,
    did    bytea NOT NULL,
    PRIMARY KEY (did, kind)
);

-- A check that is failing or awaiting confirmation and has not settled.
-- Healthy handles have no row. kind: 0 the handle, 1 the did:web document.
CREATE TABLE IF NOT EXISTS check_failures (
    first_failure_at timestamptz,
    last_attempt_at  timestamptz,
    next_attempt_at  timestamptz NOT NULL,
    nothing_at       timestamptz,
    kind             smallint NOT NULL,
    did              bytea NOT NULL,
    PRIMARY KEY (did, kind)
);
-- The verification role reads this on a timer for what has come due.
CREATE INDEX IF NOT EXISTS check_failures_next_attempt ON check_failures (next_attempt_at);

-- A handle claimed by many identities, resolved once for all of them.
-- Its claimants take their handle status from this row: verified if it
-- names them, mismatch if it names another, else `outcome`.
CREATE TABLE IF NOT EXISTS shared_handles (
    checked_at       timestamptz,
    first_failure_at timestamptz,
    last_attempt_at  timestamptz,
    next_attempt_at  timestamptz,
    nothing_at       timestamptz,
    early_refresh_at timestamptz,
    outcome          smallint NOT NULL,
    refresh_asked    boolean NOT NULL DEFAULT false,
    handle           text PRIMARY KEY,
    did              bytea
);

-- What lodestar remembers about each control check, so a restart changes
-- nothing: since when it has failed every round, and whether it has been
-- left out of the count.
CREATE TABLE IF NOT EXISTS control_state (
    handle        text NOT NULL,
    https         boolean NOT NULL,
    failing_since timestamptz,
    rounds        bigint NOT NULL DEFAULT 0,
    other_passed  bigint NOT NULL DEFAULT 0,
    left_out      boolean NOT NULL DEFAULT false,
    last          boolean,
    PRIMARY KEY (handle, https)
);

-- When each known did:web document was last fetched, and when it first
-- and last resolved.
CREATE TABLE IF NOT EXISTS web_documents (
    last_fetch_at    timestamptz NOT NULL,
    first_success_at timestamptz,
    last_success_at  timestamptz,
    did              bytea PRIMARY KEY
);

-- The readable view. The tables hold DIDs, keys and endpoints in compact
-- form; these present them as text for an operator looking by hand.
CREATE OR REPLACE FUNCTION lodestar_b32(b bytea) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    alphabet constant text := 'abcdefghijklmnopqrstuvwxyz234567';
    acc bigint := 0;
    bits integer := 0;
    result text := '';
BEGIN
    FOR i IN 0 .. octet_length(b) - 1 LOOP
        acc := (acc << 8) | get_byte(b, i);
        bits := bits + 8;
        WHILE bits >= 5 LOOP
            bits := bits - 5;
            result := result || substr(alphabet, ((acc >> bits) & 31)::integer + 1, 1);
        END LOOP;
        acc := acc & ((1::bigint << bits) - 1);
    END LOOP;
    IF bits > 0 THEN
        result := result || substr(alphabet, ((acc << (5 - bits)) & 31)::integer + 1, 1);
    END IF;
    RETURN result;
END $$;

CREATE OR REPLACE FUNCTION lodestar_b58(b bytea) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    alphabet constant text := '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
    n numeric := 0;
    result text := '';
BEGIN
    FOR i IN 0 .. octet_length(b) - 1 LOOP
        n := n * 256 + get_byte(b, i);
    END LOOP;
    WHILE n > 0 LOOP
        result := substr(alphabet, (n % 58)::integer + 1, 1) || result;
        n := div(n, 58);
    END LOOP;
    FOR i IN 0 .. octet_length(b) - 1 LOOP
        EXIT WHEN get_byte(b, i) <> 0;
        result := '1' || result;
    END LOOP;
    RETURN result;
END $$;

CREATE OR REPLACE FUNCTION lodestar_did(b bytea) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE get_byte(b, 0)
        WHEN 0 THEN 'did:plc:' || lodestar_b32(substring(b FROM 2))
        ELSE convert_from(substring(b FROM 2), 'UTF8')
    END
$$;

CREATE OR REPLACE FUNCTION lodestar_key(b bytea) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE get_byte(b, 0)
        WHEN 0 THEN convert_from(substring(b FROM 2), 'UTF8')
        ELSE 'did:key:z' || lodestar_b58(b)
    END
$$;

CREATE OR REPLACE VIEW accounts_readable AS
SELECT lodestar_did(a.did) AS did,
       CASE a.method WHEN 0 THEN 'plc' ELSE 'web' END AS method,
       p.endpoint AS pds,
       lodestar_key(a.signing_key) AS signing_key,
       a.handle_claim,
       CASE a.handle_status WHEN 0 THEN 'verified' WHEN 1 THEN 'mismatch' WHEN 2 THEN 'unresolved'
            WHEN 3 THEN 'unreachable' WHEN 4 THEN 'pending' ELSE 'none' END AS handle_status,
       a.handle_checked_at,
       CASE a.status WHEN 0 THEN 'active' WHEN 1 THEN 'tombstoned' ELSE 'unresolvable' END AS status,
       a.listed,
       a.created_at
FROM accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id;

CREATE OR REPLACE VIEW identity_history_readable AS
SELECT lodestar_did(h.did) AS did, h.seq, h.changed_at, h.handle_claim, p.endpoint AS pds
FROM identity_history h LEFT JOIN pds_endpoints p ON p.id = h.pds_id;
"#;

pub async fn init(client: &Client) -> Result<()> {
    client.batch_execute(SCHEMA).await.context("creating schema")?;
    Ok(())
}
