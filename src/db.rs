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
    let (client, conn) = tokio_postgres::connect(url, NoTls)
        .await
        .context("connecting to postgres")?;
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
-- Every role runs this when it starts. One at a time.
SELECT pg_advisory_xact_lock(7106501);

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

-- The DID document parts of the identities whose document the stored
-- claim, PDS and key cannot rebuild: labelers' second key and service,
-- for example. As the directory gives them, JSON text. `seq` is the
-- operation they came from.
CREATE TABLE IF NOT EXISTS doc_parts (
    seq bigint NOT NULL,
    did bytea PRIMARY KEY,
    doc text NOT NULL
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
-- When a one-off pass over a source began.
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS started_at timestamptz;

-- The identities known to have a repo: those a relay lists, and those an
-- active account event names. The sweep checks these first. Kept apart
-- from `accounts` because a mark there rewrites the row and its key, and
-- a walk sets some twenty million of them; here a mark is one small row.
-- A DID may be listed before it has an account row.
CREATE TABLE IF NOT EXISTS listed (did bytea PRIMARY KEY);
-- The mark was once a column of `accounts`.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = current_schema() AND table_name = 'accounts' AND column_name = 'listed') THEN
        EXECUTE 'INSERT INTO listed (did) SELECT did FROM accounts WHERE listed ON CONFLICT DO NOTHING';
        DROP VIEW IF EXISTS accounts_readable;
        ALTER TABLE accounts DROP COLUMN listed;
    END IF;
END $$;
DROP TABLE IF EXISTS walk_listed;
-- What a relay walk in progress has seen. Appended to a page at a time
-- and merged into `listed` when the listing ends. No key: nothing is
-- looked up in it.
CREATE TABLE IF NOT EXISTS walk_seen (did bytea NOT NULL);

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
-- The registrable domain of the handle to check, where it is known. The
-- queue is read past the rows of a domain that has no room.
ALTER TABLE verify_due ADD COLUMN IF NOT EXISTS domain text;
-- The order the verification role takes work in.
DROP INDEX IF EXISTS verify_due_order;
CREATE INDEX IF NOT EXISTS verify_due_read ON verify_due (kind, reason, due_at, did) INCLUDE (domain);

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

-- The status page's totals, kept live: each row is what one statement
-- changed, written by the triggers below in the statement's own
-- transaction, so the rows always sum to the truth. Only ever inserted
-- into, so writers never wait on each other; the API role folds the rows
-- together now and then, and recounts from the tables to replace them.
-- kind: 0 accounts, by status, handle status and whether the account
-- claims a shared handle it does not hold; 1 listed DIDs.
CREATE TABLE IF NOT EXISTS live_counts (
    n             bigint NOT NULL,
    kind          smallint NOT NULL,
    status        smallint,
    handle_status smallint,
    shared        boolean
);

CREATE OR REPLACE FUNCTION lodestar_count_accounts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO live_counts (n, kind, status, handle_status, shared)
        SELECT count(*), 0, a.status, a.handle_status, s.handle IS NOT NULL
        FROM new_rows a LEFT JOIN shared_handles s ON s.handle = a.handle_claim AND s.did IS DISTINCT FROM a.did
        GROUP BY 3, 4, 5;
    ELSE
        INSERT INTO live_counts (n, kind, status, handle_status, shared)
        SELECT sum(c.n), 0, c.status, c.handle_status, s.handle IS NOT NULL
        FROM (SELECT 1 AS n, status, handle_status, handle_claim, did FROM new_rows
              UNION ALL
              SELECT -1, status, handle_status, handle_claim, did FROM old_rows) c
        LEFT JOIN shared_handles s ON s.handle = c.handle_claim AND s.did IS DISTINCT FROM c.did
        GROUP BY 3, 4, 5 HAVING sum(c.n) <> 0;
    END IF;
    RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER accounts_count_insert AFTER INSERT ON accounts
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION lodestar_count_accounts();
CREATE OR REPLACE TRIGGER accounts_count_update AFTER UPDATE ON accounts
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION lodestar_count_accounts();

CREATE OR REPLACE FUNCTION lodestar_count_listed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO live_counts (n, kind) SELECT count(*), 1 FROM new_rows HAVING count(*) > 0;
    RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER listed_count AFTER INSERT ON listed
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION lodestar_count_listed();

-- A shared handle that names a new holder: the old holder becomes one
-- claimant among the rest, and the new one stops being one. A handle
-- newly found shared moves all its claimants, which only a scan could
-- count; the recount puts that right.
CREATE OR REPLACE FUNCTION lodestar_count_holder() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO live_counts (n, kind, status, handle_status, shared)
    SELECT m.n, 0, a.status, a.handle_status, m.shared
    FROM (VALUES (OLD.did, -1::bigint, false), (OLD.did, 1::bigint, true),
                 (NEW.did, -1::bigint, true), (NEW.did, 1::bigint, false)) AS m (did, n, shared)
    JOIN accounts a ON a.did = m.did
    WHERE a.handle_claim = NEW.handle;
    RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER shared_handles_count_holder AFTER UPDATE OF did ON shared_handles
    FOR EACH ROW WHEN (OLD.did IS DISTINCT FROM NEW.did) EXECUTE FUNCTION lodestar_count_holder();

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

-- A live resolution the API role is waiting on: a handle to resolve
-- forward (kind 0) or a did:web document to fetch (kind 1). The
-- verification role does the work and writes the outcome.
-- state: 0 waiting, 1 taken, 2 answered.
CREATE TABLE IF NOT EXISTS resolve_requests (
    id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    deadline timestamptz NOT NULL,
    kind     smallint NOT NULL,
    state    smallint NOT NULL DEFAULT 0,
    subject  text NOT NULL,
    outcome  text,
    did      bytea
);

-- What each role last reported about itself, as JSON, for health and
-- metrics. The roles do not talk to each other; the API role reads this.
CREATE TABLE IF NOT EXISTS role_status (
    role       text PRIMARY KEY,
    updated_at timestamptz NOT NULL DEFAULT now(),
    status     text NOT NULL
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
       EXISTS (SELECT 1 FROM listed l WHERE l.did = a.did) AS listed,
       a.created_at
FROM accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id;

CREATE OR REPLACE VIEW identity_history_readable AS
SELECT lodestar_did(h.did) AS did, h.seq, h.changed_at, h.handle_claim, p.endpoint AS pds
FROM identity_history h LEFT JOIN pds_endpoints p ON p.id = h.pds_id;
"#;

pub async fn init(client: &Client) -> Result<()> {
    client
        .batch_execute(SCHEMA)
        .await
        .context("creating schema")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_schema_can_be_created_twice() {
        let Some(client) = crate::testdb::fresh().await else {
            return;
        };
        init(&client).await.unwrap();
    }

    #[tokio::test]
    async fn an_old_listed_column_is_moved_into_its_table() {
        let Some(client) = crate::testdb::empty().await else {
            return;
        };
        client
            .batch_execute(
                "CREATE TABLE accounts (created_at timestamptz NOT NULL, handle_checked_at timestamptz, pds_id integer, \
                 method smallint NOT NULL, status smallint NOT NULL, handle_status smallint NOT NULL, \
                 listed boolean NOT NULL DEFAULT false, did bytea PRIMARY KEY, signing_key bytea, handle_claim text); \
                 INSERT INTO accounts (created_at, method, status, handle_status, listed, did) VALUES \
                 (now(), 0, 0, 5, true, '\\x01'), (now(), 0, 0, 5, false, '\\x02');",
            )
            .await
            .unwrap();
        init(&client).await.unwrap();
        let listed: Vec<Vec<u8>> = client
            .query("SELECT did FROM listed", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(listed, vec![vec![1u8]]);
        let column: i64 = client
            .query_one(
                "SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'accounts' AND column_name = 'listed'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(column, 0);
        let readable: bool = client
            .query_one("SELECT bool_or(listed) FROM accounts_readable", &[])
            .await
            .unwrap()
            .get(0);
        assert!(readable);
    }
}
