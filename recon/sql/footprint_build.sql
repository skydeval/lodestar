-- Build the indexes and variant tables the footprint report measures.
-- Run after `compact`. Long-running.
\timing on

ALTER TABLE c_accounts ADD PRIMARY KEY (did);
ALTER TABLE c_history ADD PRIMARY KEY (did, seq);

-- §4.3 as a table, text DIDs. Upper bound: every active claim verified.
DROP TABLE IF EXISTS v_handle_index;
CREATE TABLE v_handle_index AS
    SELECT DISTINCT ON (handle_claim) handle_claim AS handle, did
    FROM accounts WHERE handle_claim IS NOT NULL AND status = 0
    ORDER BY handle_claim, did;
ALTER TABLE v_handle_index ADD PRIMARY KEY (handle);

-- §4.3 as a table, binary DIDs.
DROP TABLE IF EXISTS v_handle_index_c;
CREATE TABLE v_handle_index_c AS
    SELECT DISTINCT ON (handle_claim) handle_claim AS handle, did
    FROM c_accounts WHERE handle_claim IS NOT NULL AND status = 0
    ORDER BY handle_claim, did;
ALTER TABLE v_handle_index_c ADD PRIMARY KEY (handle);

-- §4.5: the same lookup as a partial index on the accounts table.
-- Not UNIQUE here: before verification two accounts can claim one handle.
CREATE INDEX IF NOT EXISTS c_accounts_handle ON c_accounts (handle_claim)
    WHERE handle_claim IS NOT NULL AND status = 0;

-- §4.5: history rows only for accounts that have changed.
DROP TABLE IF EXISTS v_history_changed;
CREATE TABLE v_history_changed AS
    SELECT h.* FROM c_history h
    WHERE EXISTS (SELECT 1 FROM c_history x WHERE x.did = h.did AND x.seq <> h.seq);
ALTER TABLE v_history_changed ADD PRIMARY KEY (did, seq);

-- §5.4: the alternative to per-domain walkers, an index on rate key and DID.
CREATE INDEX IF NOT EXISTS c_accounts_domain ON c_accounts (domain_id, did)
    WHERE domain_id IS NOT NULL;

VACUUM (ANALYZE) accounts;
VACUUM (ANALYZE) identity_history;
VACUUM (ANALYZE) c_accounts;
VACUUM (ANALYZE) c_history;
VACUUM (ANALYZE) v_handle_index;
VACUUM (ANALYZE) v_handle_index_c;
VACUUM (ANALYZE) v_history_changed;
