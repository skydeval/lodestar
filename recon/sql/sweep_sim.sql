-- Simulate the first handle sweep on a copy of the compact accounts table,
-- to measure how much the table and its indexes grow while it runs.
--
-- The sweep updates every row that has a handle claim: it sets a status and
-- a checked-at time. Rows are visited in primary-key order, 256 slices, one
-- transaction a slice, with autovacuum left at its defaults. A real sweep
-- takes weeks, so cleanup keeps up better than it can here: this run is the
-- pessimistic case.
\timing on
\pset border 1

DROP TABLE IF EXISTS c_sweep;
DROP TABLE IF EXISTS recon_sweep_log;

CREATE TABLE c_sweep (LIKE c_accounts);
INSERT INTO c_sweep SELECT * FROM c_accounts;
ALTER TABLE c_sweep ADD PRIMARY KEY (did);
-- The verified-handle index as lodestar would have it: empty before the sweep.
CREATE INDEX c_sweep_handle ON c_sweep (handle_claim) WHERE handle_status = 0;
VACUUM (ANALYZE, PARALLEL 0) c_sweep;

CREATE TABLE recon_sweep_log (
    step  integer PRIMARY KEY,
    at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    heap  bigint NOT NULL,
    pk    bigint NOT NULL,
    hidx  bigint NOT NULL,
    dead  bigint
);
INSERT INTO recon_sweep_log (step, heap, pk, hidx, dead)
SELECT -1, pg_table_size('c_sweep'), pg_relation_size('c_sweep_pkey'), pg_relation_size('c_sweep_handle'),
       (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'c_sweep');

DO $$
DECLARE
    i integer;
    lo bytea;
    hi bytea;
    pad bytea := decode(repeat('00', 14), 'hex');
BEGIN
    FOR i IN 0..255 LOOP
        lo := '\x00'::bytea || set_byte('\x00'::bytea, 0, i) || pad;
        IF i < 255 THEN
            hi := '\x00'::bytea || set_byte('\x00'::bytea, 0, i + 1) || pad;
        ELSE
            hi := '\x01'::bytea;
        END IF;
        UPDATE c_sweep
        SET handle_checked_at = clock_timestamp(),
            handle_status = CASE
                -- the two handles claimed in bulk: one result, everyone else mismatches
                WHEN handle_claim = 'donald.trump.com' OR handle_claim LIKE '%plc.surge.sh' THEN 1
                -- 57.8% verified, as in the sample
                WHEN get_byte(did, 15) < 148 THEN 0
                ELSE 2
            END
        WHERE did >= lo AND did < hi AND handle_claim IS NOT NULL AND status = 0;
        INSERT INTO recon_sweep_log (step, heap, pk, hidx, dead)
        SELECT i, pg_table_size('c_sweep'), pg_relation_size('c_sweep_pkey'), pg_relation_size('c_sweep_handle'),
               (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'c_sweep');
        COMMIT;
    END LOOP;
END $$;

-- What a clean copy of the plain accounts table weighs, against the one the
-- row-by-row backfill left behind. The difference is the sync path's bloat.
DROP TABLE IF EXISTS t_accounts_clean;
CREATE TABLE t_accounts_clean AS SELECT * FROM accounts;
ALTER TABLE t_accounts_clean ADD PRIMARY KEY (did);
