-- Timings on the compact tables: lap passes, per-domain reads, batch lookups.
-- Hardware-dependent: these are the recon host's numbers.
\pset border 1
\timing on
SET max_parallel_workers_per_gather = 0;

-- Distinct handles (v_handle_index holds one row per distinct active claim).
SELECT count(*) AS distinct_handles FROM v_handle_index;
SELECT count(*) AS distinct_under_bsky_social FROM v_handle_index WHERE handle LIKE '%.bsky.social';

-- One sequential lap over accounts, reading what a walker reads.
SELECT count(*) FILTER (WHERE handle_status = 3) AS pending, count(*) AS accounts FROM c_accounts;

-- One domain's accounts through the (domain_id, did) index: a mid-sized domain.
SELECT count(*) AS accounts_in_domain FROM c_accounts
WHERE domain_id = (SELECT id FROM c_domains WHERE domain = 'brid.gy');

-- The same domain by sequential scan, as a walker without the index would.
SET enable_indexscan = off; SET enable_bitmapscan = off; SET enable_indexonlyscan = off;
SELECT count(*) AS accounts_in_domain FROM c_accounts
WHERE domain_id = (SELECT id FROM c_domains WHERE domain = 'brid.gy');
RESET enable_indexscan; RESET enable_bitmapscan; RESET enable_indexonlyscan;

-- Batch lookups by primary key: 1,000 DIDs spread over the table, three times.
CREATE TEMP TABLE batch AS SELECT did FROM c_accounts TABLESAMPLE SYSTEM (0.01) ORDER BY random() LIMIT 1000;
SELECT count(*) FROM c_accounts a JOIN batch b USING (did);
CREATE TEMP TABLE batch2 AS SELECT did FROM c_accounts TABLESAMPLE SYSTEM (0.01) ORDER BY random() LIMIT 1000;
SELECT count(*) FROM c_accounts a JOIN batch2 b USING (did);
CREATE TEMP TABLE batch3 AS SELECT did FROM c_accounts TABLESAMPLE SYSTEM (0.01) ORDER BY random() LIMIT 1000;
SELECT count(*), count(h.did) AS with_history FROM c_accounts a JOIN batch3 b USING (did) LEFT JOIN c_history h ON h.did = a.did AND h.seq = 0;

-- Exact handle lookup through the partial index.
SELECT count(*) FROM c_accounts WHERE handle_claim = 'bsky.app' AND status = 0;

SELECT pg_size_pretty(pg_relation_size('c_accounts_pkey')) AS accounts_pk,
       pg_size_pretty(pg_relation_size('c_accounts_handle')) AS handle_index,
       pg_size_pretty(pg_relation_size('c_history_pkey')) AS history_pk,
       current_setting('shared_buffers') AS shared_buffers;
