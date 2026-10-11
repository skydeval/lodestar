-- Results of sweep_sim.sql.
\pset border 1
SELECT count(*) AS slices_done, min(at) AS started, max(at) AS last_slice, max(at) - min(at) AS elapsed FROM recon_sweep_log;

-- Before, peak, and after.
SELECT 'before' AS point, step, pg_size_pretty(heap) AS heap, pg_size_pretty(pk) AS primary_key, pg_size_pretty(hidx) AS handle_index,
       pg_size_pretty(heap + pk + hidx) AS total, heap + pk + hidx AS total_bytes
FROM recon_sweep_log WHERE step = -1
UNION ALL
SELECT 'peak', step, pg_size_pretty(heap), pg_size_pretty(pk), pg_size_pretty(hidx), pg_size_pretty(heap + pk + hidx), heap + pk + hidx
FROM (SELECT * FROM recon_sweep_log ORDER BY heap + pk + hidx DESC LIMIT 1) p
UNION ALL
SELECT 'end', step, pg_size_pretty(heap), pg_size_pretty(pk), pg_size_pretty(hidx), pg_size_pretty(heap + pk + hidx), heap + pk + hidx
FROM (SELECT * FROM recon_sweep_log ORDER BY step DESC LIMIT 1) e;

-- Growth along the way, every 32nd slice.
SELECT step, at, pg_size_pretty(heap) AS heap, pg_size_pretty(pk) AS primary_key, pg_size_pretty(hidx) AS handle_index, dead
FROM recon_sweep_log WHERE step % 32 = 31 OR step = -1 ORDER BY step;

SELECT relname, n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count
FROM pg_stat_user_tables WHERE relname = 'c_sweep';

-- Plain accounts: as the backfill left it, against a clean copy.
SELECT c.relname, pg_size_pretty(pg_table_size(c.oid)) AS heap, pg_size_pretty(pg_indexes_size(c.oid)) AS indexes,
       pg_table_size(c.oid) AS heap_bytes, pg_indexes_size(c.oid) AS index_bytes
FROM pg_class c WHERE c.relname IN ('accounts', 't_accounts_clean') ORDER BY c.relname;

SELECT pg_size_pretty(pg_database_size('recon')) AS database;
