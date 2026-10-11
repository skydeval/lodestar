-- After the simulated sweep: what ordinary maintenance gives back.
\timing on
\pset border 1
VACUUM (ANALYZE, PARALLEL 0) c_sweep;
SELECT 'after vacuum' AS point, pg_table_size('c_sweep') AS heap, pg_relation_size('c_sweep_pkey') AS pk,
       pg_relation_size('c_sweep_handle') AS hidx;
REINDEX INDEX c_sweep_pkey;
REINDEX INDEX c_sweep_handle;
SELECT 'after reindex' AS point, pg_table_size('c_sweep') AS heap, pg_relation_size('c_sweep_pkey') AS pk,
       pg_relation_size('c_sweep_handle') AS hidx;
SELECT count(*) FILTER (WHERE handle_status = 0) AS verified, count(*) FILTER (WHERE handle_checked_at IS NOT NULL) AS checked,
       sum(pg_column_size(handle_checked_at)) AS checked_at_bytes
FROM c_sweep;
