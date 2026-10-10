-- Current size of the loaded tables, for progress checks during the backfill.
\pset border 1
SELECT c.relname, pg_size_pretty(pg_table_size(c.oid)) AS heap, pg_size_pretty(pg_indexes_size(c.oid)) AS indexes,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total, c.reltuples::bigint AS est_rows
FROM pg_class c WHERE c.relkind = 'r' AND c.relname IN ('accounts', 'identity_history', 'plc_recent_ops')
ORDER BY pg_total_relation_size(c.oid) DESC;
SELECT pg_size_pretty(pg_database_size('recon')) AS database,
       pg_size_pretty((SELECT sum(size) FROM pg_ls_waldir())) AS wal;
