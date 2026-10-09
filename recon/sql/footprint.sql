-- Footprint report: plain §4 schema and each §4.5 candidate.
\pset border 1

SELECT now() AS measured_at, version();

-- Row counts.
SELECT (SELECT count(*) FROM accounts) AS accounts,
       (SELECT count(*) FROM accounts WHERE status = 1) AS tombstoned,
       (SELECT count(*) FROM accounts WHERE handle_claim IS NOT NULL AND status = 0) AS active_with_claim,
       (SELECT count(*) FROM identity_history) AS history_rows,
       (SELECT count(*) FROM v_history_changed) AS history_rows_changed_only,
       (SELECT count(*) FROM c_pds) AS distinct_pds_endpoints,
       (SELECT count(*) FROM c_domains) AS registrable_domains;

-- History rows per account.
SELECT rows_per_account, count(*) AS accounts
FROM (SELECT LEAST(count(*), 6) AS rows_per_account FROM c_history GROUP BY did) t
GROUP BY 1 ORDER BY 1;

-- Relation sizes: heap, indexes, total. Bytes and pretty.
SELECT c.relname,
       pg_table_size(c.oid) AS heap_bytes,
       pg_indexes_size(c.oid) AS index_bytes,
       pg_total_relation_size(c.oid) AS total_bytes,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total
FROM pg_class c
WHERE c.relkind = 'r'
  AND c.relname IN ('accounts', 'identity_history', 'plc_recent_ops', 'c_accounts', 'c_history', 'c_pds', 'c_domains',
                    'v_handle_index', 'v_handle_index_c', 'v_history_changed')
ORDER BY total_bytes DESC;

-- Individual indexes.
SELECT indexrelname, pg_relation_size(indexrelid) AS bytes, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE relname IN ('accounts', 'identity_history', 'c_accounts', 'c_history', 'v_handle_index', 'v_handle_index_c',
                  'v_history_changed', 'plc_recent_ops')
ORDER BY bytes DESC;

-- Column bytes, plain accounts.
SELECT sum(pg_column_size(did)) AS did, sum(pg_column_size(pds_endpoint)) AS pds, sum(pg_column_size(signing_key)) AS key,
       sum(pg_column_size(handle_claim)) AS handle, sum(pg_column_size(updated_at)) AS updated_at,
       sum(pg_column_size(created_at)) AS created_at
FROM accounts;

-- Column bytes, compact accounts.
SELECT sum(pg_column_size(did)) AS did, sum(pg_column_size(pds_id)) AS pds, sum(pg_column_size(signing_key)) AS key,
       sum(pg_column_size(handle_claim)) AS handle, sum(pg_column_size(domain_id)) AS domain_id,
       sum(pg_column_size(updated_at)) AS updated_at
FROM c_accounts;

-- Column bytes, history, plain then compact.
SELECT sum(pg_column_size(did)) AS did, sum(pg_column_size(pds_endpoint)) AS pds, sum(pg_column_size(handle_claim)) AS handle
FROM identity_history;
SELECT sum(pg_column_size(did)) AS did, sum(pg_column_size(pds_id)) AS pds, sum(pg_column_size(handle_claim)) AS handle
FROM c_history;

-- Whole database and WAL.
SELECT pg_database_size('recon') AS db_bytes, pg_size_pretty(pg_database_size('recon')) AS db,
       (SELECT sum(size) FROM pg_ls_waldir()) AS wal_bytes,
       pg_size_pretty((SELECT sum(size) FROM pg_ls_waldir())) AS wal;
