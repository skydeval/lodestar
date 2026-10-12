-- The account and history tables, with their columns and sizes.
\pset border 1

SELECT c.relname AS table, c.reltuples::bigint AS rows_estimate,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS size,
       (SELECT string_agg(a.attname || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)
          FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS columns
FROM pg_class c
WHERE c.relkind = 'r' AND c.relnamespace = 'public'::regnamespace
  AND c.relname ~ '^(c_|accounts$|identity_history$|v_|sync_state$)'
ORDER BY 1;
