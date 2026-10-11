-- Load the accounts primary key into memory, so a following mem_test
-- measures "index cached, table not".
\timing on
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
SELECT pg_size_pretty(pg_prewarm('c_accounts_pkey', 'read') * 8192::bigint) AS index_read_into_os_cache;
