-- A random set of handles under one provider, one per line, for the
-- rate probe (no requests). Pass the sample size as `first`:
--   sql.sh rate_handles x 5000000
\pset tuples_only on
\pset format unaligned

SELECT handle_claim FROM (
    SELECT DISTINCT handle_claim
    FROM accounts TABLESAMPLE SYSTEM (12)
    WHERE handle_claim LIKE '%.bsky.social'
) AS h
ORDER BY md5(handle_claim)
LIMIT :first;
