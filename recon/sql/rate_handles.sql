-- A random set of handles under one provider, one per line, for the
-- rate probe (no requests).
\pset tuples_only on
\pset format unaligned

SELECT handle_claim FROM (
    SELECT DISTINCT handle_claim
    FROM accounts TABLESAMPLE SYSTEM (3)
    WHERE handle_claim LIKE '%.bsky.social'
) AS h
ORDER BY md5(handle_claim)
LIMIT 1000000;
