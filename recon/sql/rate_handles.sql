-- A random set of handles under one provider, one per line, for the
-- rate probe (no requests).
\pset tuples_only on
\pset format unaligned

SELECT DISTINCT handle_claim
FROM accounts TABLESAMPLE SYSTEM (0.1)
WHERE handle_claim LIKE '%.bsky.social'
LIMIT 20000;
