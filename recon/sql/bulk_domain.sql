-- What the accounts under the two bulk-claimed domains look like.
\pset border 1

WITH s AS (
    SELECT did, handle_claim, pds_endpoint, created_at, signing_key
    FROM accounts TABLESAMPLE BERNOULLI (0.002)
    WHERE status = 0 AND (handle_claim LIKE '%.trump.com' OR handle_claim LIKE '%.plc.surge.sh')
)
SELECT handle_claim, pds_endpoint, created_at FROM s ORDER BY md5(did) LIMIT 12;

WITH s AS (
    SELECT handle_claim, pds_endpoint, created_at, signing_key
    FROM accounts TABLESAMPLE BERNOULLI (0.05)
    WHERE status = 0 AND handle_claim LIKE '%.trump.com'
)
SELECT count(*) AS sampled,
       count(DISTINCT pds_endpoint) AS distinct_pds,
       count(DISTINCT signing_key) AS distinct_keys,
       min(created_at) AS first_created, max(created_at) AS last_created,
       mode() WITHIN GROUP (ORDER BY pds_endpoint) AS commonest_pds
FROM s;

WITH s AS (
    SELECT created_at FROM accounts TABLESAMPLE BERNOULLI (0.05)
    WHERE status = 0 AND handle_claim LIKE '%.trump.com'
)
SELECT to_char(date_trunc('month', created_at), 'YYYY-MM') AS month, count(*) AS sampled
FROM s GROUP BY 1 ORDER BY 1;
