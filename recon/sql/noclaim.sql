-- Active accounts with no usable handle claim: when they were created, and examples.
\pset border 1

SELECT to_char(date_trunc('year', created_at), 'YYYY') AS year,
       count(*) AS accounts,
       count(*) FILTER (WHERE status = 0 AND handle_claim IS NULL) AS active_no_claim
FROM accounts GROUP BY 1 ORDER BY 1;

SELECT did, created_at, pds_endpoint FROM accounts TABLESAMPLE BERNOULLI (0.001)
WHERE status = 0 AND handle_claim IS NULL ORDER BY md5(did) LIMIT 8;

-- Which PDS endpoints the no-claim accounts sit on.
SELECT pds_endpoint, count(*) AS n FROM accounts TABLESAMPLE BERNOULLI (1)
WHERE status = 0 AND handle_claim IS NULL GROUP BY 1 ORDER BY n DESC LIMIT 10;
