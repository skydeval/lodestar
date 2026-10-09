-- Handle claims per registrable domain (no requests; from loaded claims).
\pset border 1

SELECT count(*) AS domains, sum(n) AS claims FROM recon_domain_counts;

SELECT domain, n, round(100.0 * n / sum(n) OVER (), 3) AS pct,
       round(100.0 * sum(n) OVER (ORDER BY n DESC, domain) / sum(n) OVER (), 3) AS cumulative_pct
FROM recon_domain_counts ORDER BY n DESC, domain LIMIT 30;

-- How many domains, and how many claims, sit above each candidate threshold.
SELECT t AS threshold,
       count(*) FILTER (WHERE n > t) AS domains_above,
       coalesce(sum(n) FILTER (WHERE n > t), 0) AS claims_above,
       count(*) FILTER (WHERE n <= t) AS domains_at_or_below,
       coalesce(sum(n) FILTER (WHERE n <= t), 0) AS claims_at_or_below
FROM recon_domain_counts, (VALUES (10), (100), (1000), (10000), (100000)) AS v(t)
GROUP BY t ORDER BY t;

SELECT count(*) FILTER (WHERE n = 1) AS domains_with_one_claim FROM recon_domain_counts;
