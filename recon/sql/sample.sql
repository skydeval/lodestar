-- Handle-check sample: outcomes per resolver and round.
\pset border 1

SELECT run, round, resolver, count(*) AS checks,
       min(started_at) AS first_check, max(started_at) AS last_check
FROM recon_handle_checks GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;

-- Result classes.
SELECT round, resolver, result, count(*) AS n,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY round, resolver), 2) AS pct
FROM recon_handle_checks GROUP BY 1, 2, 3 ORDER BY 1, 2, n DESC;

-- Which step decided.
SELECT round, resolver, dns, coalesce(addr, '-') AS addr, coalesce(https, '-') AS https, conclusive, count(*) AS n
FROM recon_handle_checks GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY 1, 2, n DESC;

-- Method that produced a DID.
SELECT round, resolver,
       count(*) FILTER (WHERE dns = 'did') AS by_dns,
       count(*) FILTER (WHERE https = 'did') AS by_https,
       count(*) FILTER (WHERE http_status IS NOT NULL) AS https_requests_made
FROM recon_handle_checks GROUP BY 1, 2 ORDER BY 1, 2;

-- Timing.
SELECT round, resolver,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY dns_ms) AS dns_p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY dns_ms) AS dns_p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY dns_ms) AS dns_p99,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY https_ms) AS https_p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY https_ms) AS https_p95,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY total_ms) AS total_p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY total_ms) AS total_p95
FROM recon_handle_checks GROUP BY 1, 2 ORDER BY 1, 2;

-- Agreement between resolvers on the same handle in the same round.
SELECT a.round, a.resolver AS resolver_a, b.resolver AS resolver_b,
       count(*) AS both_checked,
       count(*) FILTER (WHERE a.result = b.result) AS same_result,
       count(*) FILTER (WHERE a.result <> b.result AND a.conclusive AND b.conclusive) AS conclusive_disagree,
       count(*) FILTER (WHERE a.conclusive <> b.conclusive) AS one_inconclusive
FROM recon_handle_checks a JOIN recon_handle_checks b
  ON b.run = a.run AND b.round = a.round AND b.did = a.did AND b.resolver > a.resolver
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;

-- Results by size of the handle's domain in the sample.
SELECT round, resolver, CASE WHEN c.n >= 1000 THEN 'domain >= 1000 claims' WHEN c.n >= 10 THEN 'domain 10-999' ELSE 'domain < 10' END AS bucket,
       h.result, count(*) AS n
FROM recon_handle_checks h JOIN recon_domain_counts c ON c.domain = h.domain
GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, n DESC;
