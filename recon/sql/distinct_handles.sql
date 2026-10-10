-- Distinct handles (what per-handle verification would check) under the
-- domains with the most claims. v_handle_index holds one row per distinct
-- active claim.
\pset border 1
\timing on
SELECT d.domain, d.n AS claims,
       (SELECT count(*) FROM v_handle_index v WHERE v.handle = d.domain OR v.handle LIKE '%.' || d.domain) AS distinct_handles
FROM (SELECT domain, n FROM recon_domain_counts ORDER BY n DESC LIMIT 12) d
ORDER BY d.n DESC;
