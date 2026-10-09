-- Relay host roster: sizes, and the request count a per-host walk would need.
SELECT count(*) AS hosts,
       sum(account_count) AS accounts,
       sum(ceil(greatest(account_count, 1) / 1000.0))::bigint AS pages_at_1000,
       count(*) FILTER (WHERE account_count > 1000) AS hosts_over_1000,
       count(*) FILTER (WHERE account_count >= 100000) AS hosts_over_100k,
       count(*) FILTER (WHERE coalesce(account_count, 0) = 0) AS hosts_empty
FROM recon_hosts;

SELECT status, count(*) AS hosts, sum(account_count) AS accounts
FROM recon_hosts GROUP BY status ORDER BY hosts DESC;

SELECT hostname, account_count, status FROM recon_hosts ORDER BY account_count DESC NULLS LAST LIMIT 12;

-- Registrable-ish grouping of host names, to see who runs the large hosts.
SELECT regexp_replace(hostname, '^.*\.([^.]+\.[^.]+\.[^.]+)$', '\1') AS suffix3,
       count(*) AS hosts, sum(account_count) AS accounts
FROM recon_hosts GROUP BY 1 ORDER BY accounts DESC NULLS LAST LIMIT 8;
