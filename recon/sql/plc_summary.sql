-- PLC backfill summary: totals, growth, forks, operation kinds.
\pset border 1

SELECT source, origin, cursor, phase, tip_at FROM sync_state ORDER BY source;

SELECT name, n FROM recon_counters ORDER BY name;

-- Operations and new DIDs per month.
SELECT to_char(date_trunc('month', day), 'YYYY-MM') AS month, sum(ops) AS ops, sum(creates) AS creates
FROM recon_plc_daily GROUP BY 1 ORDER BY 1;

-- Recent daily rate.
SELECT round(avg(ops)) AS ops_per_day_last_30, round(avg(creates)) AS creates_per_day_last_30,
       round(avg(ops) / 86400.0, 2) AS ops_per_second
FROM (SELECT * FROM recon_plc_daily WHERE day < (SELECT max(day) FROM recon_plc_daily) ORDER BY day DESC LIMIT 30) t;

SELECT round(avg(ops)) AS ops_per_day_last_365, round(avg(creates)) AS creates_per_day_last_365
FROM (SELECT * FROM recon_plc_daily WHERE day < (SELECT max(day) FROM recon_plc_daily) ORDER BY day DESC LIMIT 365) t;

SELECT max(ops) AS busiest_day_ops, (SELECT day FROM recon_plc_daily ORDER BY ops DESC LIMIT 1) AS busiest_day FROM recon_plc_daily;

-- Forks: totals, by month, and how many operations each nullified.
SELECT count(*) AS forks, count(DISTINCT did) AS dids,
       count(*) FILTER (WHERE prev_in_window) AS prev_in_window,
       count(*) FILTER (WHERE NOT prev_in_window) AS prev_older_than_window,
       sum(hist_rows_removed) AS hist_rows_removed,
       sum(array_length(string_to_array(nullified_seqs, ','), 1)) AS nullified_ops,
       max(array_length(string_to_array(nullified_seqs, ','), 1)) AS most_in_one_fork
FROM recon_forks;

SELECT to_char(date_trunc('month', created_at), 'YYYY-MM') AS month, count(*) AS forks, count(DISTINCT did) AS dids
FROM recon_forks GROUP BY 1 ORDER BY forks DESC LIMIT 8;

SELECT did, count(*) AS forks FROM recon_forks GROUP BY did ORDER BY forks DESC LIMIT 5;

-- Account states at the tip.
SELECT count(*) AS accounts,
       count(*) FILTER (WHERE status = 1) AS tombstoned,
       count(*) FILTER (WHERE status = 0 AND handle_claim IS NULL) AS active_no_claim,
       count(*) FILTER (WHERE status = 0 AND pds_endpoint IS NULL) AS active_no_pds,
       count(*) FILTER (WHERE status = 0 AND signing_key IS NULL) AS active_no_key
FROM accounts;
