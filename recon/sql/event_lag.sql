-- Live identity events: what a check made at once finds.
\pset border 1
SELECT count(*) AS events_taken,
       count(*) FILTER (WHERE handle IS NULL) AS no_claim,
       count(*) FILTER (WHERE handle_from = 'directory') AS claim_from_directory,
       count(*) FILTER (WHERE stored_handle IS DISTINCT FROM handle AND handle IS NOT NULL) AS claim_differs_from_backfill,
       count(*) FILTER (WHERE stored_handle IS NULL AND handle IS NOT NULL) AS account_new_since_backfill
FROM recon_event_lag;

SELECT first_result, count(*) AS n,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY first_ms) AS check_p50_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY first_ms) AS check_p95_ms
FROM recon_event_lag WHERE first_result IS NOT NULL GROUP BY 1 ORDER BY n DESC;

-- Delay from the event's own time to lodestar receiving it.
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - event_time)) AS receive_p50_s,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - event_time)) AS receive_p95_s,
       max(extract(epoch FROM received_at - event_time)) AS receive_max_s
FROM recon_event_lag WHERE event_time IS NOT NULL;

-- Checks that were not verified at once: did a later check verify them?
SELECT first_result, coalesce(after_60s, '(not re-checked)') AS after_60s, coalesce(after_600s, '(not re-checked)') AS after_600s, count(*) AS n
FROM recon_event_lag WHERE first_result IS NOT NULL AND first_result <> 'verified'
GROUP BY 1, 2, 3 ORDER BY n DESC;

SELECT first_dns, coalesce(first_https, '-') AS first_https, first_result, count(*) AS n
FROM recon_event_lag WHERE first_result IS NOT NULL GROUP BY 1, 2, 3 ORDER BY n DESC LIMIT 15;
