-- Forks the backfill detected (design §3.1.1).
SELECT count(*) AS forks,
       count(*) FILTER (WHERE prev_in_window) AS prev_in_window,
       count(*) FILTER (WHERE NOT prev_in_window) AS prev_older_than_window,
       sum(hist_rows_removed) AS hist_rows_removed
FROM recon_forks;

SELECT did, seq, created_at, prev_in_window, nullified_seqs, hist_rows_removed
FROM recon_forks ORDER BY seq LIMIT 15;
