-- How many detected forks have a `prev` written in a different CID text
-- encoding from the directory's own `cid` values (which are base32, 'b…').
\pset border 1
SELECT left(prev, 1) AS prev_prefix, left(head_cid, 1) AS cid_prefix,
       count(*) AS forks, count(DISTINCT did) AS dids,
       sum(hist_rows_removed) AS hist_rows_removed,
       sum(array_length(string_to_array(nullified_seqs, ','), 1)) AS ops_marked_nullified
FROM recon_forks GROUP BY 1, 2 ORDER BY forks DESC;

SELECT to_char(date_trunc('month', created_at), 'YYYY-MM') AS month, left(prev, 1) AS prev_prefix, count(*) AS forks
FROM recon_forks GROUP BY 1, 2 ORDER BY forks DESC LIMIT 8;
