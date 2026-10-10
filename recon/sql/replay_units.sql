-- Replay download progress and what the downloaded units contained.
\pset border 1
SELECT now() AS measured_at,
       count(DISTINCT segment) AS segments, count(*) AS blocks,
       sum(bytes) AS compressed_bytes, pg_size_pretty(sum(bytes)) AS compressed,
       sum(rows) AS rows, sum(identity) AS identity_rows, sum(account) AS account_rows, sum(other) AS other_rows,
       round(100.0 * (sum(identity) + sum(account)) / nullif(sum(rows), 0), 3) AS marker_pct,
       sum(with_handle) AS identity_rows_with_handle,
       count(*) FILTER (WHERE identity + account > 0) AS blocks_with_markers
FROM recon_replay_units;

SELECT count(*) AS dids, count(*) FILTER (WHERE did LIKE 'did:web:%') AS did_web,
       sum(identity) AS identity_events, sum(account) AS account_events
FROM recon_replay_dids;
