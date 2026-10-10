-- Replay membership probes: what the planner says about each DID.
\pset border 1

SELECT source = 'plc-sample' AS plc_sample, plan, count(*) AS dids,
       count(*) FILTER (WHERE units = 0) AS planner_says_absent,
       count(*) FILTER (WHERE units > 0) AS planner_says_maybe,
       count(*) FILTER (WHERE found) AS confirmed_present,
       count(*) FILTER (WHERE found = false AND units > 0) AS confirmed_absent_after_fetch,
       count(*) FILTER (WHERE truncated) AS truncated,
       sum(blocks) AS blocks_planned, sum(whole_segments) AS whole_segments, sum(bytes) AS bytes
FROM recon_replay_probe GROUP BY 1, 2 ORDER BY 1, 2;

-- Per DID: how the two plans compare.
SELECT f.units > 0 AS in_filtered_plan, u.units > 0 AS in_unfiltered_plan, count(*) AS dids
FROM recon_replay_probe f JOIN recon_replay_probe u ON u.did = f.did AND u.plan = 'unfiltered'
WHERE f.plan = 'filtered' AND f.source <> 'plc-sample'
GROUP BY 1, 2 ORDER BY 1, 2;

-- Against whether the document could be fetched.
SELECT d.outcome = 'ok' AS document_ok, u.units > 0 AS in_unfiltered_plan, f.units > 0 AS in_filtered_plan, count(*) AS dids
FROM recon_replay_probe u
JOIN recon_replay_probe f ON f.did = u.did AND f.plan = 'filtered'
JOIN recon_didweb_docs d ON d.did = u.did
WHERE u.plan = 'unfiltered'
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;

SELECT plan, percentile_cont(0.5) WITHIN GROUP (ORDER BY blocks) AS p50_blocks, max(blocks) AS max_blocks
FROM recon_replay_probe WHERE units > 0 GROUP BY plan;
