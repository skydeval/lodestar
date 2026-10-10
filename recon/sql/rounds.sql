-- How results for the same handle change between rounds (system resolver).
\pset border 1
SELECT a.round AS first_round, b.round AS later_round,
       min(b.started_at) - min(a.started_at) AS gap,
       a.result AS first_result, b.result AS later_result, count(*) AS n
FROM recon_handle_checks a
JOIN recon_handle_checks b ON b.run = a.run AND b.did = a.did AND b.resolver = a.resolver AND b.round > a.round
WHERE a.resolver = 'system'
GROUP BY 1, 2, 4, 5 ORDER BY 1, 2, n DESC;

-- Verified handles that later got a "resolves to nothing" answer, and how.
SELECT b.round, b.dns, coalesce(b.addr, '-') AS addr, coalesce(b.https, '-') AS https, count(*) AS n
FROM recon_handle_checks a
JOIN recon_handle_checks b ON b.run = a.run AND b.did = a.did AND b.resolver = a.resolver AND b.round > a.round
WHERE a.resolver = 'system' AND a.round = 'r1' AND a.result = 'verified' AND b.result <> 'verified'
GROUP BY 1, 2, 3, 4 ORDER BY 1, n DESC;

-- Inconclusive in the first round: what they became, by first-round cause.
SELECT coalesce(a.https, a.dns) AS first_cause, b.round, b.result AS later_result, count(*) AS n
FROM recon_handle_checks a
JOIN recon_handle_checks b ON b.run = a.run AND b.did = a.did AND b.resolver = a.resolver AND b.round > a.round
WHERE a.resolver = 'system' AND a.round = 'r1' AND a.result = 'inconclusive'
GROUP BY 1, 2, 3 ORDER BY 1, 2, n DESC;
