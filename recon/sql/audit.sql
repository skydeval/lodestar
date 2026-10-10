-- Audit-log comparison results.
\pset border 1
SELECT kind, count(*) AS dids, sum(ops) AS audit_entries, sum(nullified) AS nullified_entries,
       count(*) FILTER (WHERE state_match) AS state_match,
       count(*) FILTER (WHERE history_match) AS history_match,
       count(*) FILTER (WHERE NOT (state_match AND history_match)) AS differ
FROM recon_audit_compare GROUP BY kind ORDER BY kind;

SELECT a.did, a.kind, a.ops, a.nullified, a.state_match, a.history_match, a.detail,
       (SELECT count(*) FROM recon_forks f WHERE f.did = a.did) AS forks_detected,
       (SELECT sum(array_length(string_to_array(f.nullified_seqs, ','), 1)) FROM recon_forks f WHERE f.did = a.did) AS nullified_detected
FROM recon_audit_compare a WHERE NOT (a.state_match AND a.history_match) ORDER BY a.kind, a.did LIMIT 20;

-- Do detected nullified counts equal the directory's, for fork DIDs?
SELECT count(*) AS fork_dids,
       count(*) FILTER (WHERE a.nullified = d.n) AS nullified_count_equal,
       count(*) FILTER (WHERE a.nullified <> d.n) AS nullified_count_differs
FROM recon_audit_compare a
JOIN (SELECT did, sum(array_length(string_to_array(nullified_seqs, ','), 1)) AS n FROM recon_forks GROUP BY did) d ON d.did = a.did
WHERE a.kind = 'fork';
