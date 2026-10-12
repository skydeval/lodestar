-- What a PLC sync has stored, and whether it is consistent.
\pset border 1

SELECT source, origin, cursor, phase, tip_at FROM sync_state;

SELECT count(*) AS identities,
       count(*) FILTER (WHERE status = 1) AS tombstoned,
       count(*) FILTER (WHERE handle_claim IS NOT NULL) AS with_claim,
       count(*) FILTER (WHERE handle_status = 4) AS pending,
       count(*) FILTER (WHERE handle_status = 5) AS no_status,
       count(*) FILTER (WHERE pds_id IS NOT NULL) AS with_pds,
       count(*) FILTER (WHERE signing_key IS NOT NULL) AS with_key,
       count(*) FILTER (WHERE octet_length(did) = 16) AS sixteen_byte_dids
FROM accounts;

SELECT (SELECT count(*) FROM identity_history) AS history_rows,
       (SELECT count(DISTINCT did) FROM identity_history) AS identities_with_history,
       (SELECT count(*) FROM plc_recent_ops) AS recent_ops,
       (SELECT count(*) FROM pds_endpoints) AS endpoints;

-- Each of these should be 0.
SELECT
  (SELECT count(*) FROM accounts WHERE handle_claim IS NULL AND handle_status <> 5) AS claimless_not_none,
  (SELECT count(*) FROM accounts WHERE status = 1 AND (handle_claim IS NOT NULL OR pds_id IS NOT NULL OR signing_key IS NOT NULL)) AS tombstoned_with_state,
  (SELECT count(*) FROM (SELECT did FROM identity_history GROUP BY did HAVING min(seq) <> 0) x) AS history_not_from_zero,
  (SELECT count(*) FROM (SELECT did FROM identity_history GROUP BY did HAVING count(*) = 1) h
     JOIN accounts a USING (did) WHERE a.status = 0) AS active_with_only_original_row,
  (SELECT count(*) FROM identity_history h LEFT JOIN accounts a USING (did) WHERE a.did IS NULL) AS history_without_account,
  (SELECT count(*) FROM plc_recent_ops r LEFT JOIN identity_history h ON h.did = r.did AND h.seq = r.hist_seq
     WHERE r.hist_seq IS NOT NULL AND h.did IS NULL) AS credit_without_row,
  (SELECT count(*) FROM accounts a JOIN LATERAL (SELECT handle_claim, pds_id FROM identity_history h WHERE h.did = a.did ORDER BY seq DESC LIMIT 1) l ON true
     WHERE a.status = 0 AND (l.handle_claim IS DISTINCT FROM a.handle_claim OR l.pds_id IS DISTINCT FROM a.pds_id)) AS last_row_differs_from_account;

SELECT pg_size_pretty(pg_total_relation_size('accounts')) AS accounts,
       pg_size_pretty(pg_total_relation_size('identity_history')) AS history,
       pg_size_pretty(pg_total_relation_size('plc_recent_ops')) AS recent_ops;
