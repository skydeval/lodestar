-- What recon recorded for the six accounts where a fork was detected but
-- the directory shows no nullified operation.
\pset border 1
SELECT f.did, f.seq, f.created_at, f.prev, f.head_cid, f.prev_in_window, f.nullified_seqs, f.hist_rows_removed
FROM recon_forks f JOIN recon_audit_compare a ON a.did = f.did
WHERE a.kind = 'fork' AND NOT a.history_match ORDER BY f.did;
