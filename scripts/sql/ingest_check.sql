-- What the ingest role's sources have recorded.
\pset border 1

SELECT source, origin, cursor, phase, tip_at, last_op_at, position FROM sync_state ORDER BY source;

SELECT kind, reason, listed, count(*) FROM verify_due GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;

SELECT count(*) AS identities, count(*) FILTER (WHERE listed) AS listed FROM accounts;

SELECT lodestar_did(did) AS queued_did, kind, reason, listed, due_at FROM verify_due ORDER BY due_at LIMIT 5;
