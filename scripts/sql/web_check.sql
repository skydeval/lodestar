-- What is known about did:web identities.
\pset border 1

SELECT did, status, pds, left(signing_key, 24) AS signing_key, handle_claim, handle_status, listed
FROM accounts_readable WHERE method = 'web' ORDER BY did;

SELECT lodestar_did(w.did) AS did, w.last_fetch_at, w.first_success_at IS NOT NULL AS has_resolved,
       f.first_failure_at, f.next_attempt_at
FROM web_documents w LEFT JOIN check_failures f ON f.did = w.did AND f.kind = 1 ORDER BY 1;

SELECT lodestar_did(did) AS still_queued, kind, reason FROM verify_due ORDER BY 1;
