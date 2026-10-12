-- Compare what lodestar stored with recon's loaded directory, copied
-- into cmp_accounts and cmp_history by dev-compare-recon.sh. Identities
-- with operations after recon's snapshot are expected to differ and are
-- left out. Pass recon's PLC cursor as `first`.
\pset border 1
\timing on
SET work_mem = '2GB';
SET max_parallel_workers_per_gather = 4;

CREATE TEMP TABLE changed AS SELECT DISTINCT did FROM plc_recent_ops WHERE seq > :first;
CREATE UNIQUE INDEX ON changed (did);
ANALYZE changed;
SELECT count(*) AS identities_with_operations_since_the_snapshot FROM changed;

-- Accounts.
SELECT
    count(*) FILTER (WHERE a.did IS NULL) AS only_in_recon,
    count(*) FILTER (WHERE c.did IS NULL AND ch.did IS NULL) AS only_in_lodestar_unexplained,
    count(*) FILTER (WHERE c.did IS NULL AND ch.did IS NOT NULL) AS new_since_snapshot,
    count(*) FILTER (WHERE a.did IS NOT NULL AND c.did IS NOT NULL AND ch.did IS NULL) AS compared,
    count(*) FILTER (WHERE a.did IS NOT NULL AND c.did IS NOT NULL AND ch.did IS NULL AND a.handle_claim IS DISTINCT FROM c.handle_claim) AS claim_differs,
    count(*) FILTER (WHERE a.did IS NOT NULL AND c.did IS NOT NULL AND ch.did IS NULL AND p.endpoint IS DISTINCT FROM c.endpoint) AS pds_differs,
    count(*) FILTER (WHERE a.did IS NOT NULL AND c.did IS NOT NULL AND ch.did IS NULL AND a.status IS DISTINCT FROM c.status) AS status_differs,
    count(*) FILTER (WHERE a.did IS NOT NULL AND c.did IS NOT NULL AND ch.did IS NULL AND a.created_at IS DISTINCT FROM c.created_at) AS created_at_differs,
    count(*) FILTER (WHERE a.did IS NOT NULL AND c.did IS NOT NULL AND ch.did IS NULL AND c.signing_key IS NOT NULL AND a.signing_key IS DISTINCT FROM c.signing_key) AS key_differs,
    count(*) FILTER (WHERE a.did IS NOT NULL AND c.did IS NOT NULL AND ch.did IS NULL AND c.signing_key IS NULL AND a.signing_key IS NOT NULL) AS key_recon_could_not_decode
FROM accounts a
FULL JOIN cmp_accounts c USING (did)
LEFT JOIN pds_endpoints p ON p.id = a.pds_id
LEFT JOIN changed ch ON ch.did = COALESCE(a.did, c.did);

-- History: the same rows on both sides, whatever their numbering.
CREATE TEMP TABLE l AS
SELECT h.did, h.changed_at, h.handle_claim, p.endpoint
FROM identity_history h LEFT JOIN pds_endpoints p ON p.id = h.pds_id
WHERE NOT EXISTS (SELECT 1 FROM changed ch WHERE ch.did = h.did);
CREATE TEMP TABLE r AS
SELECT c.did, c.changed_at, c.handle_claim, c.endpoint FROM cmp_history c
WHERE NOT EXISTS (SELECT 1 FROM changed ch WHERE ch.did = c.did);
CREATE TEMP TABLE only_l AS SELECT * FROM l EXCEPT ALL SELECT * FROM r;
CREATE TEMP TABLE only_r AS SELECT * FROM r EXCEPT ALL SELECT * FROM l;

SELECT (SELECT count(*) FROM l) AS lodestar_rows, (SELECT count(*) FROM r) AS recon_rows,
       (SELECT count(*) FROM only_l) AS only_in_lodestar, (SELECT count(*) FROM only_r) AS only_in_recon;

-- Lodestar keeps the state a tombstone clears; recon's table did not.
SELECT CASE a.status WHEN 0 THEN 'active' WHEN 1 THEN 'tombstoned' ELSE 'other' END AS account_status, count(*) AS rows_only_in_lodestar
FROM only_l o JOIN accounts a USING (did) GROUP BY 1 ORDER BY 2 DESC;

SELECT lodestar_did(did) AS did, changed_at, handle_claim, endpoint FROM only_l
WHERE did NOT IN (SELECT did FROM accounts WHERE status = 1) ORDER BY did, changed_at LIMIT 12;
SELECT lodestar_did(did) AS did, changed_at, handle_claim, endpoint FROM only_r ORDER BY did, changed_at LIMIT 12;
