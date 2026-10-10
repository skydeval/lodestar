-- did:web documents: how many resolve, and how many are accounts.
\pset border 1

SELECT outcome, http_status, count(*) AS n FROM recon_didweb_docs GROUP BY 1, 2 ORDER BY n DESC LIMIT 20;

SELECT count(*) AS fetched_ok,
       count(*) FILTER (WHERE id_ok) AS id_matches,
       count(*) FILTER (WHERE has_pds) AS declares_pds,
       count(*) FILTER (WHERE NOT has_pds) AS no_pds,
       count(*) FILTER (WHERE handle IS NOT NULL) AS claims_handle,
       count(*) FILTER (WHERE key_type IS NULL) AS no_atproto_key
FROM recon_didweb_docs WHERE outcome = 'ok';

SELECT key_type, count(*) AS n FROM recon_didweb_docs WHERE outcome = 'ok' GROUP BY 1 ORDER BY n DESC;

SELECT services, count(*) AS n FROM recon_didweb_docs WHERE outcome = 'ok' GROUP BY 1 ORDER BY n DESC LIMIT 10;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95_ms,
       max(ms) AS max_ms
FROM recon_didweb_docs;
