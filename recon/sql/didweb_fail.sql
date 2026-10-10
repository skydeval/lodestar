-- Where the did:web connect failures sit, with a few examples to check by hand.
\pset border 1

SELECT regexp_replace(did, '^did:web:(.*\.)?([^.]+\.[^.]+)$', '\2') AS domain2,
       count(*) FILTER (WHERE outcome = 'ok') AS ok,
       count(*) FILTER (WHERE outcome = 'connect') AS connect_failed,
       count(*) FILTER (WHERE outcome NOT IN ('ok', 'connect')) AS other
FROM recon_didweb_docs GROUP BY 1 ORDER BY count(*) DESC LIMIT 12;

SELECT DISTINCT ON (regexp_replace(did, '^did:web:(.*\.)?([^.]+\.[^.]+)$', '\2')) did
FROM recon_didweb_docs WHERE outcome = 'connect' ORDER BY regexp_replace(did, '^did:web:(.*\.)?([^.]+\.[^.]+)$', '\2'), did LIMIT 8;
