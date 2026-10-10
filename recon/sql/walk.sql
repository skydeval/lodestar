-- Relay listRepos walk: what it listed, and the non-did:plc DIDs it found.
\pset border 1

SELECT source, method, active, status, n FROM recon_walk_status ORDER BY source, n DESC;

SELECT source, count(*) AS dids,
       count(*) FILTER (WHERE did LIKE 'did:web:%') AS did_web,
       count(*) FILTER (WHERE did NOT LIKE 'did:web:%') AS other_methods
FROM recon_didweb GROUP BY source ORDER BY source;

-- did:web DIDs the design would reject on syntax (path, port, percent).
SELECT count(*) FILTER (WHERE did ~ '^did:web:[^:%/]+$') AS hostname_only,
       count(*) FILTER (WHERE did !~ '^did:web:[^:%/]+$') AS has_path_port_or_percent
FROM (SELECT DISTINCT did FROM recon_didweb WHERE did LIKE 'did:web:%') t;

-- Which hosts' names they sit under.
SELECT regexp_replace(did, '^did:web:(.*\.)?([^.]+\.[^.]+)$', '\2') AS domain2, count(*) AS n
FROM (SELECT DISTINCT did FROM recon_didweb WHERE did LIKE 'did:web:%') t
GROUP BY 1 ORDER BY n DESC LIMIT 15;

-- Seen by the live Jetstream subscription but not by the walk, and the reverse.
SELECT count(*) FILTER (WHERE in_live AND NOT in_walk) AS live_only,
       count(*) FILTER (WHERE in_walk AND NOT in_live) AS walk_only,
       count(*) FILTER (WHERE in_walk AND in_live) AS both
FROM (SELECT did, bool_or(source = 'jetstream-live') AS in_live, bool_or(source = 'relay-walk') AS in_walk
      FROM recon_didweb WHERE did LIKE 'did:web:%' GROUP BY did) t;
