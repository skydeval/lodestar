-- Handle claims under a few named registrable domains (no requests).
\pset border 1

SELECT domain, n AS claims
FROM recon_domain_counts
WHERE domain IN ('blacksky.app', 'northsky.social', 'myatproto.social', 'bsky.social')
ORDER BY n DESC;
