-- One verified bsky.social handle from the sample, with its PDS, for a
-- single resolveHandle probe.
SELECT h.handle, a.did, a.pds_endpoint
FROM recon_handle_checks h JOIN accounts a ON a.did = h.did
WHERE h.round = 'r3' AND h.result = 'verified' AND h.domain = 'bsky.social' AND a.pds_endpoint LIKE '%host.bsky.network'
ORDER BY h.did LIMIT 2;
