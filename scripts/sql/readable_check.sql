-- The readable view against known values, and a few rows to look at.
\pset border 1

-- Each of these should be t.
SELECT lodestar_b32('\x68656c6c6f') = 'nbswy3dp' AS base32,
       lodestar_b58('\x0000287fb4cd') = '11233QC4' AS base58,
       lodestar_did('\x01' || convert_to('did:web:example.com', 'UTF8')) = 'did:web:example.com' AS other_did,
       lodestar_key('\x00' || convert_to('odd key', 'UTF8')) = 'odd key' AS odd_key,
       length(lodestar_did('\x00000102030405060708090a0b0c0d0e')) = 32 AS plc_did_length;

SELECT did, pds, signing_key, handle_claim, handle_status, status, created_at
FROM accounts_readable LIMIT 3;

SELECT * FROM identity_history_readable LIMIT 3;
