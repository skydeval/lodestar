-- Development only: queue a few did:web identities for a document fetch,
-- as a walk would, and mark the PLC source live so the worker will run.
INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ('plc', 'https://plc.directory', 0, 'live', now())
ON CONFLICT (source) DO NOTHING;

INSERT INTO verify_due (kind, reason, listed, did)
SELECT 1, 32, true, '\x01'::bytea || convert_to(d, 'UTF8')
FROM unnest(ARRAY[
    'did:web:areen189d.kald.shop',
    'did:web:removable-breaks-freebsd-phi.trycloudflare.com',
    'did:web:sky.charliegleason.com',
    'did:web:kll.kald.shop',
    'did:web:ra0c225.bske.site',
    'did:web:lu5a.buttercup.wizardry.systems',
    'did:web:le98bf.lnan.site',
    'did:web:apple.social.clipsymphony.com',
    'did:web:tm0473a.bsku.social',
    'did:web:04d3.bske.social',
    'did:web:example.com%3A8080',
    'did:web:box.local'
]) AS t(d)
ON CONFLICT DO NOTHING;
