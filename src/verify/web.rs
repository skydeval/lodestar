//! `did:web` documents: reading one, and storing what it says.
//!
//! A `did:web` has no registry. Its state is its document, fetched from
//! its own host, so it is the verification role's work.

use anyhow::{Context, Result};
use serde_json::Value;
use tokio_postgres::Client;

use crate::queue::{self, Item};
use crate::{db, encoding, handle};

/// What a document says about the account.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Document {
    pub claim: Option<String>,
    pub pds: Option<String>,
    pub key: Option<Vec<u8>>,
}

/// The host a `did:web` names, if it is one lodestar will ask: hostname
/// only, with no path and no port, under a TLD that can resolve.
pub fn host(did: &str) -> Option<&str> {
    let host = did.strip_prefix("did:web:")?;
    let ok = handle::normalize(host).as_deref() == Some(host) && !handle::never_resolves(host);
    ok.then_some(host)
}

/// A legacy key type and the multicodec prefix its raw key needs.
fn legacy_prefix(ty: &str) -> Option<&'static [u8]> {
    match ty {
        "EcdsaSecp256k1VerificationKey2019" => Some(&[0xe7, 0x01]),
        "EcdsaSecp256r1VerificationKey2019" => Some(&[0x80, 0x24]),
        _ => None,
    }
}

fn storable(s: &str) -> Option<String> {
    (!s.contains('\0')).then(|| s.to_owned())
}

/// Read a document fetched for `did`. None if it is not that DID's
/// document at all.
pub fn read(did: &str, body: &str) -> Option<Document> {
    let v: Value = serde_json::from_str(body).ok()?;
    if v.get("id").and_then(Value::as_str) != Some(did) {
        return None;
    }
    // The first valid handle among the at:// entries.
    let claim = v
        .get("alsoKnownAs")
        .and_then(Value::as_array)
        .and_then(|aka| {
            aka.iter()
                .filter_map(|e| e.as_str().and_then(|s| s.strip_prefix("at://")))
                .find_map(handle::normalize)
        });
    let ends = |entry: &Value, suffix: &str| {
        entry
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| id.ends_with(suffix))
    };
    let pds = v
        .get("service")
        .and_then(Value::as_array)
        .and_then(|services| {
            services
                .iter()
                .find(|s| {
                    ends(s, "#atproto_pds")
                        && s.get("type").and_then(Value::as_str)
                            == Some("AtprotoPersonalDataServer")
                })
                .and_then(|s| s.get("serviceEndpoint").and_then(Value::as_str))
                .and_then(storable)
        });
    let key = v
        .get("verificationMethod")
        .and_then(Value::as_array)
        .and_then(|methods| {
            methods
                .iter()
                .filter(|m| {
                    ends(m, "#atproto") && m.get("controller").and_then(Value::as_str) == Some(did)
                })
                .find_map(|m| {
                    let multibase = m.get("publicKeyMultibase").and_then(Value::as_str)?;
                    match m.get("type").and_then(Value::as_str)? {
                        "Multikey" => storable(multibase)
                            .map(|k| encoding::key_bytes(&format!("did:key:{k}"))),
                        // The older form carries the bare key: it gains its prefix.
                        other => {
                            let raw = encoding::b58_decode(multibase.strip_prefix('z')?)?;
                            let mut bytes = legacy_prefix(other)?.to_vec();
                            bytes.extend_from_slice(&raw);
                            Some(bytes)
                        }
                    }
                })
        });
    Some(Document { claim, pds, key })
}

/// How long a failing fetch waits before the next: as long again as it
/// has been failing, between five minutes and the refresh interval.
fn retry_wait_secs(failing_for: f64, refresh_secs: f64) -> f64 {
    failing_for.clamp(300.0, refresh_secs)
}

pub struct Rules {
    /// A document is fetched again this often.
    pub refresh_secs: f64,
    /// A document that has failed this long makes its account unresolvable.
    pub grace_secs: f64,
}

impl Default for Rules {
    fn default() -> Self {
        Rules {
            refresh_secs: 86_400.0,
            grace_secs: 14.0 * 86_400.0,
        }
    }
}

async fn intern(
    tx: &tokio_postgres::Transaction<'_>,
    endpoint: Option<&str>,
) -> Result<Option<i32>> {
    let Some(e) = endpoint else {
        return Ok(None);
    };
    tx.execute(
        "INSERT INTO pds_endpoints (endpoint) VALUES ($1) ON CONFLICT (md5(endpoint)) DO NOTHING",
        &[&e],
    )
    .await?;
    Ok(Some(
        tx.query_one(
            "SELECT id FROM pds_endpoints WHERE md5(endpoint) = md5($1)",
            &[&e],
        )
        .await?
        .get(0),
    ))
}

/// Store the outcome of fetching a document. `listed` is the mark its
/// queue entry carried.
pub async fn apply(
    client: &mut Client,
    rules: &Rules,
    did: &[u8],
    fetched: Option<&Document>,
    listed: bool,
) -> Result<()> {
    let tx = client.transaction().await?;
    let row = tx
        .query_opt(
            "SELECT a.status, a.handle_claim, p.endpoint, w.first_success_at IS NOT NULL \
             FROM accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id LEFT JOIN web_documents w ON w.did = a.did \
             WHERE a.did = $1 FOR UPDATE OF a",
            &[&did],
        )
        .await
        .context("loading the account")?;

    match fetched {
        Some(doc) => {
            let handle_status = match &doc.claim {
                Some(c) if !handle::never_resolves(c) => db::HANDLE_PENDING,
                _ => db::HANDLE_NONE,
            };
            let pds_id = intern(&tx, doc.pds.as_deref()).await?;
            let claim_changed = match &row {
                None => {
                    tx.execute(
                        "INSERT INTO accounts (created_at, pds_id, method, status, handle_status, listed, did, signing_key, handle_claim) \
                         VALUES (now(), $2, $3, $4, $5, $6, $1, $7, $8)",
                        &[&did, &pds_id, &db::METHOD_WEB, &db::STATUS_ACTIVE, &handle_status, &listed, &doc.key, &doc.claim],
                    )
                    .await
                    .context("inserting the account")?;
                    true
                }
                Some(r) => {
                    let was_claim: Option<String> = r.get(1);
                    let was_pds: Option<String> = r.get(2);
                    let had_state: bool = r.get(3);
                    let status: i16 = r.get(0);
                    let changed = was_claim != doc.claim || was_pds != doc.pds;
                    // History is kept only for accounts that have changed.
                    // A first successful fetch is the first known state.
                    if had_state && changed {
                        tx.execute(
                            "INSERT INTO identity_history (changed_at, seq, pds_id, did, handle_claim) \
                             SELECT w.first_success_at, 0, a.pds_id, a.did, a.handle_claim FROM accounts a JOIN web_documents w ON w.did = a.did \
                             WHERE a.did = $1 AND NOT EXISTS (SELECT 1 FROM identity_history h WHERE h.did = a.did)",
                            &[&did],
                        )
                        .await?;
                        tx.execute(
                            "INSERT INTO identity_history (changed_at, seq, pds_id, did, handle_claim) \
                             SELECT now(), (SELECT max(seq) + 1 FROM identity_history WHERE did = $1), $2, $1, $3",
                            &[&did, &pds_id, &doc.claim],
                        )
                        .await?;
                    }
                    let reset = was_claim != doc.claim || status != db::STATUS_ACTIVE;
                    tx.execute(
                        "UPDATE accounts SET status = $2, pds_id = $3, signing_key = $4, handle_claim = $5, listed = listed OR $6, \
                             handle_status = CASE WHEN $7 THEN $8 ELSE handle_status END, \
                             handle_checked_at = CASE WHEN $7 THEN NULL ELSE handle_checked_at END \
                         WHERE did = $1",
                        &[&did, &db::STATUS_ACTIVE, &pds_id, &doc.key, &doc.claim, &listed, &reset, &handle_status],
                    )
                    .await
                    .context("updating the account")?;
                    reset
                }
            };
            if claim_changed {
                // A new claim never inherits an old claim's failures, and
                // is checked ahead of everything else.
                tx.execute(
                    "DELETE FROM check_failures WHERE did = $1 AND kind = 0",
                    &[&did],
                )
                .await?;
                if handle_status == db::HANDLE_PENDING {
                    queue::push(
                        &tx,
                        queue::REASON_CLAIM,
                        &[Item {
                            did,
                            kind: queue::KIND_HANDLE,
                            listed: false,
                        }],
                    )
                    .await?;
                }
            }
            tx.execute(
                "INSERT INTO web_documents (did, last_fetch_at, first_success_at, last_success_at) VALUES ($1, now(), now(), now()) \
                 ON CONFLICT (did) DO UPDATE SET last_fetch_at = now(), last_success_at = now(), \
                     first_success_at = COALESCE(web_documents.first_success_at, now())",
                &[&did],
            )
            .await?;
            tx.execute(
                "DELETE FROM check_failures WHERE did = $1 AND kind = 1",
                &[&did],
            )
            .await?;
        }
        None => {
            if row.is_none() {
                // Known, but never resolved: stored so it is not found
                // afresh by every walk.
                tx.execute(
                    "INSERT INTO accounts (created_at, method, status, handle_status, listed, did) VALUES (now(), $2, $3, $4, $5, $1)",
                    &[&did, &db::METHOD_WEB, &db::STATUS_UNRESOLVABLE, &db::HANDLE_NONE, &listed],
                )
                .await
                .context("inserting the unresolvable account")?;
            } else if listed {
                tx.execute(
                    "UPDATE accounts SET listed = true WHERE did = $1 AND NOT listed",
                    &[&did],
                )
                .await?;
            }
            tx.execute(
                "INSERT INTO web_documents (did, last_fetch_at) VALUES ($1, now()) ON CONFLICT (did) DO UPDATE SET last_fetch_at = now()",
                &[&did],
            )
            .await?;
            let failing_for: f64 = tx
                .query_one(
                    "INSERT INTO check_failures (did, kind, first_failure_at, last_attempt_at, next_attempt_at) VALUES ($1, 1, now(), now(), now()) \
                     ON CONFLICT (did, kind) DO UPDATE SET last_attempt_at = now() \
                     RETURNING extract(epoch FROM now() - first_failure_at)::float8",
                    &[&did],
                )
                .await?
                .get(0);
            tx.execute(
                "UPDATE check_failures SET next_attempt_at = now() + make_interval(secs => $2::float8) WHERE did = $1 AND kind = 1",
                &[&did, &retry_wait_secs(failing_for, rules.refresh_secs)],
            )
            .await?;
            // A document that has stopped resolving keeps its last state
            // for the grace period. After that the account is unresolvable:
            // its claim can no longer be read, so its handle leaves the index.
            if failing_for >= rules.grace_secs {
                tx.execute(
                    "UPDATE accounts SET status = $2, handle_checked_at = NULL, \
                         handle_status = CASE WHEN handle_claim IS NULL THEN $4 ELSE $3 END \
                     WHERE did = $1 AND status = $5",
                    &[
                        &did,
                        &db::STATUS_UNRESOLVABLE,
                        &db::HANDLE_PENDING,
                        &db::HANDLE_NONE,
                        &db::STATUS_ACTIVE,
                    ],
                )
                .await?;
                tx.execute(
                    "DELETE FROM check_failures WHERE did = $1 AND kind = 0",
                    &[&did],
                )
                .await?;
            }
        }
    }
    tx.execute(
        "DELETE FROM verify_due WHERE did = $1 AND kind = $2",
        &[&did, &queue::KIND_DOCUMENT],
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const DID: &str = "did:web:example.com";

    #[test]
    fn hosts() {
        assert_eq!(host("did:web:example.com"), Some("example.com"));
        assert_eq!(host("did:web:sub.example.co.uk"), Some("sub.example.co.uk"));
        for bad in [
            "did:web:example.com:path",
            "did:web:example.com%3A8080",
            "did:web:Example.com",
            "did:web:localhost",
            "did:web:box.local",
            "did:plc:abc",
            "did:web:",
        ] {
            assert_eq!(host(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_full_document() {
        let body = r##"{
            "id": "did:web:example.com",
            "alsoKnownAs": ["https://example.com", "at://Example.com", "at://second.example.com"],
            "verificationMethod": [
                {"id": "did:web:example.com#other", "type": "Multikey", "controller": "did:web:example.com", "publicKeyMultibase": "zNope"},
                {"id": "did:web:example.com#atproto", "type": "Multikey", "controller": "did:web:someone.else", "publicKeyMultibase": "zNope"},
                {"id": "did:web:example.com#atproto", "type": "Multikey", "controller": "did:web:example.com", "publicKeyMultibase": "zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF"}
            ],
            "service": [
                {"id": "#other", "type": "Something", "serviceEndpoint": "https://no.example"},
                {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example.com"}
            ]
        }"##;
        let d = read(DID, body).unwrap();
        assert_eq!(d.claim.as_deref(), Some("example.com"));
        assert_eq!(d.pds.as_deref(), Some("https://pds.example.com"));
        assert_eq!(
            encoding::key_text(d.key.as_ref().unwrap()).as_deref(),
            Some("did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF")
        );
    }

    #[test]
    fn a_document_for_another_did_is_not_this_ones() {
        assert_eq!(read(DID, r#"{"id":"did:web:other.example"}"#), None);
        assert_eq!(read(DID, "not json"), None);
        assert_eq!(read(DID, r#"{"alsoKnownAs":[]}"#), None);
    }

    #[test]
    fn a_document_with_no_account_parts_reads_as_empty() {
        let d = read(DID, r##"{"id":"did:web:example.com","service":[{"id":"#atproto_pds","type":"Wrong","serviceEndpoint":"https://x"}]}"##).unwrap();
        assert_eq!(d, Document::default());
    }

    #[test]
    fn a_legacy_key_gains_its_prefix() {
        // The same key as above, written the older way: the bare key bytes.
        let full =
            encoding::b58_decode("Q3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF").unwrap();
        assert_eq!(&full[..2], &[0xe7, 0x01]);
        let bare = format!("z{}", encoding::b58_encode(&full[2..]));
        let body = format!(
            r##"{{"id":"did:web:example.com","verificationMethod":[{{"id":"#atproto","type":"EcdsaSecp256k1VerificationKey2019","controller":"did:web:example.com","publicKeyMultibase":"{bare}"}}]}}"##
        );
        let d = read(DID, &body).unwrap();
        assert_eq!(d.key, Some(full));
        // A type lodestar does not know gives no key.
        let unknown = body.replace("EcdsaSecp256k1VerificationKey2019", "SomethingNew2030");
        assert_eq!(read(DID, &unknown).unwrap().key, None);
    }

    #[test]
    fn failing_fetches_back_off_to_the_refresh_interval() {
        assert_eq!(retry_wait_secs(0.0, 86_400.0), 300.0);
        assert_eq!(retry_wait_secs(3_600.0, 86_400.0), 3_600.0);
        assert_eq!(retry_wait_secs(10.0 * 86_400.0, 86_400.0), 86_400.0);
    }
}
