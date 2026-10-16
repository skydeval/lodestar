//! `plc.directory`'s DID document read, `GET /{did}`, answered from what
//! lodestar stores, in the shape the directory gives it.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use super::{App, Method, parse_did, source};
use crate::encoding;
use crate::plc::op::rebuilt_parts;

/// The context a key's type adds, as the directory adds it. None for a key
/// type it does not know.
fn key_context(key: &str) -> Option<&'static str> {
    let bytes = encoding::key_bytes(key);
    if bytes.starts_with(&[0xe7, 0x01]) {
        Some("https://w3id.org/security/suites/secp256k1-2019/v1")
    } else if bytes.starts_with(&[0x80, 0x24]) {
        Some("https://w3id.org/security/suites/ecdsa-2019/v1")
    } else {
        None
    }
}

/// The DID document for `did` from its parts: `alsoKnownAs`,
/// `verificationMethods` and `services` as an operation carries them.
pub fn document(did: &str, parts: &Value) -> Value {
    let mut context = vec![
        "https://www.w3.org/ns/did/v1",
        "https://w3id.org/security/multikey/v1",
    ];
    let mut methods = Vec::new();
    if let Some(vm) = parts["verificationMethods"].as_object() {
        for (id, key) in vm {
            let key = key.as_str().unwrap_or_default();
            if let Some(c) = key_context(key)
                && !context.contains(&c)
            {
                context.push(c);
            }
            methods.push(json!({
                "id": format!("{did}#{id}"),
                "type": "Multikey",
                "controller": did,
                "publicKeyMultibase": key.strip_prefix("did:key:").unwrap_or(key),
            }));
        }
    }
    let mut services = Vec::new();
    if let Some(sv) = parts["services"].as_object() {
        for (id, s) in sv {
            services.push(json!({
                "id": format!("#{id}"),
                "type": s["type"],
                "serviceEndpoint": s["endpoint"],
            }));
        }
    }
    json!({
        "@context": context,
        "id": did,
        "alsoKnownAs": parts["alsoKnownAs"],
        "verificationMethod": methods,
        "service": services,
    })
}

fn message(code: StatusCode, text: String) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        json!({ "message": text }).to_string(),
    )
        .into_response()
}

pub(super) async fn get_document(State(app): State<Arc<App>>, Path(did): Path<String>) -> Response {
    match answer(&app, &did).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("request failed: {e:#}");
            message(StatusCode::INTERNAL_SERVER_ERROR, "Internal error".into())
        }
    }
}

async fn answer(app: &App, did: &str) -> anyhow::Result<Response> {
    if parse_did(did) != Some(Method::Plc) {
        return Ok(message(
            StatusCode::BAD_REQUEST,
            format!("Invalid DID: {did}"),
        ));
    }
    let row = app
        .db()
        .query_opt(
            "SELECT a.status, a.handle_claim, p.endpoint, a.signing_key, d.doc \
             FROM accounts a LEFT JOIN pds_endpoints p ON p.id = a.pds_id \
             LEFT JOIN doc_parts d ON d.did = a.did WHERE a.did = $1",
            &[&encoding::did_bytes(did)],
        )
        .await?;
    let Some(row) = row else {
        let (_, live) = source(app).await?;
        return Ok(if live {
            message(StatusCode::NOT_FOUND, format!("DID not registered: {did}"))
        } else {
            message(
                StatusCode::SERVICE_UNAVAILABLE,
                "lodestar is still copying the directory".into(),
            )
        });
    };
    if row.get::<_, i16>(0) != crate::db::STATUS_ACTIVE {
        return Ok(message(
            StatusCode::NOT_FOUND,
            format!("DID not available: {did}"),
        ));
    }
    let parts = match row.get::<_, Option<String>>(4) {
        Some(doc) => serde_json::from_str(&doc)?,
        None => rebuilt_parts(
            row.get::<_, Option<String>>(1).as_deref(),
            row.get::<_, Option<String>>(2).as_deref(),
            row.get::<_, Option<Vec<u8>>>(3).as_deref(),
        ),
    };
    Ok((
        [(header::CONTENT_TYPE, "application/did+ld+json")],
        document(did, &parts).to_string(),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_account_as_the_directory_shows_it() {
        let key = "did:key:zQ3shQo6TF2moaqMTrUZEM1jeuYRQXeHEx4evX9751y2qPqRA";
        let parts = rebuilt_parts(
            Some("bsky.app"),
            Some("https://puffball.us-east.host.bsky.network"),
            Some(&encoding::key_bytes(key)),
        );
        // plc.directory's answer for this account, 16 October 2026.
        let expected: Value = serde_json::from_str(r##"{"@context":["https://www.w3.org/ns/did/v1","https://w3id.org/security/multikey/v1","https://w3id.org/security/suites/secp256k1-2019/v1"],"id":"did:plc:z72i7hdynmk6r22z27h6tvur","alsoKnownAs":["at://bsky.app"],"verificationMethod":[{"id":"did:plc:z72i7hdynmk6r22z27h6tvur#atproto","type":"Multikey","controller":"did:plc:z72i7hdynmk6r22z27h6tvur","publicKeyMultibase":"zQ3shQo6TF2moaqMTrUZEM1jeuYRQXeHEx4evX9751y2qPqRA"}],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://puffball.us-east.host.bsky.network"}]}"##).unwrap();
        assert_eq!(
            document("did:plc:z72i7hdynmk6r22z27h6tvur", &parts),
            expected
        );
    }

    #[test]
    fn a_labeler_as_the_directory_shows_it() {
        let parts = json!({
            "alsoKnownAs": ["at://moderation.bsky.app"],
            "verificationMethods": {
                "atproto": "did:key:zQ3shoG4QW9B3zvKSSiRwwc1De7MFQLNBT9A71gr12GKwMgHu",
                "atproto_label": "did:key:zQ3shmV1BNcX17coaDbfen6zArEad6SCLT3jVWCbC6Y9iinTa",
            },
            "services": {
                "atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": "https://inkcap.us-east.host.bsky.network"},
                "atproto_labeler": {"type": "AtprotoLabeler", "endpoint": "https://mod.bsky.app"},
            },
        });
        let doc = document("did:plc:ar7c4by46qjdydhdevvrndac", &parts);
        // plc.directory's answer, 16 October 2026, compared as sets: a stored
        // document keeps its entries in name order, the directory in the
        // operation's order.
        let expected: Value = serde_json::from_str(r##"{"@context":["https://www.w3.org/ns/did/v1","https://w3id.org/security/multikey/v1","https://w3id.org/security/suites/secp256k1-2019/v1"],"id":"did:plc:ar7c4by46qjdydhdevvrndac","alsoKnownAs":["at://moderation.bsky.app"],"verificationMethod":[{"id":"did:plc:ar7c4by46qjdydhdevvrndac#atproto","type":"Multikey","controller":"did:plc:ar7c4by46qjdydhdevvrndac","publicKeyMultibase":"zQ3shoG4QW9B3zvKSSiRwwc1De7MFQLNBT9A71gr12GKwMgHu"},{"id":"did:plc:ar7c4by46qjdydhdevvrndac#atproto_label","type":"Multikey","controller":"did:plc:ar7c4by46qjdydhdevvrndac","publicKeyMultibase":"zQ3shmV1BNcX17coaDbfen6zArEad6SCLT3jVWCbC6Y9iinTa"}],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://inkcap.us-east.host.bsky.network"},{"id":"#atproto_labeler","type":"AtprotoLabeler","serviceEndpoint":"https://mod.bsky.app"}]}"##).unwrap();
        let sorted = |v: &Value, k: &str| {
            let mut a = v[k].as_array().unwrap().clone();
            a.sort_by_key(|x| x["id"].as_str().unwrap().to_owned());
            a
        };
        for k in ["verificationMethod", "service"] {
            assert_eq!(sorted(&doc, k), sorted(&expected, k), "{k}");
        }
        for k in ["@context", "id", "alsoKnownAs"] {
            assert_eq!(doc[k], expected[k], "{k}");
        }
    }

    #[test]
    fn contexts_follow_the_key_types() {
        assert_eq!(
            key_context("did:key:zDnaerDaTF5BXEavCrfRZEk316dpbLsfPDZ3WJ5hRTPFU2169"),
            Some("https://w3id.org/security/suites/ecdsa-2019/v1")
        );
        assert_eq!(key_context("not a key"), None);
        let doc = document(
            "did:plc:x",
            &json!({"alsoKnownAs": [], "verificationMethods": {}, "services": {}}),
        );
        assert_eq!(doc["@context"].as_array().unwrap().len(), 2);
        assert_eq!(doc["verificationMethod"], json!([]));
        assert_eq!(doc["service"], json!([]));
    }
}
