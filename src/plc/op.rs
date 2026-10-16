//! One line of the directory's sequenced export, reduced to what lodestar
//! keeps: the handle claim, the PDS and the signing key, and the DID
//! document's parts when those three cannot rebuild them.

use anyhow::{Context, Result, anyhow};
use chrono::DateTime;
use serde_json::{Value, json};

use crate::{encoding, handle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A `plc_operation` or a legacy `create`: carries the full state.
    State,
    Tombstone,
    /// A type lodestar does not know. It changes nothing, but it is part
    /// of the chain, so later operations can follow it.
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
    pub did: String,
    pub seq: i64,
    pub cid: Vec<u8>,
    /// The CID this operation follows, by value. None for a genesis.
    pub prev: Option<Vec<u8>>,
    /// As the directory wrote it, RFC 3339.
    pub created_at: String,
    pub created_ms: i64,
    pub kind: Kind,
    pub claim: Option<String>,
    pub pds: Option<String>,
    pub key: Option<Vec<u8>>,
    /// The DID document's parts as the directory gives them, when the
    /// claim, PDS and key cannot rebuild them exactly. None for most
    /// operations.
    pub doc: Option<Parts>,
}

/// An operation's DID document parts, kept because the stored columns
/// cannot rebuild them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parts {
    /// `alsoKnownAs`, `verificationMethods` and `services`, JSON.
    pub json: String,
    /// Only the names (`alsoKnownAs`) differ: more than one, or one kept
    /// in another form. Keys and services are rebuilt exactly. Such parts
    /// are kept only for identities with a repo; spam registrations carry
    /// tens of millions of them.
    pub names_only: bool,
}

/// Text Postgres can store. JSON may carry a NUL, which it cannot.
fn storable(s: &str) -> Option<String> {
    (!s.contains('\0')).then(|| s.to_owned())
}

/// The DID document parts a stored claim, PDS and key stand for: what an
/// identity's document shows when it has no parts of its own.
pub fn rebuilt_parts(claim: Option<&str>, pds: Option<&str>, key: Option<&[u8]>) -> Value {
    json!({
        "alsoKnownAs": claim.map(|c| vec![format!("at://{c}")]).unwrap_or_default(),
        "verificationMethods": match key.and_then(encoding::key_text) {
            Some(k) => json!({ "atproto": k }),
            None => json!({}),
        },
        "services": match pds {
            Some(p) => json!({
                "atproto_pds": { "type": "AtprotoPersonalDataServer", "endpoint": p }
            }),
            None => json!({}),
        },
    })
}

/// The parts as given, kept only when the stored columns rebuild
/// something else.
fn own_parts(
    given: Value,
    claim: Option<&str>,
    pds: Option<&str>,
    key: Option<&[u8]>,
) -> Option<Parts> {
    let rebuilt = rebuilt_parts(claim, pds, key);
    (given != rebuilt).then(|| Parts {
        names_only: given["verificationMethods"] == rebuilt["verificationMethods"]
            && given["services"] == rebuilt["services"],
        json: given.to_string(),
    })
}

/// The directory reads a legacy `create` as an operation with these
/// parts; its prefixes are added as it adds them.
fn legacy_parts(op: &Value) -> Value {
    let field = |name: &str| op.get(name).and_then(Value::as_str).unwrap_or("");
    let handle = field("handle");
    let name = if handle.starts_with("at://") {
        handle.to_owned()
    } else {
        format!(
            "at://{}",
            handle
                .replacen("http://", "", 1)
                .replacen("https://", "", 1)
        )
    };
    let service = field("service");
    let endpoint = if service.starts_with("http://") || service.starts_with("https://") {
        service.to_owned()
    } else {
        format!("https://{service}")
    };
    json!({
        "alsoKnownAs": [name],
        "verificationMethods": { "atproto": field("signingKey") },
        "services": {
            "atproto_pds": { "type": "AtprotoPersonalDataServer", "endpoint": endpoint }
        },
    })
}

type Derived = (
    Kind,
    Option<String>,
    Option<String>,
    Option<Vec<u8>>,
    Option<Parts>,
);

/// The claim, PDS, key and document parts an operation sets.
fn derive(op: &Value) -> Derived {
    let text = |v: Option<&Value>| v.and_then(Value::as_str).and_then(storable);
    match op.get("type").and_then(Value::as_str).unwrap_or("") {
        "plc_tombstone" => (Kind::Tombstone, None, None, None, None),
        "create" => {
            let claim = op
                .get("handle")
                .and_then(Value::as_str)
                .and_then(handle::normalize);
            let pds = text(op.get("service"));
            let key = text(op.get("signingKey")).map(|k| encoding::key_bytes(&k));
            let doc = own_parts(
                legacy_parts(op),
                claim.as_deref(),
                pds.as_deref(),
                key.as_deref(),
            );
            (Kind::State, claim, pds, key, doc)
        }
        "plc_operation" => {
            // The first valid at:// entry is the claim.
            let claim = op
                .get("alsoKnownAs")
                .and_then(Value::as_array)
                .and_then(|aka| {
                    aka.iter()
                        .filter_map(|e| e.as_str().and_then(|s| s.strip_prefix("at://")))
                        .find_map(handle::normalize)
                });
            let pds = op.pointer("/services/atproto_pds").and_then(|s| {
                (s.get("type").and_then(Value::as_str) == Some("AtprotoPersonalDataServer"))
                    .then(|| text(s.get("endpoint")))
                    .flatten()
            });
            let key =
                text(op.pointer("/verificationMethods/atproto")).map(|k| encoding::key_bytes(&k));
            let part = |name: &str, empty: Value| op.get(name).cloned().unwrap_or(empty);
            let doc = own_parts(
                json!({
                    "alsoKnownAs": part("alsoKnownAs", json!([])),
                    "verificationMethods": part("verificationMethods", json!({})),
                    "services": part("services", json!({})),
                }),
                claim.as_deref(),
                pds.as_deref(),
                key.as_deref(),
            );
            (Kind::State, claim, pds, key, doc)
        }
        _ => (Kind::Other, None, None, None, None),
    }
}

/// Parse one export line.
pub fn parse_line(line: &str) -> Result<Op> {
    let v: Value = serde_json::from_str(line).context("export line is not JSON")?;
    let field = |name: &str| {
        v.get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("export line has no {name}"))
    };
    let did = field("did")?;
    let cid = field("cid")?;
    let created_at = field("createdAt")?;
    let seq = v
        .get("seq")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("export line has no seq"))?;
    let created_ms = DateTime::parse_from_rfc3339(created_at)
        .with_context(|| format!("bad createdAt {created_at}"))?
        .timestamp_millis();
    let op = v
        .get("operation")
        .ok_or_else(|| anyhow!("export line has no operation"))?;
    if did.contains('\0') {
        return Err(anyhow!("export line has a NUL in its did"));
    }
    let (kind, claim, pds, key, doc) = derive(op);
    Ok(Op {
        did: did.to_owned(),
        seq,
        cid: encoding::cid_bytes(cid),
        prev: op
            .get("prev")
            .and_then(Value::as_str)
            .map(encoding::cid_bytes),
        created_at: created_at.to_owned(),
        created_ms,
        kind,
        claim,
        pds,
        key,
        doc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(operation: &str) -> String {
        format!(
            r#"{{"type":"sequenced_op","did":"did:plc:ewvi7nxzyoun6zhxrhs64oiz","cid":"bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqcsgz7soitae","seq":7,"createdAt":"2023-04-12T04:53:57.057Z","operation":{operation}}}"#
        )
    }

    #[test]
    fn plc_operation_fields() {
        let op = parse_line(&line(
            r#"{"type":"plc_operation","prev":null,"alsoKnownAs":["https://x.example","at://not a handle","at://Alice.Example.com","at://second.example.com"],"services":{"atproto_pds":{"type":"AtprotoPersonalDataServer","endpoint":"https://pds.example.com"}},"verificationMethods":{"atproto":"did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF"}}"#,
        ))
        .unwrap();
        assert_eq!(op.kind, Kind::State);
        assert_eq!(op.seq, 7);
        assert_eq!(op.prev, None);
        assert_eq!(op.claim.as_deref(), Some("alice.example.com"));
        assert_eq!(op.pds.as_deref(), Some("https://pds.example.com"));
        assert_eq!(
            encoding::key_text(op.key.as_ref().unwrap()).as_deref(),
            Some("did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF")
        );
    }

    #[test]
    fn no_valid_handle_is_no_claim() {
        let op = parse_line(&line(
            r#"{"type":"plc_operation","prev":"bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqcsgz7soitae","alsoKnownAs":["some other data","at://nodot"],"services":{"atproto_pds":{"type":"SomethingElse","endpoint":"https://pds.example.com"}},"verificationMethods":{}}"#,
        ))
        .unwrap();
        assert_eq!(op.claim, None);
        assert_eq!(op.pds, None);
        assert_eq!(op.key, None);
        assert_eq!(op.prev.as_deref(), Some(&op.cid[..]));
    }

    #[test]
    fn legacy_create_and_tombstone() {
        let op = parse_line(&line(
            r#"{"type":"create","prev":null,"handle":"Old.Bsky.Social","service":"https://bsky.social","signingKey":"did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF","recoveryKey":"did:key:zX"}"#,
        ))
        .unwrap();
        assert_eq!(op.claim.as_deref(), Some("old.bsky.social"));
        assert_eq!(op.pds.as_deref(), Some("https://bsky.social"));
        assert!(op.key.is_some());

        let op = parse_line(&line(r#"{"type":"plc_tombstone","prev":"zabc"}"#)).unwrap();
        assert_eq!(op.kind, Kind::Tombstone);
        assert_eq!((op.claim, op.pds, op.key), (None, None, None));
    }

    #[test]
    fn unknown_types_are_kept_and_bad_lines_fail() {
        let op = parse_line(&line(r#"{"type":"something_new","prev":null}"#)).unwrap();
        assert_eq!(op.kind, Kind::Other);
        assert_eq!(op.seq, 7);
        assert!(parse_line("not json").is_err());
        assert!(parse_line(r#"{"did":"did:plc:x"}"#).is_err());
    }

    #[test]
    fn document_parts_are_kept_only_when_the_columns_cannot_rebuild_them() {
        let key = "did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF";
        // An ordinary account: rebuilt from its claim, PDS and key.
        let plain = format!(
            r#"{{"type":"plc_operation","prev":null,"alsoKnownAs":["at://alice.example.com"],"rotationKeys":["{key}"],"services":{{"atproto_pds":{{"type":"AtprotoPersonalDataServer","endpoint":"https://pds.example.com"}}}},"verificationMethods":{{"atproto":"{key}"}}}}"#
        );
        assert_eq!(parse_line(&line(&plain)).unwrap().doc, None);
        // A labeler: a second key and a second service.
        let labeler = format!(
            r#"{{"type":"plc_operation","prev":null,"alsoKnownAs":["at://mod.example.com"],"services":{{"atproto_pds":{{"type":"AtprotoPersonalDataServer","endpoint":"https://pds.example.com"}},"atproto_labeler":{{"type":"AtprotoLabeler","endpoint":"https://mod.example.com"}}}},"verificationMethods":{{"atproto":"{key}","atproto_label":"{key}"}}}}"#
        );
        let op = parse_line(&line(&labeler)).unwrap();
        assert_eq!(op.pds.as_deref(), Some("https://pds.example.com"));
        let parts = op.doc.unwrap();
        assert!(!parts.names_only, "a labeler's parts are kept for everyone");
        let doc: Value = serde_json::from_str(&parts.json).unwrap();
        assert_eq!(
            doc["services"]["atproto_labeler"]["endpoint"],
            "https://mod.example.com"
        );
        assert_eq!(doc["verificationMethods"]["atproto_label"], key);
        assert_eq!(doc["alsoKnownAs"], json!(["at://mod.example.com"]));
        // A name kept in another form, and more than one name.
        for aka in [
            r#"["at://Alice.Example.com"]"#,
            r#"["at://alice.example.com","https://alice.example.com"]"#,
            r#"["at://nodot"]"#,
        ] {
            let op = parse_line(&line(&format!(
                r#"{{"type":"plc_operation","prev":null,"alsoKnownAs":{aka},"services":{{}},"verificationMethods":{{}}}}"#
            )))
            .unwrap();
            let parts = op.doc.expect(aka);
            assert!(parts.names_only, "{aka}");
            let doc: Value = serde_json::from_str(&parts.json).unwrap();
            assert_eq!(
                doc["alsoKnownAs"],
                serde_json::from_str::<Value>(aka).unwrap()
            );
        }
        // A legacy create as the directory reads it.
        let create = format!(
            r#"{{"type":"create","prev":null,"handle":"old.example.com","service":"https://pds.example.com","signingKey":"{key}","recoveryKey":"{key}"}}"#
        );
        assert_eq!(parse_line(&line(&create)).unwrap().doc, None);
        let create = format!(
            r#"{{"type":"create","prev":null,"handle":"Old.Example.com","service":"pds.example.com","signingKey":"{key}","recoveryKey":"{key}"}}"#
        );
        let parts = parse_line(&line(&create)).unwrap().doc.unwrap();
        assert!(!parts.names_only, "its PDS is kept in another form too");
        let doc: Value = serde_json::from_str(&parts.json).unwrap();
        assert_eq!(doc["alsoKnownAs"], json!(["at://Old.Example.com"]));
        assert_eq!(
            doc["services"]["atproto_pds"]["endpoint"],
            "https://pds.example.com"
        );
        // Tombstones and unknown types have no parts.
        assert_eq!(
            parse_line(&line(r#"{"type":"plc_tombstone","prev":"zabc"}"#))
                .unwrap()
                .doc,
            None
        );
    }

    #[test]
    fn nul_bytes_are_dropped() {
        let op = parse_line(&line(
            r#"{"type":"plc_operation","prev":null,"alsoKnownAs":[],"services":{"atproto_pds":{"type":"AtprotoPersonalDataServer","endpoint":"https://a\u0000b"}},"verificationMethods":{"atproto":"k\u0000"}}"#,
        ))
        .unwrap();
        assert_eq!(op.pds, None);
        assert_eq!(op.key, None);
    }
}
