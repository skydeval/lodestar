//! One line of the directory's sequenced export, reduced to what lodestar
//! keeps: the handle claim, the PDS and the signing key.

use anyhow::{Context, Result, anyhow};
use chrono::DateTime;
use serde_json::Value;

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
}

/// Text Postgres can store. JSON may carry a NUL, which it cannot.
fn storable(s: &str) -> Option<String> {
    (!s.contains('\0')).then(|| s.to_owned())
}

/// The claim, PDS and key an operation sets.
fn derive(op: &Value) -> (Kind, Option<String>, Option<String>, Option<Vec<u8>>) {
    let text = |v: Option<&Value>| v.and_then(Value::as_str).and_then(storable);
    match op.get("type").and_then(Value::as_str).unwrap_or("") {
        "plc_tombstone" => (Kind::Tombstone, None, None, None),
        "create" => {
            let claim = op
                .get("handle")
                .and_then(Value::as_str)
                .and_then(handle::normalize);
            let pds = text(op.get("service"));
            let key = text(op.get("signingKey")).map(|k| encoding::key_bytes(&k));
            (Kind::State, claim, pds, key)
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
            (Kind::State, claim, pds, key)
        }
        _ => (Kind::Other, None, None, None),
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
    let (kind, claim, pds, key) = derive(op);
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
    fn nul_bytes_are_dropped() {
        let op = parse_line(&line(
            r#"{"type":"plc_operation","prev":null,"alsoKnownAs":[],"services":{"atproto_pds":{"type":"AtprotoPersonalDataServer","endpoint":"https://a\u0000b"}},"verificationMethods":{"atproto":"k\u0000"}}"#,
        ))
        .unwrap();
        assert_eq!(op.pds, None);
        assert_eq!(op.key, None);
    }
}
