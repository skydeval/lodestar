//! Build compact-encoded copies of the loaded tables so each §4.5 candidate
//! can be measured on real data.

use std::collections::HashMap;
use std::fmt::Write as _;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use tokio_postgres::Client;

use crate::{handle, verify};

const TABLES: &str = r#"
DROP TABLE IF EXISTS c_accounts, c_history, c_pds, c_domains;
CREATE TABLE c_pds (id integer PRIMARY KEY, endpoint text NOT NULL);
CREATE TABLE c_domains (id integer PRIMARY KEY, domain text NOT NULL, n bigint NOT NULL);
CREATE TABLE c_accounts (
    did                  bytea NOT NULL,
    method               smallint NOT NULL,
    pds_id               integer,
    signing_key          bytea,
    handle_claim         text,
    domain_id            integer,
    handle_status        smallint NOT NULL,
    handle_checked_at    timestamptz,
    handle_failing_since timestamptz,
    status               smallint NOT NULL,
    created_at           timestamptz NOT NULL,
    updated_at           timestamptz NOT NULL
);
CREATE TABLE c_history (
    did          bytea NOT NULL,
    seq          integer NOT NULL,
    changed_at   timestamptz NOT NULL,
    handle_claim text,
    pds_id       integer
);
"#;

const B32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const B58: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// did:plc → 0x00 + 15 bytes. Anything else → 0x01 + the DID text.
pub fn did_bytes(did: &str) -> Vec<u8> {
    if let Some(id) = did.strip_prefix("did:plc:") {
        if id.len() == 24 {
            let mut out = Vec::with_capacity(16);
            out.push(0u8);
            let (mut acc, mut bits) = (0u32, 0u32);
            let mut ok = true;
            for c in id.bytes() {
                let Some(v) = B32.iter().position(|x| *x == c) else {
                    ok = false;
                    break;
                };
                acc = (acc << 5) | v as u32;
                bits += 5;
                if bits >= 8 {
                    bits -= 8;
                    out.push((acc >> bits) as u8);
                    acc &= (1 << bits) - 1;
                }
            }
            if ok && out.len() == 16 {
                return out;
            }
        }
    }
    let mut out = Vec::with_capacity(1 + did.len());
    out.push(1u8);
    out.extend_from_slice(did.as_bytes());
    out
}

/// did:key:z… → the multicodec-prefixed key bytes. None if it does not decode.
pub fn key_bytes(key: &str) -> Option<Vec<u8>> {
    b58_decode(key.strip_prefix("did:key:z")?)
}

/// Lowercase RFC 4648 base32 without padding.
pub fn b32_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 8 / 5 + 1);
    let (mut acc, mut bits) = (0u32, 0u32);
    for b in data {
        acc = (acc << 8) | *b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(B32[((acc >> bits) & 31) as usize] as char);
        }
        acc &= (1 << bits) - 1;
    }
    if bits > 0 {
        out.push(B32[((acc << (5 - bits)) & 31) as usize] as char);
    }
    out
}

/// A CID string in the directory's own form: multibase base32 (`b…`).
/// The directory accepts a `prev` written in base58btc (`z…`) as the same
/// CID, so comparing the raw strings is wrong.
pub fn normalize_cid(s: &str) -> String {
    match s.strip_prefix('z').and_then(b58_decode) {
        Some(bytes) => format!("b{}", b32_encode(&bytes)),
        None => s.to_owned(),
    }
}

/// Base58 (Bitcoin alphabet) to bytes.
pub fn b58_decode(s: &str) -> Option<Vec<u8>> {
    let mut num: Vec<u8> = Vec::with_capacity(40); // big-endian base-256
    for c in s.bytes() {
        let mut carry = B58.iter().position(|x| *x == c)? as u32;
        for b in num.iter_mut().rev() {
            let v = (*b as u32) * 58 + carry;
            *b = (v & 0xff) as u8;
            carry = v >> 8;
        }
        while carry > 0 {
            num.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let zeros = s.bytes().take_while(|c| *c == b'1').count();
    let mut out = vec![0u8; zeros];
    out.extend_from_slice(&num);
    Some(out)
}

fn hex(out: &mut String, b: &[u8]) {
    out.push_str("\\x");
    for x in b {
        let _ = write!(out, "{x:02x}");
    }
}

fn csv_text(out: &mut String, s: Option<&str>) {
    if let Some(s) = s {
        out.push('"');
        for ch in s.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
    }
}

fn csv_opt<T: std::fmt::Display>(out: &mut String, v: Option<T>) {
    if let Some(v) = v {
        let _ = write!(out, "{v}");
    }
}

pub async fn build(read: &Client, write: &Client) -> Result<()> {
    write.batch_execute(TABLES).await.context("creating compact tables")?;

    // Intern PDS endpoints from both tables.
    let mut pds: HashMap<String, i32> = HashMap::new();
    for r in read
        .query(
            "SELECT pds_endpoint FROM accounts WHERE pds_endpoint IS NOT NULL \
             UNION SELECT pds_endpoint FROM identity_history WHERE pds_endpoint IS NOT NULL",
            &[],
        )
        .await?
    {
        let e: String = r.get(0);
        let id = pds.len() as i32 + 1;
        pds.insert(e, id);
    }
    {
        let ids: Vec<i32> = pds.values().copied().collect();
        let eps: Vec<&str> = pds.keys().map(String::as_str).collect();
        write
            .execute(
                "INSERT INTO c_pds (id, endpoint) SELECT a, b FROM UNNEST($1::int4[], $2::text[]) AS t(a, b)",
                &[&ids, &eps],
            )
            .await?;
    }
    eprintln!("pds endpoints interned: {}", pds.len());

    let mut domains: HashMap<String, (i32, i64)> = HashMap::new();
    let (mut key_ok, mut key_bad, mut rows_a) = (0i64, 0i64, 0i64);

    // accounts
    {
        let params: Vec<String> = Vec::new();
        let rows = read
            .query_raw(
                "SELECT did, method, pds_endpoint, signing_key, handle_claim, handle_status, handle_checked_at::text, \
                 handle_failing_since::text, status, created_at::text, updated_at::text FROM accounts",
                params,
            )
            .await?;
        tokio::pin!(rows);
        let sink = write
            .copy_in::<_, Bytes>(
                "COPY c_accounts (did, method, pds_id, signing_key, handle_claim, domain_id, handle_status, handle_checked_at, \
                 handle_failing_since, status, created_at, updated_at) FROM STDIN (FORMAT csv)",
            )
            .await?;
        tokio::pin!(sink);
        let mut buf = String::with_capacity(1 << 20);
        while let Some(r) = rows.try_next().await? {
            rows_a += 1;
            let did: String = r.get(0);
            let method: i16 = r.get(1);
            let p: Option<String> = r.get(2);
            let k: Option<String> = r.get(3);
            let h: Option<String> = r.get(4);
            let hs: i16 = r.get(5);
            let hc: Option<String> = r.get(6);
            let hf: Option<String> = r.get(7);
            let st: i16 = r.get(8);
            let ca: String = r.get(9);
            let ua: String = r.get(10);
            hex(&mut buf, &did_bytes(&did));
            let _ = write!(buf, ",{method},");
            csv_opt(&mut buf, p.as_ref().and_then(|p| pds.get(p)).copied());
            buf.push(',');
            if let Some(k) = &k {
                match key_bytes(k) {
                    Some(b) => {
                        key_ok += 1;
                        hex(&mut buf, &b);
                    }
                    None => {
                        key_bad += 1;
                        hex(&mut buf, k.as_bytes());
                    }
                }
            }
            buf.push(',');
            csv_text(&mut buf, h.as_deref());
            buf.push(',');
            let dom = h.as_deref().filter(|h| !handle::never_resolves(h)).map(verify::registrable);
            if let Some(d) = dom {
                let next = domains.len() as i32 + 1;
                let e = domains.entry(d).or_insert((next, 0));
                e.1 += 1;
                let _ = write!(buf, "{}", e.0);
            }
            let _ = write!(buf, ",{hs},");
            csv_opt(&mut buf, hc);
            buf.push(',');
            csv_opt(&mut buf, hf);
            let _ = writeln!(buf, ",{st},{ca},{ua}");
            if buf.len() >= (1 << 20) {
                sink.send(Bytes::from(std::mem::take(&mut buf))).await?;
            }
        }
        if !buf.is_empty() {
            sink.send(Bytes::from(buf)).await?;
        }
        sink.finish().await?;
    }
    eprintln!("accounts copied: {rows_a}; keys decoded {key_ok}, not decoded {key_bad}");

    {
        let all: Vec<(&String, &(i32, i64))> = domains.iter().collect();
        for chunk in all.chunks(20_000) {
            let ids: Vec<i32> = chunk.iter().map(|x| x.1 .0).collect();
            let ds: Vec<&str> = chunk.iter().map(|x| x.0.as_str()).collect();
            let ns: Vec<i64> = chunk.iter().map(|x| x.1 .1).collect();
            write
                .execute(
                    "INSERT INTO c_domains (id, domain, n) SELECT a, b, c FROM UNNEST($1::int4[], $2::text[], $3::int8[]) AS t(a, b, c)",
                    &[&ids, &ds, &ns],
                )
                .await?;
        }
    }

    // history
    let mut rows_h = 0i64;
    {
        let params: Vec<String> = Vec::new();
        let rows = read
            .query_raw("SELECT did, seq, changed_at::text, handle_claim, pds_endpoint FROM identity_history", params)
            .await?;
        tokio::pin!(rows);
        let sink = write
            .copy_in::<_, Bytes>("COPY c_history (did, seq, changed_at, handle_claim, pds_id) FROM STDIN (FORMAT csv)")
            .await?;
        tokio::pin!(sink);
        let mut buf = String::with_capacity(1 << 20);
        while let Some(r) = rows.try_next().await? {
            rows_h += 1;
            let did: String = r.get(0);
            let seq: i32 = r.get(1);
            let at: String = r.get(2);
            let h: Option<String> = r.get(3);
            let p: Option<String> = r.get(4);
            hex(&mut buf, &did_bytes(&did));
            let _ = write!(buf, ",{seq},{at},");
            csv_text(&mut buf, h.as_deref());
            buf.push(',');
            csv_opt(&mut buf, p.as_ref().and_then(|p| pds.get(p)).copied());
            buf.push('\n');
            if buf.len() >= (1 << 20) {
                sink.send(Bytes::from(std::mem::take(&mut buf))).await?;
            }
        }
        if !buf.is_empty() {
            sink.send(Bytes::from(buf)).await?;
        }
        sink.finish().await?;
    }
    println!(
        "{}",
        serde_json::json!({"accounts": rows_a, "history": rows_h, "pds_endpoints": pds.len(), "domains": domains.len(),
                           "keys_decoded": key_ok, "keys_not_decoded": key_bad})
    );
    Ok(())
}
