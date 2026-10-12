//! The compact, lossless encodings the schema stores: DIDs, signing keys
//! and CIDs as bytes.

const B32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const B58: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

const DID_PLC: u8 = 0;
const DID_OTHER: u8 = 1;
/// A signing key that is not a decodable `did:key:z…` is kept as text
/// behind this byte. No key multicodec starts with it.
const KEY_TEXT: u8 = 0;

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

/// Lowercase base32 without padding to bytes. Trailing bits must be zero.
pub fn b32_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes() {
        let v = B32.iter().position(|x| *x == c)? as u32;
        acc = (acc << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    if acc != 0 {
        return None;
    }
    Some(out)
}

/// Base58 (Bitcoin alphabet) to bytes.
pub fn b58_decode(s: &str) -> Option<Vec<u8>> {
    let mut num: Vec<u8> = Vec::with_capacity(40);
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

/// Bytes to base58 (Bitcoin alphabet).
pub fn b58_encode(data: &[u8]) -> String {
    let zeros = data.iter().take_while(|b| **b == 0).count();
    let mut digits: Vec<u8> = Vec::with_capacity(data.len() * 138 / 100 + 1);
    for b in &data[zeros..] {
        let mut carry = *b as u32;
        for d in digits.iter_mut() {
            let v = (*d as u32) * 256 + carry;
            *d = (v % 58) as u8;
            carry = v / 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(zeros + digits.len());
    out.extend(std::iter::repeat_n('1', zeros));
    out.extend(digits.iter().rev().map(|d| B58[*d as usize] as char));
    out
}

/// A `did:plc` is one tag byte and its 15 bytes. Any other DID is a tag
/// byte and its text.
pub fn did_bytes(did: &str) -> Vec<u8> {
    if let Some(id) = did.strip_prefix("did:plc:") {
        if id.len() == 24 {
            if let Some(raw) = b32_decode(id) {
                let mut out = Vec::with_capacity(16);
                out.push(DID_PLC);
                out.extend_from_slice(&raw);
                return out;
            }
        }
    }
    let mut out = Vec::with_capacity(1 + did.len());
    out.push(DID_OTHER);
    out.extend_from_slice(did.as_bytes());
    out
}

/// The DID a stored value stands for.
pub fn did_text(bytes: &[u8]) -> Option<String> {
    match bytes.split_first()? {
        (&DID_PLC, raw) if raw.len() == 15 => Some(format!("did:plc:{}", b32_encode(raw))),
        (&DID_OTHER, text) => String::from_utf8(text.to_vec()).ok(),
        _ => None,
    }
}

/// A signing key as stored: the multicodec-prefixed key bytes of a
/// `did:key:z…`, or the text as given when it is not one.
pub fn key_bytes(key: &str) -> Vec<u8> {
    if let Some(raw) = key.strip_prefix("did:key:z").and_then(b58_decode) {
        if raw.first().is_some_and(|b| *b != KEY_TEXT) && format!("did:key:z{}", b58_encode(&raw)) == key {
            return raw;
        }
    }
    let mut out = Vec::with_capacity(1 + key.len());
    out.push(KEY_TEXT);
    out.extend_from_slice(key.as_bytes());
    out
}

/// The key string a stored value stands for.
pub fn key_text(bytes: &[u8]) -> Option<String> {
    match bytes.split_first()? {
        (&KEY_TEXT, text) => String::from_utf8(text.to_vec()).ok(),
        _ => Some(format!("did:key:z{}", b58_encode(bytes))),
    }
}

/// A CID's bytes, whichever text form it was written in. The directory
/// publishes base32 and accepts a `prev` in base58btc as the same CID, so
/// CIDs are compared by value. Text that is neither is kept as it is,
/// which equals no decoded CID.
pub fn cid_bytes(s: &str) -> Vec<u8> {
    let decoded = match s.split_at_checked(1) {
        Some(("b", rest)) => b32_decode(rest),
        Some(("z", rest)) => b58_decode(rest),
        _ => None,
    };
    decoded.filter(|d| !d.is_empty()).unwrap_or_else(|| s.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn did_plc_round_trips_in_sixteen_bytes() {
        let did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz";
        let b = did_bytes(did);
        assert_eq!(b.len(), 16);
        assert_eq!(b[0], 0);
        assert_eq!(did_text(&b).as_deref(), Some(did));
    }

    #[test]
    fn other_dids_keep_their_text() {
        for did in ["did:web:example.com", "did:plc:short", "did:plc:EWVI7NXZYOUN6ZHXRHS64OIZ"] {
            let b = did_bytes(did);
            assert_eq!(b[0], 1);
            assert_eq!(did_text(&b).as_deref(), Some(did));
        }
    }

    #[test]
    fn keys_round_trip() {
        let k = "did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF";
        let b = key_bytes(k);
        assert_ne!(b[0], 0);
        assert!(b.len() < k.len());
        assert_eq!(key_text(&b).as_deref(), Some(k));
        for odd in ["not a key", "did:key:z0OIl", "did:key:z"] {
            let b = key_bytes(odd);
            assert_eq!(b[0], 0);
            assert_eq!(key_text(&b).as_deref(), Some(odd));
        }
    }

    #[test]
    fn base58_round_trips() {
        for data in [&b""[..], &[0, 0, 1], &[255; 40], b"hello world"] {
            assert_eq!(b58_decode(&b58_encode(data)).as_deref(), Some(data));
        }
    }

    #[test]
    fn cids_compare_by_value() {
        let b32 = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqcsgz7soitae";
        let raw = b32_decode(&b32[1..]).unwrap();
        let b58 = format!("z{}", b58_encode(&raw));
        assert_ne!(b32, b58);
        assert_eq!(cid_bytes(b32), cid_bytes(&b58));
        assert_eq!(cid_bytes(b32), raw);
        assert_ne!(cid_bytes("something else"), raw);
    }
}
