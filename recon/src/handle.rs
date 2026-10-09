//! Handle syntax, per the ATProto handle specification.

/// TLDs under which a handle may never resolve.
const DISALLOWED_TLDS: &[&str] = &[
    "alt",
    "arpa",
    "example",
    "internal",
    "invalid",
    "local",
    "localhost",
    "onion",
];

/// Lowercases `s` and returns it if it is a syntactically valid handle.
pub fn normalize(s: &str) -> Option<String> {
    if s.is_empty() || s.len() > 253 || !s.is_ascii() {
        return None;
    }
    let h = s.to_ascii_lowercase();
    let labels: Vec<&str> = h.split('.').collect();
    if labels.len() < 2 {
        return None;
    }
    for l in &labels {
        let b = l.as_bytes();
        if b.is_empty() || b.len() > 63 {
            return None;
        }
        if b[0] == b'-' || b[b.len() - 1] == b'-' {
            return None;
        }
        if !b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'-') {
            return None;
        }
    }
    let tld = labels[labels.len() - 1];
    if tld.as_bytes()[0].is_ascii_digit() {
        return None;
    }
    Some(h)
}

/// True for handles the design never sends a request for (§5.2).
pub fn never_resolves(handle: &str) -> bool {
    if handle == "handle.invalid" {
        return true;
    }
    let tld = handle.rsplit('.').next().unwrap_or("");
    tld == "test" || DISALLOWED_TLDS.contains(&tld)
}
