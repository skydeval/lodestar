//! Handle syntax, per the ATProto handle specification.

/// TLDs under which a handle may never resolve.
const DISALLOWED_TLDS: &[&str] = &["alt", "arpa", "example", "internal", "invalid", "local", "localhost", "onion", "test"];

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

/// True for handles no request is ever made for.
pub fn never_resolves(handle: &str) -> bool {
    let tld = handle.rsplit('.').next().unwrap_or("");
    DISALLOWED_TLDS.contains(&tld)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_and_lowercases() {
        assert_eq!(normalize("Alice.Bsky.Social").as_deref(), Some("alice.bsky.social"));
        assert_eq!(normalize("a-b.example.com").as_deref(), Some("a-b.example.com"));
    }

    #[test]
    fn rejects_bad_syntax() {
        for bad in ["", "nodot", "a..b", "-a.com", "a-.com", "a.1com", "a_b.com", "é.com", "a.com."] {
            assert_eq!(normalize(bad), None, "{bad}");
        }
        assert_eq!(normalize(&format!("{}.com", "a".repeat(64))), None);
    }

    #[test]
    fn reserved_tlds_never_resolve() {
        assert!(never_resolves("handle.invalid"));
        assert!(never_resolves("me.test"));
        assert!(never_resolves("box.local"));
        assert!(!never_resolves("alice.bsky.social"));
    }
}
