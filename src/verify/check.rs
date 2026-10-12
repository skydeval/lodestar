//! Resolving one handle to a DID: the DNS record first, then HTTPS on the
//! handle's own host, behind the address guard. No database here.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hickory_resolver::TokioResolver;
use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig};
use hickory_resolver::name_server::TokioConnectionProvider;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// What a check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The handle resolves to this DID.
    Did(String),
    /// The domain answered, and the handle resolves to nothing.
    Nothing,
    /// Nothing reliable came back. The string says what, for logs and health.
    Inconclusive(&'static str),
}

#[derive(Debug, Clone)]
pub struct Check {
    pub outcome: Outcome,
    /// The host asked lodestar to wait this long.
    pub retry_after: Option<Duration>,
    /// Decided by the DNS record, without an HTTPS request.
    pub by_dns: bool,
    pub took: Duration,
}

pub struct Limits {
    pub dns: Duration,
    pub connect: Duration,
    pub total: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { dns: Duration::from_secs(5), connect: Duration::from_secs(8), total: Duration::from_secs(20) }
    }
}

const BODY_CAP: usize = 4096;
const MAX_REDIRECTS: usize = 3;

/// True for an address a request may be sent to. Handles are untrusted
/// input, and lodestar runs on a private network.
pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(a) => {
            let o = a.octets();
            !(a.is_private()
                || a.is_loopback()
                || a.is_link_local()
                || a.is_broadcast()
                || a.is_documentation()
                || a.is_unspecified()
                || a.is_multicast()
                || o[0] == 0
                || (o[0] == 100 && (o[1] & 0xc0) == 64)
                || (o[0] == 192 && o[1] == 0 && o[2] == 0)
                || (o[0] == 198 && (o[1] & 0xfe) == 18)
                || o[0] >= 240)
        }
        IpAddr::V6(a) => {
            if let Some(v4) = a.to_ipv4_mapped() {
                return is_public(IpAddr::V4(v4));
            }
            let s = a.segments();
            !(a.is_loopback()
                || a.is_unspecified()
                || a.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00
                || (s[0] & 0xffc0) == 0xfe80
                || (s[0] == 0x2001 && s[1] == 0x0db8))
        }
    }
}

pub fn valid_did(s: &str) -> bool {
    if s.len() > 2048 {
        return false;
    }
    let Some(rest) = s.strip_prefix("did:") else { return false };
    let Some((method, id)) = rest.split_once(':') else { return false };
    !method.is_empty()
        && method.bytes().all(|b| b.is_ascii_lowercase())
        && !id.is_empty()
        && !id.ends_with(':')
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b"._:%-".contains(&b))
}

/// The resolver named in config: `system`, or an address.
pub fn build_resolver(spec: &str, limits: &Limits) -> Result<TokioResolver> {
    let mut builder = if spec == "system" {
        TokioResolver::builder_tokio().context("reading the system resolver's configuration")?
    } else {
        let addr: SocketAddr = match spec.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(_) => SocketAddr::new(spec.parse::<IpAddr>().with_context(|| format!("bad resolver address {spec}"))?, 53),
        };
        let group = NameServerConfigGroup::from_ips_clear(&[addr.ip()], addr.port(), true);
        TokioResolver::builder_with_config(ResolverConfig::from_parts(None, vec![], group), TokioConnectionProvider::default())
    };
    let o = builder.options_mut();
    o.timeout = limits.dns;
    o.attempts = 1;
    Ok(builder.build())
}

enum NoAddress {
    /// The name does not exist, or has no address, or only addresses a
    /// request may not be sent to.
    Nothing,
    Failed,
}

/// The addresses to use for a host: IPv4, and IPv6 only when there is no
/// IPv4 one. Non-public addresses are removed.
async fn addresses(resolver: &TokioResolver, host: &str) -> Result<Vec<IpAddr>, NoAddress> {
    let name = format!("{}.", host.trim_end_matches('.'));
    let mut found: Vec<IpAddr> = match resolver.ipv4_lookup(name.as_str()).await {
        Ok(l) => l.iter().map(|a| IpAddr::V4(a.0)).collect(),
        Err(e) if e.is_nx_domain() => return Err(NoAddress::Nothing),
        Err(e) if e.is_no_records_found() => Vec::new(),
        Err(_) => return Err(NoAddress::Failed),
    };
    if found.is_empty() {
        found = match resolver.ipv6_lookup(name.as_str()).await {
            Ok(l) => l.iter().map(|a| IpAddr::V6(a.0)).collect(),
            Err(e) if e.is_nx_domain() || e.is_no_records_found() => return Err(NoAddress::Nothing),
            Err(_) => return Err(NoAddress::Failed),
        };
    }
    found.retain(|ip| is_public(*ip));
    if found.is_empty() { Err(NoAddress::Nothing) } else { Ok(found) }
}

/// DNS for the HTTPS client, so every hop of a redirect is guarded too.
struct GuardedDns(TokioResolver);

impl Resolve for GuardedDns {
    fn resolve(&self, name: Name) -> Resolving {
        let r = self.0.clone();
        Box::pin(async move {
            match addresses(&r, name.as_str()).await {
                Ok(ips) => {
                    let it: Addrs = Box::new(ips.into_iter().map(|ip| SocketAddr::new(ip, 0)));
                    Ok(it)
                }
                Err(_) => Err("no public address".into()),
            }
        })
    }
}

pub fn build_http(resolver: TokioResolver, user_agent: &str, limits: &Limits) -> Result<reqwest::Client> {
    let policy = reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= MAX_REDIRECTS {
            return attempt.error("too many redirects");
        }
        let url = attempt.url();
        if url.scheme() != "https" || url.port().is_some_and(|p| p != 443) {
            return attempt.error("redirect leaves https on port 443");
        }
        match url.host() {
            Some(url::Host::Domain(_)) => attempt.follow(),
            _ => attempt.error("redirect to an address literal"),
        }
    });
    Ok(reqwest::Client::builder()
        .user_agent(user_agent.to_owned())
        .dns_resolver(Arc::new(GuardedDns(resolver)))
        .redirect(policy)
        .connect_timeout(limits.connect)
        .timeout(limits.total)
        // Every handle is its own host: nothing is gained by keeping connections.
        .pool_max_idle_per_host(0)
        .build()?)
}

#[derive(Debug, PartialEq, Eq)]
enum Txt {
    Did(String),
    /// Records naming different DIDs: resolution fails.
    Conflict,
    /// No record that counts.
    None,
}

/// Only values starting `did=` count. One DID, or several that agree, is
/// the answer.
fn read_txt<'a>(records: impl Iterator<Item = &'a str>) -> Txt {
    let mut dids: Vec<&str> = Vec::new();
    for r in records {
        if let Some(d) = r.strip_prefix("did=") {
            let d = d.trim();
            if valid_did(d) && !dids.contains(&d) {
                dids.push(d);
            }
        }
    }
    match dids.as_slice() {
        [] => Txt::None,
        [one] => Txt::Did((*one).to_owned()),
        _ => Txt::Conflict,
    }
}

/// What an HTTPS status says. `None` means the body decides.
fn read_status(status: u16) -> Option<Outcome> {
    match status {
        200..=299 => None,
        // The host refusing the client says nothing about the handle.
        401 | 403 => Some(Outcome::Inconclusive("refused")),
        408 | 425 | 429 => Some(Outcome::Inconclusive("asked to wait")),
        400..=499 => Some(Outcome::Nothing),
        500..=599 => Some(Outcome::Inconclusive("server error")),
        _ => Some(Outcome::Inconclusive("unexpected status")),
    }
}

fn read_body(body: &[u8]) -> Outcome {
    let text = String::from_utf8_lossy(body);
    let d = text.trim();
    if valid_did(d) { Outcome::Did(d.to_owned()) } else { Outcome::Nothing }
}

pub struct Checker {
    resolver: TokioResolver,
    http: reqwest::Client,
}

impl Checker {
    pub fn new(resolver_spec: &str, user_agent: &str, limits: &Limits) -> Result<Self> {
        let resolver = build_resolver(resolver_spec, limits)?;
        let http = build_http(resolver.clone(), user_agent, limits)?;
        Ok(Checker { resolver, http })
    }

    /// Resolve a handle. `dns` and `https` choose the steps; a control
    /// check uses one of them alone.
    pub async fn resolve(&self, handle: &str, dns: bool, https: bool) -> Check {
        let started = Instant::now();
        let done = |outcome, by_dns, retry_after| Check { outcome, retry_after, by_dns, took: started.elapsed() };

        // A name too long for the prefix has no record to ask for.
        if dns && handle.len() <= 244 {
            match self.resolver.txt_lookup(format!("_atproto.{handle}.")).await {
                Ok(found) => {
                    let records: Vec<String> = found
                        .iter()
                        .map(|t| String::from_utf8_lossy(&t.txt_data().iter().flat_map(|p| p.iter().copied()).collect::<Vec<u8>>()).into_owned())
                        .collect();
                    match read_txt(records.iter().map(String::as_str)) {
                        Txt::Did(d) => return done(Outcome::Did(d), true, None),
                        Txt::Conflict => return done(Outcome::Nothing, true, None),
                        Txt::None => {}
                    }
                }
                Err(e) if e.is_nx_domain() || e.is_no_records_found() => {}
                // While the preferred method's answer is unknown, HTTPS does not decide.
                Err(_) => return done(Outcome::Inconclusive("dns failed"), true, None),
            }
        }
        if !https {
            return done(Outcome::Nothing, true, None);
        }

        // Does the handle's own name exist?
        match addresses(&self.resolver, handle).await {
            Ok(_) => {}
            Err(NoAddress::Nothing) => return done(Outcome::Nothing, false, None),
            Err(NoAddress::Failed) => return done(Outcome::Inconclusive("address lookup failed"), false, None),
        }

        let mut resp = match self.http.get(format!("https://{handle}/.well-known/atproto-did")).send().await {
            Ok(r) => r,
            Err(e) => {
                let why = if e.is_timeout() {
                    "timed out"
                } else if e.is_redirect() {
                    "refused redirect"
                } else if e.is_connect() {
                    let s = format!("{e:?}").to_ascii_lowercase();
                    if s.contains("certificate") || s.contains("tls") || s.contains("handshake") { "tls failed" } else { "connect failed" }
                } else {
                    "request failed"
                };
                return done(Outcome::Inconclusive(why), false, None);
            }
        };
        let retry_after =
            resp.headers().get("retry-after").and_then(|v| v.to_str().ok()).and_then(|v| v.trim().parse::<u64>().ok()).map(Duration::from_secs);
        if let Some(outcome) = read_status(resp.status().as_u16()) {
            return done(outcome, false, retry_after);
        }
        let mut body: Vec<u8> = Vec::new();
        loop {
            match resp.chunk().await {
                Ok(Some(chunk)) => {
                    body.extend_from_slice(&chunk);
                    if body.len() > BODY_CAP {
                        // A DID is far shorter than the cap.
                        return done(Outcome::Nothing, false, None);
                    }
                }
                Ok(None) => break,
                Err(_) => return done(Outcome::Inconclusive("body failed"), false, None),
            }
        }
        done(read_body(&body), false, None)
    }

    /// The whole procedure: DNS, then HTTPS if DNS has no record.
    pub async fn check(&self, handle: &str) -> Check {
        self.resolve(handle, true, true).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txt_records() {
        assert_eq!(read_txt(["did=did:plc:abc"].into_iter()), Txt::Did("did:plc:abc".into()));
        assert_eq!(read_txt(["v=spf1 -all", "did=did:plc:abc", "did=did:plc:abc"].into_iter()), Txt::Did("did:plc:abc".into()));
        assert_eq!(read_txt(["did=did:plc:abc", "did=did:web:other.example"].into_iter()), Txt::Conflict);
        assert_eq!(read_txt(["v=spf1 -all", "did:plc:abc", "did=not a did"].into_iter()), Txt::None);
        assert_eq!(read_txt([].into_iter()), Txt::None);
    }

    #[test]
    fn statuses() {
        assert_eq!(read_status(200), None);
        assert_eq!(read_status(204), None);
        assert_eq!(read_status(404), Some(Outcome::Nothing));
        assert_eq!(read_status(410), Some(Outcome::Nothing));
        for refused in [401, 403] {
            assert_eq!(read_status(refused), Some(Outcome::Inconclusive("refused")));
        }
        for wait in [408, 425, 429] {
            assert_eq!(read_status(wait), Some(Outcome::Inconclusive("asked to wait")));
        }
        assert_eq!(read_status(503), Some(Outcome::Inconclusive("server error")));
        assert_eq!(read_status(302), Some(Outcome::Inconclusive("unexpected status")));
    }

    #[test]
    fn bodies() {
        assert_eq!(read_body(b"  did:plc:ewvi7nxzyoun6zhxrhs64oiz\n"), Outcome::Did("did:plc:ewvi7nxzyoun6zhxrhs64oiz".into()));
        assert_eq!(read_body(b"<html>not found</html>"), Outcome::Nothing);
        assert_eq!(read_body(b""), Outcome::Nothing);
    }

    #[test]
    fn dids() {
        assert!(valid_did("did:plc:ewvi7nxzyoun6zhxrhs64oiz"));
        assert!(valid_did("did:web:example.com%3A8080"));
        for bad in ["", "did:", "did:plc", "did:PLC:abc", "did:plc:", "did:plc:a b", "plc:abc", "did:web:example.com:"] {
            assert!(!valid_did(bad), "{bad}");
        }
    }

    #[test]
    fn private_addresses_are_refused() {
        for bad in ["127.0.0.1", "10.1.2.3", "192.168.0.1", "172.16.5.5", "169.254.1.1", "100.64.0.1", "0.0.0.0", "::1", "fe80::1", "fc00::1", "::ffff:10.0.0.1"] {
            assert!(!is_public(bad.parse().unwrap()), "{bad}");
        }
        for good in ["8.8.8.8", "184.192.21.37", "2606:4700::1111"] {
            assert!(is_public(good.parse().unwrap()), "{good}");
        }
    }
}
