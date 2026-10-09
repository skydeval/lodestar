//! Handle checks for sampling: the procedure in design §5.3, classified as in
//! §5.1, behind the address guard in §3.3. Sampling only; never the whole
//! network.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig};
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::TokioResolver;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::sync::{Mutex, Semaphore};
use tokio_postgres::Client;

use crate::handle;

const TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS recon_domain_counts (
    domain text PRIMARY KEY,
    n      bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_sample (
    run    text NOT NULL,
    did    text NOT NULL,
    handle text NOT NULL,
    domain text NOT NULL,
    PRIMARY KEY (run, did)
);
CREATE TABLE IF NOT EXISTS recon_handle_checks (
    run        text NOT NULL,
    round      text NOT NULL,
    resolver   text NOT NULL,
    did        text NOT NULL,
    handle     text NOT NULL,
    domain     text NOT NULL,
    started_at timestamptz NOT NULL,
    total_ms   integer NOT NULL,
    dns        text NOT NULL,
    dns_ms     integer,
    addr       text,
    https      text,
    https_ms   integer,
    http_status integer,
    answer_did text,
    result     text NOT NULL,
    conclusive boolean NOT NULL,
    PRIMARY KEY (run, round, resolver, did)
);
"#;

pub async fn init(client: &Client) -> Result<()> {
    client.batch_execute(TABLES).await.context("creating verify tables")?;
    Ok(())
}

/// The registrable domain (public suffix plus one label) of a host name.
pub fn registrable(host: &str) -> String {
    psl::domain_str(host).unwrap_or(host).to_owned()
}

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

pub fn build_resolver(spec: &str) -> Result<TokioResolver> {
    let mut builder = if spec == "system" {
        TokioResolver::builder_tokio().context("reading system resolver config")?
    } else {
        let addr: SocketAddr = if spec.contains(':') {
            spec.parse().with_context(|| format!("bad resolver address {spec}"))?
        } else {
            SocketAddr::new(spec.parse().with_context(|| format!("bad resolver address {spec}"))?, 53)
        };
        let group = NameServerConfigGroup::from_ips_clear(&[addr.ip()], addr.port(), true);
        let cfg = ResolverConfig::from_parts(None, vec![], group);
        TokioResolver::builder_with_config(cfg, TokioConnectionProvider::default())
    };
    let o = builder.options_mut();
    o.timeout = Duration::from_secs(5);
    o.attempts = 2;
    Ok(builder.build())
}

/// DNS for the HTTPS client: the same resolver, with non-public addresses
/// removed so a handle cannot point a request at an internal host.
struct GuardedDns(TokioResolver);

impl Resolve for GuardedDns {
    fn resolve(&self, name: Name) -> Resolving {
        let r = self.0.clone();
        Box::pin(async move {
            let found = r.lookup_ip(name.as_str()).await?;
            let addrs: Vec<SocketAddr> = found
                .iter()
                .filter(|ip| is_public(*ip))
                .map(|ip| SocketAddr::new(ip, 0))
                .collect();
            if addrs.is_empty() {
                return Err("no public address".into());
            }
            let it: Addrs = Box::new(addrs.into_iter());
            Ok(it)
        })
    }
}

pub fn build_http(resolver: TokioResolver, user_agent: &str) -> Result<reqwest::Client> {
    let policy = reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= 3 {
            return attempt.error("too many redirects");
        }
        let url = attempt.url();
        if url.scheme() != "https" {
            return attempt.error("redirect leaves https");
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
        .connect_timeout(Duration::from_secs(8))
        .timeout(Duration::from_secs(12))
        .pool_max_idle_per_host(0)
        .build()?)
}

fn valid_did(s: &str) -> bool {
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

#[derive(Debug, Default, Clone)]
pub struct Check {
    pub dns: String,
    pub dns_ms: Option<i32>,
    pub addr: Option<String>,
    pub https: Option<String>,
    pub https_ms: Option<i32>,
    pub http_status: Option<i32>,
    pub answer_did: Option<String>,
    pub conclusive: bool,
    pub total_ms: i32,
}

/// Resolve a handle to a DID the way §5.3 describes.
pub async fn check(resolver: &TokioResolver, http: &reqwest::Client, h: &str) -> Check {
    let started = Instant::now();
    let mut c = Check::default();

    if h.len() > 244 {
        c.dns = "skipped_too_long".into();
    } else {
        let t = Instant::now();
        let name = format!("_atproto.{h}.");
        match resolver.txt_lookup(name).await {
            Ok(found) => {
                let mut dids: Vec<String> = Vec::new();
                for txt in found.iter() {
                    let joined: Vec<u8> = txt.txt_data().iter().flat_map(|p| p.iter().copied()).collect();
                    let s = String::from_utf8_lossy(&joined);
                    if let Some(d) = s.strip_prefix("did=") {
                        let d = d.trim();
                        if valid_did(d) && !dids.iter().any(|x| x == d) {
                            dids.push(d.to_owned());
                        }
                    }
                }
                c.dns_ms = Some(t.elapsed().as_millis() as i32);
                match dids.len() {
                    0 => c.dns = "no_usable_record".into(),
                    1 => {
                        c.dns = "did".into();
                        c.answer_did = dids.pop();
                        c.conclusive = true;
                        c.total_ms = started.elapsed().as_millis() as i32;
                        return c;
                    }
                    _ => {
                        c.dns = "multiple_dids".into();
                        c.total_ms = started.elapsed().as_millis() as i32;
                        return c;
                    }
                }
            }
            Err(e) => {
                c.dns_ms = Some(t.elapsed().as_millis() as i32);
                if e.is_nx_domain() {
                    c.dns = "nxdomain".into();
                } else if e.is_no_records_found() {
                    c.dns = "no_records".into();
                } else {
                    c.dns = format!("fail:{}", short(&e.to_string()));
                    c.total_ms = started.elapsed().as_millis() as i32;
                    return c;
                }
            }
        }
    }

    // DNS answered with no usable record. Does the handle's own name exist?
    match resolver.lookup_ip(format!("{h}.")).await {
        Ok(found) => {
            let all: Vec<IpAddr> = found.iter().collect();
            if all.is_empty() {
                c.addr = Some("no_address".into());
                c.conclusive = true;
                c.total_ms = started.elapsed().as_millis() as i32;
                return c;
            }
            if !all.iter().any(|ip| is_public(*ip)) {
                c.addr = Some("non_public".into());
                c.conclusive = true;
                c.total_ms = started.elapsed().as_millis() as i32;
                return c;
            }
            c.addr = Some("ok".into());
        }
        Err(e) => {
            if e.is_nx_domain() {
                c.addr = Some("nxdomain".into());
                c.conclusive = true;
            } else if e.is_no_records_found() {
                c.addr = Some("no_address".into());
                c.conclusive = true;
            } else {
                c.addr = Some(format!("fail:{}", short(&e.to_string())));
            }
            c.total_ms = started.elapsed().as_millis() as i32;
            return c;
        }
    }

    let t = Instant::now();
    let url = format!("https://{h}/.well-known/atproto-did");
    match http.get(&url).send().await {
        Ok(mut resp) => {
            let status = resp.status().as_u16();
            c.http_status = Some(status as i32);
            if (200..300).contains(&status) {
                let mut body: Vec<u8> = Vec::new();
                let mut err = false;
                loop {
                    match resp.chunk().await {
                        Ok(Some(chunk)) => {
                            body.extend_from_slice(&chunk);
                            if body.len() > 4096 {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            err = true;
                            break;
                        }
                    }
                }
                if err {
                    c.https = Some("body_error".into());
                } else {
                    let text = String::from_utf8_lossy(&body);
                    let d = text.trim();
                    if valid_did(d) {
                        c.https = Some("did".into());
                        c.answer_did = Some(d.to_owned());
                    } else {
                        c.https = Some("not_a_did".into());
                    }
                    c.conclusive = true;
                }
            } else if matches!(status, 408 | 425 | 429) || status >= 500 {
                c.https = Some("retryable_status".into());
            } else if (400..500).contains(&status) {
                c.https = Some("refused_4xx".into());
                c.conclusive = true;
            } else {
                c.https = Some("other_status".into());
            }
        }
        Err(e) => {
            let kind = if e.is_timeout() {
                "timeout"
            } else if e.is_redirect() {
                "redirect_refused"
            } else if e.is_connect() {
                let s = format!("{e:?}").to_ascii_lowercase();
                if s.contains("certificate") || s.contains("tls") || s.contains("handshake") {
                    "tls"
                } else if s.contains("no public address") {
                    "non_public"
                } else {
                    "connect"
                }
            } else {
                "error"
            };
            c.https = Some(kind.into());
        }
    }
    c.https_ms = Some(t.elapsed().as_millis() as i32);
    c.total_ms = started.elapsed().as_millis() as i32;
    c
}

fn short(s: &str) -> String {
    let s = s.to_ascii_lowercase();
    for k in ["timed out", "timeout", "servfail", "refused", "no connections", "io error"] {
        if s.contains(k) {
            return k.replace(' ', "_");
        }
    }
    s.chars().take(40).collect()
}

pub fn result_for(c: &Check, did: &str) -> &'static str {
    if !c.conclusive {
        return "inconclusive";
    }
    match &c.answer_did {
        Some(d) if d == did => "verified",
        Some(_) => "mismatch",
        None => "unresolved",
    }
}

/// Count active accounts' handle claims per registrable domain. No requests.
pub async fn domain_counts(client: &Client) -> Result<()> {
    init(client).await?;
    let params: Vec<String> = Vec::new();
    let rows = client
        .query_raw(
            "SELECT handle_claim FROM accounts WHERE handle_claim IS NOT NULL AND status = 0",
            params,
        )
        .await?;
    tokio::pin!(rows);
    let mut counts: HashMap<String, i64> = HashMap::new();
    let (mut total, mut never) = (0i64, 0i64);
    while let Some(r) = rows.try_next().await? {
        let h: String = r.get(0);
        total += 1;
        if handle::never_resolves(&h) {
            never += 1;
            continue;
        }
        *counts.entry(registrable(&h)).or_insert(0) += 1;
    }
    client.batch_execute("TRUNCATE recon_domain_counts").await?;
    let all: Vec<(String, i64)> = counts.into_iter().collect();
    for chunk in all.chunks(20_000) {
        let d: Vec<&str> = chunk.iter().map(|x| x.0.as_str()).collect();
        let n: Vec<i64> = chunk.iter().map(|x| x.1).collect();
        client
            .execute(
                "INSERT INTO recon_domain_counts (domain, n) SELECT a, b FROM UNNEST($1::text[], $2::int8[]) AS t(a, b)",
                &[&d, &n],
            )
            .await?;
    }
    println!(
        "{}",
        serde_json::json!({"claims": total, "never_resolve": never, "registrable_domains": all.len()})
    );
    Ok(())
}

/// Draw a sample of claims, capped per registrable domain.
pub async fn sample_build(client: &Client, run: &str, percent: f64, per_domain_cap: i64, target: i64) -> Result<()> {
    init(client).await?;
    let sql = format!(
        "SELECT did, handle_claim FROM accounts TABLESAMPLE BERNOULLI ({percent}) \
         WHERE handle_claim IS NOT NULL AND status = 0"
    );
    let rows = client.query(sql.as_str(), &[]).await?;
    let mut per: HashMap<String, i64> = HashMap::new();
    let (mut dids, mut handles, mut domains) = (Vec::new(), Vec::new(), Vec::new());
    let drawn = rows.len();
    for r in rows {
        let did: String = r.get(0);
        let h: String = r.get(1);
        if handle::never_resolves(&h) {
            continue;
        }
        let d = registrable(&h);
        let n = per.entry(d.clone()).or_insert(0);
        if *n >= per_domain_cap {
            continue;
        }
        *n += 1;
        dids.push(did);
        handles.push(h);
        domains.push(d);
        if dids.len() as i64 >= target {
            break;
        }
    }
    client
        .execute(
            "INSERT INTO recon_sample (run, did, handle, domain) \
             SELECT $1, a, b, c FROM UNNEST($2::text[], $3::text[], $4::text[]) AS t(a, b, c) ON CONFLICT DO NOTHING",
            &[&run, &dids, &handles, &domains],
        )
        .await?;
    println!(
        "{}",
        serde_json::json!({"run": run, "drawn": drawn, "kept": dids.len(), "domains": per.len()})
    );
    Ok(())
}

pub struct RunOpts {
    pub run: String,
    pub round: String,
    pub resolver: String,
    pub user_agent: String,
    pub concurrency: usize,
    pub per_domain_ms: u64,
    /// Only re-check sample members whose result in this earlier round was one of these.
    pub only_round: Option<String>,
    pub only_results: Vec<String>,
}

/// Check every member of a sample once, paced per registrable domain.
pub async fn sample_run(client: Arc<Client>, o: RunOpts) -> Result<()> {
    init(&client).await?;
    let resolver = build_resolver(&o.resolver)?;
    let http = build_http(resolver.clone(), &o.user_agent)?;
    let rows = match &o.only_round {
        None => {
            client
                .query(
                    "SELECT s.did, s.handle, s.domain FROM recon_sample s WHERE s.run = $1 \
                     AND NOT EXISTS (SELECT 1 FROM recon_handle_checks c WHERE c.run = s.run AND c.round = $2 \
                                     AND c.resolver = $3 AND c.did = s.did) ORDER BY md5(s.did)",
                    &[&o.run, &o.round, &o.resolver],
                )
                .await?
        }
        Some(prev) => {
            client
                .query(
                    "SELECT s.did, s.handle, s.domain FROM recon_sample s \
                     JOIN recon_handle_checks p ON p.run = s.run AND p.did = s.did AND p.round = $4 AND p.result = ANY($5) \
                     WHERE s.run = $1 \
                     AND NOT EXISTS (SELECT 1 FROM recon_handle_checks c WHERE c.run = s.run AND c.round = $2 \
                                     AND c.resolver = $3 AND c.did = s.did) ORDER BY md5(s.did)",
                    &[&o.run, &o.round, &o.resolver, prev, &o.only_results],
                )
                .await?
        }
    };
    let total = rows.len();
    let sem = Arc::new(Semaphore::new(o.concurrency));
    let gates: Arc<Mutex<HashMap<String, Arc<Mutex<Instant>>>>> = Arc::new(Mutex::new(HashMap::new()));
    let gap = Duration::from_millis(o.per_domain_ms);
    let o = Arc::new(o);
    let mut tasks = Vec::with_capacity(total);
    for r in rows {
        let did: String = r.get(0);
        let h: String = r.get(1);
        let domain: String = r.get(2);
        let gate = {
            let mut g = gates.lock().await;
            g.entry(domain.clone()).or_insert_with(|| Arc::new(Mutex::new(Instant::now()))).clone()
        };
        let (sem, resolver, http, client, o) = (sem.clone(), resolver.clone(), http.clone(), client.clone(), o.clone());
        tasks.push(tokio::spawn(async move {
            // One request stream per registrable domain, spaced by `gap`.
            let mut next = gate.lock().await;
            let now = Instant::now();
            if *next > now {
                tokio::time::sleep(*next - now).await;
            }
            let _permit = sem.acquire().await.expect("semaphore open");
            let c = check(&resolver, &http, &h).await;
            *next = Instant::now() + gap;
            drop(next);
            let result = result_for(&c, &did);
            let res = client
                .execute(
                    "INSERT INTO recon_handle_checks (run, round, resolver, did, handle, domain, started_at, total_ms, dns, dns_ms, \
                     addr, https, https_ms, http_status, answer_did, result, conclusive) \
                     VALUES ($1, $2, $3, $4, $5, $6, now() - make_interval(secs => $7::int4 / 1000.0), $7::int4, $8, $9, $10, $11, $12, $13, $14, $15, $16) \
                     ON CONFLICT DO NOTHING",
                    &[
                        &o.run, &o.round, &o.resolver, &did, &h, &domain, &c.total_ms, &c.dns, &c.dns_ms, &c.addr, &c.https,
                        &c.https_ms, &c.http_status, &c.answer_did, &result, &c.conclusive,
                    ],
                )
                .await;
            if let Err(e) = res {
                eprintln!("recording check for {did}: {e}");
            }
        }));
    }
    let started = Instant::now();
    for t in tasks {
        let _ = t.await;
    }
    println!(
        "{}",
        serde_json::json!({"run": o.run, "round": o.round, "resolver": o.resolver, "checked": total, "elapsed_s": started.elapsed().as_secs()})
    );
    Ok(())
}
