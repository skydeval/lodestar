//! Rate probe: handle checks at stepped rates, to see whether a provider
//! limits them. Each handle is asked once. Every phase of a check is timed
//! on its own (TXT lookup, address lookup, connect, TLS, wait for the
//! answer), and a series file gets one line every ten seconds, so a
//! slowdown can be placed. Stops at the first refusal or when the resolver
//! pushes back; when answers slow it can drop to a low rate and keep
//! watching. Needs no database.

use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hickory_resolver::TokioResolver;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{timeout, MissedTickBehavior};
use tokio_rustls::TlsConnector;

use crate::verify;

pub struct Opts {
    pub handles: String,
    /// Rate and length of each step, in order.
    pub steps: Vec<(u32, u64)>,
    pub resolver: String,
    pub user_agent: String,
    pub max_in_flight: usize,
    /// Answers are slow when the 95th percentile of the last 500 (connect,
    /// TLS and wait together) passes this.
    pub slow_ms: u64,
    /// Stop when the 95th percentile of the last 200 TXT lookups passes
    /// this, or 5 of them failed: the resolver is pushing back.
    pub dns_slow_ms: u64,
    /// When answers slow, drop to this rate and keep watching. 0 stops.
    pub on_slow_rate: u32,
    pub on_slow_seconds: u64,
    /// One line every ten seconds is appended here.
    pub series: Option<String>,
}

/// Parse `rate` or `rate:seconds`.
pub fn parse_step(s: &str, default_seconds: u64) -> Result<(u32, u64)> {
    match s.split_once(':') {
        Some((r, secs)) => Ok((r.parse().context("step rate")?, secs.parse().context("step seconds")?)),
        None => Ok((s.parse().context("step rate")?, default_seconds)),
    }
}

#[derive(Default)]
struct Outcome {
    /// HTTP status, or the phase that failed.
    what: String,
    /// What the TXT lookup did: `none`, `record` or `error`.
    txt: &'static str,
    refused: bool,
    failed: bool,
    did: bool,
    txt_ms: u64,
    addr_ms: u64,
    connect_ms: u64,
    tls_ms: u64,
    wait_ms: u64,
    addr: Option<IpAddr>,
    error: Option<String>,
    limit_headers: Vec<(String, String)>,
}

impl Outcome {
    /// Time spent talking to the provider's servers.
    fn server_ms(&self) -> u64 {
        self.connect_ms + self.tls_ms + self.wait_ms
    }
}

#[derive(Default)]
struct Phases {
    txt: Vec<u64>,
    addr: Vec<u64>,
    connect: Vec<u64>,
    tls: Vec<u64>,
    wait: Vec<u64>,
}

impl Phases {
    fn push(&mut self, o: &Outcome) {
        self.txt.push(o.txt_ms);
        self.addr.push(o.addr_ms);
        if o.addr.is_some() {
            self.connect.push(o.connect_ms);
        }
        if !o.failed {
            self.tls.push(o.tls_ms);
            self.wait.push(o.wait_ms);
        }
    }
    fn sort(&mut self) {
        for v in [&mut self.txt, &mut self.addr, &mut self.connect, &mut self.tls, &mut self.wait] {
            v.sort_unstable();
        }
    }
}

#[derive(Default)]
struct Step {
    sent: u64,
    done: u64,
    with_did: u64,
    whats: BTreeMap<String, u64>,
    txts: BTreeMap<&'static str, u64>,
    phases: Phases,
    by_addr: BTreeMap<IpAddr, Vec<u64>>,
    with_limit_headers: u64,
    last_headers: BTreeMap<String, String>,
    errors: BTreeMap<String, u64>,
}

fn tls_config() -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Arc::new(rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth())
}

fn fail(o: &mut Outcome, phase: &str, error: impl ToString) {
    o.failed = true;
    o.what = phase.to_owned();
    let mut text = error.to_string();
    text.truncate(60);
    o.error = Some(format!("{phase}: {text}"));
}

async fn one(resolver: TokioResolver, tls: TlsConnector, user_agent: Arc<str>, handle: String) -> Outcome {
    let mut o = Outcome { txt: "none", ..Outcome::default() };

    // The DNS step of a real check. The provider's handles have no record.
    let t = Instant::now();
    match resolver.txt_lookup(format!("_atproto.{handle}.")).await {
        Ok(_) => o.txt = "record",
        Err(e) if e.is_no_records_found() || e.is_nx_domain() => o.txt = "none",
        Err(_) => o.txt = "error",
    }
    o.txt_ms = t.elapsed().as_millis() as u64;

    let t = Instant::now();
    let found = resolver.lookup_ip(format!("{handle}.")).await;
    o.addr_ms = t.elapsed().as_millis() as u64;
    let ips: Vec<IpAddr> = match found {
        Ok(l) => l.iter().filter(|ip| ip.is_ipv4() && verify::is_public(*ip)).collect(),
        Err(e) => {
            fail(&mut o, "address", e);
            return o;
        }
    };
    if ips.is_empty() {
        fail(&mut o, "address", "no public IPv4 address");
        return o;
    }
    let pick = handle.bytes().map(usize::from).sum::<usize>() % ips.len();
    let ip = ips[pick];
    o.addr = Some(ip);

    let t = Instant::now();
    let tcp = match timeout(Duration::from_secs(8), TcpStream::connect(SocketAddr::new(ip, 443))).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            o.connect_ms = t.elapsed().as_millis() as u64;
            fail(&mut o, "connect", e);
            return o;
        }
        Err(_) => {
            o.connect_ms = t.elapsed().as_millis() as u64;
            fail(&mut o, "connect", "timed out");
            return o;
        }
    };
    let _ = tcp.set_nodelay(true);
    o.connect_ms = t.elapsed().as_millis() as u64;

    let t = Instant::now();
    let name = match ServerName::try_from(handle.clone()) {
        Ok(n) => n,
        Err(e) => {
            fail(&mut o, "tls", e);
            return o;
        }
    };
    let mut stream = match timeout(Duration::from_secs(8), tls.connect(name, tcp)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            o.tls_ms = t.elapsed().as_millis() as u64;
            fail(&mut o, "tls", e);
            return o;
        }
        Err(_) => {
            o.tls_ms = t.elapsed().as_millis() as u64;
            fail(&mut o, "tls", "timed out");
            return o;
        }
    };
    o.tls_ms = t.elapsed().as_millis() as u64;

    let t = Instant::now();
    let request = format!(
        "GET /.well-known/atproto-did HTTP/1.1\r\nHost: {handle}\r\nUser-Agent: {user_agent}\r\nAccept: */*\r\nConnection: close\r\n\r\n"
    );
    let answer = timeout(Duration::from_secs(12), async {
        stream.write_all(request.as_bytes()).await?;
        let mut buf = vec![0u8; 16 * 1024];
        let mut len = 0;
        let mut first: Option<u64> = None;
        loop {
            // A server that closes without a TLS close is still an answer.
            let n = match stream.read(&mut buf[len..]).await {
                Ok(n) => n,
                Err(_) if len > 0 => 0,
                Err(e) => return Err(e),
            };
            if first.is_none() {
                first = Some(t.elapsed().as_millis() as u64);
            }
            if n == 0 {
                break;
            }
            len += n;
            if len == buf.len() {
                break;
            }
        }
        buf.truncate(len);
        Ok::<_, std::io::Error>((buf, first.unwrap_or(0)))
    })
    .await;
    let (bytes, first) = match answer {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            o.wait_ms = t.elapsed().as_millis() as u64;
            fail(&mut o, "response", e);
            return o;
        }
        Err(_) => {
            o.wait_ms = t.elapsed().as_millis() as u64;
            fail(&mut o, "response", "timed out");
            return o;
        }
    };
    o.wait_ms = first;
    let text = String::from_utf8_lossy(&bytes);
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let mut lines = head.lines();
    let status: u16 = lines.next().and_then(|l| l.split(' ').nth(1)).and_then(|s| s.parse().ok()).unwrap_or(0);
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            if k.contains("ratelimit") || k == "retry-after" {
                o.limit_headers.push((k, v.trim().to_owned()));
            }
        }
    }
    if status == 0 {
        fail(&mut o, "response", "no status line");
        return o;
    }
    o.did = status == 200 && body.contains("did:");
    o.refused = status == 429 || status == 403;
    o.failed = status >= 500;
    o.what = status.to_string();
    o
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() - 1) * p / 100]
}

fn spread(name: &str, v: &[u64]) -> String {
    format!("{name}: p50={} p95={} p99={} max={}", pct(v, 50), pct(v, 95), pct(v, 99), v.last().copied().unwrap_or(0))
}

fn report(rate: u32, seconds: f64, s: &mut Step) {
    s.phases.sort();
    println!(
        "step rate={rate}/s seconds={seconds:.0} sent={} done={} achieved={:.1}/s with_did={}",
        s.sent,
        s.done,
        s.sent as f64 / seconds.max(1.0),
        s.with_did,
    );
    println!("  ms {}", spread("txt lookup", &s.phases.txt));
    println!("  ms {}", spread("address lookup", &s.phases.addr));
    println!("  ms {}", spread("connect", &s.phases.connect));
    println!("  ms {}", spread("tls", &s.phases.tls));
    println!("  ms {}", spread("wait for answer", &s.phases.wait));
    for (addr, v) in s.by_addr.iter_mut() {
        v.sort_unstable();
        println!("  server {addr}: n={} {}", v.len(), spread("connect+tls+wait", v));
    }
    println!("  txt: {:?}  results: {:?}", s.txts, s.whats);
    if !s.errors.is_empty() {
        println!("  errors: {:?}", s.errors);
    }
    println!("  responses with limit headers: {} last seen: {:?}", s.with_limit_headers, s.last_headers);
}

fn series_line(file: &mut Option<std::fs::File>, rate: u32, sent: u64, in_flight: u64, failed: u64, p: &mut Phases) {
    let Some(f) = file else { return };
    p.sort();
    let n = p.txt.len();
    let _ = writeln!(
        f,
        "{},{rate},{sent},{in_flight},{n},{failed},{},{},{},{},{},{},{},{},{},{}",
        chrono::Utc::now().format("%H:%M:%S"),
        pct(&p.txt, 50),
        pct(&p.txt, 95),
        pct(&p.addr, 50),
        pct(&p.addr, 95),
        pct(&p.connect, 50),
        pct(&p.connect, 95),
        pct(&p.tls, 50),
        pct(&p.tls, 95),
        pct(&p.wait, 50),
        pct(&p.wait, 95),
    );
    *p = Phases::default();
}

pub async fn probe(o: Opts) -> Result<()> {
    let text = std::fs::read_to_string(&o.handles).with_context(|| format!("reading {}", o.handles))?;
    let mut handles = text.lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_owned);
    let resolver = verify::build_resolver(&o.resolver)?;
    let tls = TlsConnector::from(tls_config());
    let user_agent: Arc<str> = Arc::from(o.user_agent.as_str());
    let mut series = match &o.series {
        Some(path) => {
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path).with_context(|| format!("opening {path}"))?;
            writeln!(f, "utc,rate,sent,in_flight,done,failed,txt_p50,txt_p95,addr_p50,addr_p95,connect_p50,connect_p95,tls_p50,tls_p95,wait_p50,wait_p95")?;
            Some(f)
        }
        None => None,
    };
    let (tx, mut rx) = mpsc::unbounded_channel::<Outcome>();
    let mut recent_failed: VecDeque<bool> = VecDeque::new();
    let mut recent_ms: VecDeque<u64> = VecDeque::new();
    let mut recent_dns: VecDeque<(u64, bool)> = VecDeque::new();
    let mut stop: Option<String> = None;
    let run_started = Instant::now();

    'steps: for (rate, step_seconds) in &o.steps {
        let (mut rate, step_seconds) = (*rate, *step_seconds);
        let mut s = Step::default();
        let started = Instant::now();
        let mut deadline = started + Duration::from_secs(step_seconds);
        let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate as f64));
        tick.set_missed_tick_behavior(MissedTickBehavior::Burst);
        let mut sending = true;
        let mut watching: Option<String> = None;
        let mut bucket = Phases::default();
        let mut bucket_failed = 0u64;
        let mut every_ten = tokio::time::interval(Duration::from_secs(10));
        every_ten.tick().await;
        let mut ticks = 0u64;
        loop {
            if !sending && s.done == s.sent {
                break;
            }
            let drain_until = deadline + Duration::from_secs(40);
            tokio::select! {
                _ = tick.tick(), if sending => {
                    if Instant::now() >= deadline {
                        sending = false;
                        continue;
                    }
                    if (s.sent - s.done) as usize >= o.max_in_flight {
                        stop = Some(format!("{} requests in flight at {rate}/s: the rate is not being kept", s.sent - s.done));
                        sending = false;
                        continue;
                    }
                    let Some(h) = handles.next() else {
                        stop = Some("ran out of handles".into());
                        sending = false;
                        continue;
                    };
                    s.sent += 1;
                    let (r, c, ua, t) = (resolver.clone(), tls.clone(), user_agent.clone(), tx.clone());
                    tokio::spawn(async move {
                        let _ = t.send(one(r, c, ua, h).await);
                    });
                }
                Some(out) = rx.recv() => {
                    s.done += 1;
                    *s.whats.entry(out.what.clone()).or_default() += 1;
                    *s.txts.entry(out.txt).or_default() += 1;
                    s.phases.push(&out);
                    bucket.push(&out);
                    if out.failed {
                        bucket_failed += 1;
                    }
                    if out.did {
                        s.with_did += 1;
                    }
                    if let (Some(addr), false) = (out.addr, out.failed) {
                        s.by_addr.entry(addr).or_default().push(out.server_ms());
                    }
                    if let Some(e) = &out.error {
                        if s.errors.len() < 12 || s.errors.contains_key(e) {
                            *s.errors.entry(e.clone()).or_default() += 1;
                        }
                    }
                    if !out.limit_headers.is_empty() {
                        s.with_limit_headers += 1;
                        for (k, v) in &out.limit_headers {
                            s.last_headers.insert(k.clone(), v.clone());
                        }
                    }
                    recent_failed.push_back(out.failed);
                    if recent_failed.len() > 200 {
                        recent_failed.pop_front();
                    }
                    if !out.failed {
                        recent_ms.push_back(out.server_ms());
                        if recent_ms.len() > 500 {
                            recent_ms.pop_front();
                        }
                    }
                    recent_dns.push_back((out.txt_ms, out.txt == "error"));
                    if recent_dns.len() > 200 {
                        recent_dns.pop_front();
                    }
                    if stop.is_none() && out.refused {
                        stop = Some(format!(
                            "refused with {} after {} requests at {rate}/s, {:.0} s into the run",
                            out.what,
                            s.sent,
                            run_started.elapsed().as_secs_f64()
                        ));
                        sending = false;
                    }
                    if stop.is_none() && recent_dns.len() == 200 {
                        let errors = recent_dns.iter().filter(|d| d.1).count();
                        let mut v: Vec<u64> = recent_dns.iter().map(|d| d.0).collect();
                        v.sort_unstable();
                        let p95 = pct(&v, 95);
                        if errors >= 5 {
                            stop = Some(format!("backing off: {errors} of the last 200 DNS lookups failed at {rate}/s"));
                        } else if p95 > o.dns_slow_ms {
                            stop = Some(format!("backing off: DNS slowed to a 95th percentile of {p95} ms at {rate}/s"));
                        }
                        if stop.is_some() {
                            sending = false;
                        }
                    }
                    if stop.is_none() && watching.is_none() {
                        let mut why = None;
                        if recent_failed.len() == 200 && recent_failed.iter().filter(|f| **f).count() >= 20 {
                            why = Some(format!("20 of the last 200 requests failed at {rate}/s"));
                        } else if recent_ms.len() == 500 {
                            let mut v: Vec<u64> = recent_ms.iter().copied().collect();
                            v.sort_unstable();
                            let p95 = pct(&v, 95);
                            if p95 > o.slow_ms {
                                why = Some(format!("answers slowed to a 95th percentile of {p95} ms at {rate}/s"));
                            }
                        }
                        if let Some(why) = why {
                            let at = started.elapsed().as_secs_f64();
                            if o.on_slow_rate > 0 {
                                println!("  at {at:.0} s: {why}; dropping to {}/s for {} s", o.on_slow_rate, o.on_slow_seconds);
                                watching = Some(format!("{why}, {at:.0} s into the step after {} requests", s.sent));
                                rate = o.on_slow_rate;
                                tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate as f64));
                                tick.set_missed_tick_behavior(MissedTickBehavior::Burst);
                                deadline = Instant::now() + Duration::from_secs(o.on_slow_seconds);
                            } else {
                                stop = Some(why);
                                sending = false;
                            }
                        }
                    }
                }
                _ = every_ten.tick() => {
                    ticks += 1;
                    series_line(&mut series, rate, s.sent, s.sent - s.done, bucket_failed, &mut bucket);
                    bucket_failed = 0;
                    if ticks % 6 == 0 {
                        let mut d: Vec<u64> = recent_dns.iter().map(|d| d.0).collect();
                        d.sort_unstable();
                        let mut h: Vec<u64> = recent_ms.iter().copied().collect();
                        h.sort_unstable();
                        println!(
                            "  at {:.0} s: rate={rate}/s sent={} in_flight={} recent txt lookup p95={} ms, connect+tls+wait p95={} ms",
                            started.elapsed().as_secs_f64(),
                            s.sent,
                            s.sent - s.done,
                            pct(&d, 95),
                            pct(&h, 95)
                        );
                    }
                }
                _ = tokio::time::sleep_until(drain_until.into()), if !sending => break,
            }
        }
        series_line(&mut series, rate, s.sent, s.sent - s.done, bucket_failed, &mut bucket);
        let seconds = started.elapsed().as_secs_f64();
        report(rate, seconds, &mut s);
        if stop.is_none() {
            stop = watching;
        }
        if stop.is_some() {
            break 'steps;
        }
    }
    match stop {
        Some(why) => println!("stopped: {why}"),
        None => println!("finished every step with no refusal"),
    }
    Ok(())
}
