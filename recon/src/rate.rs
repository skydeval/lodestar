//! Rate probe: handle checks at stepped rates, to see whether a provider
//! limits them. Each handle is asked once. Stops at the first refusal, when
//! requests start failing, or when answers slow down. Needs no database.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hickory_resolver::TokioResolver;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::verify;

pub struct Opts {
    pub handles: String,
    /// Rate and length of each step, in order.
    pub steps: Vec<(u32, u64)>,
    pub resolver: String,
    pub user_agent: String,
    pub max_in_flight: usize,
    /// Stop when the 95th percentile of the last 500 answers passes this.
    pub slow_ms: u64,
    /// Stop when the 95th percentile of the last 200 TXT lookups passes
    /// this, or 5 of them failed: the resolver is pushing back.
    pub dns_slow_ms: u64,
}

/// Parse `rate` or `rate:seconds`.
pub fn parse_step(s: &str, default_seconds: u64) -> Result<(u32, u64)> {
    match s.split_once(':') {
        Some((r, secs)) => Ok((r.parse().context("step rate")?, secs.parse().context("step seconds")?)),
        None => Ok((s.parse().context("step rate")?, default_seconds)),
    }
}

struct Outcome {
    /// HTTP status, or the kind of failure.
    what: String,
    /// What the TXT lookup did: `none`, `record` or `error`.
    txt: &'static str,
    refused: bool,
    failed: bool,
    did: bool,
    dns_ms: u64,
    https_ms: u64,
    error: Option<String>,
    limit_headers: Vec<(String, String)>,
}

#[derive(Default)]
struct Step {
    sent: u64,
    done: u64,
    with_did: u64,
    whats: BTreeMap<String, u64>,
    txts: BTreeMap<&'static str, u64>,
    dns_ms: Vec<u64>,
    https_ms: Vec<u64>,
    with_limit_headers: u64,
    last_headers: BTreeMap<String, String>,
    errors: BTreeMap<String, u64>,
}

async fn one(resolver: TokioResolver, http: reqwest::Client, handle: String) -> Outcome {
    let mut o = Outcome {
        what: String::new(),
        txt: "none",
        refused: false,
        failed: false,
        did: false,
        dns_ms: 0,
        https_ms: 0,
        error: None,
        limit_headers: Vec::new(),
    };
    // The DNS step of a real check. The provider's handles have no record.
    let t = Instant::now();
    match resolver.txt_lookup(format!("_atproto.{handle}.")).await {
        Ok(_) => o.txt = "record",
        Err(e) if e.is_no_records_found() || e.is_nx_domain() => o.txt = "none",
        Err(_) => o.txt = "error",
    }
    o.dns_ms = t.elapsed().as_millis() as u64;
    let t = Instant::now();
    let url = format!("https://{handle}/.well-known/atproto-did");
    match http.get(&url).send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            for (k, v) in resp.headers() {
                let k = k.as_str();
                if k.contains("ratelimit") || k == "retry-after" {
                    o.limit_headers.push((k.to_owned(), v.to_str().unwrap_or("").to_owned()));
                }
            }
            let body = resp.text().await.unwrap_or_default();
            o.did = status == 200 && body.trim().starts_with("did:");
            o.refused = status == 429 || status == 403;
            o.failed = status >= 500;
            o.what = status.to_string();
        }
        Err(e) => {
            o.failed = true;
            o.what = if e.is_timeout() {
                "timeout".into()
            } else if e.is_connect() {
                "connect".into()
            } else {
                "error".into()
            };
            // The innermost cause, without the URL.
            let mut cause: &dyn std::error::Error = &e;
            while let Some(next) = cause.source() {
                cause = next;
            }
            let mut text = cause.to_string();
            text.truncate(80);
            o.error = Some(text);
        }
    }
    o.https_ms = t.elapsed().as_millis() as u64;
    o
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() - 1) * p / 100]
}

fn report(rate: u32, seconds: f64, s: &mut Step) {
    s.dns_ms.sort_unstable();
    s.https_ms.sort_unstable();
    println!(
        "step rate={rate}/s seconds={seconds:.0} sent={} done={} achieved={:.1}/s with_did={}",
        s.sent,
        s.done,
        s.sent as f64 / seconds.max(1.0),
        s.with_did,
    );
    println!(
        "  https ms: p50={} p95={} p99={} max={}",
        pct(&s.https_ms, 50),
        pct(&s.https_ms, 95),
        pct(&s.https_ms, 99),
        s.https_ms.last().copied().unwrap_or(0)
    );
    println!(
        "  dns ms:   p50={} p95={} p99={} max={}  txt: {:?}",
        pct(&s.dns_ms, 50),
        pct(&s.dns_ms, 95),
        pct(&s.dns_ms, 99),
        s.dns_ms.last().copied().unwrap_or(0),
        s.txts
    );
    println!("  results: {:?}", s.whats);
    if !s.errors.is_empty() {
        println!("  errors: {:?}", s.errors);
    }
    println!("  responses with limit headers: {} last seen: {:?}", s.with_limit_headers, s.last_headers);
}

pub async fn probe(o: Opts) -> Result<()> {
    let text = std::fs::read_to_string(&o.handles).with_context(|| format!("reading {}", o.handles))?;
    let mut handles = text.lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_owned);
    let resolver = verify::build_resolver(&o.resolver)?;
    let http = verify::build_http(resolver.clone(), &o.user_agent)?;
    let (tx, mut rx) = mpsc::unbounded_channel::<Outcome>();
    let mut recent_failed: VecDeque<bool> = VecDeque::new();
    let mut recent_ms: VecDeque<u64> = VecDeque::new();
    let mut recent_dns: VecDeque<(u64, bool)> = VecDeque::new();
    let mut stop: Option<String> = None;
    let run_started = Instant::now();

    'steps: for (rate, step_seconds) in &o.steps {
        let (rate, step_seconds) = (*rate, *step_seconds);
        let mut s = Step::default();
        let started = Instant::now();
        let deadline = started + Duration::from_secs(step_seconds);
        let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate as f64));
        tick.set_missed_tick_behavior(MissedTickBehavior::Burst);
        let mut sending = true;
        let mut progress = tokio::time::interval(Duration::from_secs(60));
        progress.tick().await;
        loop {
            if !sending && s.done == s.sent {
                break;
            }
            let drain_until = deadline + Duration::from_secs(25);
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
                    let (r, c, t) = (resolver.clone(), http.clone(), tx.clone());
                    tokio::spawn(async move {
                        let _ = t.send(one(r, c, h).await);
                    });
                }
                Some(out) = rx.recv() => {
                    s.done += 1;
                    *s.whats.entry(out.what.clone()).or_default() += 1;
                    *s.txts.entry(out.txt).or_default() += 1;
                    s.dns_ms.push(out.dns_ms);
                    s.https_ms.push(out.https_ms);
                    if out.did {
                        s.with_did += 1;
                    }
                    if let Some(e) = out.error {
                        if s.errors.len() < 12 || s.errors.contains_key(&e) {
                            *s.errors.entry(e).or_default() += 1;
                        }
                    }
                    if !out.limit_headers.is_empty() {
                        s.with_limit_headers += 1;
                        for (k, v) in out.limit_headers {
                            s.last_headers.insert(k, v);
                        }
                    }
                    recent_failed.push_back(out.failed);
                    if recent_failed.len() > 200 {
                        recent_failed.pop_front();
                    }
                    if !out.failed {
                        recent_ms.push_back(out.https_ms);
                        if recent_ms.len() > 500 {
                            recent_ms.pop_front();
                        }
                    }
                    recent_dns.push_back((out.dns_ms, out.txt == "error"));
                    if recent_dns.len() > 200 {
                        recent_dns.pop_front();
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
                    if stop.is_none() {
                        if out.refused {
                            stop = Some(format!(
                                "refused with {} after {} requests at {rate}/s, {:.0} s into the run",
                                out.what,
                                s.sent,
                                run_started.elapsed().as_secs_f64()
                            ));
                        } else if recent_failed.len() == 200 && recent_failed.iter().filter(|f| **f).count() >= 20 {
                            stop = Some(format!("20 of the last 200 requests failed at {rate}/s"));
                        } else if recent_ms.len() == 500 {
                            let mut v: Vec<u64> = recent_ms.iter().copied().collect();
                            v.sort_unstable();
                            let p95 = pct(&v, 95);
                            if p95 > o.slow_ms {
                                stop = Some(format!("answers slowed to a 95th percentile of {p95} ms at {rate}/s"));
                            }
                        }
                        if stop.is_some() {
                            sending = false;
                        }
                    }
                }
                _ = progress.tick() => {
                    let mut d: Vec<u64> = recent_dns.iter().map(|d| d.0).collect();
                    d.sort_unstable();
                    let mut h: Vec<u64> = recent_ms.iter().copied().collect();
                    h.sort_unstable();
                    println!(
                        "  at {:.0} s: rate={rate}/s sent={} in_flight={} recent dns p95={} ms, https p95={} ms",
                        started.elapsed().as_secs_f64(),
                        s.sent,
                        s.sent - s.done,
                        pct(&d, 95),
                        pct(&h, 95)
                    );
                }
                _ = tokio::time::sleep_until(drain_until.into()), if !sending => break,
            }
        }
        let seconds = started.elapsed().as_secs_f64().min(step_seconds as f64);
        report(rate, seconds, &mut s);
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
