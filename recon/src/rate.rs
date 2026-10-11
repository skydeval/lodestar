//! Rate probe: handle checks at stepped rates, to see whether a provider
//! limits them. Each handle is asked once. Stops at the first refusal.
//! Needs no database.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hickory_resolver::TokioResolver;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::verify;

pub struct Opts {
    pub handles: String,
    pub steps: Vec<u32>,
    pub step_seconds: u64,
    pub resolver: String,
    pub user_agent: String,
    pub max_in_flight: usize,
}

struct Outcome {
    /// HTTP status, or the kind of failure.
    what: String,
    refused: bool,
    failed: bool,
    did: bool,
    ms: u64,
    limit_headers: Vec<(String, String)>,
}

#[derive(Default)]
struct Step {
    sent: u64,
    done: u64,
    with_did: u64,
    whats: BTreeMap<String, u64>,
    ms: Vec<u64>,
    with_limit_headers: u64,
    last_headers: BTreeMap<String, String>,
}

async fn one(resolver: TokioResolver, http: reqwest::Client, handle: String) -> Outcome {
    let started = Instant::now();
    // The DNS step of a real check. The provider's handles have no record.
    let _ = resolver.txt_lookup(format!("_atproto.{handle}.")).await;
    let url = format!("https://{handle}/.well-known/atproto-did");
    let mut o = Outcome { what: String::new(), refused: false, failed: false, did: false, ms: 0, limit_headers: Vec::new() };
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
        }
    }
    o.ms = started.elapsed().as_millis() as u64;
    o
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() - 1) * p / 100]
}

fn report(rate: u32, seconds: f64, s: &mut Step) {
    s.ms.sort_unstable();
    println!(
        "step rate={rate}/s seconds={seconds:.0} sent={} done={} achieved={:.1}/s with_did={} p50_ms={} p95_ms={} max_ms={}",
        s.sent,
        s.done,
        s.sent as f64 / seconds.max(1.0),
        s.with_did,
        pct(&s.ms, 50),
        pct(&s.ms, 95),
        s.ms.last().copied().unwrap_or(0),
    );
    println!("  results: {:?}", s.whats);
    println!("  responses with limit headers: {} last seen: {:?}", s.with_limit_headers, s.last_headers);
}

pub async fn probe(o: Opts) -> Result<()> {
    let text = std::fs::read_to_string(&o.handles).with_context(|| format!("reading {}", o.handles))?;
    let mut handles = text.lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_owned);
    let resolver = verify::build_resolver(&o.resolver)?;
    let http = verify::build_http(resolver.clone(), &o.user_agent)?;
    let (tx, mut rx) = mpsc::unbounded_channel::<Outcome>();
    let mut recent: VecDeque<bool> = VecDeque::new();
    let mut stop: Option<String> = None;
    let run_started = Instant::now();

    'steps: for rate in &o.steps {
        let rate = *rate;
        let mut s = Step::default();
        let started = Instant::now();
        let deadline = started + Duration::from_secs(o.step_seconds);
        let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate as f64));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut sending = true;
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
                        stop = Some(format!("{} requests in flight: the rate is not being kept", s.sent - s.done));
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
                    s.ms.push(out.ms);
                    if out.did {
                        s.with_did += 1;
                    }
                    if !out.limit_headers.is_empty() {
                        s.with_limit_headers += 1;
                        for (k, v) in out.limit_headers {
                            s.last_headers.insert(k, v);
                        }
                    }
                    recent.push_back(out.failed);
                    if recent.len() > 50 {
                        recent.pop_front();
                    }
                    if out.refused && stop.is_none() {
                        stop = Some(format!(
                            "refused with {} after {} requests at {rate}/s, {:.0} s into the run",
                            out.what,
                            s.sent,
                            run_started.elapsed().as_secs_f64()
                        ));
                        sending = false;
                    } else if recent.len() == 50 && recent.iter().filter(|f| **f).count() >= 15 && stop.is_none() {
                        stop = Some(format!("15 of the last 50 requests failed at {rate}/s"));
                        sending = false;
                    }
                    if !sending && s.done == s.sent {
                        break;
                    }
                }
                _ = tokio::time::sleep_until(drain_until.into()), if !sending => break,
            }
        }
        let seconds = started.elapsed().as_secs_f64().min(o.step_seconds as f64);
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
