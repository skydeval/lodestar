//! Politeness: how fast, and how many at once, lodestar asks any one
//! registrable domain, and everything together.
//!
//! A domain's rate is in checks a second. Its bucket holds at most one
//! second of that rate and starts empty, so an idle or stalled domain
//! cannot be sent a burst. A cap on what is in flight to a domain keeps a
//! slow one from taking every slot, and is also what backs lodestar away
//! from a provider that has stopped answering.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DomainLimit {
    /// Checks a second.
    pub rate: f64,
    /// Checks in flight at once.
    pub in_flight: usize,
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub default: DomainLimit,
    /// Per registrable domain.
    pub overrides: HashMap<String, DomainLimit>,
    pub global_rate: f64,
    pub global_in_flight: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits::profile("vps")
    }
}

impl Limits {
    /// The starting values of a named profile. `vps` is sized for the
    /// reference host. `hobbyist` is for a small board or a shared
    /// machine: it lowers pace only, never what lodestar does.
    pub fn profile(name: &str) -> Self {
        let mut overrides = HashMap::new();
        if name == "hobbyist" {
            overrides.insert("bsky.social".to_owned(), DomainLimit { rate: 20.0, in_flight: 8 });
            Limits { default: DomainLimit { rate: 1.0, in_flight: 4 }, overrides, global_rate: 40.0, global_in_flight: 24 }
        } else {
            overrides.insert("bsky.social".to_owned(), DomainLimit { rate: 320.0, in_flight: 100 });
            Limits { default: DomainLimit { rate: 1.0, in_flight: 8 }, overrides, global_rate: 400.0, global_in_flight: 160 }
        }
    }

    /// Apply `domain=rate:in_flight` settings over the profile's.
    pub fn with_overrides(mut self, specs: &[String]) -> anyhow::Result<Self> {
        for spec in specs {
            let parsed = spec.split_once('=').and_then(|(d, v)| {
                let (rate, cap) = v.split_once(':')?;
                Some((d.trim().to_ascii_lowercase(), rate.parse::<f64>().ok()?, cap.parse::<usize>().ok()?))
            });
            match parsed {
                Some((domain, rate, in_flight)) if rate > 0.0 && in_flight > 0 => {
                    self.overrides.insert(domain, DomainLimit { rate, in_flight });
                }
                _ => anyhow::bail!("a domain rate is written domain=rate:in_flight, not {spec}"),
            }
        }
        Ok(self)
    }
}

/// The registrable domain of a host name: public suffix plus one label.
pub fn registrable(host: &str) -> &str {
    psl::domain_str(host).unwrap_or(host)
}

struct Bucket {
    tokens: f64,
    rate: f64,
    at: Instant,
}

impl Bucket {
    fn new(rate: f64, now: Instant) -> Self {
        Bucket { tokens: 0.0, rate, at: now }
    }

    /// At most one second of rate, and never less than room for one check.
    fn depth(&self) -> f64 {
        self.rate.max(1.0)
    }

    fn fill(&mut self, now: Instant) {
        self.tokens = (self.tokens + now.duration_since(self.at).as_secs_f64() * self.rate).min(self.depth());
        self.at = now;
    }

    /// How long until a token is there. Zero if one is.
    fn wait(&self) -> Duration {
        if self.tokens >= 1.0 { Duration::ZERO } else { Duration::from_secs_f64((1.0 - self.tokens) / self.rate) }
    }
}

struct Domain {
    bucket: Bucket,
    cap: usize,
    in_flight: usize,
    /// The host asked lodestar to wait until then.
    hold_until: Option<Instant>,
    used: Instant,
}

struct State {
    domains: HashMap<String, Domain>,
    global: Bucket,
    global_in_flight: usize,
    swept: Instant,
}

struct Inner {
    limits: Limits,
    state: Mutex<State>,
    freed: Notify,
}

#[derive(Clone)]
pub struct Limiter(Arc<Inner>);

/// A check in flight. Dropping it frees its slot.
pub struct Permit {
    inner: Arc<Inner>,
    domain: String,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut s = self.inner.state.lock().expect("limiter lock");
        s.global_in_flight = s.global_in_flight.saturating_sub(1);
        if let Some(d) = s.domains.get_mut(&self.domain) {
            d.in_flight = d.in_flight.saturating_sub(1);
        }
        drop(s);
        self.inner.freed.notify_waiters();
    }
}

enum Try {
    Go,
    /// Try again after this long, or when a slot frees.
    Wait(Duration),
    /// Only a slot freeing will help.
    Full,
}

/// Domains not used for this long are forgotten.
const IDLE: Duration = Duration::from_secs(600);

impl Limiter {
    pub fn new(limits: Limits) -> Self {
        let now = Instant::now();
        let global = Bucket::new(limits.global_rate, now);
        Limiter(Arc::new(Inner {
            limits,
            state: Mutex::new(State { domains: HashMap::new(), global, global_in_flight: 0, swept: now }),
            freed: Notify::new(),
        }))
    }

    fn limit(&self, domain: &str) -> DomainLimit {
        self.0.limits.overrides.get(domain).copied().unwrap_or(self.0.limits.default)
    }

    fn attempt(&self, domain: &str) -> Try {
        let now = Instant::now();
        let limit = self.limit(domain);
        let mut s = self.0.state.lock().expect("limiter lock");
        if now.duration_since(s.swept) > IDLE {
            s.domains.retain(|_, d| d.in_flight > 0 || now.duration_since(d.used) < IDLE || d.hold_until.is_some_and(|h| h > now));
            s.swept = now;
        }
        s.global.fill(now);
        let global_wait = s.global.wait();
        let global_full = s.global_in_flight >= self.0.limits.global_in_flight;
        let d = s.domains.entry(domain.to_owned()).or_insert_with(|| Domain {
            bucket: Bucket::new(limit.rate, now),
            cap: limit.in_flight,
            in_flight: 0,
            hold_until: None,
            used: now,
        });
        d.used = now;
        d.bucket.fill(now);
        if let Some(until) = d.hold_until {
            if until > now {
                return Try::Wait(until - now);
            }
            d.hold_until = None;
        }
        if d.in_flight >= d.cap || global_full {
            return Try::Full;
        }
        let wait = d.bucket.wait().max(global_wait);
        if wait > Duration::ZERO {
            return Try::Wait(wait);
        }
        d.bucket.tokens -= 1.0;
        d.in_flight += 1;
        s.global.tokens -= 1.0;
        s.global_in_flight += 1;
        Try::Go
    }

    fn permit(&self, domain: &str) -> Permit {
        Permit { inner: self.0.clone(), domain: domain.to_owned() }
    }

    /// Take a slot for a check of `domain` if one is free right now.
    pub fn try_acquire(&self, domain: &str) -> Option<Permit> {
        matches!(self.attempt(domain), Try::Go).then(|| self.permit(domain))
    }

    /// Wait for a slot for a check of `domain`.
    pub async fn acquire(&self, domain: &str) -> Permit {
        loop {
            // Register for wake-ups before looking, so a slot freed in
            // between is not missed.
            let freed = self.0.freed.notified();
            tokio::pin!(freed);
            freed.as_mut().enable();
            match self.attempt(domain) {
                Try::Go => return self.permit(domain),
                Try::Wait(d) => {
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = &mut freed => {}
                    }
                }
                Try::Full => freed.await,
            }
        }
    }

    /// The host asked lodestar to wait. Nothing is sent to its domain
    /// until then, whoever asks.
    pub fn hold(&self, domain: &str, wait: Duration) {
        let now = Instant::now();
        let limit = self.limit(domain);
        let mut s = self.0.state.lock().expect("limiter lock");
        let d = s.domains.entry(domain.to_owned()).or_insert_with(|| Domain {
            bucket: Bucket::new(limit.rate, now),
            cap: limit.in_flight,
            in_flight: 0,
            hold_until: None,
            used: now,
        });
        let until = now + wait;
        if d.hold_until.is_none_or(|h| h < until) {
            d.hold_until = Some(until);
        }
    }

    /// True while the domain has asked lodestar to wait.
    pub fn is_held(&self, domain: &str) -> bool {
        let now = Instant::now();
        let s = self.0.state.lock().expect("limiter lock");
        s.domains.get(domain).and_then(|d| d.hold_until).is_some_and(|until| until > now)
    }

    /// Domains whose in-flight cap is full, for health.
    pub fn full_domains(&self) -> Vec<String> {
        let s = self.0.state.lock().expect("limiter lock");
        s.domains.iter().filter(|(_, d)| d.in_flight >= d.cap).map(|(name, _)| name.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(rate: f64, in_flight: usize) -> Limits {
        Limits { default: DomainLimit { rate, in_flight }, overrides: HashMap::new(), global_rate: 1000.0, global_in_flight: 1000 }
    }

    #[test]
    fn registrable_domains() {
        assert_eq!(registrable("alice.bsky.social"), "bsky.social");
        assert_eq!(registrable("a.b.example.co.uk"), "example.co.uk");
        assert_eq!(registrable("example.com"), "example.com");
    }

    #[tokio::test(start_paused = true)]
    async fn a_domain_starts_empty_and_fills_at_its_rate() {
        let l = Limiter::new(limits(2.0, 8));
        assert!(l.try_acquire("example.com").is_none());
        let start = Instant::now();
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(l.acquire("example.com").await);
        }
        // Four checks at two a second, from empty: the last at two seconds.
        let took = start.elapsed().as_secs_f64();
        assert!((1.9..2.2).contains(&took), "{took}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_domain_saves_at_most_one_second() {
        let l = Limiter::new(limits(5.0, 100));
        drop(l.acquire("example.com").await);
        tokio::time::sleep(Duration::from_secs(60)).await;
        let mut held = Vec::new();
        while let Some(p) = l.try_acquire("example.com") {
            held.push(p);
        }
        assert_eq!(held.len(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn the_in_flight_cap_holds_until_a_slot_frees() {
        let l = Limiter::new(limits(100.0, 2));
        tokio::time::sleep(Duration::from_secs(1)).await;
        let a = l.acquire("example.com").await;
        let _b = l.acquire("example.com").await;
        assert!(l.try_acquire("example.com").is_none());
        assert_eq!(l.full_domains(), vec!["example.com".to_owned()]);
        // Another domain is not affected, once its own bucket has a token.
        assert!(l.try_acquire("other.org").is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(l.try_acquire("other.org").is_some());
        let l2 = l.clone();
        let waiter = tokio::spawn(async move { l2.acquire("example.com").await });
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!waiter.is_finished());
        drop(a);
        let _c = waiter.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_hold_stops_a_domain_until_it_ends() {
        let l = Limiter::new(limits(10.0, 8));
        tokio::time::sleep(Duration::from_secs(1)).await;
        l.hold("example.com", Duration::from_secs(30));
        // A shorter hold does not shorten it.
        l.hold("example.com", Duration::from_secs(5));
        assert!(l.try_acquire("example.com").is_none());
        let start = Instant::now();
        let _p = l.acquire("example.com").await;
        assert!(start.elapsed() >= Duration::from_secs(30));
        assert!(start.elapsed() < Duration::from_secs(31));
    }

    #[tokio::test(start_paused = true)]
    async fn the_global_cap_covers_every_domain() {
        let l = Limiter::new(Limits { global_in_flight: 2, ..limits(100.0, 8) });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _a = l.acquire("one.com").await;
        let _b = l.acquire("two.com").await;
        assert!(l.try_acquire("three.com").is_none());
    }

    #[test]
    fn profiles_and_overrides() {
        let small = Limits::profile("hobbyist");
        assert!(small.global_rate < Limits::profile("vps").global_rate);
        assert_eq!(small.overrides["bsky.social"].rate, 20.0);
        let l = Limits::profile("vps").with_overrides(&["Example.com=5:20".to_owned(), "bsky.social=160:60".to_owned()]).unwrap();
        assert_eq!(l.overrides["example.com"], DomainLimit { rate: 5.0, in_flight: 20 });
        assert_eq!(l.overrides["bsky.social"], DomainLimit { rate: 160.0, in_flight: 60 });
        for bad in ["example.com", "example.com=5", "example.com=0:5", "example.com=x:y"] {
            assert!(Limits::profile("vps").with_overrides(&[bad.to_owned()]).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_largest_provider_has_its_own_limit() {
        let l = Limiter::new(Limits::default());
        assert_eq!(l.limit("bsky.social"), DomainLimit { rate: 320.0, in_flight: 100 });
        assert_eq!(l.limit("example.com"), DomainLimit { rate: 1.0, in_flight: 8 });
    }
}
