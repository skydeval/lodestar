//! The two per-domain pauses. Pure: times are milliseconds, passed in.
//!
//! - **Failing.** When most first attempts for a registrable domain are
//!   failing, checks for it stop, apart from a probe now and then. Nothing
//!   attempted means nothing failing, so a provider's outage is not
//!   charged to its handles.
//! - **Resolving to nothing.** When most answers for a domain's verified
//!   handles are "nothing", confirming checks wait, so nothing is demoted
//!   on answers that are probably the provider's fault.
//!
//! Each has a hold: after a day the pause ends anyway and cannot start
//! again until the domain has behaved, so a domain that is broken for good
//! is not left as it was for ever.

use std::collections::{HashMap, VecDeque};

const SAMPLE: usize = 200;
const SAMPLE_MIN: usize = 20;
const HOUR: i64 = 3_600_000;
const FIRST_PROBE: i64 = 60_000;
const PROBE_EVERY: i64 = 600_000;
const HOLD: i64 = 24 * HOUR;

/// What a check is, for the pauses' purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A handle with no open failure state that has not settled as failing.
    First,
    Retry,
    Confirm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    Go,
    /// Go, as the domain's probe.
    Probe,
    /// The domain is paused for this kind of check.
    Wait,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Did,
    Nothing,
    /// Inconclusive.
    Failed,
}

/// A change worth storing and reporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub domain: String,
    /// 2 for failing, 3 for resolving to nothing.
    pub pause: u8,
    pub state: State,
    pub since: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Paused,
    /// Ended by a probe or by the domain recovering.
    Clear,
    /// Ended by its hold, and unable to start again yet.
    Released,
}

#[derive(Default)]
struct Sample(VecDeque<(i64, bool)>);

impl Sample {
    /// Add a result, keeping the last 200 from the last hour.
    fn push(&mut self, at: i64, hit: bool, hour_limit: bool) {
        self.0.push_back((at, hit));
        while self.0.len() > SAMPLE || (hour_limit && self.0.front().is_some_and(|(t, _)| at - t > HOUR)) {
            self.0.pop_front();
        }
    }

    /// More than `num/den` of at least 20 are hits.
    fn over(&self, num: usize, den: usize) -> bool {
        self.0.len() >= SAMPLE_MIN && self.0.iter().filter(|(_, h)| *h).count() * den > self.0.len() * num
    }

    fn enough(&self) -> bool {
        self.0.len() >= SAMPLE_MIN
    }
}

struct Failing {
    since: i64,
    next_probe: i64,
    /// A check for the domain has had a conclusive result since the pause began.
    answered: bool,
}

struct Nothing {
    since: i64,
    next_probe: i64,
    /// Answers for verified handles since the pause began.
    sample: Sample,
}

#[derive(Default)]
struct Domain {
    failing_sample: Sample,
    nothing_sample: Sample,
    failing: Option<Failing>,
    failing_released: bool,
    nothing: Option<Nothing>,
    nothing_released: bool,
}

#[derive(Default)]
pub struct Pauses {
    domains: HashMap<String, Domain>,
}

impl Pauses {
    /// A pause that was in force when lodestar stopped.
    pub fn restore(&mut self, domain: &str, pause: u8, state: State, since: i64, now: i64) {
        let d = self.domains.entry(domain.to_owned()).or_default();
        match (pause, state) {
            (2, State::Paused) => d.failing = Some(Failing { since, next_probe: now + FIRST_PROBE, answered: false }),
            (2, State::Released) => d.failing_released = true,
            (3, State::Paused) => d.nothing = Some(Nothing { since, next_probe: now + FIRST_PROBE, sample: Sample::default() }),
            (3, State::Released) => d.nothing_released = true,
            _ => {}
        }
    }

    /// May a check of this kind be made for the domain now?
    pub fn admit(&mut self, domain: &str, kind: Kind, now: i64) -> Admit {
        let Some(d) = self.domains.get_mut(domain) else {
            return Admit::Go;
        };
        if let Some(p) = &mut d.failing {
            if now < p.next_probe {
                return Admit::Wait;
            }
            p.next_probe = now + PROBE_EVERY;
            return Admit::Probe;
        }
        if let (Some(p), Kind::Confirm) = (&mut d.nothing, kind) {
            if now < p.next_probe {
                return Admit::Wait;
            }
            p.next_probe = now + PROBE_EVERY;
            return Admit::Probe;
        }
        Admit::Go
    }

    /// True while no "nothing" result may demote a handle under the domain.
    pub fn hold_demotions(&self, domain: &str) -> bool {
        self.domains.get(domain).is_some_and(|d| d.nothing.is_some())
    }

    /// True while every check for the domain waits, apart from the probe.
    pub fn failing(&self, domain: &str) -> bool {
        self.domains.get(domain).is_some_and(|d| d.failing.is_some())
    }

    /// A check's result. `kind` is what the check was; `was_verified` is
    /// whether the handle was verified when it was checked.
    pub fn record(&mut self, domain: &str, kind: Kind, was_verified: bool, answer: Answer, probe: bool, now: i64) -> Vec<Change> {
        let d = self.domains.entry(domain.to_owned()).or_default();
        let mut changes = Vec::new();
        let conclusive = answer != Answer::Failed;
        let change = |pause: u8, state: State, since: i64| Change { domain: domain.to_owned(), pause, state, since };

        if conclusive {
            // The domain has given a different answer: it may be paused again.
            d.failing_released = false;
        }
        match &mut d.failing {
            Some(p) => {
                if conclusive {
                    p.answered = true;
                    if probe {
                        changes.push(change(2, State::Clear, p.since));
                        d.failing = None;
                        d.failing_sample = Sample::default();
                    }
                }
            }
            None => {
                if kind == Kind::First && !probe {
                    d.failing_sample.push(now, !conclusive, true);
                    if !d.failing_released && d.failing_sample.over(6, 10) {
                        d.failing = Some(Failing { since: now, next_probe: now + FIRST_PROBE, answered: false });
                        changes.push(change(2, State::Paused, now));
                    }
                }
            }
        }

        // Only answers count here: an error is not evidence of health.
        if was_verified && conclusive && kind != Kind::Retry {
            let nothing = answer == Answer::Nothing;
            match &mut d.nothing {
                Some(p) => {
                    p.sample.push(now, nothing, false);
                    if p.sample.enough() && !p.sample.over(1, 2) {
                        changes.push(change(3, State::Clear, p.since));
                        d.nothing = None;
                        d.nothing_sample = Sample::default();
                    }
                }
                None => {
                    d.nothing_sample.push(now, nothing, true);
                    if d.nothing_released {
                        if d.nothing_sample.enough() && !d.nothing_sample.over(1, 2) {
                            d.nothing_released = false;
                        }
                    } else if d.nothing_sample.over(1, 2) {
                        d.nothing = Some(Nothing { since: now, next_probe: now + PROBE_EVERY, sample: Sample::default() });
                        d.nothing_sample = Sample::default();
                        changes.push(change(3, State::Paused, now));
                    }
                }
            }
        }
        changes
    }

    /// Let the holds run out. Call now and then.
    pub fn tick(&mut self, now: i64) -> Vec<Change> {
        let mut changes = Vec::new();
        for (name, d) in &mut self.domains {
            if d.failing.as_ref().is_some_and(|p| now - p.since >= HOLD && !p.answered) {
                let since = d.failing.take().map_or(now, |p| p.since);
                d.failing_released = true;
                d.failing_sample = Sample::default();
                changes.push(Change { domain: name.clone(), pause: 2, state: State::Released, since });
            }
            if d.nothing.as_ref().is_some_and(|p| now - p.since >= HOLD) {
                let since = d.nothing.take().map_or(now, |p| p.since);
                d.nothing_released = true;
                d.nothing_sample = Sample::default();
                changes.push(Change { domain: name.clone(), pause: 3, state: State::Released, since });
            }
        }
        // Domains with nothing to remember are forgotten.
        self.domains.retain(|_, d| {
            d.failing.is_some() || d.nothing.is_some() || d.failing_released || d.nothing_released || !d.failing_sample.0.is_empty() || !d.nothing_sample.0.is_empty()
        });
        changes
    }

    /// A control pause started: what the samples hold is suspect.
    pub fn clear_samples(&mut self) {
        for d in self.domains.values_mut() {
            d.failing_sample = Sample::default();
            d.nothing_sample = Sample::default();
        }
    }

    /// A control pause ended: the holds did not run while it lasted.
    pub fn shift(&mut self, ms: i64) {
        for d in self.domains.values_mut() {
            if let Some(p) = &mut d.failing {
                p.since += ms;
                p.next_probe += ms;
            }
            if let Some(p) = &mut d.nothing {
                p.since += ms;
                p.next_probe += ms;
            }
        }
    }

    /// The domains under each pause, for health.
    pub fn paused(&self) -> (Vec<String>, Vec<String>) {
        let pick = |f: fn(&Domain) -> bool| self.domains.iter().filter(|(_, d)| f(d)).map(|(n, _)| n.clone()).collect();
        (pick(|d| d.failing.is_some()), pick(|d| d.nothing.is_some()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const D: &str = "example.com";
    const SEC: i64 = 1000;
    const MIN: i64 = 60_000;

    fn feed(p: &mut Pauses, kind: Kind, verified: bool, answer: Answer, n: usize, start: i64) -> Vec<Change> {
        let mut all = Vec::new();
        for i in 0..n {
            all.extend(p.record(D, kind, verified, answer, false, start + i as i64 * SEC));
        }
        all
    }

    #[test]
    fn a_failing_domain_pauses_and_a_conclusive_probe_ends_it() {
        let mut p = Pauses::default();
        // Nineteen failures are not enough.
        assert!(feed(&mut p, Kind::First, false, Answer::Failed, 19, 0).is_empty());
        let started = feed(&mut p, Kind::First, false, Answer::Failed, 1, 19 * SEC);
        assert_eq!(started, vec![Change { domain: D.into(), pause: 2, state: State::Paused, since: 19 * SEC }]);
        assert!(p.failing(D));
        // Everything waits until the first probe, a minute in.
        assert_eq!(p.admit(D, Kind::First, 30 * SEC), Admit::Wait);
        assert_eq!(p.admit(D, Kind::Retry, 30 * SEC), Admit::Wait);
        assert_eq!(p.admit(D, Kind::First, 80 * SEC), Admit::Probe);
        // One probe at a time: the next is ten minutes on.
        assert_eq!(p.admit(D, Kind::First, 81 * SEC), Admit::Wait);
        // A failing probe changes nothing.
        assert!(p.record(D, Kind::First, false, Answer::Failed, true, 85 * SEC).is_empty());
        assert_eq!(p.admit(D, Kind::First, 80 * SEC + 10 * MIN), Admit::Probe);
        // A conclusive one ends the pause, and "nothing" is conclusive.
        let ended = p.record(D, Kind::First, false, Answer::Nothing, true, 81 * SEC + 10 * MIN);
        assert_eq!(ended[0].state, State::Clear);
        assert!(!p.failing(D));
        assert_eq!(p.admit(D, Kind::First, 82 * SEC + 10 * MIN), Admit::Go);
    }

    #[test]
    fn a_domain_that_mostly_answers_is_not_paused() {
        let mut p = Pauses::default();
        for i in 0..200 {
            // Half fail: under the 60% it takes.
            let a = if i % 2 == 0 { Answer::Failed } else { Answer::Did };
            assert!(p.record(D, Kind::First, false, a, false, i * SEC).is_empty());
        }
        assert!(!p.failing(D));
    }

    #[test]
    fn retries_cannot_pause_a_working_domain() {
        let mut p = Pauses::default();
        feed(&mut p, Kind::First, false, Answer::Did, 30, 0);
        // A wave of retries of handles broken on their own account.
        assert!(feed(&mut p, Kind::Retry, false, Answer::Failed, 500, 100 * SEC).is_empty());
        assert!(feed(&mut p, Kind::Confirm, true, Answer::Failed, 500, 700 * SEC).is_empty());
        assert!(!p.failing(D));
    }

    #[test]
    fn old_failures_age_out_of_the_sample() {
        let mut p = Pauses::default();
        feed(&mut p, Kind::First, false, Answer::Failed, 15, 0);
        // More than an hour later, ten more: only those ten are in the sample.
        assert!(feed(&mut p, Kind::First, false, Answer::Failed, 10, 2 * HOUR).is_empty());
        assert!(!p.failing(D));
    }

    #[test]
    fn the_hold_releases_a_dead_domain_and_it_cannot_pause_again_until_it_answers() {
        let mut p = Pauses::default();
        feed(&mut p, Kind::First, false, Answer::Failed, 20, 0);
        assert!(p.tick(23 * HOUR).is_empty());
        let released = p.tick(25 * HOUR);
        assert_eq!(released[0].state, State::Released);
        assert!(!p.failing(D));
        // Still dead: checks resume and it is not paused again.
        assert!(feed(&mut p, Kind::First, false, Answer::Failed, 100, 25 * HOUR).is_empty());
        assert_eq!(p.admit(D, Kind::First, 26 * HOUR), Admit::Go);
        // Once it has answered, it can be.
        p.record(D, Kind::First, false, Answer::Did, false, 27 * HOUR);
        assert!(!feed(&mut p, Kind::First, false, Answer::Failed, 40, 27 * HOUR + SEC).is_empty());
    }

    #[test]
    fn a_domain_that_answered_during_the_pause_is_not_released_by_the_hold() {
        let mut p = Pauses::default();
        feed(&mut p, Kind::First, false, Answer::Failed, 20, 0);
        // A check that was in flight when the pause began comes back with an answer.
        p.record(D, Kind::First, false, Answer::Did, false, 30 * SEC);
        assert!(p.tick(30 * HOUR).is_empty());
        assert!(p.failing(D));
    }

    #[test]
    fn verified_handles_resolving_to_nothing_hold_demotions() {
        let mut p = Pauses::default();
        // Unverified handles finding nothing do not count.
        assert!(feed(&mut p, Kind::First, false, Answer::Nothing, 50, 0).is_empty());
        assert!(!p.hold_demotions(D));
        let started = feed(&mut p, Kind::First, true, Answer::Nothing, 20, 60 * SEC);
        assert_eq!(started.last().map(|c| (c.pause, c.state)), Some((3, State::Paused)));
        assert!(p.hold_demotions(D));
        // First attempts go on. Confirming checks wait, apart from a probe.
        assert_eq!(p.admit(D, Kind::First, 100 * SEC), Admit::Go);
        assert_eq!(p.admit(D, Kind::Confirm, 100 * SEC), Admit::Wait);
        assert_eq!(p.admit(D, Kind::Confirm, 80 * SEC + 10 * MIN), Admit::Probe);
        assert_eq!(p.admit(D, Kind::Confirm, 81 * SEC + 10 * MIN), Admit::Wait);
    }

    #[test]
    fn one_handle_resolving_does_not_end_it_but_recovery_does() {
        let mut p = Pauses::default();
        feed(&mut p, Kind::First, true, Answer::Nothing, 20, 0);
        // The provider answers wrongly four times in five.
        for i in 0..100 {
            let a = if i % 5 == 0 { Answer::Did } else { Answer::Nothing };
            assert!(p.record(D, Kind::First, true, a, false, 30 * SEC + i * SEC).is_empty(), "ended at {i}");
        }
        assert!(p.hold_demotions(D));
        // It recovers: once most of the last 200 answers are right, it ends.
        let ended = feed(&mut p, Kind::First, true, Answer::Did, 200, 200 * SEC);
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, State::Clear);
        assert!(!p.hold_demotions(D));
    }

    #[test]
    fn errors_are_not_evidence_of_health() {
        let mut p = Pauses::default();
        feed(&mut p, Kind::First, true, Answer::Nothing, 20, 0);
        assert!(p.hold_demotions(D));
        // The provider turns to errors and 404s with no right answers: 55% and 45%.
        for i in 0..200 {
            let a = if i % 20 < 11 { Answer::Failed } else { Answer::Nothing };
            p.record(D, Kind::Confirm, true, a, false, 30 * SEC + i * SEC);
        }
        assert!(p.hold_demotions(D));
    }

    #[test]
    fn the_nothing_hold_releases_and_rearms_only_after_good_answers() {
        let mut p = Pauses::default();
        feed(&mut p, Kind::First, true, Answer::Nothing, 20, 0);
        assert_eq!(p.tick(25 * HOUR)[0], Change { domain: D.into(), pause: 3, state: State::Released, since: 19 * SEC });
        assert!(!p.hold_demotions(D));
        // Still answering nothing: it does not start again.
        assert!(feed(&mut p, Kind::First, true, Answer::Nothing, 60, 25 * HOUR).is_empty());
        // After a run of good answers it can.
        feed(&mut p, Kind::First, true, Answer::Did, 200, 26 * HOUR);
        assert!(!feed(&mut p, Kind::First, true, Answer::Nothing, 150, 26 * HOUR + 300 * SEC).is_empty());
    }

    #[test]
    fn a_control_pause_stops_the_holds_and_clears_the_samples() {
        let mut p = Pauses::default();
        feed(&mut p, Kind::First, false, Answer::Failed, 20, 0);
        // A control pause of two days: the hold did not run meanwhile.
        p.shift(48 * HOUR);
        assert!(p.tick(49 * HOUR).is_empty());
        assert!(p.failing(D));
        // Samples gathered before a control pause do not start a pause after it.
        let mut q = Pauses::default();
        feed(&mut q, Kind::First, false, Answer::Failed, 19, 0);
        q.clear_samples();
        assert!(feed(&mut q, Kind::First, false, Answer::Failed, 1, 20 * SEC).is_empty());
    }

    #[test]
    fn a_stored_pause_is_restored() {
        let mut p = Pauses::default();
        p.restore(D, 2, State::Paused, 0, 5 * HOUR);
        assert_eq!(p.admit(D, Kind::First, 5 * HOUR), Admit::Wait);
        assert_eq!(p.admit(D, Kind::First, 5 * HOUR + 61 * SEC), Admit::Probe);
        // The hold counts from when the pause began, not from the restart.
        assert_eq!(p.tick(25 * HOUR)[0].state, State::Released);
        p.restore("other.org", 3, State::Released, 0, 0);
        assert!(feed(&mut p, Kind::First, true, Answer::Nothing, 30, 0).iter().all(|c| c.domain == D));
    }
}
