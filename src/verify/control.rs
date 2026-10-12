//! Control checks: is the fault lodestar's own?
//!
//! A few handles with known DIDs are resolved every minute, each by one
//! method, DNS or HTTPS. If more than half of either method's controls
//! fail, the fault is lodestar's resolver or network, and all outbound
//! verification pauses: nothing is attempted, so nothing can fail and be
//! charged to a handle. This module holds the judging, which is pure, and
//! the gate the rest of the role waits on.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};
use tokio::sync::Notify;
use tokio::time::Instant;

use super::limiter::registrable;
use crate::handle;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Control {
    pub handle: String,
    pub did: String,
    /// Resolved by HTTPS. Otherwise by DNS.
    pub https: bool,
}

/// Parse `dns:handle=did` or `https:handle=did`.
pub fn parse(spec: &str) -> Result<Control> {
    let (method, rest) = spec.split_once(':').unwrap_or(("", spec));
    let https = match method {
        "dns" => false,
        "https" => true,
        _ => bail!("a control is written dns:handle=did or https:handle=did, not {spec}"),
    };
    let Some((h, did)) = rest.split_once('=') else {
        bail!("a control is written dns:handle=did or https:handle=did, not {spec}");
    };
    let Some(handle) = handle::normalize(h) else {
        bail!("{h} is not a valid handle");
    };
    if !super::check::valid_did(did) {
        bail!("{did} is not a valid DID");
    }
    Ok(Control {
        handle,
        did: did.to_owned(),
        https,
    })
}

/// At least two controls of each method, each on its own registrable domain.
pub fn validate(list: &[Control]) -> Result<()> {
    for https in [false, true] {
        let name = if https { "HTTPS" } else { "DNS" };
        let mut domains: Vec<&str> = list
            .iter()
            .filter(|c| c.https == https)
            .map(|c| registrable(&c.handle))
            .collect();
        let n = domains.len();
        domains.sort_unstable();
        domains.dedup();
        if domains.len() != n {
            bail!("two {name} controls share a registrable domain; each needs its own");
        }
        if n < 2 {
            bail!(
                "{n} {name} controls are configured; at least two are needed, each on its own domain"
            );
        }
    }
    Ok(())
}

/// A control left out after failing alone this long.
const LEAVE_OUT_AFTER_MS: i64 = 24 * 3_600_000;
/// Rounds without a failing method before a pause ends.
const ROUNDS_TO_RESUME: u32 = 3;

/// What lodestar remembers about one control. Stored, so a restart
/// changes nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Memory {
    /// Since when it has failed every round.
    pub failing_since: Option<i64>,
    /// Rounds since then, and those in which another control of its
    /// method passed.
    pub rounds: i64,
    pub other_passed: i64,
    pub left_out: bool,
    /// What it did in its last round.
    pub last: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pause,
    Resume,
    /// As it was.
    Stay,
}

pub struct Judge {
    https: Vec<bool>,
    pub memory: Vec<Memory>,
    pub paused: bool,
    clear: u32,
    /// Clear rounds needed to open. One after a start, three after a pause.
    need: u32,
}

impl Judge {
    /// `paused` is whether a pause was in force when lodestar stopped.
    /// Either way nothing runs until the controls say so.
    pub fn new(controls: &[Control], memory: Vec<Memory>, paused: bool) -> Self {
        let mut memory = memory;
        memory.resize(controls.len(), Memory::default());
        Judge {
            https: controls.iter().map(|c| c.https).collect(),
            memory,
            paused: true,
            clear: 0,
            need: if paused { ROUNDS_TO_RESUME } else { 1 },
        }
    }

    fn failing_method(&self, https: bool, passed: &[bool]) -> bool {
        let counted: Vec<usize> = (0..self.https.len())
            .filter(|i| self.https[*i] == https && !self.memory[*i].left_out)
            .collect();
        let failed = counted.iter().filter(|i| !passed[**i]).count();
        failed * 2 > counted.len()
    }

    /// Judge one round. `results[i]` is None for a control that was not
    /// asked because its domain has told lodestar to wait: it counts as
    /// whatever its last round was, or as failed if it has had none.
    pub fn round(&mut self, results: &[Option<bool>], now: i64) -> Verdict {
        let passed: Vec<bool> = results
            .iter()
            .zip(&self.memory)
            .map(|(r, m)| r.or(m.last).unwrap_or(false))
            .collect();

        for i in 0..passed.len() {
            let other_passed =
                (0..passed.len()).any(|j| j != i && self.https[j] == self.https[i] && passed[j]);
            let m = &mut self.memory[i];
            m.last = Some(passed[i]);
            if passed[i] {
                *m = Memory {
                    last: Some(true),
                    ..Memory::default()
                };
            } else {
                m.failing_since.get_or_insert(now);
                m.rounds += 1;
                m.other_passed += i64::from(other_passed);
            }
        }
        // Leave out a control that has gone bad on its own, the one that
        // has failed longest first, never taking a method below two.
        for https in [false, true] {
            let mut counted = (0..passed.len())
                .filter(|i| self.https[*i] == https && !self.memory[*i].left_out)
                .count();
            let mut candidates: Vec<usize> = (0..passed.len())
                .filter(|i| {
                    let m = &self.memory[*i];
                    self.https[*i] == https
                        && !m.left_out
                        && m.failing_since
                            .is_some_and(|since| now - since >= LEAVE_OUT_AFTER_MS)
                        && m.other_passed * 2 > m.rounds
                })
                .collect();
            candidates.sort_by_key(|i| (self.memory[*i].failing_since, *i));
            for i in candidates {
                if counted <= 2 {
                    break;
                }
                self.memory[i].left_out = true;
                counted -= 1;
            }
        }

        if self.failing_method(false, &passed) || self.failing_method(true, &passed) {
            self.clear = 0;
            if self.paused {
                // A pause already in force, or the start-up wait: now a real pause.
                self.need = ROUNDS_TO_RESUME;
                return Verdict::Stay;
            }
            self.paused = true;
            self.need = ROUNDS_TO_RESUME;
            return Verdict::Pause;
        }
        self.clear += 1;
        if self.paused && self.clear >= self.need {
            self.paused = false;
            return Verdict::Resume;
        }
        Verdict::Stay
    }
}

/// What the rest of the role waits on. Closed while a control pause is in
/// force, and until the first clear round after a start.
pub struct Gate {
    open: AtomicBool,
    /// Goes up each time the gate closes. A check that began under an
    /// earlier value was in flight when a pause started, and its result
    /// is dropped.
    epoch: AtomicU64,
    opened: Notify,
    /// DIDs whose results were applied recently. When a pause starts they
    /// are suspect: the fault began before a round caught it.
    recent: Mutex<VecDeque<(Instant, Vec<u8>)>>,
    lookback: Duration,
}

impl Gate {
    pub fn new(lookback: Duration) -> Self {
        Gate {
            open: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            opened: Notify::new(),
            recent: Mutex::new(VecDeque::new()),
            lookback,
        }
    }

    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub async fn wait_open(&self) {
        loop {
            let opened = self.opened.notified();
            tokio::pin!(opened);
            opened.as_mut().enable();
            if self.is_open() {
                return;
            }
            opened.await;
        }
    }

    pub fn close(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.open.store(false, Ordering::Release);
    }

    pub fn open(&self) {
        self.open.store(true, Ordering::Release);
        self.opened.notify_waiters();
    }

    /// Record that a result was applied for this DID.
    pub fn note(&self, did: &[u8]) {
        let now = Instant::now();
        let mut recent = self.recent.lock().expect("gate lock");
        while recent
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > self.lookback)
        {
            recent.pop_front();
        }
        recent.push_back((now, did.to_vec()));
    }

    /// The DIDs whose results were applied inside the lookback, emptying the list.
    pub fn take_recent(&self) -> Vec<Vec<u8>> {
        let now = Instant::now();
        let mut recent = self.recent.lock().expect("gate lock");
        recent
            .drain(..)
            .filter(|(at, _)| now.duration_since(*at) <= self.lookback)
            .map(|(_, did)| did)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000;

    fn controls(dns: usize, https: usize) -> Vec<Control> {
        let one = |i: usize, https: bool| Control {
            handle: format!("c{i}.example{i}.com"),
            did: "did:plc:x".into(),
            https,
        };
        (0..dns)
            .map(|i| one(i, false))
            .chain((dns..dns + https).map(|i| one(i, true)))
            .collect()
    }

    fn all(n: usize, pass: bool) -> Vec<Option<bool>> {
        vec![Some(pass); n]
    }

    /// A judge past its start-up round.
    fn running(dns: usize, https: usize) -> Judge {
        let mut j = Judge::new(&controls(dns, https), Vec::new(), false);
        assert_eq!(j.round(&all(dns + https, true), 0), Verdict::Resume);
        j
    }

    #[test]
    fn specs_are_parsed_and_checked() {
        let c = parse("dns:Example.com=did:plc:abc").unwrap();
        assert_eq!(
            c,
            Control {
                handle: "example.com".into(),
                did: "did:plc:abc".into(),
                https: false
            }
        );
        assert!(
            parse("https:a.example.org=did:web:example.org")
                .unwrap()
                .https
        );
        for bad in [
            "example.com=did:plc:abc",
            "ftp:example.com=did:plc:abc",
            "dns:example.com",
            "dns:nodot=did:plc:abc",
            "dns:example.com=nope",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
        assert!(validate(&controls(2, 2)).is_ok());
        assert!(validate(&controls(1, 2)).is_err());
        assert!(validate(&controls(2, 1)).is_err());
        let mut same = controls(2, 2);
        same[1].handle = "other.example0.com".into();
        assert!(validate(&same).is_err());
    }

    #[test]
    fn nothing_runs_after_a_start_until_one_clear_round() {
        let mut j = Judge::new(&controls(3, 3), Vec::new(), false);
        assert!(j.paused);
        assert_eq!(j.round(&all(6, false), 0), Verdict::Stay);
        assert!(j.paused);
        // Having failed, it is now a pause like any other: three clear rounds.
        assert_eq!(j.round(&all(6, true), MIN), Verdict::Stay);
        assert_eq!(j.round(&all(6, true), 2 * MIN), Verdict::Stay);
        assert_eq!(j.round(&all(6, true), 3 * MIN), Verdict::Resume);
    }

    #[test]
    fn a_stored_pause_needs_three_clear_rounds_after_a_restart() {
        let mut j = Judge::new(&controls(2, 2), Vec::new(), true);
        assert_eq!(j.round(&all(4, true), 0), Verdict::Stay);
        assert_eq!(j.round(&all(4, true), MIN), Verdict::Stay);
        assert_eq!(j.round(&all(4, true), 2 * MIN), Verdict::Resume);
    }

    #[test]
    fn either_method_failing_pauses_and_the_methods_are_judged_apart() {
        let mut j = running(3, 3);
        // One DNS control down: one of three is not more than half.
        assert_eq!(
            j.round(
                &[
                    Some(false),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(true)
                ],
                MIN
            ),
            Verdict::Stay
        );
        assert!(!j.paused);
        // Every HTTPS control down while DNS is fine: the HTTPS path is broken.
        assert_eq!(
            j.round(
                &[
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    Some(false)
                ],
                2 * MIN
            ),
            Verdict::Pause
        );
        // Two of three is still failing.
        assert_eq!(
            j.round(
                &[
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false)
                ],
                3 * MIN
            ),
            Verdict::Stay
        );
        // Back: three clear rounds, and a failing round in between starts the count again.
        assert_eq!(j.round(&all(6, true), 4 * MIN), Verdict::Stay);
        assert_eq!(j.round(&all(6, true), 5 * MIN), Verdict::Stay);
        assert_eq!(
            j.round(
                &[
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    Some(true)
                ],
                6 * MIN
            ),
            Verdict::Stay
        );
        assert_eq!(j.round(&all(6, true), 7 * MIN), Verdict::Stay);
        assert_eq!(j.round(&all(6, true), 8 * MIN), Verdict::Stay);
        assert_eq!(j.round(&all(6, true), 9 * MIN), Verdict::Resume);
    }

    #[test]
    fn one_dead_control_of_two_cannot_start_a_pause_or_keep_one_going() {
        let mut j = running(2, 2);
        let dead = [Some(true), Some(true), Some(false), Some(true)];
        for minute in 1..10 {
            assert_eq!(j.round(&dead, minute * MIN), Verdict::Stay);
        }
        assert!(!j.paused);
        // The live one blips: both fail, a pause starts.
        assert_eq!(
            j.round(
                &[Some(true), Some(true), Some(false), Some(false)],
                10 * MIN
            ),
            Verdict::Pause
        );
        // It comes back. The dead one is still dead, and the pause still ends.
        assert_eq!(j.round(&dead, 11 * MIN), Verdict::Stay);
        assert_eq!(j.round(&dead, 12 * MIN), Verdict::Stay);
        assert_eq!(j.round(&dead, 13 * MIN), Verdict::Resume);
    }

    #[test]
    fn a_control_that_fails_alone_for_a_day_is_left_out_down_to_two() {
        let mut j = running(2, 3);
        let one_dead = [Some(true), Some(true), Some(false), Some(true), Some(true)];
        let mut now = 0;
        for _ in 0..(24 * 60 + 1) {
            now += MIN;
            j.round(&one_dead, now);
        }
        assert!(j.memory[2].left_out);
        // A second goes bad. At two counted it stays counted.
        let two_dead = [Some(true), Some(true), Some(false), Some(false), Some(true)];
        for _ in 0..(24 * 60 + 5) {
            now += MIN;
            assert_eq!(j.round(&two_dead, now), Verdict::Stay);
        }
        assert!(!j.memory[3].left_out);
        assert!(!j.paused);
        // The first one recovers and is counted again; now the second can go.
        now += MIN;
        j.round(
            &[Some(true), Some(true), Some(true), Some(false), Some(true)],
            now,
        );
        assert!(!j.memory[2].left_out);
        now += MIN;
        j.round(
            &[Some(true), Some(true), Some(true), Some(false), Some(true)],
            now,
        );
        assert!(j.memory[3].left_out);
    }

    #[test]
    fn a_total_outage_leaves_nothing_out() {
        let mut j = running(3, 3);
        let mut now = 0;
        assert_eq!(j.round(&all(6, false), MIN), Verdict::Pause);
        for _ in 0..(3 * 24 * 60) {
            now += MIN;
            j.round(&all(6, false), now + MIN);
        }
        assert!(j.memory.iter().all(|m| !m.left_out));
        assert!(j.paused);
    }

    #[test]
    fn a_held_control_keeps_its_last_result() {
        let mut j = running(2, 2);
        // Both HTTPS controls held: they count as passed, as last round.
        assert_eq!(
            j.round(&[Some(true), Some(true), None, None], MIN),
            Verdict::Stay
        );
        assert!(!j.paused);
        // With no earlier round a held control counts as failed.
        let mut fresh = Judge::new(&controls(2, 2), Vec::new(), false);
        assert_eq!(
            fresh.round(&[Some(true), Some(true), None, None], 0),
            Verdict::Stay
        );
        assert!(fresh.paused);
    }

    #[test]
    fn the_left_out_set_survives_a_restart() {
        let mut memory = vec![Memory::default(); 5];
        memory[2] = Memory {
            failing_since: Some(0),
            rounds: 2000,
            other_passed: 2000,
            left_out: true,
            last: Some(false),
        };
        memory[3] = Memory {
            failing_since: Some(0),
            rounds: 10,
            other_passed: 10,
            left_out: false,
            last: Some(false),
        };
        let mut j = Judge::new(&controls(2, 3), memory, false);
        // Two of three HTTPS controls are dead, one of them left out: one
        // of two counted is not more than half, so it opens at once.
        assert_eq!(
            j.round(
                &[Some(true), Some(true), Some(false), Some(false), Some(true)],
                MIN
            ),
            Verdict::Resume
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_gate_remembers_recent_results_only() {
        let g = Gate::new(Duration::from_secs(600));
        assert!(!g.is_open());
        g.note(b"old");
        tokio::time::sleep(Duration::from_secs(700)).await;
        g.note(b"new");
        assert_eq!(g.take_recent(), vec![b"new".to_vec()]);
        assert!(g.take_recent().is_empty());
        let before = g.epoch();
        g.open();
        g.wait_open().await;
        g.close();
        assert_eq!(g.epoch(), before + 1);
        assert!(!g.is_open());
    }
}
