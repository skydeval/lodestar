//! What a check's result does to a handle's status. Pure: times are
//! milliseconds since the epoch, and nothing here touches the database.

use std::time::Duration;

use super::check::Outcome;
use crate::db;

#[derive(Debug, Clone)]
pub struct Rules {
    /// A verified handle that resolves to nothing is demoted only by a
    /// second such result at least this long after the first.
    pub confirming_delay: Duration,
    /// A handle that keeps failing settles at the first failure after
    /// this long, if it is not verified.
    pub settle_unverified: Duration,
    /// The same, for a verified handle.
    pub grace: Duration,
    /// The shortest wait before a retry.
    pub retry_min: Duration,
    /// The longest: the re-check interval.
    pub recheck_interval: Duration,
    /// A host's `Retry-After` is honoured up to this.
    pub retry_after_ceiling: Duration,
}

impl Default for Rules {
    fn default() -> Self {
        const HOUR: u64 = 3600;
        Rules {
            confirming_delay: Duration::from_secs(HOUR),
            settle_unverified: Duration::from_secs(24 * HOUR),
            grace: Duration::from_secs(14 * 24 * HOUR),
            retry_min: Duration::from_secs(12 * HOUR),
            recheck_interval: Duration::from_secs(7 * 24 * HOUR),
            retry_after_ceiling: Duration::from_secs(HOUR),
        }
    }
}

/// A handle's open failure state: failing, awaiting confirmation, or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Failure {
    /// When the first failing result came. None in a row that exists only
    /// because the handle is awaiting confirmation.
    pub first_failure_at: Option<i64>,
    pub last_attempt_at: Option<i64>,
    /// When the next retry or confirming check is due.
    pub next_attempt_at: i64,
    /// When a verified handle first resolved to nothing.
    pub nothing_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureChange {
    /// Leave whatever is there.
    Keep,
    /// There is no failure state any more.
    Clear,
    Set(Failure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// The status to write, if it changes.
    pub status: Option<i16>,
    /// The check set or confirmed the status, so its time is recorded.
    pub checked: bool,
    pub failure: FailureChange,
}

impl Decision {
    fn settled(status: i16) -> Self {
        Decision { status: Some(status), checked: true, failure: FailureChange::Clear }
    }
    fn unchanged() -> Self {
        Decision { status: None, checked: false, failure: FailureChange::Keep }
    }
}

fn ms(d: Duration) -> i64 {
    d.as_millis() as i64
}

/// Decide what a result does.
///
/// - `status` is the handle's current status.
/// - `named_self` is whether a DID the handle resolved to is this account.
/// - `failure` is the open failure state, if any.
/// - `at` is when the resolution was made.
/// - `retry_after` is a wait the host asked for.
/// - `hold_demotions` is set while the domain's verified handles are
///   resolving to nothing in bulk: no such result demotes anything.
pub fn decide(
    rules: &Rules,
    status: i16,
    outcome: &Outcome,
    named_self: bool,
    failure: Option<Failure>,
    at: i64,
    retry_after: Option<Duration>,
    hold_demotions: bool,
) -> Decision {
    match outcome {
        Outcome::Did(_) if named_self => Decision::settled(db::HANDLE_VERIFIED),
        // The handle is someone else's. That account's own verification
        // moves the index.
        Outcome::Did(_) => Decision::settled(db::HANDLE_MISMATCH),
        Outcome::Nothing if status != db::HANDLE_VERIFIED => Decision::settled(db::HANDLE_UNRESOLVED),
        Outcome::Nothing => {
            let open = failure.unwrap_or_default();
            match open.nothing_at {
                // The first such result marks the handle; it stays verified.
                None => Decision {
                    status: None,
                    checked: false,
                    failure: FailureChange::Set(Failure { nothing_at: Some(at), next_attempt_at: at + ms(rules.confirming_delay), ..open }),
                },
                // A later one confirms it, if it is late enough.
                Some(first) if at - first >= ms(rules.confirming_delay) && !hold_demotions => Decision::settled(db::HANDLE_UNRESOLVED),
                // Too soon, whichever path produced it: it cannot stand in
                // for the confirmation.
                Some(_) => Decision::unchanged(),
            }
        }
        Outcome::Inconclusive(_) => {
            // A handle that has already settled as failing gets one attempt
            // a cycle and no failure state.
            if status == db::HANDLE_UNREACHABLE {
                return Decision { status: None, checked: true, failure: FailureChange::Clear };
            }
            let open = failure.unwrap_or_default();
            let first = open.first_failure_at.unwrap_or(at);
            let settle_after = if status == db::HANDLE_VERIFIED { rules.grace } else { rules.settle_unverified };
            if open.first_failure_at.is_some() && at - first >= ms(settle_after) {
                return Decision::settled(db::HANDLE_UNREACHABLE);
            }
            // Wait as long again as it has been failing, within bounds, and
            // at least as long as the host asked.
            let mut wait = (at - first).clamp(ms(rules.retry_min), ms(rules.recheck_interval));
            if let Some(asked) = retry_after {
                wait = wait.max(ms(asked.min(rules.retry_after_ceiling)));
            }
            Decision {
                status: None,
                checked: false,
                failure: FailureChange::Set(Failure {
                    first_failure_at: Some(first),
                    last_attempt_at: Some(at),
                    next_attempt_at: at + wait,
                    nothing_at: open.nothing_at,
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = 3_600_000;

    fn run(status: i16, outcome: Outcome, me: bool, failure: Option<Failure>, at: i64) -> Decision {
        decide(&Rules::default(), status, &outcome, me, failure, at, None, false)
    }

    fn did() -> Outcome {
        Outcome::Did("did:plc:x".into())
    }

    fn failing() -> Outcome {
        Outcome::Inconclusive("test")
    }

    fn set(d: &Decision) -> Failure {
        match &d.failure {
            FailureChange::Set(f) => *f,
            other => panic!("expected a failure row, got {other:?}"),
        }
    }

    #[test]
    fn a_did_is_conclusive_whatever_came_before() {
        let open = Some(Failure { first_failure_at: Some(0), last_attempt_at: Some(0), next_attempt_at: 12 * H, nothing_at: Some(0) });
        for status in [db::HANDLE_PENDING, db::HANDLE_UNREACHABLE, db::HANDLE_VERIFIED, db::HANDLE_MISMATCH] {
            assert_eq!(run(status, did(), true, open, 5), Decision::settled(db::HANDLE_VERIFIED));
            assert_eq!(run(status, did(), false, open, 5), Decision::settled(db::HANDLE_MISMATCH));
        }
    }

    #[test]
    fn nothing_applies_at_once_to_an_unverified_handle() {
        for status in [db::HANDLE_PENDING, db::HANDLE_MISMATCH, db::HANDLE_UNREACHABLE, db::HANDLE_UNRESOLVED] {
            assert_eq!(run(status, Outcome::Nothing, false, None, 5), Decision::settled(db::HANDLE_UNRESOLVED));
        }
    }

    #[test]
    fn a_verified_handle_needs_a_confirming_result() {
        // The first "nothing" only marks it.
        let first = run(db::HANDLE_VERIFIED, Outcome::Nothing, false, None, 10 * H);
        assert_eq!(first.status, None);
        assert!(!first.checked);
        let mark = set(&first);
        assert_eq!(mark.nothing_at, Some(10 * H));
        assert_eq!(mark.next_attempt_at, 11 * H);
        assert_eq!(mark.first_failure_at, None);
        // Two minutes later is too soon, whoever asked.
        assert_eq!(run(db::HANDLE_VERIFIED, Outcome::Nothing, false, Some(mark), 10 * H + 120_000), Decision::unchanged());
        // An hour later it is confirmed.
        assert_eq!(run(db::HANDLE_VERIFIED, Outcome::Nothing, false, Some(mark), 11 * H), Decision::settled(db::HANDLE_UNRESOLVED));
        // A result naming the account clears the mark.
        assert_eq!(run(db::HANDLE_VERIFIED, did(), true, Some(mark), 11 * H), Decision::settled(db::HANDLE_VERIFIED));
    }

    #[test]
    fn nothing_is_demoted_while_demotions_are_held() {
        let mark = Failure { nothing_at: Some(0), next_attempt_at: H, ..Failure::default() };
        let d = decide(&Rules::default(), db::HANDLE_VERIFIED, &Outcome::Nothing, false, Some(mark), 5 * H, None, true);
        assert_eq!(d, Decision::unchanged());
    }

    #[test]
    fn a_failing_confirming_check_keeps_the_mark() {
        let mark = Failure { nothing_at: Some(0), next_attempt_at: H, ..Failure::default() };
        let d = run(db::HANDLE_VERIFIED, failing(), false, Some(mark), H);
        let f = set(&d);
        assert_eq!(f.nothing_at, Some(0));
        assert_eq!(f.first_failure_at, Some(H));
        assert_eq!(f.next_attempt_at, 13 * H);
        assert_eq!(d.status, None);
    }

    #[test]
    fn retries_follow_the_schedule_and_an_unverified_handle_settles_after_a_day() {
        // First failure at 0; retries at 12, 24 hours.
        let d0 = run(db::HANDLE_PENDING, failing(), false, None, 0);
        let f0 = set(&d0);
        assert_eq!((f0.first_failure_at, f0.next_attempt_at), (Some(0), 12 * H));
        let d1 = run(db::HANDLE_PENDING, failing(), false, Some(f0), 12 * H);
        let f1 = set(&d1);
        assert_eq!(f1.next_attempt_at, 24 * H);
        assert_eq!(f1.first_failure_at, Some(0));
        // The first failing result after 24 hours settles it.
        assert_eq!(run(db::HANDLE_PENDING, failing(), false, Some(f1), 24 * H), Decision::settled(db::HANDLE_UNREACHABLE));
        // An early extra attempt, say a consumer's, cannot settle it.
        assert_eq!(set(&run(db::HANDLE_PENDING, failing(), false, Some(f1), 13 * H)).first_failure_at, Some(0));
    }

    #[test]
    fn a_verified_handle_has_the_grace_period() {
        let mut f = set(&run(db::HANDLE_VERIFIED, failing(), false, None, 0));
        let mut at = f.next_attempt_at;
        let mut attempts = vec![0];
        loop {
            attempts.push(at / H);
            let d = run(db::HANDLE_VERIFIED, failing(), false, Some(f), at);
            if d.status == Some(db::HANDLE_UNREACHABLE) {
                break;
            }
            f = set(&d);
            at = f.next_attempt_at;
        }
        // 12, 24, 48, 96 and 192 hours after the first failure, then every
        // interval; settled at the first attempt past 14 days.
        assert_eq!(attempts, vec![0, 12, 24, 48, 96, 192, 360]);
    }

    #[test]
    fn a_settled_handle_that_fails_again_gets_no_row() {
        let d = run(db::HANDLE_UNREACHABLE, failing(), false, None, 99 * H);
        assert_eq!(d, Decision { status: None, checked: true, failure: FailureChange::Clear });
    }

    #[test]
    fn a_host_that_asks_to_wait_is_given_at_least_that() {
        let rules = Rules { retry_min: Duration::from_secs(60), ..Rules::default() };
        let asked = decide(&rules, db::HANDLE_PENDING, &failing(), false, None, 0, Some(Duration::from_secs(600)), false);
        assert_eq!(set(&asked).next_attempt_at, 600_000);
        // Up to the ceiling, an hour.
        let long = decide(&rules, db::HANDLE_PENDING, &failing(), false, None, 0, Some(Duration::from_secs(86_400)), false);
        assert_eq!(set(&long).next_attempt_at, H);
        // And never less than the schedule's own wait.
        let short = decide(&Rules::default(), db::HANDLE_PENDING, &failing(), false, None, 0, Some(Duration::from_secs(5)), false);
        assert_eq!(set(&short).next_attempt_at, 12 * H);
    }
}
