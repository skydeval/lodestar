//! Applying one PLC operation to an identity, in memory. No database here:
//! `store` loads what this needs and writes what it changed.
//!
//! Current state is the newest accepted operation. History is kept only
//! for identities whose claim or PDS has changed, and operations that a
//! recovery nullifies have their history rows removed.

use super::op::{Kind, Op};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Active,
    Tombstoned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistRow {
    pub seq: i32,
    pub claim: Option<String>,
    pub pds: Option<String>,
    /// Set on rows this run wrote. Rows read from the store are not
    /// rewritten, so theirs is not loaded.
    pub changed_at: Option<String>,
    pub new: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentOp {
    pub seq: i64,
    pub cid: Vec<u8>,
    pub created_ms: i64,
    pub created_at: Option<String>,
    /// The history row this operation is credited with, if any.
    pub hist_seq: Option<i32>,
    pub new: bool,
}

/// What lodestar holds for one `did:plc`, and what a page has changed.
///
/// `hist` must hold every history row when `recent` is not empty, and at
/// least the last row otherwise.
#[derive(Debug, Clone, Default)]
pub struct Identity {
    /// A row exists in `accounts`.
    pub exists: bool,
    /// An operation has been applied in this run.
    pub dirty: bool,
    pub claim: Option<String>,
    pub pds: Option<String>,
    pub key: Option<Vec<u8>>,
    pub status: Status,
    pub created_at: Option<String>,
    /// The claim is not what the stored row has, so the handle's status
    /// starts again.
    pub claim_changed: bool,
    pub hist: Vec<HistRow>,
    pub hist_deleted: Vec<i32>,
    pub recent: Vec<RecentOp>,
    pub recent_deleted: Vec<i64>,
}

impl Identity {
    fn known(&self) -> bool {
        self.exists || self.dirty
    }

    fn drop_row(&mut self, ix: usize) {
        let row = self.hist.remove(ix);
        if !row.new {
            self.hist_deleted.push(row.seq);
        }
    }

    /// Remove the recent operations a fork nullifies, and the history rows
    /// they are credited with. Returns how many operations were nullified.
    fn nullify_after(&mut self, prev: Option<&[u8]>) -> usize {
        let keep = match prev.and_then(|p| self.recent.iter().position(|r| r.cid == p)) {
            Some(ix) => ix + 1,
            None => 0,
        };
        let gone = self.recent.split_off(keep);
        for r in &gone {
            if let Some(h) = r.hist_seq {
                if let Some(ix) = self.hist.iter().position(|x| x.seq == h) {
                    self.drop_row(ix);
                }
            }
            if !r.new {
                self.recent_deleted.push(r.seq);
            }
        }
        gone.len()
    }

    /// The original-state row: what the identity had until its first
    /// change, dated from its creation and credited to no operation.
    fn write_original(&mut self) {
        self.hist.push(HistRow {
            seq: 0,
            claim: self.claim.clone(),
            pds: self.pds.clone(),
            changed_at: self.created_at.clone(),
            new: true,
        });
    }

    /// Apply the next operation for this DID. `window_ms` is the recovery
    /// window, measured in operation time. Returns the number of earlier
    /// operations this one nullified.
    pub fn apply(&mut self, op: &Op, window_ms: i64) -> usize {
        // Operations older than the window can no longer be nullified.
        let stale = self.recent.iter().take_while(|r| op.created_ms - r.created_ms > window_ms).count();
        self.recent.drain(..stale);

        let mut nullified = 0;
        let forked = match self.recent.last() {
            Some(head) => op.prev.as_deref() != Some(head.cid.as_slice()),
            None => false,
        };
        if forked {
            nullified = self.nullify_after(op.prev.as_deref());
        }

        let mut credited = None;
        match op.kind {
            Kind::Other => {
                if !self.known() {
                    self.created_at = Some(op.created_at.clone());
                    self.claim_changed = true;
                }
            }
            Kind::Tombstone => {
                if self.known() && self.hist.is_empty() {
                    self.write_original();
                }
                if !self.known() {
                    self.created_at = Some(op.created_at.clone());
                }
                self.claim_changed |= self.claim.is_some() || !self.known();
                self.status = Status::Tombstoned;
                self.claim = None;
                self.pds = None;
                self.key = None;
            }
            Kind::State => {
                if !self.known() {
                    self.created_at = Some(op.created_at.clone());
                    self.claim_changed = true;
                } else {
                    // The last surviving state: the last history row, or
                    // the account's own claim and PDS when there is none.
                    let unchanged = match self.hist.last() {
                        Some(h) => h.claim == op.claim && h.pds == op.pds,
                        None => self.claim == op.claim && self.pds == op.pds,
                    };
                    if !unchanged {
                        if self.hist.is_empty() {
                            self.write_original();
                        }
                        let seq = self.hist.last().map_or(0, |h| h.seq + 1);
                        self.hist.push(HistRow {
                            seq,
                            claim: op.claim.clone(),
                            pds: op.pds.clone(),
                            changed_at: Some(op.created_at.clone()),
                            new: true,
                        });
                        credited = Some(seq);
                    }
                    self.claim_changed |= self.claim != op.claim;
                }
                self.status = Status::Active;
                self.claim = op.claim.clone();
                self.pds = op.pds.clone();
                self.key = op.key.clone();

                // A recovery that leaves only the original-state row, equal
                // to the current state, leaves an identity that has never
                // changed. It goes back to having no rows.
                if forked && self.hist.len() == 1 {
                    let h = &self.hist[0];
                    if h.seq == 0 && h.claim == self.claim && h.pds == self.pds {
                        self.drop_row(0);
                    }
                }
            }
        }

        self.dirty = true;
        self.recent.push(RecentOp {
            seq: op.seq,
            cid: op.cid.clone(),
            created_ms: op.created_ms,
            created_at: Some(op.created_at.clone()),
            hist_seq: credited,
            new: true,
        });
        nullified
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000;
    const WINDOW: i64 = 84 * HOUR;

    fn op(seq: i64, hour: i64, prev: Option<i64>, claim: Option<&str>, pds: Option<&str>) -> Op {
        Op {
            did: "did:plc:test".into(),
            seq,
            cid: vec![seq as u8],
            prev: prev.map(|p| vec![p as u8]),
            created_at: format!("t{hour}"),
            created_ms: hour * HOUR,
            kind: Kind::State,
            claim: claim.map(str::to_owned),
            pds: pds.map(str::to_owned),
            key: Some(vec![9]),
        }
    }

    fn tombstone(seq: i64, hour: i64, prev: i64) -> Op {
        Op { kind: Kind::Tombstone, claim: None, pds: None, key: None, ..op(seq, hour, Some(prev), None, None) }
    }

    fn rows(id: &Identity) -> Vec<(i32, Option<&str>, Option<&str>)> {
        id.hist.iter().map(|h| (h.seq, h.claim.as_deref(), h.pds.as_deref())).collect()
    }

    /// What the store would hold after writing `id`: nothing is new any more.
    fn stored(mut id: Identity) -> Identity {
        id.exists = true;
        id.dirty = false;
        id.claim_changed = false;
        id.hist_deleted.clear();
        id.recent_deleted.clear();
        id.hist.iter_mut().for_each(|h| h.new = false);
        id.recent.iter_mut().for_each(|r| r.new = false);
        id
    }

    #[test]
    fn genesis_writes_no_history() {
        let mut id = Identity::default();
        assert_eq!(id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW), 0);
        assert!(id.hist.is_empty());
        assert!(id.claim_changed);
        assert_eq!(id.created_at.as_deref(), Some("t0"));
        assert_eq!(id.recent.len(), 1);
        assert_eq!(id.recent[0].hist_seq, None);
    }

    #[test]
    fn key_only_change_writes_no_history() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        let mut id = stored(id);
        let mut rotate = op(2, 1, Some(1), Some("a.example"), Some("p1"));
        rotate.key = Some(vec![7]);
        id.apply(&rotate, WINDOW);
        assert!(id.hist.is_empty());
        assert!(!id.claim_changed);
        assert_eq!(id.key, Some(vec![7]));
    }

    #[test]
    fn first_change_writes_the_original_row_and_the_new_one() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        let mut id = stored(id);
        id.apply(&op(2, 200, Some(1), Some("b.example"), Some("p1")), WINDOW);
        assert_eq!(rows(&id), vec![(0, Some("a.example"), Some("p1")), (1, Some("b.example"), Some("p1"))]);
        assert_eq!(id.hist[0].changed_at.as_deref(), Some("t0"));
        assert_eq!(id.hist[1].changed_at.as_deref(), Some("t200"));
        assert!(id.claim_changed);
        // Only the second row is credited to the operation.
        assert_eq!(id.recent.last().unwrap().hist_seq, Some(1));
        // A PDS move alone is a change of history but not of claim.
        let mut id = stored(id);
        id.apply(&op(3, 400, Some(2), Some("b.example"), Some("p2")), WINDOW);
        assert_eq!(rows(&id).last(), Some(&(2, Some("b.example"), Some("p2"))));
        assert!(!id.claim_changed);
    }

    #[test]
    fn old_operations_leave_the_window() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), None), WINDOW);
        // 85 hours later, prev does not matter: nothing recent can fork.
        assert_eq!(id.apply(&op(2, 85, Some(99), Some("a.example"), None), WINDOW), 0);
        assert_eq!(id.recent.len(), 1);
        assert!(id.hist.is_empty());
    }

    #[test]
    fn recovery_removes_the_rows_of_nullified_operations() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        id.apply(&op(2, 1, Some(1), Some("thief.example"), Some("evil")), WINDOW);
        id.apply(&op(3, 2, Some(2), Some("thief2.example"), Some("evil")), WINDOW);
        let mut id = stored(id);
        assert_eq!(rows(&id).len(), 3);
        // The recovery follows operation 1 and moves the account.
        assert_eq!(id.apply(&op(4, 3, Some(1), Some("a.example"), Some("p2")), WINDOW), 2);
        assert_eq!(rows(&id), vec![(0, Some("a.example"), Some("p1")), (1, Some("a.example"), Some("p2"))]);
        assert_eq!(id.hist_deleted, vec![1, 2]);
        assert_eq!(id.recent_deleted, vec![2, 3]);
        assert_eq!(id.recent.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 4]);
    }

    #[test]
    fn recovery_to_the_original_state_leaves_no_history() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        id.apply(&op(2, 1, Some(1), Some("thief.example"), Some("evil")), WINDOW);
        let mut id = stored(id);
        id.apply(&op(3, 2, Some(1), Some("a.example"), Some("p1")), WINDOW);
        assert!(id.hist.is_empty());
        assert_eq!(id.hist_deleted, vec![1, 0]);
        assert_eq!(id.claim.as_deref(), Some("a.example"));
        assert_eq!(id.status, Status::Active);
    }

    #[test]
    fn a_fork_inside_one_page_writes_nothing_it_then_removes() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        id.apply(&op(2, 1, Some(1), Some("thief.example"), Some("evil")), WINDOW);
        id.apply(&op(3, 2, Some(1), Some("a.example"), Some("p1")), WINDOW);
        assert!(id.hist.is_empty());
        assert!(id.hist_deleted.is_empty());
        assert!(id.recent_deleted.is_empty());
    }

    #[test]
    fn prev_older_than_the_window_nullifies_everything_recent() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        let mut id = stored(id);
        id.apply(&op(2, 100, Some(1), Some("b.example"), Some("p1")), WINDOW);
        id.apply(&op(3, 101, Some(2), Some("c.example"), Some("p1")), WINDOW);
        let mut id = stored(id);
        // Follows operation 1, which left the window long ago.
        assert_eq!(id.apply(&op(4, 102, Some(1), Some("d.example"), Some("p1")), WINDOW), 2);
        assert_eq!(rows(&id), vec![(0, Some("a.example"), Some("p1")), (1, Some("d.example"), Some("p1"))]);
    }

    #[test]
    fn tombstone_keeps_the_state_it_clears() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        let mut id = stored(id);
        id.apply(&tombstone(2, 1, 1), WINDOW);
        assert_eq!(id.status, Status::Tombstoned);
        assert_eq!((id.claim.clone(), id.pds.clone(), id.key.clone()), (None, None, None));
        assert!(id.claim_changed);
        assert_eq!(rows(&id), vec![(0, Some("a.example"), Some("p1"))]);
        assert_eq!(id.recent.last().unwrap().hist_seq, None);
    }

    #[test]
    fn an_undone_tombstone_is_compared_with_the_state_before_it() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        id.apply(&tombstone(2, 1, 1), WINDOW);
        let mut id = stored(id);
        // Recovery restores the same state: an identity that never changed.
        let mut same = id.clone();
        same.apply(&op(3, 2, Some(1), Some("a.example"), Some("p1")), WINDOW);
        assert_eq!(same.status, Status::Active);
        assert!(same.hist.is_empty());
        assert_eq!(same.hist_deleted, vec![0]);
        assert!(same.claim_changed);
        // Recovery to a new PDS: the original row stays and one is added.
        id.apply(&op(3, 2, Some(1), Some("a.example"), Some("p2")), WINDOW);
        assert_eq!(rows(&id), vec![(0, Some("a.example"), Some("p1")), (1, Some("a.example"), Some("p2"))]);
    }

    #[test]
    fn only_the_last_row_is_needed_when_nothing_is_recent() {
        // An identity with five rows in the store, of which the last is loaded.
        let mut id = Identity {
            exists: true,
            claim: Some("e.example".into()),
            pds: Some("p1".into()),
            created_at: Some("t0".into()),
            hist: vec![HistRow { seq: 4, claim: Some("e.example".into()), pds: Some("p1".into()), changed_at: None, new: false }],
            ..Identity::default()
        };
        id.apply(&op(10, 1000, Some(9), Some("f.example"), Some("p1")), WINDOW);
        // Nullified within the same page by a recovery whose prev is old.
        id.apply(&op(11, 1001, Some(9), Some("e.example"), Some("p1")), WINDOW);
        assert_eq!(rows(&id), vec![(4, Some("e.example"), Some("p1"))]);
        assert!(id.hist_deleted.is_empty());
    }

    #[test]
    fn unknown_operations_stay_in_the_chain() {
        let mut id = Identity::default();
        id.apply(&op(1, 0, None, Some("a.example"), Some("p1")), WINDOW);
        let other = Op { kind: Kind::Other, claim: None, pds: None, key: None, ..op(2, 1, Some(1), None, None) };
        assert_eq!(id.apply(&other, WINDOW), 0);
        assert_eq!(id.claim.as_deref(), Some("a.example"));
        // The next operation follows it, so it is no fork.
        assert_eq!(id.apply(&op(3, 2, Some(2), Some("a.example"), Some("p1")), WINDOW), 0);
        assert!(id.hist.is_empty());
    }
}
