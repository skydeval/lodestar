//! The verification role: the only one that makes requests to arbitrary
//! hosts. It checks handles and fetches `did:web` documents.

pub mod check;
pub mod control;
pub mod decide;
pub mod domains;
pub mod lap;
pub mod limiter;
pub mod pause;
pub mod shared;
pub mod store;
pub mod web;
pub mod worker;

use anyhow::Result;
use tokio_postgres::Client;

use crate::{encoding, handle};

/// Check the handles of the named accounts once, now, and apply what is
/// found. For trying the verification path by hand.
pub async fn check_accounts(client: &mut Client, checker: &check::Checker, dids: &[String]) -> Result<()> {
    let rules = decide::Rules::default();
    for did in dids {
        let key = encoding::did_bytes(did);
        let row = client.query_opt("SELECT handle_claim FROM accounts WHERE did = $1 AND status = 0", &[&key]).await?;
        let claim: Option<String> = row.and_then(|r| r.get(0));
        let Some(claim) = claim.filter(|c| !handle::never_resolves(c)) else {
            println!("{did}: no handle to check");
            continue;
        };
        let found = checker.check(&claim).await;
        let at = chrono::Utc::now().timestamp_millis();
        let applied = store::apply_handle_result(client, &rules, &key, &claim, &found, at, false).await?;
        println!("{did}: {claim} -> {:?} -> {applied:?}", found.outcome);
    }
    Ok(())
}
