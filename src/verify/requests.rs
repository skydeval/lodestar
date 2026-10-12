//! Live resolutions asked for by the API role. The API role makes no
//! outbound request: it writes a row, and the verification role does the
//! work and writes the outcome back.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::check::Outcome;
use super::domains::{Admit, Kind};
use super::limiter;
use super::store::{self, Applied};
use super::web;
use super::worker::Shared;
use crate::{db, encoding};

/// What is asked for.
pub const ASK_HANDLE: i16 = 0;
pub const ASK_DOCUMENT: i16 = 1;

/// The words written back.
pub const FOUND: &str = "found";
pub const NOT_FOUND: &str = "not_found";
pub const UNAVAILABLE: &str = "unavailable";
/// The handle resolves to a `did:plc` lodestar does not have.
pub const UNKNOWN_PLC: &str = "unknown_plc";
/// It resolves to an account whose stored claim is a different handle.
pub const CLAIM_DIFFERS: &str = "claim_differs";
pub const STORED: &str = "stored";
pub const NOT_AN_ACCOUNT: &str = "not_an_account";

pub struct Request {
    pub id: i64,
    pub kind: i16,
    pub subject: String,
}

/// Take the requests still waiting and still worth doing.
pub async fn take(client: &Client) -> Result<Vec<Request>> {
    let rows = client
        .query(
            "UPDATE resolve_requests SET state = 1 WHERE id IN ( \
                 SELECT id FROM resolve_requests WHERE state = 0 AND deadline > now() ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED \
             ) RETURNING id, kind, subject",
            &[],
        )
        .await
        .context("taking live requests")?;
    Ok(rows
        .iter()
        .map(|r| Request {
            id: r.get(0),
            kind: r.get(1),
            subject: r.get(2),
        })
        .collect())
}

async fn answer(client: &Client, id: i64, outcome: &str, did: Option<&[u8]>) -> Result<()> {
    client
        .execute(
            "UPDATE resolve_requests SET state = 2, outcome = $2, did = $3 WHERE id = $1",
            &[&id, &outcome, &did],
        )
        .await?;
    Ok(())
}

/// The account a DID names, if lodestar has it: whether it is active, and
/// its claim.
async fn account(client: &Client, did: &[u8]) -> Result<Option<(bool, Option<String>)>> {
    let row = client
        .query_opt(
            "SELECT status, handle_claim FROM accounts WHERE did = $1",
            &[&did],
        )
        .await?;
    Ok(row.map(|r| (r.get::<_, i16>(0) == db::STATUS_ACTIVE, r.get(1))))
}

/// Fetch a did:web's document and store the account if it is one. None
/// if the fetch failed or was dropped; otherwise whether it was stored.
async fn fetch_and_store(shared: &Shared, did_text: &str, did: &[u8]) -> Result<Option<bool>> {
    let Some(host) = web::host(did_text) else {
        return Ok(Some(false));
    };
    let domain = limiter::registrable(host).to_owned();
    if !shared.gate.is_open() {
        return Ok(None);
    }
    let epoch = shared.gate.epoch();
    let (body, retry_after) = {
        let _permit = shared.limiter.acquire(&domain).await;
        shared.checker.fetch_document(host).await
    };
    if let Some(wait) = retry_after {
        shared
            .limiter
            .hold(&domain, wait.min(shared.rules.retry_after_ceiling));
    }
    if shared.gate.epoch() != epoch {
        return Ok(None);
    }
    let Some(document) = body.and_then(|b| web::read(did_text, &b)) else {
        return Ok(None);
    };
    // On this path a document that declares no PDS is not an account, and
    // nothing is stored.
    if document.pds.is_none() {
        return Ok(Some(false));
    }
    let mut client = shared.db().lock().await;
    web::apply(
        &mut client,
        &web::Rules::default(),
        did,
        Some(&document),
        false,
    )
    .await?;
    Ok(Some(true))
}

async fn resolve_handle(shared: &Shared, handle: &str) -> Result<(&'static str, Option<Vec<u8>>)> {
    let domain = limiter::registrable(handle).to_owned();
    // While a pause covers the work, the request is answered at once,
    // unless it is taken as the domain's probe.
    if !shared.gate.is_open() {
        return Ok((UNAVAILABLE, None));
    }
    let admit = shared.pauses.lock().expect("pauses lock").admit(
        &domain,
        Kind::First,
        chrono::Utc::now().timestamp_millis(),
    );
    if admit == Admit::Wait {
        return Ok((UNAVAILABLE, None));
    }
    let Some(found) = shared.resolve(handle, &domain, None).await else {
        return Ok((UNAVAILABLE, None));
    };
    let at = chrono::Utc::now().timestamp_millis();
    let did_text = match &found.outcome {
        Outcome::Nothing => return Ok((NOT_FOUND, None)),
        Outcome::Inconclusive(_) => return Ok((UNAVAILABLE, None)),
        Outcome::Did(d) => d.clone(),
    };
    let did = encoding::did_bytes(&did_text);
    let mut known = account(&*shared.db().lock().await, &did).await?;
    if known.is_none() {
        if !did_text.starts_with("did:web:") {
            // A did:plc comes only from PLC: either it does not exist or
            // lodestar has not caught up to it.
            return Ok((UNKNOWN_PLC, Some(did)));
        }
        match fetch_and_store(shared, &did_text, &did).await? {
            None => return Ok((UNAVAILABLE, None)),
            Some(false) => return Ok((NOT_FOUND, None)),
            Some(true) => known = account(&*shared.db().lock().await, &did).await?,
        }
    }
    match known {
        // It resolves to an account that claims it: that is the two-way
        // verification, and it is recorded.
        Some((true, Some(claim))) if claim == handle => {
            let mut client = shared.db().lock().await;
            match store::apply_handle_result(
                &mut client,
                &shared.rules,
                &did,
                handle,
                &found,
                at,
                false,
            )
            .await?
            {
                Applied::Status { now, .. } if now == db::HANDLE_VERIFIED => {
                    shared.gate.note(&did);
                    Ok((FOUND, Some(did)))
                }
                // A newer check got there first; what it found stands.
                _ => Ok((FOUND, Some(did))),
            }
        }
        Some(_) => Ok((CLAIM_DIFFERS, Some(did))),
        None => Ok((NOT_FOUND, None)),
    }
}

/// Do one request and write its outcome.
pub(super) async fn serve(shared: Arc<Shared>, request: Request) -> Result<()> {
    let (outcome, did) = match request.kind {
        ASK_HANDLE => resolve_handle(&shared, &request.subject).await?,
        _ => {
            let did = encoding::did_bytes(&request.subject);
            match fetch_and_store(&shared, &request.subject, &did).await? {
                None => (UNAVAILABLE, None),
                Some(false) => (NOT_AN_ACCOUNT, None),
                Some(true) => (STORED, Some(did)),
            }
        }
    };
    let client = shared.db().lock().await;
    answer(&client, request.id, outcome, did.as_deref()).await
}
