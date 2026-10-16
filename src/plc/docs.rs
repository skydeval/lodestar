//! Filling `doc_parts` for a database whose copy of the directory was made
//! before lodestar kept them: one pass over the export, from the start up
//! to where the PLC source was when the pass began. Operations after that
//! write their own parts as they are applied, and the pass never replaces
//! what one of them wrote.
//!
//! A database synced from the start with parts kept has nothing to fill, and
//! the pass is marked done before it begins.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::{mpsc, watch};
use tokio_postgres::Client;

use super::op::Op;
use super::store;
use super::sync::{self, Opts};
use crate::encoding;

/// The first pass, `plc-documents`, kept parts that differ only in their
/// names for every identity, which spam registrations made tens of
/// gigabytes. Under a new name the pass runs again with the rule that
/// keeps those only for identities with a repo, and removes the rest.
pub const SOURCE: &str = "plc-documents-2";

/// What the pass has to do: the operations after `after`, up to and
/// including `until`.
struct Pass {
    after: i64,
    until: i64,
    started: String,
}

/// What the pass is to do next.
enum Next {
    /// Done, or nothing to do.
    Nothing,
    /// Not yet: the first relay walk has not finished.
    Wait,
    Run(Pass),
}

/// Begin a pass, or find the one under way.
///
/// The pass begins only once a relay walk has finished. Parts that differ
/// only in their names are kept for identities with a repo, and on a new
/// copy nothing is known to have one until the first walk has listed them.
/// So a first sync, which writes parts as it goes, still needs this pass
/// after its walk, for the listed identities' names.
///
/// The pass relies on the recovery window (84 hours by default) to see which
/// identities the PLC source has changed since it began, so one that began
/// more than 72 hours ago starts again.
async fn prepare(client: &Client, origin: &str) -> Result<Next> {
    let row = client
        .query_opt(
            "SELECT cursor, phase, position, started_at::text, \
                    COALESCE(started_at > now() - interval '72 hours', false) \
             FROM sync_state WHERE source = $1",
            &[&SOURCE],
        )
        .await?;
    if let Some(r) = row {
        let phase: String = r.get(1);
        if phase == "done" {
            return Ok(Next::Nothing);
        }
        if r.get::<_, bool>(4) {
            let until: Option<String> = r.get(2);
            return Ok(Next::Run(Pass {
                after: r.get(0),
                until: until
                    .and_then(|u| u.parse().ok())
                    .context("the document pass has no end")?,
                started: r.get(3),
            }));
        }
        tracing::warn!("the document pass began more than 72 hours ago; starting it again");
    }
    let walked: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM sync_state WHERE source = 'relay-walk' AND phase = 'done')",
            &[],
        )
        .await?
        .get(0);
    if !walked {
        return Ok(Next::Wait);
    }
    let plc: i64 = client
        .query_opt("SELECT cursor FROM sync_state WHERE source = 'plc'", &[])
        .await?
        .map_or(0, |r| r.get(0));
    let row = client
        .query_one(
            "INSERT INTO sync_state (source, origin, cursor, phase, position, started_at) \
             VALUES ($1, $2, 0, 'backfill', $3, now()) \
             ON CONFLICT (source) DO UPDATE SET cursor = 0, phase = EXCLUDED.phase, \
                 position = EXCLUDED.position, started_at = EXCLUDED.started_at \
             RETURNING started_at::text",
            &[&SOURCE, &origin, &plc.to_string()],
        )
        .await
        .context("beginning the document pass")?;
    Ok(Next::Run(Pass {
        after: 0,
        until: plc,
        started: row.get(0),
    }))
}

/// Write one page's parts, leaving out identities the PLC source has
/// changed since the pass's end, and move the pass on.
async fn apply(client: &mut Client, ops: &[Op], until: i64) -> Result<()> {
    let Some(last) = ops.last() else {
        return Ok(());
    };
    let keyed: Vec<Vec<u8>> = ops.iter().map(|o| encoding::did_bytes(&o.did)).collect();
    let mut docs = store::docs_of(ops, &keyed);
    let tx = client.transaction().await?;
    let touched: Vec<&[u8]> = docs.keys().copied().collect();
    let newer = tx
        .query(
            "SELECT DISTINCT did FROM plc_recent_ops WHERE did = ANY($1) AND seq > $2",
            &[&touched, &until],
        )
        .await
        .context("finding identities changed since the pass began")?;
    for r in newer {
        let did: Vec<u8> = r.get(0);
        docs.remove(did.as_slice());
    }
    store::write_docs(&tx, &docs).await?;
    tx.execute(
        "UPDATE sync_state SET cursor = $2 WHERE source = $1",
        &[&SOURCE, &last.seq],
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// How often a pass waiting for the first walk looks again.
const WAIT_LOOK: Duration = Duration::from_secs(60);

pub async fn run(mut client: Client, opts: Opts, mut stop: watch::Receiver<bool>) -> Result<()> {
    let pass = loop {
        match prepare(&client, &opts.plc_url).await? {
            Next::Nothing => break None,
            Next::Run(pass) => break Some(pass),
            Next::Wait => {
                tokio::select! {
                    _ = stop.changed() => return Ok(()),
                    _ = tokio::time::sleep(WAIT_LOOK) => {}
                }
            }
        }
    };
    if let Some(pass) = pass {
        tracing::info!(
            after = pass.after,
            until = pass.until,
            started = pass.started,
            "filling the DID document parts"
        );
        let http = reqwest::Client::builder()
            .user_agent(opts.user_agent.clone())
            .timeout(Duration::from_secs(60))
            .build()?;
        let (tx, mut rx) = mpsc::channel::<Vec<Op>>(4);
        let fetching = tokio::spawn(sync::page_loop(http, opts.clone(), pass.after, tx));
        let (began, mut pages, mut at) = (Instant::now(), 0u64, pass.after);
        let finished = loop {
            let page = tokio::select! {
                _ = stop.changed() => break false,
                page = rx.recv() => page,
            };
            let Some(mut ops) = page else {
                // The export ended short of the pass's end: it has nothing more.
                break true;
            };
            let ends = ops.last().is_none_or(|o| o.seq >= pass.until);
            ops.retain(|o| o.seq <= pass.until);
            apply(&mut client, &ops, pass.until).await?;
            if let Some(o) = ops.last() {
                at = o.seq;
            }
            pages += 1;
            if pages % 1000 == 0 {
                tracing::info!(
                    at,
                    until = pass.until,
                    elapsed = ?began.elapsed(),
                    "filling the DID document parts"
                );
            }
            if ends {
                break true;
            }
        };
        drop(rx);
        let fetched = fetching.await;
        if finished {
            client
                .execute(
                    "UPDATE sync_state SET phase = 'done' WHERE source = $1",
                    &[&SOURCE],
                )
                .await?;
            tracing::info!(at, elapsed = ?began.elapsed(), "the DID document parts are filled");
        } else {
            return Ok(());
        }
        if let Ok(Err(e)) = fetched {
            tracing::warn!("the document pass's export reader: {e:#}");
        }
    }
    // Every source ending ends the role, so a finished pass waits for the
    // role to stop.
    if !*stop.borrow() {
        let _ = stop.changed().await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plc::op::parse_line;

    fn line(did: &str, seq: i64, operation: &str) -> String {
        format!(
            r#"{{"type":"sequenced_op","did":"{did}","cid":"bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqcsgz7soitae","seq":{seq},"createdAt":"2023-04-12T04:53:57.057Z","operation":{operation}}}"#
        )
    }

    const LABELER: &str = r#"{"type":"plc_operation","prev":null,"alsoKnownAs":["at://mod.example.com"],"services":{"atproto_pds":{"type":"AtprotoPersonalDataServer","endpoint":"https://pds.example.com"},"atproto_labeler":{"type":"AtprotoLabeler","endpoint":"https://mod.example.com"}},"verificationMethods":{"atproto":"did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF"}}"#;
    const PLAIN: &str = r#"{"type":"plc_operation","prev":null,"alsoKnownAs":["at://mod.example.com"],"services":{"atproto_pds":{"type":"AtprotoPersonalDataServer","endpoint":"https://pds.example.com"}},"verificationMethods":{"atproto":"did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF"}}"#;

    #[tokio::test]
    async fn the_pass_fills_parts_but_never_replaces_newer_ones() {
        let Some(mut client) = crate::testdb::fresh().await else {
            return;
        };
        let a = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let b = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        // b was changed by the PLC source after the pass's end (seq 10), to
        // a document with no parts of its own.
        client
            .execute(
                "INSERT INTO plc_recent_ops (seq, created_at, did, cid) VALUES (11, now(), $1, '\\x00')",
                &[&encoding::did_bytes(b)],
            )
            .await
            .unwrap();
        let ops = vec![
            parse_line(&line(a, 1, LABELER)).unwrap(),
            parse_line(&line(b, 2, LABELER)).unwrap(),
        ];
        apply(&mut client, &ops, 10).await.unwrap();
        let rows: Vec<Vec<u8>> = client
            .query("SELECT did FROM doc_parts", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(rows, vec![encoding::did_bytes(a)]);
        // A later operation in the pass that needs no parts removes them.
        apply(&mut client, &[parse_line(&line(a, 3, PLAIN)).unwrap()], 10)
            .await
            .unwrap();
        let n: i64 = client
            .query_one("SELECT count(*) FROM doc_parts", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn parts_that_differ_only_in_names_are_kept_only_with_a_repo() {
        let Some(mut client) = crate::testdb::fresh().await else {
            return;
        };
        let names = r#"{"type":"plc_operation","prev":null,"alsoKnownAs":["at://a.example.com","at://b.example.com"],"services":{"atproto_pds":{"type":"AtprotoPersonalDataServer","endpoint":"https://pds.example.com"}},"verificationMethods":{"atproto":"did:key:zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF"}}"#;
        let spam = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let account = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        let labeler = "did:plc:cccccccccccccccccccccccc";
        client
            .execute(
                "INSERT INTO listed (did) VALUES ($1)",
                &[&encoding::did_bytes(account)],
            )
            .await
            .unwrap();
        let ops = vec![
            parse_line(&line(spam, 1, names)).unwrap(),
            parse_line(&line(account, 2, names)).unwrap(),
            parse_line(&line(labeler, 3, LABELER)).unwrap(),
        ];
        apply(&mut client, &ops, 10).await.unwrap();
        let mut rows: Vec<Vec<u8>> = client
            .query("SELECT did FROM doc_parts", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![encoding::did_bytes(account), encoding::did_bytes(labeler)],
            "the unlisted identity's names are not kept; the labeler's parts are, listed or not"
        );
    }

    #[tokio::test]
    async fn a_new_copy_fills_its_parts_after_the_first_walk() {
        let Some(client) = crate::testdb::fresh().await else {
            return;
        };
        let next = || async { prepare(&client, "https://plc.example").await.unwrap() };
        // No walk yet: nothing is known to have a repo, so it waits.
        assert!(matches!(next().await, Next::Wait));
        client
            .batch_execute(
                "INSERT INTO sync_state (source, origin, cursor, phase) VALUES \
                 ('plc', 'https://plc.example', 500, 'live'), \
                 ('relay-walk', 'https://relay.example', 0, 'walking')",
            )
            .await
            .unwrap();
        // A walk under way is not enough.
        assert!(matches!(next().await, Next::Wait));
        client
            .execute(
                "UPDATE sync_state SET phase = 'done' WHERE source = 'relay-walk'",
                &[],
            )
            .await
            .unwrap();
        // Once it is done, the pass covers everything the source has applied.
        match next().await {
            Next::Run(pass) => assert_eq!((pass.after, pass.until), (0, 500)),
            _ => panic!("the pass should run once the walk is done"),
        }
        // And, begun, it is found again rather than begun twice.
        match next().await {
            Next::Run(pass) => assert_eq!(pass.until, 500),
            _ => panic!("the pass under way should be found"),
        }
    }
}
