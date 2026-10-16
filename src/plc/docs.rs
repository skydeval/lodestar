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

pub const SOURCE: &str = "plc-documents";

/// What the pass has to do: the operations after `after`, up to and
/// including `until`.
struct Pass {
    after: i64,
    until: i64,
    started: String,
}

/// Begin a pass, or find the one under way. None when there is nothing to do.
///
/// The pass relies on the recovery window (84 hours by default) to see which
/// identities the PLC source has changed since it began, so one that began
/// more than 72 hours ago starts again.
async fn prepare(client: &Client, origin: &str) -> Result<Option<Pass>> {
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
            return Ok(None);
        }
        if r.get::<_, bool>(4) {
            let until: Option<String> = r.get(2);
            return Ok(Some(Pass {
                after: r.get(0),
                until: until
                    .and_then(|u| u.parse().ok())
                    .context("the document pass has no end")?,
                started: r.get(3),
            }));
        }
        tracing::warn!("the document pass began more than 72 hours ago; starting it again");
    }
    let plc: i64 = client
        .query_opt("SELECT cursor FROM sync_state WHERE source = 'plc'", &[])
        .await?
        .map_or(0, |r| r.get(0));
    // A first sync keeps the parts as it goes.
    let phase = if plc == 0 { "done" } else { "backfill" };
    let row = client
        .query_one(
            "INSERT INTO sync_state (source, origin, cursor, phase, position, started_at) \
             VALUES ($1, $2, 0, $3, $4, now()) \
             ON CONFLICT (source) DO UPDATE SET cursor = 0, phase = EXCLUDED.phase, \
                 position = EXCLUDED.position, started_at = EXCLUDED.started_at \
             RETURNING started_at::text",
            &[&SOURCE, &origin, &phase, &plc.to_string()],
        )
        .await
        .context("beginning the document pass")?;
    Ok((plc > 0).then(|| Pass {
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

pub async fn run(mut client: Client, opts: Opts, mut stop: watch::Receiver<bool>) -> Result<()> {
    if let Some(pass) = prepare(&client, &opts.plc_url).await? {
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
        apply(
            &mut client,
            &[parse_line(&line(a, 3, PLAIN)).unwrap()],
            10,
        )
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
    async fn a_first_sync_has_nothing_to_fill() {
        let Some(client) = crate::testdb::fresh().await else {
            return;
        };
        assert!(prepare(&client, "https://plc.example").await.unwrap().is_none());
        let phase: String = client
            .query_one(
                "SELECT phase FROM sync_state WHERE source = $1",
                &[&SOURCE],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(phase, "done");
    }
}
