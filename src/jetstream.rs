//! The Jetstream source: identity and account events. An event never
//! writes identity state. It schedules work, and an active account event
//! marks its identity as listed.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::watch;
use tokio_postgres::Client;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::encoding;
use crate::queue::{self, Item};

pub const SOURCE: &str = "jetstream";

#[derive(Clone)]
pub struct Opts {
    /// The instance, as `wss://host`.
    pub url: String,
    pub user_agent: String,
    /// With no message for this long the connection is reopened.
    pub quiet: Duration,
    /// Most events that may wait in the queue. More are dropped; the laps
    /// cover them.
    pub event_cap: i64,
}

const BATCH: usize = 200;
const BATCH_AGE: Duration = Duration::from_millis(250);
const PING: Duration = Duration::from_secs(30);
const BACKOFF_START: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(300);

#[derive(Debug, PartialEq, Eq)]
enum Kind {
    Identity,
    Account { active: bool },
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Event {
        seq: i64,
        did: String,
        kind: Kind,
    },
    /// The server's own notice. `FutureCursor` and `OutdatedCursor` mean
    /// the cursor is lost.
    Info(String),
    Other,
}

fn parse(text: &str) -> Parsed {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return Parsed::Other;
    };
    let p = v.get("payload").unwrap_or(&v);
    let ty = p.get("$type").and_then(Value::as_str).unwrap_or("");
    match ty.rsplit('#').next().unwrap_or("") {
        "info" => Parsed::Info(
            p.get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
        ),
        name @ ("identity" | "account") => {
            // The cursor is the envelope's seq, not the relay's inside.
            let (Some(seq), Some(did)) = (
                p.get("seq").and_then(Value::as_i64),
                p.get("did").and_then(Value::as_str),
            ) else {
                return Parsed::Other;
            };
            let kind = if name == "identity" {
                Kind::Identity
            } else {
                Kind::Account {
                    active: p
                        .pointer("/account/active")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                }
            };
            Parsed::Event {
                seq,
                did: did.to_owned(),
                kind,
            }
        }
        _ => Parsed::Other,
    }
}

/// The lookback floor named in a refusal of a cursor that is too old.
fn floor_from_refusal(body: &str) -> Option<i64> {
    if !body.contains("CursorTooOld") {
        return None;
    }
    let rest = &body[body.find("lookback floor ")? + "lookback floor ".len()..];
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

fn url(opts: &Opts, cursor: Option<i64>) -> String {
    let mut u = format!(
        "{}/xrpc/network.bsky.jetstream.subscribeEvents?kinds=identity&kinds=account",
        opts.url
    );
    if let Some(c) = cursor {
        u.push_str(&format!("&cursor={c}"));
    }
    u
}

struct Event {
    seq: i64,
    did: String,
    kind: Kind,
}

/// Turn a batch of events into scheduled work and listed marks, and commit
/// it with the cursor.
async fn apply(client: &mut Client, opts: &Opts, events: &[Event]) -> Result<()> {
    let Some(last) = events.last() else {
        return Ok(());
    };
    // Per DID: was any event an identity event, and was any an active account event.
    let mut seen: HashMap<Vec<u8>, (bool, bool, bool)> = HashMap::new();
    for e in events {
        let web = e.did.starts_with("did:web:");
        if !web && !e.did.starts_with("did:plc:") {
            continue;
        }
        let entry = seen
            .entry(encoding::did_bytes(&e.did))
            .or_insert((web, false, false));
        match e.kind {
            Kind::Identity => entry.1 = true,
            Kind::Account { active } => entry.2 |= active,
        }
    }
    let dids: Vec<&Vec<u8>> = seen.keys().collect();

    let tx = client.transaction().await?;
    let known: HashSet<Vec<u8>> = tx
        .query("SELECT did FROM accounts WHERE did = ANY($1)", &[&dids])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    // Handle checks wait for the PLC source to reach the tip.
    let plc_live = tx
        .query_opt(
            "SELECT 1 FROM sync_state WHERE source = 'plc' AND phase = 'live'",
            &[],
        )
        .await?
        .is_some();
    let room = opts.event_cap - queue::depth(&tx, queue::REASON_EVENT, opts.event_cap).await?;

    let mut work: Vec<Item> = Vec::new();
    let mut mark: Vec<&Vec<u8>> = Vec::new();
    for (did, (web, identity, active)) in &seen {
        let has_row = known.contains(did);
        if *active && has_row {
            mark.push(did);
        }
        if *web {
            // A did:web's state comes from its document: fetch it, whether
            // or not lodestar has seen it before.
            work.push(Item {
                did: did.as_slice(),
                kind: queue::KIND_DOCUMENT,
                listed: *active,
            });
        } else if has_row && *identity && plc_live {
            // A did:plc's state comes only from PLC; the event asks for its
            // handle to be checked again. One with no row is left to PLC.
            work.push(Item {
                did: did.as_slice(),
                kind: queue::KIND_HANDLE,
                listed: false,
            });
        }
    }
    work.truncate(room.max(0) as usize);
    queue::push(&tx, queue::REASON_EVENT, &work).await?;
    if !mark.is_empty() {
        tx.execute(
            "UPDATE accounts SET listed = true WHERE did = ANY($1) AND NOT listed",
            &[&mark],
        )
        .await
        .context("marking listed")?;
    }
    tx.execute(
        "INSERT INTO sync_state (source, origin, cursor, phase, tip_at) VALUES ($1, $2, $3, 'live', now()) \
         ON CONFLICT (source) DO UPDATE SET origin = EXCLUDED.origin, cursor = EXCLUDED.cursor, phase = 'live', tip_at = now()",
        &[&SOURCE, &opts.url, &last.seq],
    )
    .await
    .context("committing the cursor")?;
    tx.commit().await?;
    Ok(())
}

async fn confirm_tip(client: &Client) -> Result<()> {
    client
        .execute(
            "UPDATE sync_state SET tip_at = now() WHERE source = $1",
            &[&SOURCE],
        )
        .await?;
    Ok(())
}

/// The cursor is lost: ask for a new walk, since events may have been missed.
async fn ask_for_walk(client: &Client) -> Result<()> {
    client
        .execute(
            "UPDATE sync_state SET phase = 'due' WHERE source = $1",
            &[&crate::walk::SOURCE],
        )
        .await?;
    Ok(())
}

enum End {
    Stop,
    /// Reconnect from the committed cursor.
    Again(String),
    /// The cursor is lost. Find the floor and resume there.
    Lost(String),
}

async fn session(
    client: &mut Client,
    opts: &Opts,
    cursor: &mut Option<i64>,
    stop: &mut watch::Receiver<bool>,
) -> Result<End> {
    let mut request = url(opts, *cursor)
        .as_str()
        .into_client_request()
        .context("building the subscription request")?;
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static("xrpc.v1.json"),
    );
    if let Ok(v) = HeaderValue::from_str(&opts.user_agent) {
        request.headers_mut().insert("User-Agent", v);
    }
    let mut ws = match tokio::time::timeout(
        Duration::from_secs(30),
        tokio_tungstenite::connect_async(request),
    )
    .await
    {
        Ok(Ok((ws, _))) => ws,
        Ok(Err(WsError::Http(resp))) => {
            let body = resp
                .body()
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            if let Some(floor) = floor_from_refusal(&body) {
                // Refused with the floor: resume from it.
                *cursor = Some(floor);
                ask_for_walk(client).await?;
                return Ok(End::Again(format!(
                    "cursor refused; resuming from the floor at {floor}"
                )));
            }
            return Ok(End::Again(format!("refused with {}", resp.status())));
        }
        Ok(Err(e)) => return Ok(End::Again(e.to_string())),
        Err(_) => return Ok(End::Again("timed out connecting".into())),
    };
    tracing::info!(cursor = ?*cursor, "Jetstream subscription open");

    let mut buf: Vec<Event> = Vec::new();
    let mut seen = cursor.unwrap_or(0);
    let mut flush_at: Option<tokio::time::Instant> = None;
    let start = tokio::time::Instant::now();
    let mut ping = tokio::time::interval_at(start + PING, PING);
    let mut quiet = Box::pin(tokio::time::sleep(opts.quiet));

    let end = loop {
        tokio::select! {
            _ = stop.changed() => break End::Stop,
            _ = &mut quiet => break End::Again("quiet for too long".into()),
            _ = ping.tick() => {
                if ws.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break End::Again("could not send a ping".into());
                }
            }
            _ = async { tokio::time::sleep_until(flush_at.unwrap_or(start)).await }, if flush_at.is_some() => {
                apply(client, opts, &buf).await?;
                *cursor = Some(seen);
                buf.clear();
                flush_at = None;
            }
            msg = ws.next() => {
                let text = match msg {
                    None => break End::Again("the stream ended".into()),
                    Some(Err(e)) => break End::Again(format!("stream error: {e}")),
                    Some(Ok(Message::Text(t))) => t,
                    Some(Ok(Message::Pong(_))) => {
                        // The connection is alive and there is nothing waiting: this is the tip.
                        quiet.as_mut().reset(tokio::time::Instant::now() + opts.quiet);
                        if buf.is_empty() && cursor.is_some() {
                            confirm_tip(client).await?;
                        }
                        continue;
                    }
                    Some(Ok(Message::Close(_))) => break End::Again("the server closed the stream".into()),
                    Some(Ok(_)) => continue,
                };
                quiet.as_mut().reset(tokio::time::Instant::now() + opts.quiet);
                match parse(text.as_str()) {
                    Parsed::Info(name) if name == "FutureCursor" || name == "OutdatedCursor" => break End::Lost(name),
                    Parsed::Info(_) | Parsed::Other => {}
                    // The cursor is inclusive and delivery is at least once.
                    Parsed::Event { seq, .. } if seq <= seen => {}
                    Parsed::Event { seq, did, kind } => {
                        seen = seq;
                        buf.push(Event { seq, did, kind });
                        flush_at.get_or_insert(tokio::time::Instant::now() + BATCH_AGE);
                        if buf.len() >= BATCH {
                            apply(client, opts, &buf).await?;
                            *cursor = Some(seen);
                            buf.clear();
                            flush_at = None;
                        }
                    }
                }
            }
        }
    };
    apply(client, opts, &buf).await?;
    if !buf.is_empty() {
        *cursor = Some(seen);
    }
    Ok(end)
}

pub async fn run(mut client: Client, opts: Opts, mut stop: watch::Receiver<bool>) -> Result<()> {
    let row = client
        .query_opt(
            "SELECT origin, cursor FROM sync_state WHERE source = $1",
            &[&SOURCE],
        )
        .await?;
    let mut cursor: Option<i64> = match row {
        Some(r) if r.get::<_, String>(0) == opts.url => Some(r.get(1)),
        Some(r) => {
            // A seq means nothing on another instance.
            tracing::warn!(was = %r.get::<_, String>(0), now = %opts.url, "the Jetstream instance changed; the old cursor is dropped");
            ask_for_walk(&client).await?;
            None
        }
        None => None,
    };
    tracing::info!(cursor = ?cursor, "starting the Jetstream source");
    let mut backoff = BACKOFF_START;
    loop {
        let before = cursor;
        let wait = match session(&mut client, &opts, &mut cursor, &mut stop).await? {
            End::Stop => break,
            End::Again(why) => {
                tracing::info!("Jetstream reconnecting: {why}");
                // A session that made progress reconnects at once.
                if cursor != before {
                    backoff = BACKOFF_START;
                    Duration::from_secs(1)
                } else {
                    let w = backoff;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    w
                }
            }
            End::Lost(name) => {
                // A seq of 1 is refused with the floor, which is where to resume.
                tracing::warn!("the Jetstream cursor is lost ({name}); resuming from the floor");
                ask_for_walk(&client).await?;
                cursor = Some(1);
                Duration::from_secs(1)
            }
        };
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(wait) => {}
        }
    }
    tracing::info!(cursor = ?cursor, "Jetstream source stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_read_from_the_envelope() {
        let account = r#"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#account","account":{"active":false,"did":"did:plc:by5ofb2yfxley72ma7iqwx5j","seq":34413048622,"status":"deleted","time":"2026-10-12T09:39:13.492Z"},"did":"did:plc:by5ofb2yfxley72ma7iqwx5j","seq":26894478980,"time":"2026-10-12T09:39:13.755960Z"}}"#;
        assert_eq!(
            parse(account),
            Parsed::Event {
                seq: 26894478980,
                did: "did:plc:by5ofb2yfxley72ma7iqwx5j".into(),
                kind: Kind::Account { active: false }
            }
        );
        let identity = r#"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","identity":{"did":"did:web:example.com","seq":5},"did":"did:web:example.com","seq":77}}"#;
        assert_eq!(
            parse(identity),
            Parsed::Event {
                seq: 77,
                did: "did:web:example.com".into(),
                kind: Kind::Identity
            }
        );
        let active = r#"{"payload":{"$type":"x#account","account":{"active":true},"did":"did:plc:a","seq":1}}"#;
        assert_eq!(
            parse(active),
            Parsed::Event {
                seq: 1,
                did: "did:plc:a".into(),
                kind: Kind::Account { active: true }
            }
        );
    }

    #[test]
    fn notices_and_noise() {
        let info = r#"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info","message":"…","name":"FutureCursor"}}"#;
        assert_eq!(parse(info), Parsed::Info("FutureCursor".into()));
        assert_eq!(parse("not json"), Parsed::Other);
        assert_eq!(
            parse(r#"{"payload":{"$type":"x#commit","seq":1,"did":"did:plc:a"}}"#),
            Parsed::Other
        );
        assert_eq!(
            parse(r#"{"payload":{"$type":"x#identity","did":"did:plc:a"}}"#),
            Parsed::Other
        );
    }

    #[test]
    fn the_floor_is_read_from_a_refusal() {
        let body = r#"{"error":"CursorTooOld","message":"subscribe: cursor too old: cursor 1 below lookback floor 26847088910; re-backfill from your last seq"}"#;
        assert_eq!(floor_from_refusal(body), Some(26847088910));
        assert_eq!(
            floor_from_refusal(r#"{"error":"Other","message":"lookback floor 5"}"#),
            None
        );
        assert_eq!(floor_from_refusal("CursorTooOld"), None);
    }
}
