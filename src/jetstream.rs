//! The Jetstream source: identity and account events. An event never
//! writes identity state. It schedules work, and an active account event
//! marks its identity as listed.
//!
//! Lodestar subscribes to several instances at once, each with its own
//! position. The same event arriving from two of them schedules the same
//! work twice, which the queue collapses, so nothing is deduplicated here.
//!
//! A position is the time of the last event, not the instance's sequence
//! number. Instances number their events independently, and one address
//! can stand in front of several instances, so a sequence number can mean
//! nothing after a reconnect. A time means the same everywhere. A session
//! resumes a little before the last event it saw.

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

/// Each instance's position is stored under this prefix and its host.
const SOURCE_PREFIX: &str = "jetstream:";
/// Where a single position was stored before there were several.
const OLD_SOURCE: &str = "jetstream";
/// A session resumes this long before the last event it saw: events from
/// different instances, or delivered out of order, are not missed.
const OVERLAP_US: i64 = 30_000_000;
/// Another subscription counts as live if it has confirmed the tip within
/// this long.
const LIVE_WITHIN_SECS: f64 = 300.0;

#[derive(Clone)]
pub struct Opts {
    /// The instances, each as `wss://host`.
    pub urls: Vec<String>,
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
        /// The event's time in microseconds, which is its position.
        time: Option<i64>,
        did: String,
        kind: Kind,
    },
    /// The server's own notice. `OutdatedCursor` means the position is
    /// older than the instance keeps.
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
            let Some(did) = p.get("did").and_then(Value::as_str) else {
                return Parsed::Other;
            };
            // The envelope's time: when this instance saw the event.
            let time = ["time", "witnessedAt"]
                .iter()
                .filter_map(|k| p.get(*k).and_then(Value::as_str))
                .find_map(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|t| t.timestamp_micros());
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
                time,
                did: did.to_owned(),
                kind,
            }
        }
        _ => Parsed::Other,
    }
}

/// Where an instance's position is stored.
fn source_of(url: &str) -> String {
    let host = url.split("://").nth(1).unwrap_or(url);
    format!("{SOURCE_PREFIX}{}", host.trim_end_matches('/'))
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

/// The subscription address, resuming a little before `time`.
fn url(base: &str, time: Option<i64>) -> String {
    let mut u =
        format!("{base}/xrpc/network.bsky.jetstream.subscribeEvents?kinds=identity&kinds=account");
    if let Some(t) = time {
        u.push_str(&format!("&cursor={}", t - OVERLAP_US));
    }
    u
}

struct Event {
    did: String,
    kind: Kind,
}

/// One subscription: an instance and where its position is stored.
struct Sub<'a> {
    opts: &'a Opts,
    url: &'a str,
    source: String,
}

/// Turn a batch of events into scheduled work and listed marks, and commit
/// it with the position.
async fn apply(client: &mut Client, sub: &Sub<'_>, events: &[Event], time: i64) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let opts = sub.opts;
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
                domain: None,
            });
        } else if has_row && *identity && plc_live {
            // A did:plc's state comes only from PLC; the event asks for its
            // handle to be checked again. One with no row is left to PLC.
            work.push(Item {
                did: did.as_slice(),
                kind: queue::KIND_HANDLE,
                listed: false,
                domain: None,
            });
        }
    }
    work.truncate(room.max(0) as usize);
    queue::push(&tx, queue::REASON_EVENT, &work).await?;
    if !mark.is_empty() {
        tx.execute(
            "INSERT INTO listed (did) SELECT unnest($1::bytea[]) ON CONFLICT DO NOTHING",
            &[&mark],
        )
        .await
        .context("marking listed")?;
    }
    tx.execute(
        "UPDATE sync_state SET origin = $2, cursor = GREATEST(cursor, $3), phase = 'live', tip_at = now() WHERE source = $1",
        &[&sub.source, &sub.url, &time],
    )
    .await
    .context("committing the position")?;
    tx.commit().await?;
    Ok(())
}

async fn confirm_tip(client: &Client, source: &str) -> Result<()> {
    client
        .execute(
            "UPDATE sync_state SET tip_at = now() WHERE source = $1",
            &[&source],
        )
        .await?;
    Ok(())
}

/// Whether another subscription has delivered every event since `time`
/// and is live now. `last_op_at` holds when a subscription's unbroken run
/// of events began.
async fn covered_by_another(client: &Client, source: &str, time: Option<i64>) -> Result<bool> {
    let Some(time) = time else {
        return Ok(false);
    };
    Ok(client
        .query_opt(
            "SELECT 1 FROM sync_state WHERE source LIKE $1 AND source <> $2 \
               AND last_op_at <= to_timestamp($3::float8 / 1000000) \
               AND tip_at > now() - make_interval(secs => $4)",
            &[
                &format!("{SOURCE_PREFIX}%"),
                &source,
                &(time as f64),
                &LIVE_WITHIN_SECS,
            ],
        )
        .await?
        .is_some())
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
    /// Reconnect from the committed position.
    Again(String),
    /// The position is older than the instance keeps. Start at the tip.
    Lost(String),
}

async fn session(
    client: &mut Client,
    sub: &Sub<'_>,
    position: &mut Option<i64>,
    stop: &mut watch::Receiver<bool>,
) -> Result<End> {
    let opts = sub.opts;
    let mut request = url(sub.url, *position)
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
            if floor_from_refusal(&body).is_some() {
                // Only a sequence number is refused this way; a time that
                // is too old is answered with a notice instead.
                return Ok(End::Lost("CursorTooOld".into()));
            }
            return Ok(End::Again(format!("refused with {}", resp.status())));
        }
        Ok(Err(e)) => return Ok(End::Again(e.to_string())),
        Err(_) => return Ok(End::Again("timed out connecting".into())),
    };
    tracing::info!(instance = %sub.url, position = ?*position, "Jetstream subscription open");

    let mut buf: Vec<Event> = Vec::new();
    let mut seen = position.unwrap_or(0);
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
                apply(client, sub, &buf, seen).await?;
                *position = Some(seen);
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
                        if buf.is_empty() && position.is_some() {
                            confirm_tip(client, &sub.source).await?;
                        }
                        continue;
                    }
                    Some(Ok(Message::Close(_))) => break End::Again("the server closed the stream".into()),
                    Some(Ok(_)) => continue,
                };
                quiet.as_mut().reset(tokio::time::Instant::now() + opts.quiet);
                match parse(text.as_str()) {
                    Parsed::Info(name) if name == "OutdatedCursor" => break End::Lost(name),
                    // A time ahead of the instance starts at its tip, which
                    // is where the position already is.
                    Parsed::Info(_) | Parsed::Other => {}
                    Parsed::Event { time, did, kind } => {
                        // Events resent by the overlap schedule work again;
                        // the queue collapses it.
                        if let Some(t) = time {
                            seen = seen.max(t);
                        }
                        buf.push(Event { did, kind });
                        flush_at.get_or_insert(tokio::time::Instant::now() + BATCH_AGE);
                        if buf.len() >= BATCH {
                            apply(client, sub, &buf, seen).await?;
                            *position = Some(seen);
                            buf.clear();
                            flush_at = None;
                        }
                    }
                }
            }
        }
    };
    apply(client, sub, &buf, seen).await?;
    if !buf.is_empty() {
        *position = Some(seen);
    }
    Ok(end)
}

/// Where a subscription starts: its own stored position, or, the first
/// time, the newest position any subscription has, including the single
/// one of earlier versions. None starts at the tip.
async fn starting_position(client: &Client, source: &str) -> Result<Option<i64>> {
    if let Some(r) = client
        .query_opt(
            "SELECT cursor FROM sync_state WHERE source = $1",
            &[&source],
        )
        .await?
    {
        return Ok(Some(r.get(0)));
    }
    let newest: Option<i64> = client
        .query_one(
            "SELECT max(t) FROM ( \
                 SELECT cursor AS t FROM sync_state WHERE source LIKE $1 \
                 UNION ALL \
                 SELECT (extract(epoch FROM tip_at) * 1000000)::bigint FROM sync_state WHERE source = $2) x",
            &[&format!("{SOURCE_PREFIX}%"), &OLD_SOURCE],
        )
        .await?
        .get(0);
    Ok(newest)
}

/// Record that a subscription's unbroken run of events begins at `time`,
/// or now.
async fn begin_run(client: &Client, sub: &Sub<'_>, time: Option<i64>) -> Result<()> {
    client
        .execute(
            "INSERT INTO sync_state (source, origin, cursor, phase, last_op_at) \
             VALUES ($1, $2, COALESCE($3::int8, 0), 'starting', COALESCE(to_timestamp($3::int8::float8 / 1000000), now())) \
             ON CONFLICT (source) DO UPDATE SET origin = EXCLUDED.origin, last_op_at = EXCLUDED.last_op_at",
            &[&sub.source, &sub.url, &time],
        )
        .await?;
    Ok(())
}

/// One instance's subscription, until asked to stop.
pub async fn run(
    mut client: Client,
    opts: Opts,
    url: String,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let sub = Sub {
        opts: &opts,
        url: &url,
        source: source_of(&url),
    };
    let mut position = starting_position(&client, &sub.source).await?;
    let fresh = client
        .query_opt("SELECT 1 FROM sync_state WHERE source = $1", &[&sub.source])
        .await?
        .is_none();
    if fresh {
        begin_run(&client, &sub, position).await?;
    }
    tracing::info!(instance = %url, position = ?position, "starting a Jetstream subscription");
    let mut backoff = BACKOFF_START;
    loop {
        let before = position;
        let wait = match session(&mut client, &sub, &mut position, &mut stop).await? {
            End::Stop => break,
            End::Again(why) => {
                tracing::info!(instance = %url, "Jetstream reconnecting: {why}");
                // A session that made progress reconnects at once.
                if position != before {
                    backoff = BACKOFF_START;
                    Duration::from_secs(1)
                } else {
                    let w = backoff;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    w
                }
            }
            End::Lost(name) => {
                // Events since the position may have been missed. Another
                // subscription that was live throughout has them; failing
                // that, a relay walk catches up.
                if covered_by_another(&client, &sub.source, position).await? {
                    tracing::info!(instance = %url, "the Jetstream position is lost ({name}); another subscription covered the gap");
                } else {
                    tracing::warn!(instance = %url, "the Jetstream position is lost ({name}); asking for a relay walk");
                    ask_for_walk(&client).await?;
                }
                position = None;
                begin_run(&client, &sub, None).await?;
                Duration::from_secs(1)
            }
        };
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(wait) => {}
        }
    }
    tracing::info!(instance = %url, position = ?position, "Jetstream subscription stopped");
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
                time: Some(1_791_797_953_755_960),
                did: "did:plc:by5ofb2yfxley72ma7iqwx5j".into(),
                kind: Kind::Account { active: false }
            }
        );
        let identity = r#"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","identity":{"did":"did:web:example.com","seq":5},"did":"did:web:example.com","seq":77}}"#;
        assert_eq!(
            parse(identity),
            Parsed::Event {
                time: None,
                did: "did:web:example.com".into(),
                kind: Kind::Identity
            }
        );
        let active = r#"{"payload":{"$type":"x#account","account":{"active":true},"did":"did:plc:a","seq":1}}"#;
        assert_eq!(
            parse(active),
            Parsed::Event {
                time: None,
                did: "did:plc:a".into(),
                kind: Kind::Account { active: true }
            }
        );
    }

    #[test]
    fn positions_are_times_and_resume_with_an_overlap() {
        assert_eq!(
            source_of("wss://jetstream.us-west.bsky.network"),
            "jetstream:jetstream.us-west.bsky.network"
        );
        let u = url("wss://example.org", Some(1_000_000_000));
        assert!(u.ends_with("&cursor=970000000"), "{u}");
        assert!(!url("wss://example.org", None).contains("cursor"));
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
            parse(r#"{"payload":{"$type":"x#identity"}}"#),
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
