# lodestar

A compact, read-only mirror of the PLC directory, small enough for a
75 GB VPS. Lodestar keeps a copy of who every account on the ATProto
network is, so that services near it can ask it instead of asking
`plc.directory` and the network each time:

- where an account lives (its PDS),
- what key signs its data,
- what its handle is, checked both ways,
- how its handle and PDS have changed.

It mirrors the PLC directory for `did:plc` identities, finds
`did:web` accounts through Jetstream and a relay's listing, and
verifies every handle itself. It keeps each identity's current state
and history, not the signed operations, which is what keeps it
small; it never writes to the directory.

The read API is meant to be public, behind a reverse proxy that
passes only the read paths (see [API](#api)). There is no
authentication, and nothing public can change anything.

**Status:** 0.1 is in progress. See `CHANGELOG.md`.

## What it needs

- Postgres 16.
- About 30 GB of disk for the whole network today, on an SSD, and
  room for it to grow. The reference host is a VPS with a 75 GB disk,
  8 GB of memory and 4 vCPUs.
- Outbound network access for two of its three roles.

On the reference host in October 2026, the first sync of the PLC
directory took 9 hours, and verifying every handle on the network
after it took 43 hours. See [docs/measurements.md](docs/measurements.md).

## Roles

One binary, three roles, each its own process:

| Role | Command | Talks to |
|---|---|---|
| Ingest | `lodestar ingest` | the PLC directory, a Jetstream instance, a relay |
| Verification | `lodestar verify` | any host a handle or `did:web` names |
| API | `lodestar api` | nothing outside; it serves consumers |

They coordinate only through the database.

## Running it

You need `compose.yaml` and `.env.example` from this repository,
nothing else:

```sh
cp .env.example .env    # then fill it in
docker compose up -d
```

That pulls the released image, `ghcr.io/skydeval/lodestar`, at the
version `compose.yaml` names. To build the image from a checkout
instead, run `docker compose build` first.

`.env` needs three things:

- `POSTGRES_PASSWORD`.
- `LODESTAR_USER_AGENT`, sent on every outbound request. Name
  yourself and a way to reach you.
- `LODESTAR_CONTROLS`, the control checks: handles whose DIDs you
  know, which lodestar resolves every minute to tell its own outage
  from the network's. At least two that resolve by DNS and two by
  HTTPS, each on its own domain. Lodestar will not verify anything
  without them.

The API listens on `127.0.0.1:8080` by default.

## Configuration

Everything else has a default that suits the reference host. Any
setting can go in `.env` by its name, or be passed to a role as a
flag of the same name in lower case (`LODESTAR_WALK_EVERY_DAYS` is
`--walk-every-days`). A list is comma-separated.

| Setting | Role | Default | What it does |
|---|---|---|---|
| `LODESTAR_DATABASE_URL` | all | set by `compose.yaml` | Postgres connection string |
| `LODESTAR_USER_AGENT` | all | none, required | sent on every outbound request |
| `LODESTAR_PLC_URL` | ingest | `https://plc.directory` | the PLC directory |
| `LODESTAR_PLC_PAGE_SIZE` | ingest | 1000 | operations asked for a page, at most 1,000 |
| `LODESTAR_PLC_MIN_INTERVAL_MS` | ingest | 250 | least time between export requests in the first sync |
| `LODESTAR_PLC_POLL_SECONDS` | ingest | 5 | how often the export is polled when there is no stream |
| `LODESTAR_PLC_TIP_CHECK_SECONDS` | ingest | 120 | how often the export confirms the tip while streaming |
| `LODESTAR_PLC_WINDOW_HOURS` | ingest | 84 | the recovery window with its margin |
| `LODESTAR_JETSTREAM_URL` | ingest | Bluesky's east and west instances | the Jetstream instances, each subscribed to at once |
| `LODESTAR_JETSTREAM_QUIET_SECONDS` | ingest | 300 | reconnect after this long without a message |
| `LODESTAR_JETSTREAM_EVENT_CAP` | ingest | 10000 | most event checks that may wait in the queue |
| `LODESTAR_RELAY_URL` | ingest | Bluesky's east and west relays | the relays a walk may use; it picks the fastest |
| `LODESTAR_WALK_MIN_INTERVAL_MS` | ingest | 250 | least time between listing requests |
| `LODESTAR_WALK_EVERY_DAYS` | ingest | 7 | how often the relay walk runs again |
| `LODESTAR_CONTROLS` | verify | none, required | the control checks |
| `LODESTAR_PROFILE` | verify | `vps` | starting rates: `vps` or `hobbyist` |
| `LODESTAR_RATES` | verify | none | fixed rates for named domains, as `domain=rate:in_flight` |
| `LODESTAR_RESOLVER` | verify | `system` | the DNS resolver: `system`, or addresses separated by commas, asked in turn when one fails |
| `LODESTAR_LARGE_DOMAINS` | verify | `bsky.social` | domains swept by a walker of their own |
| `LODESTAR_SHARED_THRESHOLD` | verify | 100 | a handle claimed by more identities is resolved once for all |
| `LODESTAR_SHARED_REFRESH_MINUTES` | verify | 60 | least time between early refreshes of a shared handle |
| `LODESTAR_LAP_CHUNK_BLOCKS` | verify | 500 | how much of `accounts` a lap reads at a time |
| `LODESTAR_LOOKBACK_SECONDS` | verify | 600 | results this recent are checked again after a control pause |
| `LODESTAR_VERIFY_CONNECTIONS` | verify | 8 | database connections for applying results |
| `LODESTAR_LISTEN` | api | `127.0.0.1:8080` | where the API listens inside its container |
| `LODESTAR_API_CONNECTIONS` | api | 4 | database connections held |
| `LODESTAR_PLC_FRESH_SECONDS` | api | 300 | the PLC source counts as fresh this long after a tip confirmation |
| `LODESTAR_MAX_BATCH` | api | 1000 | most DIDs in one batch lookup |
| `LODESTAR_LIVE_DEADLINE_SECONDS` | api | 5 | how long a lookup waits on a live check |
| `LODESTAR_CONSUMER_QUEUE_CAP` | api | 10000 | most consumer-requested checks waiting, per kind |
| `LODESTAR_STATUS_LISTEN` | api | off | where the status page listens inside its container |
| `LODESTAR_STATUS_TOTALS_MINUTES` | api | 60 | how often the status page's totals are counted; each count reads all of `accounts` |

`LODESTAR_API_BIND`, `LODESTAR_STATUS_BIND` and `POSTGRES_PASSWORD`
are read by `compose.yaml` itself.

A resolver at one address is asked about three queries per check at
the most, so the default global rate of 400 checks a second can
reach 1,200 a second. Public resolvers serve one address about
1,500 a second and drop the rest; verification warns at start-up
when its rates could pass 1,400.

## API

| Endpoint | Answers |
|---|---|
| `GET /account/{did}` | PDS, signing key, handle and its status, account status |
| `GET /account/{did}/history` | handle-claim and PDS changes, newest first |
| `POST /accounts` | the same as the first, for up to 1,000 DIDs: `{"dids": [...]}` |
| `GET /handle/{handle}` | the DID that holds a verified handle |
| `GET /{did}` | a `did:plc`'s DID document, as `plc.directory` gives it |
| `POST /account/{did}/refresh` | re-fetches a `did:web` document and re-checks the handle |
| `GET /ops/health` | sync position, verification, storage |
| `GET /ops/metrics` | the same, in Prometheus format |

The status page, when `LODESTAR_STATUS_LISTEN` turns it on, is on an
address of its own and serves only `GET /status/` and its figures at
`GET /status/data.json`. It names no control handle, so it can be
made public while the API stays private.

A handle is reported as an account's `handle` only when lodestar
has verified it: the account claims the handle, and the handle
resolves back to the account. Otherwise it appears as
`claimed_handle`, with a `handle_status` saying why.

Every account answer carries the PLC source's position, so a
consumer can tell how fresh it is. An identity lodestar does not
have is `not_found` only when the source is at the directory's tip;
before that it is `syncing`.

## Politeness

Lodestar asks other people's servers a great many small questions.
It limits itself per registrable domain (one check a second by
default), backs away from a host that stalls or asks it to wait, and
stops verifying altogether when its own network looks broken. Rates
are config: `LODESTAR_PROFILE=hobbyist` lowers them all, and
`LODESTAR_RATES` sets one domain's.

## Building

```sh
cargo build --release
cargo test
```

`recon/` is separate measurement tooling with its own workspace. It
is not part of lodestar.

## Releasing

Set the version in `Cargo.toml` and `compose.yaml`, give the version
its section in `CHANGELOG.md`, and push a tag `v<version>`. The
`image` workflow builds the image, pushes it, and creates the
release from that section of the changelog.

## Licence

MIT or Apache-2.0, at your option. See `LICENSE-MIT` and
`LICENSE-APACHE`.
