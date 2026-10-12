# lodestar

An identity index for the ATProto network. Lodestar keeps a local
copy of who every account is, so that other services can ask it
instead of asking the network each time:

- where an account lives (its PDS),
- what key signs its data,
- what its handle is, checked both ways,
- how its handle and PDS have changed.

It mirrors the PLC directory for `did:plc` identities, finds
`did:web` accounts through Jetstream and a relay's listing, and
verifies every handle itself. It is internal infrastructure: there
is no authentication, and it is meant to be reached over a private
network.

**Status:** 0.1 is in progress. See `CHANGELOG.md`.

## What it needs

- Postgres 16.
- About 35 GB of disk for the whole network today, on an SSD. The
  reference host is a VPS with a 75 GB disk, 8 GB of memory and
  4 vCPUs.
- Outbound network access for two of its three roles.

The first sync of the PLC directory takes most of a day. After it,
verifying every handle on the network takes about two days at the
default rates.

## Roles

One binary, three roles, each its own process:

| Role | Command | Talks to |
|---|---|---|
| Ingest | `lodestar ingest` | the PLC directory, a Jetstream instance, a relay |
| Verification | `lodestar verify` | any host a handle or `did:web` names |
| API | `lodestar api` | nothing outside; it serves consumers |

They coordinate only through the database.

## Running it

```sh
cp .env.example .env    # then fill it in
docker compose up -d
```

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

## API

| Endpoint | Answers |
|---|---|
| `GET /account/{did}` | PDS, signing key, handle and its status, account status |
| `GET /account/{did}/history` | handle-claim and PDS changes, newest first |
| `POST /accounts` | the same as the first, for up to 1,000 DIDs: `{"dids": [...]}` |
| `GET /handle/{handle}` | the DID that holds a verified handle |
| `POST /account/{did}/refresh` | re-fetches a `did:web` document and re-checks the handle |
| `GET /ops/health` | sync position, verification, storage |
| `GET /ops/metrics` | the same, in Prometheus format |

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
