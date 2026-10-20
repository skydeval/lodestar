# Changelog

All notable changes to lodestar are recorded in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `GET /accounts`: every identity lodestar holds, a page at a time, in
  the order each was created, for consumers that walk the whole network
  or keep up with new identities. Pages start `?after=` a time or
  continue from a `?cursor=`; each entry is the account object. Pages
  stop two seconds short of what the PLC source has applied, so nothing
  can be committed behind a consumer's position. It reads along a new
  index, `accounts (created_at, did)`, about 3 to 5 GB, which the
  verification role builds without blocking the first time it finds it
  missing; until then the listing answers `unavailable`.

- The README names lodestar's consumers, starting with Farsight. The
  measurements cover serving one: Farsight's handle checks about 10
  times faster with lodestar in the same datacenter, and what distance
  costs. The deployment design says where to place lodestar, why to
  give its name an IPv4 address, and how not to expose the server
  behind a CDN.

### Fixed

- The status page's note on the totals ran "last" into the time since
  the last full count.

## [0.1.0] - 2026-10-19

The first release: a compact, read-only mirror of the PLC directory,
with every handle verified: the whole network in under 30 GB.

### Added

**Ingest** (`lodestar ingest`)

- The PLC source. It pages the directory's export from its committed
  cursor, one transaction a page, then follows the directory's stream
  at the tip, confirming the tip every two minutes and falling back to
  polling the export if the stream cannot be opened. Identities, PDS
  endpoints, keys and CIDs are stored in compact binary form, history
  only for identities whose handle claim or PDS changed, and
  operations a recovery nullifies are undone.
- DID document parts the stored columns cannot rebuild, such as a
  labeler's second key and service, kept as the directory gives them.
  Parts that differ only in their names are kept only for identities
  with a repo. A one-off pass after the first relay walk fills in the
  parts the first sync could not know to keep.
- Jetstream, for identity and account events only, from several
  instances at once (Bluesky's east and west by default), each with its
  own position, kept as the time of the last event. Events schedule
  work and never write identity state. A position lost on one instance
  costs nothing while another has been live.
- The relay walk: every repo a relay lists, weekly, from whichever of
  the configured relays answers soonest. It appends each page as it
  goes and merges the result when the listing ends.

**Verification** (`lodestar verify`)

- The handle checker: the `_atproto` DNS record, then HTTPS on the
  handle's own host, with non-public addresses refused on every hop
  and redirects limited. Each answer is a DID, nothing, or
  inconclusive. `lodestar check-handle` and `lodestar check-account`
  run checks by hand.
- Results that hold up: a verified handle is demoted only by a second
  "nothing" an hour later; a failing check is retried on a lengthening
  schedule and settles as unreachable after a day, or 14 days if it
  was verified. A handle that resolves to an account takes the handle
  from whoever held it.
- The weekly sweep, listed identities first. Every domain is checked
  under its own rate, which starts at one a second and climbs while
  the domain answers quickly, up to 20; `bsky.social` has a limit and
  walker of its own. A host that asks to wait is waited for. Up to
  eight pages of the sweep are under way at once.
- Shared handles: a handle claimed by more than 100 identities, as in
  a spam wave, is resolved once for all its claimants.
- Control checks: a few well-known handles resolved every minute as a
  test of lodestar's own network. When most fail, verification stops
  until they pass, instead of marking real handles broken.
- Two per-domain pauses: for a domain whose checks mostly fail, and
  for one whose verified handles suddenly resolve to nothing. Each
  probes, ends on good answers, and lasts 24 hours at most.
- `did:web` accounts: documents fetched from their own hosts, daily,
  with backoff when they fail.
- A queue ahead of the sweep for changed claims, consumers' lookups of
  handles that are due, retries and document fetches. A queued handle
  carries its domain, and the queue is read past domains with no room.
- Index upkeep: an `accounts` index that has grown half again past its
  size after a rebuild is rebuilt without blocking, while no lap runs.
- Reasons for failed checks (timed out, DNS failed, TLS failed and so
  on) in the minute log line and in health.

**API** (`lodestar api`)

- Account lookup, history, batch lookup, and handle lookup from the
  verified index. Every answer carries the PLC source's position; an
  identity lodestar lacks is `not_found` only when the source is at
  the tip, `syncing` otherwise.
- `GET /{did}`: the DID document in the PLC directory's shape and
  errors, so a client of `plc.directory` can read from lodestar.
- Live resolution: an unseen handle or `did:web` is resolved while the
  request waits, up to 5 seconds. `POST /account/{did}/refresh` asks
  for a fresh look.
- `/ops/health` and `/ops/metrics` (Prometheus).
- A read-only status page at `/status/`, on an address of its own: the
  sync position, the streams, verification, and live totals, kept by
  triggers and checked against a full count hourly. Names of control
  handles and domains appear only as counts, so it can be public.

**Running it**

- A container image, `ghcr.io/skydeval/lodestar`, and a compose file:
  Postgres, the three roles and Caddy. Caddy serves the status page and
  the reads, and nothing else, on HTTPS for `LODESTAR_DOMAIN`; a
  certificate of your own goes in `certs/`. A host with its own proxy
  turns Caddy off with `LODESTAR_PROXY=0`.
- Every setting by flag or environment, with defaults for the reference
  host, profiles for rates (`vps`, `hobbyist`) and per-domain
  overrides. Start-up warns about rates that could outrun a DNS
  resolver or the global limit.
- Documentation: running it, configuration, the API, development, the
  design in `docs/design/`, and measurements of a full build on the
  reference host.
- Views `accounts_readable` and `identity_history_readable` for looking
  by hand.
- CI (formatting, clippy, tests against Postgres, a dependency check)
  and a release workflow that builds the image from a tag.
- Licensed under MIT or Apache-2.0.

[Unreleased]: https://github.com/skydeval/lodestar/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/skydeval/lodestar/releases/tag/v0.1.0
