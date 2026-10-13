# Changelog

All notable changes to lodestar are recorded in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Work towards 0.1.0.

### Added

- The ingest role's PLC source. It pages the directory's sequenced
  export from the committed cursor, applies each page in one
  transaction with its cursor, and once at the tip takes the
  directory's stream, falling back to polling the export when the
  stream cannot be opened. While streaming it asks the export every
  two minutes to confirm it is still at the tip.
  - Identities, PDS endpoints, signing keys and CIDs are stored in
    compact binary form, with endpoints interned.
  - History is kept only for identities whose handle claim or PDS has
    changed, and rows written by operations that a recovery nullifies
    are removed. CIDs are compared by value.
  - When the first sync reaches the tip, the `accounts` key and the
    recovery-window table are rebuilt.
- The ingest role's Jetstream source. It subscribes to identity and
  account events, schedules work from them without writing identity
  state, marks an identity as listed on an active account event,
  reads the server's lost-cursor notices, resumes from the lookback
  floor when its cursor is refused, and reconnects after a quiet
  period.
- The ingest role's relay walk. Once the PLC source is at the tip it
  pages the relay's listing, marks listed identities and queues
  unseen `did:web` accounts for a document fetch, and runs again
  every 7 days or when a lost Jetstream cursor asks for it.
- `verify_due`, the queue of work for the verification role. A
  changed handle claim is queued once the PLC source is at the tip.
- The three ingest sources run together, each on its own database
  connection, and stop together.
- The handle checker: the DNS record first, then HTTPS on the
  handle's own host, with non-public addresses refused on every
  hop, redirects limited, and each answer classed as a DID, nothing,
  or inconclusive. A 401 or 403 is inconclusive. `lodestar
  check-handle` resolves handles once and prints the result.
- Applying a check's result. A handle that resolves to the account
  is verified and takes the handle from whoever held it; a verified
  handle that resolves to nothing is demoted only by a second such
  result at least an hour later; a check that keeps failing is
  retried on a lengthening schedule and settles as unreachable after
  a day, or after 14 days if the handle was verified. A changed
  claim clears the old claim's failure state. `lodestar
  check-account` checks named accounts and stores the result.
- The verification role, `lodestar verify`. It takes work from the
  queue in priority order, checks each handle under a limiter (a
  rate and an in-flight cap per registrable domain, global limits,
  and a hold when a host asks to wait), applies the result, and
  queues retries and confirming checks as they come due. A full
  domain at the head of the queue does not hold up the rest.
- Shared handles. A handle claimed by more than 100 identities is
  resolved once into its own row, its claimants take their status
  from that row, and a changed claim asks for the row to be
  refreshed, at most once an hour. The API reads a claimant's
  status from the row.
- The background sweep. A lap reads `accounts` in storage order and
  checks every handle that is due, listed identities first. One lap
  covers every domain but the large ones and sends what it cannot
  check at once to the queue; each large domain has a walker of its
  own, paced by that domain's rate. A lap starts once the PLC source
  is at the tip and the first relay walk is done, and no sooner than
  7 days after the last one started.
- Control checks. A few handles with known DIDs, each resolved by
  DNS or by HTTPS, are checked every minute. If more than half of
  either method's controls fail, all outbound verification pauses;
  it resumes after three clear rounds, and nothing runs after a
  start until one clear round. A check in flight when a pause
  starts is dropped, results applied in the ten minutes before are
  checked again, and when the pause ends every failure clock moves
  forward by its length. A control that has gone bad on its own is
  left out of the count after a day, never taking a method below
  two. `lodestar verify` refuses to start without at least two
  controls of each method.
- The two per-domain pauses. When more than 60% of a domain's first
  attempts fail, checks for it stop apart from a probe one minute
  in and every ten minutes after, and a conclusive probe ends the
  pause. When more than half the answers for a domain's verified
  handles are "nothing", confirming checks wait and nothing is
  demoted until most answers are right again. Each ends after 24
  hours regardless and cannot start again until the domain has
  behaved. Their state is stored, and their holds do not run during
  a control pause.
- `did:web` documents. The verification role fetches each queued
  document from its own host, reads the handle claim, PDS and
  signing key (including the older key form), and stores the
  account. A document that cannot be fetched leaves an
  `unresolvable` account that is retried with backoff; one that
  stops resolving keeps its last state for 14 days. Each known
  document is fetched again daily. A changed claim is queued for a
  handle check. A `did:web` with a path, a port or a TLD that cannot
  resolve is rejected without a request.
- Live resolution through the API. A handle that is not in the index
  is resolved by the verification role while the request waits, up
  to 5 seconds; if it resolves to an account that claims it, the
  verification is recorded and returned. A `did:web` lodestar has
  not seen is fetched the same way and stored if it declares a PDS,
  and reported as `not_an_account` if it does not. Misses are
  remembered for a minute.
- `POST /account/{did}/refresh`: fetches a `did:web`'s document
  again and queues the account's handle for a check ahead of the
  sweep.
- A never-checked handle a consumer looks up, alone or in a batch,
  is queued ahead of the sweep, up to a bound.
- `/ops/health` reports verification: the worker's own counts, the
  queue by reason, lap positions, pauses, control checks, shared
  handles and failing checks. `/ops/metrics` gives the sources,
  table sizes, queue and verification figures in Prometheus format.
  The verification role writes its status to the database each
  minute for the API role to read.
- A container image and a compose file: Postgres and the three
  roles, each role its own container from one image.
- Config profiles for rates, `vps` and `hobbyist`, and per-domain
  rate overrides.
- A README for operators.
- A release workflow. A tag `v<version>` builds the x86-64 image,
  pushes it to `ghcr.io/skydeval/lodestar` under that version, and
  creates the release from the version's section of this file. The
  compose file names that image.
- A CI workflow: formatting, clippy, tests and a dependency check on
  every push.
- Licensed under MIT or Apache-2.0.
- Views `accounts_readable` and `identity_history_readable`, which
  show DIDs, signing keys and endpoints as text.
- The API role, `lodestar api`: account lookup, history, batch
  lookup, handle lookup from the verified index, and `/ops/health`.
  Every account answer carries the PLC source's position, and an
  identity lodestar does not have is `not_found` only when the
  source is at the tip and fresh, `syncing` otherwise. Live
  resolution of unseen handles and `did:web` identities, and the
  refresh endpoint, wait on the verification role.
- `lodestar init-db` and `lodestar ingest`.
- Scripts to build, test and run on the build host.
- Recon tooling under `recon/`, kept as its own Cargo workspace so the
  main build does not depend on it. It measures what the design left
  to be measured and is not the implementation.
  - PLC backfill that pages the directory's sequenced export and
    applies each operation, including detection of nullified
    operations from a recovery window.
  - Jetstream v2 live subscription recorder and cursor probe.
  - Relay host roster and `listRepos` walk.
  - Handle-check sampler: DNS then HTTPS, classified as conclusive or
    inconclusive, behind an address guard, paced per registrable
    domain, with a selectable DNS resolver.
  - `did:web` document fetcher.
  - Comparison of stored state and history against the directory's
    audit log.
  - Timing of handle checks triggered by live identity events.
  - Compact-encoded copies of the loaded tables, and SQL that reports
    the footprint of each encoding.
  - Decoder for Jetstream v2 archive segments and block frames.
  - Jetstream replay planner that sizes a replay before any download,
    for a plan limited to identity and account events or an
    unfiltered plan, and a downloader that records the DIDs found.
  - Replay membership probe that asks the planner about individual
    DIDs and confirms with a few of the planned blocks.
  - Scripts to build and run the tooling on the recon host.
  - SQL reports for the PLC backfill, relay walk, `did:web` documents,
    replay probes, per-domain handle counts and audit comparison.
  - A simulation of the first handle sweep that records table and
    index growth, and a measure of what maintenance recovers.
  - Batch lookup timings for running the database under a memory
    cap.
  - A rate probe that checks handles at stepped rates and stops at
    the first refusal, with a build script for hosts older than the
    recon host.

### Changed

- The mark for an identity that has a repo is a row in its own table,
  `listed`, and no longer a column of `accounts`. Setting the column
  rewrote the row and its key, some twenty million times in a relay
  walk, which made the first walk take over four hours on the
  reference host. A database that has the column is moved across
  when a role starts.

### Fixed

- The verification role counted handle claims once, when it started.
  Started beside a first sync, it counted an empty table and would
  later have checked every claimant of a shared handle separately. It
  now counts when the PLC source reaches the tip, and checks no handle
  before then.
- An empty rate or control setting in the environment stopped the
  verification role at start-up. It is now treated as unset.
- With a long queue the verification role stopped keeping up with it.
  Each read of the queue sorted all of it and could read all of
  `accounts`, so changed claims waited hours behind a lap's overflow
  and the role's status went unreported. The queue is now read along
  an index, only the rows read are looked up in `accounts`, and it is
  read at most four times a second.
- A domain with a great many handles queued filled every read of the
  queue, so the domains behind it were not reached. A queued handle
  now carries its registrable domain, and the queue is read past the
  rows of a domain that has no room or is paused. A read is also
  kept to half a second, so that consumers' requests and changed
  claims do not wait on it.

- Recon PLC backfill compared an operation's `prev` with the previous
  operation's CID as text. The directory accepts a `prev` written in
  base58btc as the same CID it publishes in base32, so 111 ordinary
  updates were recorded as forks. CIDs are now compared by value.
