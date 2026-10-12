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

### Fixed

- Recon PLC backfill compared an operation's `prev` with the previous
  operation's CID as text. The directory accepts a `prev` written in
  base58btc as the same CID it publishes in base32, so 111 ordinary
  updates were recorded as forks. CIDs are now compared by value.
