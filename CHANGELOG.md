# Changelog

All notable changes to lodestar are recorded in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Work towards 0.1.0.

### Added

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

### Fixed

- Recon PLC backfill compared an operation's `prev` with the previous
  operation's CID as text. The directory accepts a `prev` written in
  base58btc as the same CID it publishes in base32, so 111 ordinary
  updates were recorded as forks. CIDs are now compared by value.
