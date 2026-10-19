# Measurements

How long lodestar took to build a copy of the whole network from
nothing, measured on the reference host in October 2026. The network
grows, so read these as a dated snapshot, not a promise.

The reference host is a VPS with 4 vCPUs, 8 GB of memory and a 75 GB
SSD. The run below is a clean install: the database deleted, then
[Running lodestar](running.md) followed from the start, with the
released image and the compose file's defaults.

## From empty to every handle checked

| Stage | When (UTC) | Took |
|---|---|---|
| First sync of the PLC directory | 16 Oct 20:23 to 17 Oct 05:40 | 9 hours 17 minutes |
| Relay walk | 17 Oct 05:42 to 07:20 | 1 hour 38 minutes |
| Document pass | 17 Oct 07:22 to about 16:50 | about 9 hours 30 minutes |
| First sweep of every handle | 17 Oct 07:21 to 19 Oct 04:23 | 40 hours 14 minutes |

- **Sync:** about 130.4 million operations, giving about 124.1 million
  identities, at about 3,970 operations a second. That pace is the
  directory's own limit, so a faster host would not sync faster.
- **Relay walk:** 20.5 million identities with a repo. The merge at the
  end took 80 seconds.
- **Document pass:** runs beside the sweep once the walk is done, and
  carried on across two restarts.
- **Sweep:** checks ran at about 320 a second, most of them on
  `bsky.social`, held there by its own limit. Every other domain started
  at one a second and climbed as it answered; those were done within
  the first day. The sweep's first three hours ran slower, before two
  fixes made during this run: the sweep waited on each page's slowest
  check, and the DNS resolver had been set to Google's IPv4 addresses
  directly, which stalled every 22 minutes or so. With both fixed from
  the start, expect the sweep to take a couple of hours less. The
  same sweep took 43 hours a week earlier, before the fixes of this
  run.

## What the first sweep found

Active accounts that claim a handle of their own, just after the sweep:

| Handle status | Accounts |
|---|---|
| Verified | 45,756,458 |
| Mismatch: the handle points at another account | 503,383 |
| Unresolved: the handle points at nothing | 3,176,184 |
| Unreachable: the handle's domain did not answer | 565,489 |
| Still being retried | 201,221 |

About 51 million more accounts claim one of 126 handles claimed very
widely, most of them `donald.trump.com` (48.9 million) and
`plc.surge.sh` (1.7 million). A handle claimed that widely is resolved
once and its claimants take their status from it. About 23 million
accounts claim no handle.

Near the end the sweep reached about 196,000 handles of the form
`word.name.bsky.social`, registered in April 2025 in a spam wave. Bluesky's
certificate covers one label before `bsky.social`, so these fail TLS
every time. Each is retried for a day before settling as unreachable,
which is why some handles are still being retried after the sweep ends.

## Disk

| When | Database |
|---|---|
| After the first sync | 21 GB |
| After the document pass | 30 GB |
| After the first sweep | 27.3 GB |

Most of the growth after the sync is the first sweep's writes: each
check rewrites its account's row, and the space the old rows leave is
reused by later laps.

The 30 GB includes about 7 GB that a correct install does not have.
In this run the relay walk's merge failed several times before a fix
made during the run, and each failed attempt left 20 million dead rows
in `listed`. Compacting the table (`VACUUM FULL listed`) during the
sweep took the database from 35.1 GB to 27.6 GB.

## After the first run

Every handle is checked again once a week, and a changed handle is
checked within seconds of the PLC directory publishing it.
