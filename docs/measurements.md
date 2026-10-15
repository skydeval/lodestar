# Measurements

How long lodestar took to build its index of the whole network, measured
on the reference host in October 2026. The network grows, so read these
as a dated snapshot, not a promise.

The reference host is a VPS with 4 vCPUs, 8 GB of memory and
a 75 GB SSD, running the compose file in this repository with the `vps`
profile.

## From empty to every handle verified

| Stage | When (UTC) | Took |
|---|---|---|
| First sync of the PLC directory | 12 Oct 18:44 to 13 Oct 03:48 | 9 hours 4 minutes |
| Relay walk | 13 Oct 18:32 to 19:59 | 1 hour 27 minutes |
| First verification of every handle | 13 Oct 08:00 to 15 Oct 03:24 | 43 hours 23 minutes |

- **Sync:** 129,331,345 operations, giving 123,002,202 identities. The
  pace, about 4,000 operations a second, is the directory's own limit.
- **Relay walk:** 20,475,713 identities listed, in 20,476 pages. This is
  the walk as lodestar now does it. The first walk on this host, before
  the walk was reworked, took 4 hours 9 minutes, and verification
  started after it.
- **Verification:** handles on `bsky.social` were checked at 320 a
  second; every other domain started at one a second and climbed as it
  answered. During this run lodestar was restarted five times to deploy
  fixes, so a run on the current version should be no slower.

## What the first verification found

| Handle status | Accounts |
|---|---|
| Verified | 45,687,150 |
| Mismatch: the handle points at another account | 501,962 |
| Unresolved: the handle points at nothing | 3,196,816 |
| Unreachable: the handle's domain did not answer | 113,946 |

About 50 million more accounts claim one of two handles,
`donald.trump.com` (48.3 million) and `plc.surge.sh` (1.7 million). A
handle claimed that widely is resolved once and its claimants take their
status from it, so they did not need checks of their own.

After the first verification the database was 28 GB.

## After the first run

Every handle is checked again once a week, and a changed handle is
checked within seconds of the PLC directory publishing it.
