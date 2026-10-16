# Deployment

How lodestar is meant to be run, and what it costs to run. The
[README](../../README.md#running-it) has the steps; this page has the
reasoning and the budgets.

## The reference host

Lodestar is designed for, and measured on, a VPS with a 75 GB SSD,
8 GB of memory and 4 vCPUs, running nothing else, with the compose
file in this repository and the `vps` profile. Nothing else shares its
disk or memory, so no other service can starve it.

From an empty database, on that host in October 2026
([measurements](../measurements.md)):

| Stage | Took |
|---|---|
| First sync of the PLC directory | about 9 hours |
| Relay walk | about 1.5 hours |
| First verification of every handle | about 43 hours |

The first sync is paced by the directory, not by the host. The first
verification is paced by the rate toward the largest provider.

Consumers should switch over to lodestar once it is `live` and `fresh`
([API](api.md#freshness)). Until then, it answers what it does not have
yet with `syncing`.

## Disk

| On a 75 GB disk (about 70 GiB usable) | |
|---|---|
| Database at its peak, during the first sync and sweep | about 32 GiB |
| Operating system, container runtime, images | about 7 GiB |
| Write-ahead log (a 3 GB target, not a hard cap) | about 3 GiB |
| Logs | about 1 GiB |
| Free at the worst moment | about 26 GiB |

After the first sweep the database settles at about 28 GB. The
directory grows by roughly 100,000 to 250,000 identities a day, which
is about 1 GiB a month in lodestar's schema. Growth is driven mostly by
bulk registration, so it can change without warning.

Lodestar never drops data to make room. Health reports every table's
size and the combined size of its indexes, so an operator can see a full disk coming months
ahead. The remedy is a larger disk; choose a host whose disk can grow.

## Memory

Postgres gets 2 GB of shared buffers and is told to expect 5 GB of
cache by default (`POSTGRES_SHARED_BUFFERS`, `POSTGRES_CACHE_SIZE`),
which suits an 8 GB host shared with the three roles. Lookups by DID
are spread evenly over the `accounts` primary key, about 5 GB, so they
are fastest when memory can hold it. The sweep reads `accounts`
sequentially and does not need it in memory.

## Network

**Transfer.** The first sync reads about 140 GB of JSON from the
directory, less on the wire, and a relay walk about 3 GB. Verification
is the standing cost: every handle at the largest provider is its own
hostname, so every check opens its own TLS connection, and a weekly lap
makes tens of millions of them. That is estimated at 200 to 300 GB a
week; it has not been measured. A host with a transfer allowance should
check it against this before choosing its rates.

**Inbound.** Only the proxy needs to be reachable. The API and the
status page listen on the host's loopback.

**Outbound.** Ingest needs the PLC directory, Jetstream and a relay.
Verification needs any host on the internet, by DNS and by HTTPS on
443. The API needs nothing.

### DNS

Verification is the heaviest DNS user most hosts will ever see: up to
three queries per check, about 1,200 a second at the default rates.

- **Public resolvers have per-client limits.** Measured from one address
  in October 2026: Google Public DNS answered cleanly up to about 1,500
  queries a second and silently dropped queries above that; Cloudflare's
  answered cleanly up to about 600 a second and from 1,000 a second
  failed a fifth to a third of queries with `SERVFAIL`. Every dropped or
  failed query is an inconclusive check.
- **Resolvers that block domains are unsuitable.** A resolver that
  answers a blocked domain with "no such domain" makes lodestar record
  those handles as resolving to nothing.
- **The container path costs CPU.** By default a container's queries go
  through Docker's resolver and often the host's own stub resolver.
  `LODESTAR_RESOLVER` points verification at resolvers directly, several
  separated by commas, which skips both.
- **Lodestar warns at start-up** when its rates could pass about 1,400
  queries a second.

An operator who raises the rates past that needs a resolver without a
per-client cap, such as a recursive resolver of their own.

## Going public

The read API and the status page are meant to be public; the ops
endpoints and the refresh request are not. The rules that make that true
live in the proxy, in the repository's [Caddyfile](../../Caddyfile):

| Path | Goes to |
|---|---|
| `/` | redirect to `/status/` |
| `/status*` | the status page |
| `GET`/`HEAD` `/account/*`, `/handle/*`, `/did:plc:*` | the API |
| `POST /accounts` | the API |
| anything else, including `/ops/*` and `POST …/refresh` | 404 |

**By default** the compose file runs Caddy with these rules, and the
status page with it, so a new install has a safe public face from its
first start: plain HTTP on port 80. Set `LODESTAR_DOMAIN`, point the
domain at the host and open ports 80 and 443, and Caddy fetches and
renews its own certificate. Inside compose it reaches the API and status
page by their service names, so their host ports stay on loopback.

**On a host that already runs a proxy**, such as one shared with a PDS,
set `LODESTAR_PROXY=0` to leave the bundled one off, and copy the
Caddyfile's rules into yours, with `127.0.0.1:8080` and
`127.0.0.1:8090` in place of the service names.

**Behind a CDN** such as Cloudflare's proxy, the CDN is the place for
rate limits on the public API; lodestar has none of its own. To keep the
CDN's proxying on, give Caddy a certificate the CDN trusts, such as an
origin certificate, through the Caddyfile's `tls` line.

## Maintenance

**Index growth.** Every weekly lap rewrites the account row of every
handle it checks, about 50 million rows. Postgres reuses the space this
frees in the table, but an index keeps the size it grew to; one full
sweep was measured to double the primary key. So lodestar looks after
the two `accounts` indexes itself:

- After every rebuild it keeps each index's bytes per entry as a
  baseline. Bytes per entry, not size, because the verified-handle index
  rightly grows as handles are verified.
- Once an hour the verification role compares. An index past 1.5 times
  its baseline is rebuilt with `REINDEX … CONCURRENTLY`, so reads and
  writes go on, and only while no lap is running.
- An index with no baseline yet, on a database from before this was
  kept, is rebuilt once to get one.

A rebuild needs free space equal to the index, about 5 GiB for the key,
plus room for sorting and write-ahead log while it runs. A rebuild that
fails, for lack of space or otherwise, is logged and tried again an hour
later; the half-built copy it leaves is dropped first.

**Upgrades.** Every role creates or updates the schema when it starts.
After an upgrade that adds something to fill in, such as the document
pass, the ingest role does it in the background while everything keeps
serving.

## Other shapes

These follow from the design but have not been measured.

- **A larger host shared with other services.** Lodestar needs 25 to
  35 GiB of disk and benefits from enough memory to keep the `accounts`
  key cached.
- **Spinning disks.** The sweep reads in storage order and the live
  sources write a few rows a second, both of which suit them. Lookups by
  DID are random and depend on memory. Do the first sync on an SSD and
  copy the database over.
- **Small machines.** The `hobbyist` profile lowers the largest
  provider's rate, the global rate and every cap, so the first sweep
  takes far longer, but nothing is left out.
- **ARM.** Nothing in lodestar is tied to x86-64; ARM64 images are
  planned, and until then an ARM host can build the image itself.
