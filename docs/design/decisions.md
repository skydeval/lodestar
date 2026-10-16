# Decisions

The choices that shape lodestar, and why they were made. Each was weighed
against the alternative it names.

## Keep the result of each operation, not the operation

**Chosen:** store each identity's current claim, PDS and key, its
handle and PDS history, and the DID document parts the columns cannot
rebuild. **Instead of:** a full replica of the directory with its signed
operation log.

The signed operations are most of the directory's size. Without them the
whole network fits in about 28 GB, on a 75 GB server, and lodestar can
still answer everything a service asks the directory in ordinary use:
where an account lives, what key signs its data, its DID document, and
what it has been called. What it gives up is the ability to audit an
identity's chain of operations, and the log endpoints that serve it.
Lodestar also does not validate operations; it trusts the directory's
ordering, as every reader of the export does.

## Verify every handle, centrally

**Chosen:** lodestar resolves every claimed handle on the network itself,
and re-checks each weekly. **Instead of:** trusting claims, or leaving
each consumer to verify the handles it shows.

A claim proves nothing; anyone can claim any handle. Every service that
shows handles needs them verified, and verifying them is the expensive
part: one request per handle, against tens of thousands of domains,
under rate limits. Doing it once, politely, for everyone, is cheaper for
the network than every consumer doing it. Handles at the account's own
host are verified too, not assumed.

## Index every identity up front

**Chosen:** copy every identity the directory holds, and every `did:web`
the other sources show, before anyone asks. **Instead of:** fetching on
first use.

Fetching on first use puts the network's latency in front of the first
lookup of every account, and makes lodestar's answers depend on which
accounts someone happened to ask about. A full copy answers every
lookup locally and makes history available for every identity.

## Compact encodings as the only format

**Chosen:** binary DIDs, keys and CIDs, interned endpoints, history only
for identities that changed, a partial index for handle lookups.
**Instead of:** text columns and one history row per identity, which took
twice the disk.

Disk is what binds at the small end, and every one of these was measured
on the full directory before it was adopted. Readable views give an
operator the plain form when looking by hand.

## Postgres

**Chosen:** one Postgres database for everything, including the queues
between roles. **Instead of:** an embedded store, or a message queue.

It works the same at 75 GB and at 10 TB, does not need the working set
in memory, and makes every source's position transactional with the data
it covers, which is what makes restarts exact. Using it for coordination
means the roles need no other channel between them.

## Three roles, one binary

**Chosen:** ingest, verification and API as separate processes from one
binary. **Instead of:** one process.

Only verification contacts arbitrary hosts, so every outbound safety rule
and rate limit lives in one place, and the API can run with no outbound
network. A slow verification lap cannot slow the API, and a role can be
restarted without the others noticing.

## Jetstream for signals, a relay listing for enumeration

**Chosen:** Jetstream's identity and account events, from several
instances at once, plus a weekly walk of a relay's repo listing.
**Instead of:** a relay's full firehose, or Jetstream's paid replay
archive.

Lodestar needs only to know when an identity *might* have changed and
which accounts exist; the full firehose is almost entirely commits it
does not want. Jetstream's filtered stream is light and needs no key.
Two instances at once mean losing one costs nothing. The archive was
measured and found to miss hundreds of `did:web` accounts the relay
lists, at the cost of a 200 GB download, so the listing is the source of
record and replay was not built.

Positions in Jetstream are event times, not sequence numbers: one
instance address can stand in front of servers that number events
differently, and a time means the same on every one.

## Weekly re-checks, measured from each lap's start

**Chosen:** re-check every handle once a week, with claim changes,
events and consumer requests checked ahead of that. **Instead of:** a
cadence that varies by how recently each handle changed.

A week is long enough that the full sweep, under two days at the default
rates, leaves the largest provider alone most of the time, and short
enough that a handle that broke quietly is caught soon. Anything that
signals a change is checked within seconds anyway. Measuring due-ness
from a lap's start, not a fixed age, means each lap checks each handle
exactly once.

## Rates per registrable domain

**Chosen:** limits keyed on the registrable domain, adapting to how the
domain answers. **Instead of:** limits per hostname.

Every handle is its own hostname, so per-hostname limits would not limit
a provider whose users are its subdomains at all. Rates start at one a
second and climb only while a domain answers quickly and conclusively,
which suits the long tail of small self-hosted servers. The largest
provider has its own rate, measured from a single address and held for
hours without a refusal.

## Never punish a handle for lodestar's own fault

**Chosen:** control checks that pause all verification when lodestar's
own network fails, and domain pauses when one provider fails or starts
answering wrongly. **Instead of:** counting failed attempts.

A resolver outage or a blocked port would otherwise make every handle
look broken at once. Failure clocks are frozen through a control pause,
a provider's errors stop its checks instead of piling up, and a verified
handle needs a "nothing" answer confirmed an hour later before it is
demoted.

## Shared handles

**Chosen:** a handle claimed by many identities is resolved once, and its
claimants read that result. **Instead of:** checking each claimant.

Two handles are claimed by about 50 million identities between them.
Checking each claimant would cost 50 million requests a week to two
domains for two answers, and rewrite 50 million rows each time.

## Public and read-only

**Chosen:** the read API is public, behind a proxy that passes only the
reads. **Instead of:** an internal service on a private network.

Lodestar exists so that services near it do not have to ask
`plc.directory`; any service that can reach the directory should be able
to reach a mirror of it. Nothing it serves can change anything, so there
is nothing to authenticate. The requests that make lodestar contact
other servers on demand, and the endpoints that describe its internals,
stay behind the proxy.

## Out of scope

- The signed operation log, and the directory's log endpoints.
- Validating PLC operations.
- Writing to the directory.
- Whether an account's host considers it active.
- A stream of changes, or a signal when a re-check finishes.
- Listing every DID, handle prefix search, point-in-time queries.
- Rate limiting of consumers, and caching of answers, inside lodestar:
  the proxy does those. Lodestar itself only remembers misses for a
  minute and the status page's figures for 30 seconds.

## Known limitations

- **Extra names of identities with no repo are not kept.** `GET /{did}`
  for such an identity leaves out the extra `alsoKnownAs` entries the
  directory shows; everything else matches. Keeping them would cost
  several gigabytes for spam registrations.
- **One verification role.** Its rate limits live in its memory, so a
  second one would double them.
- **`did:web` coverage is incomplete.** No source lists every `did:web`;
  one no source shows is found the first time someone asks for it.
- **A provider that wrongly answers 404 for unchecked handles** gets them
  written `unresolved` until the next lap. Only verified handles are
  protected by confirmation ([Handle verification](handle-verification.md#3-one-domains-verified-handles-are-resolving-to-nothing)).
- **Most timings are fixed** in 0.1: the re-check interval, grace
  periods, retry schedule and pause thresholds have no setting yet.
- **Event work past its cap is dropped.** The weekly lap covers what it
  would have done.
- **x86-64 images only.**
