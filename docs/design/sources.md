# Sources

Lodestar needs more than the PLC directory. `did:web` accounts never
appear in it, and handles can change or break with no directory
operation at all. So it reads four things:

| Source | Gives | Role |
|---|---|---|
| The PLC directory | Every `did:plc` identity: its handle claim, PDS, signing key and DID document | ingest |
| Jetstream | Identity and account events: `did:web` discovery, and signals that an identity may have changed | ingest |
| A relay's repo listing | Which identities have a repo, and `did:web` accounts no event has shown | ingest |
| The hosts themselves | `did:web` documents and handle resolution | verify |

## The PLC directory

### Paging the export

The PLC source pages `GET /export?after=<seq>&count=1000`, the
directory's sequenced export. Each line is one operation with a dense,
strictly increasing `seq`, and the last line's `seq` is the next
`after`. Lodestar uses only this integer-cursor form.

- Requests are at least 250 ms apart, four a second, which is the pace
  the directory allows. A whole first sync is about 130 million
  operations and takes about 9 hours.
- The next pages are fetched while the current one is applied, up to
  four ahead.
- **Each page is applied in one transaction together with the new
  cursor.** A crash can lose at most the page in hand, and a restart
  resumes from the last committed `seq`.
- Pages must arrive in strictly increasing `seq`. One that does not, or
  a line that cannot be parsed, stops the source rather than risk
  applying the wrong thing.
- A failed request is retried with backoff from 2 seconds to 5 minutes,
  honouring `Retry-After`.
- A page shorter than `count` means the tip has been reached.

The cursor is stored with the directory URL it came from. A `seq`
belongs to one directory, so lodestar refuses to start against a
different URL with a stored cursor.

### Following the stream

At the tip, the source opens the directory's stream,
`wss://…/export/stream?cursor=<seq>`, which delivers the same objects
from just after the cursor and then continues in real time. The export
and the stream share one `seq` space, so the handoff has no gap, and
any operation at or below the committed cursor is ignored, so overlap
cannot apply anything twice.

- Operations from the stream are applied in batches of up to 500, or
  every quarter of a second.
- Every 2 minutes the source also asks the export for a page from its
  cursor. A stream can stay connected while delivering nothing; this is
  the independent check that it is really at the tip. A full page means
  the stream has fallen behind, and the source goes back to paging.
- If the stream closes, the source goes back to paging until it reaches
  the tip again, then reopens the stream. If the stream cannot be opened
  at all, the source polls the export every 5 seconds instead.
- A stream that says the cursor is in its future stops the source
  without touching the cursor.

**Freshness.** The source records when it last confirmed it was at the
tip: when the export returned a short page, and every 15 seconds while
a healthy stream stays open. That time, not the age of the last
operation, says how far behind lodestar is: on a quiet directory the
last operation can be old while lodestar is exactly at the tip. The API
reports it with every answer ([API](api.md#freshness)).

### What is read from an operation

| Form | Handle claim | PDS | Signing key |
|---|---|---|---|
| `plc_operation` | the first valid `at://` entry of `alsoKnownAs`, lowercased | `services.atproto_pds.endpoint`, where its `type` is `AtprotoPersonalDataServer` | `verificationMethods.atproto` |
| `create` (legacy, genesis only) | `handle` | `service` | `signingKey` |
| `plc_tombstone` | none | none | none |

Each operation carries the identity's full state, so the latest
accepted operation is the current state.

- A claim must be a syntactically valid handle. An `alsoKnownAs` with no
  valid handle gives no claim, whatever else it holds; about 23 million
  identities use the field for other data.
- An operation of a type lodestar does not know changes nothing, but it
  stays in the identity's chain so later operations can follow it.
- A tombstone clears the claim, PDS and key and marks the identity
  tombstoned.
- A claim, PDS or key holding a NUL byte, which Postgres cannot store,
  is dropped. A DID holding one stops the source.

**DID document parts.** Most identities' DID documents can be rebuilt
exactly from the claim, PDS and key above. Some cannot: a labeler has a
second key (`#atproto_label`) and a second service (`#atproto_labeler`);
some identities name more than one `alsoKnownAs` entry; some names are
kept in another form or not kept at all, such as a handle with capitals
or an `at://` entry that is not a valid handle. For those, lodestar keeps the operation's
`alsoKnownAs`, `verificationMethods` and `services` exactly as the
directory gives them, which is what lets it answer the directory's DID
document read exactly ([API](api.md#get-did)).

- **Extra keys or services** are kept for every identity. About 1% of
  operations carry them.
- **Extra names only** are kept only for identities with a repo. More
  than half of all operations differ only in their names, nearly all
  of them spam registrations: one wave alone lists seven names on each
  of about 48 million identities, and keeping them all would cost
  20 GB. Of the identities whose name lists are their own, about 0.2%
  have a repo. For the rest, the document is rebuilt from the claim,
  PDS and key, and differs from the directory's only in leaving out the
  extra `alsoKnownAs` entries.

An identity that gains a repo later gets its names kept with its next
operation.

### Recoveries

For 72 hours after an operation, a higher-authority rotation key can
replace it. The directory then accepts a recovery operation whose
`prev` points at an earlier operation, and the operations after that
point are *nullified*. The sequenced export includes nullified
operations and does not flag them.

Current state needs nothing special: the recovery is the newest
operation and carries the full state, including undoing a tombstone.

History does. Lodestar keeps every operation from inside the recovery
window in `plc_recent_ops`, with its CID and the history row it
produced, if any. When an operation's `prev` is not the identity's
latest recent CID, it is a fork:

- the recent operations after the one named by `prev`, or all of them
  if `prev` is older than the window, are nullified;
- the history rows they produced are deleted;
- the new operation is compared against the last state that survives.

**CIDs are compared by value, never as text.** The directory publishes
CIDs in base32 and accepts a `prev` written in base58btc as the same
CID. Comparing the strings would report ordinary updates as forks and
delete valid history.

The window is measured in operation time (`createdAt`), not wall time,
so it works the same during the first sync. It is 84 hours by default,
72 plus a margin. Operations older than that are pruned regularly.

How history rows are written is in
[Data model](data-model.md#history).

### During the first sync

Claim changes are not queued for verification during the first sync:
there would be one for every identity, and the first lap covers them
all. Queuing starts when the source first reaches the tip.

## The document pass

The first sync writes DID document parts as it goes, but it runs
before the first relay walk, when nothing is known to have a repo yet,
so the names that are kept only for listed identities are missing. The
document pass fills them in: a one-off job in the ingest role that
reads the export from the start up to where the PLC source was when
the pass began, and writes each identity's parts as of its last
operation in that range.

- It runs beside the live source, at the directory's pace, about 9
  hours for the whole directory, and resumes from its own position
  after a restart.
- It never replaces parts the live source has written since it began:
  any identity with an operation after the pass's end in the recovery
  window is left alone. Because it relies on that window, a pass that
  began more than 72 hours ago starts again.
- It begins only once the first relay walk has finished, so it runs
  beside the first sweep.

## Jetstream

Lodestar subscribes to Jetstream's `subscribeEvents` for `identity` and
`account` events only, so no commit events are sent. The live tail is
light: tens of thousands of events a day.

**Events never write identity state.** An identity event says an
identity *may* have changed, not what changed, and changes can happen
with no event. So lodestar uses events only to schedule work:

| Event | DID | Lodestar |
|---|---|---|
| identity or account | `did:web` | Fetches the document, known or not. |
| identity | `did:plc` it has | Re-checks the handle, once the PLC source is live. Document state comes only from the directory. |
| identity | `did:plc` it does not have | Nothing; the PLC source will bring it. |
| account, active | any it has | Marks the identity as listed. |

Queued event work is capped (10,000 by default). Past the cap, the
work is dropped and the sweep covers it.

### Several instances

Lodestar subscribes to several Jetstream instances at once, two by
default, each on its own connection with its own position. Losing one
costs nothing while another stays live.

**Positions are event times.** One instance address can stand in front
of several servers that number their events differently, so a sequence
number may mean nothing after a reconnect. An event's time means the
same on every instance. Lodestar resumes 30 seconds before its stored
position; a repeated event only asks for work already queued, and the
queue keeps one entry. A new instance starts from the newest
position any other instance has.

**At the tip.** The filtered stream can be silent for minutes, so the
arrival of messages is no test. Lodestar pings every 30 seconds and
treats an answer with nothing buffered as being at the tip. After 5
minutes with no message at all it reconnects.

**A lost position.** An instance refuses a position older than its
lookback window, or reports it as outdated. Lodestar then restarts that
subscription at the live tip. If another instance has been live
throughout the gap, nothing is lost. If not, events from the gap may be
missing, and lodestar asks for a relay walk, which finds the `did:web`
accounts and repos the gap would have shown.

## The relay walk

Which identities have a repo, and `did:web` accounts no event has
shown, are found by listing a relay's `com.atproto.sync.listRepos`, a
thousand per page.

- **Which relay.** Several can be configured, two by default. When a walk
  starts, lodestar times each and uses the fastest; a walk in progress
  stays on the relay it began with.
- **Pages** are appended to a holding table with no lookups, at up to
  four requests a second. When the listing ends, what was seen is merged
  into `listed` in key order, in one pass. A walk takes about an hour
  and a half on the reference host.
- **An unseen `did:web`** is queued for a document fetch, marked as
  listed.
- **When.** The first walk runs once the PLC source is live. After
  that, every 7 days, and whenever a lost Jetstream position asks for
  one.

The listing has no DID-method filter, and only `did:plc` and `did:web`
are kept. A relay's listing is known to be incomplete, and no source
lists every `did:web`, which has no registry: a `did:web` no source has
shown is resolved the first time someone asks for it
([API](api.md#get-accountdid)).

## The per-host walk

What a relay's listing lacks, a host may still list itself: a host the
relay has stopped crawling, or repos it never picked up. So the
verification role also reads the relay's `com.atproto.sync.listHosts`
(about 1,900 hosts it lists as active, October 2026) and then each
host's own `listRepos`, a thousand per page.

- **Where.** In the verification role, not ingest: these requests go to
  hosts lodestar does not control, so they go through the same address
  guard as a handle check ([Outbound safety](#outbound-safety)) and under
  each host's registrable domain's rate.
- **What a host is believed.** Only identities lodestar already holds
  are marked as listed, so a host cannot add DIDs that do not exist. At
  most 1,000 unseen `did:web` accounts from one host are queued for a
  document fetch, and at most 1,000 pages of one host are read.
- **When.** Once the relay walk has run, then every 7 days. It keeps its
  place host by host across restarts. `LODESTAR_HOST_WALK=false` turns
  it off.

## `did:web` documents

The verification role fetches `https://<host>/.well-known/did.json`. In
atproto a `did:web` is a hostname only, with no path and no port, under a
top-level domain that can resolve; anything else is refused without a
request.

**Reading the document.**

- Its `id` must equal the DID asked for, and it must be valid JSON of at
  most 64 KB.
- Handle claim: the first valid handle among the `at://` entries of
  `alsoKnownAs`, lowercased.
- PDS: the service whose `id` ends `#atproto_pds` with type
  `AtprotoPersonalDataServer`.
- Signing key: the `Multikey` verification method whose `id` ends
  `#atproto` and whose controller is the DID. The legacy key
  representation that some `did:web` documents still use is converted.

**When.** A document is fetched when an event names it, when a walk
finds a `did:web` lodestar has never seen, when a consumer asks for it,
and again every 24 hours.

**Not every `did:web` is an account.** A document with no atproto PDS,
asked for by a consumer, is answered `not_an_account` and not stored. A
`did:web` found by Jetstream or a walk is stored either way.

**Documents that will not resolve.** This is common: most of the
`did:web` accounts a relay lists cannot be fetched, often because the
host's certificate does not cover its name.

- A `did:web` whose document has never resolved is stored as
  `unresolvable`, with no claim, PDS or key, so it is known and not
  queued again by every walk.
- A failing fetch is retried with backoff, from 5 minutes up to the 24
  hour refresh interval.
- A stored account whose document has failed for 14 days becomes
  `unresolvable`. Its claim can no longer be read, so its handle leaves
  the index. A later successful fetch makes it active again.
- Its first successful fetch is its first known state; there is no
  history from before it.

## Outbound safety

Every handle and every `did:web` names a host chosen by someone else.
All such requests come from the verification role, under these rules:

- **Every address a connection would use is checked**, and loopback,
  private, link-local, documentation, multicast and other non-public
  ranges are refused, for IPv4 and IPv6, including IPv4-mapped IPv6.
  The check sits in the resolver the HTTP client uses, so it covers
  every hop.
- **Redirects**: at most three, each staying on HTTPS and port 443,
  never to an address literal.
- **HTTPS on port 443 only.**
- **Bounded**: 5 seconds for a DNS query, 8 seconds to connect, 20
  seconds for a whole request, and capped response bodies.
- **Identified**: every request carries a User-Agent naming the
  operator, which is a required setting.
