# Handle verification

An identity's DID document only *claims* a handle. Anyone can claim
any handle, so a claim on its own means nothing. A handle is the
account's only when the handle also resolves back to the account's
DID. Lodestar checks this itself, for every claimed handle on the
network, and re-checks every one each week. All of it runs in the
verification role.

## Handle states

| State | Means |
|---|---|
| `pending` | A claim exists and has not been checked since it was set. |
| `verified` | The last conclusive check resolved the handle to this DID. |
| `mismatch` | The last conclusive check resolved the handle to another DID. |
| `unresolved` | The last conclusive check found that the handle resolves to nothing. |
| `unreachable` | Checks over at least a day have all failed to get an answer. |
| `none` | There is no usable claim: none at all, an invalid one, or one that can never resolve (below). Nothing is checked. |

Only a `verified` handle is ever reported as an account's handle. Any
other claim is reported as a claim, with its state beside it
([API](api.md#get-accountdid)).

Handles are checked only for `active` identities. A tombstoned or
`unresolvable` identity is never checked, and a result that arrives for
one is dropped.

### Handles that are never resolved

Handles under these top-level domains get `none` with no request:
`.alt`, `.arpa`, `.example`, `.internal`, `.invalid`, `.local`,
`.localhost`, `.onion` and `.test`. That includes `handle.invalid`.

## How a handle is checked

1. **DNS.** Ask for the TXT records at `_atproto.<handle>`. Only values
   starting `did=` that hold a valid DID count. One DID, or several
   records that agree, is the answer. Records naming different DIDs
   mean resolution fails: the handle resolves to nothing.
2. **HTTPS**, only if DNS found no such record, or the name would be too
   long for DNS. `GET https://<handle>/.well-known/atproto-did`. A 2xx
   body that, trimmed, is a valid DID is the answer. The content type is
   not checked.
3. **Compare** the answer with the identity's DID.

DNS decides when both exist, as the spec prefers. If DNS itself fails,
the whole check fails: HTTPS is not used to decide while the preferred
method's answer is unknown.

The rules in [Sources](sources.md#outbound-safety) apply to every
request: public addresses only, HTTPS on 443, at most three redirects,
time limits, a User-Agent naming the operator.

### Conclusive and inconclusive

A check is **conclusive** when the domain answered:

| Answer | Result |
|---|---|
| A DID, by DNS or HTTPS | that DID |
| TXT records naming different DIDs | nothing |
| The name does not exist, or has no address | nothing |
| The name resolves only to non-public addresses | nothing |
| HTTPS 2xx with a body that is not a DID, or a body over 4 KB | nothing |
| HTTPS 4xx other than 401, 403, 408, 425 and 429 | nothing |

A check is **inconclusive** when nothing reliable came back: a DNS
failure or timeout, a failure to connect to a name that does resolve, a
TLS failure, an HTTP timeout, a refused redirect, a 5xx, or a 401, 403,
408, 425 or 429.

A 401 or 403 is the host refusing the client, which says nothing about
the handle. A host that has blocked lodestar's address answers that
way, and reading it as "resolves to nothing" would unverify every handle
it serves.

## Applying a result

- **A result applies only if the identity still claims the handle that
  was checked**, is still active, and the result was resolved after the
  status it would replace was set. Otherwise it is dropped, and a check
  for the new claim is still due.
- **A DID result applies at once.** If it is this identity's DID, the
  handle is `verified`, and any other identity that held it as verified
  becomes `mismatch`. If it is another DID, the handle is `mismatch`.
- **A "nothing" result** makes a handle that was not verified
  `unresolved` at once. A verified handle needs it confirmed (below).
- **An inconclusive result** leaves the status as it is and schedules a
  retry.

### Confirming a verified handle that resolves to nothing

A `verified` handle is not demoted by one "nothing" answer. Providers
have short outages that answer 404.

- The first such answer marks the handle as awaiting confirmation and
  schedules a confirming check an hour later. The status stays
  `verified`.
- A later "nothing" demotes it to `unresolved` only if it was resolved
  **at least an hour after the first**. An earlier one, from whatever
  path, is ignored, so a repeated lookup a minute later cannot stand in
  for the confirmation.
- A result naming this DID clears the mark. A result naming another DID
  is `mismatch` at once.

### Retrying and settling

A handle whose checks keep failing is retried. The wait before each
retry is the time since its first failure, but at least 12 hours and at
most 7 days: so retries come 12, 24, 48, 96 and 192 hours after the
first failure, and weekly after that. A host that asks lodestar to wait
(`Retry-After`, honoured up to an hour) never gets a retry sooner than
it asked.

A failing handle does not stay that way for ever, and does not leave it
on a blip. It **settles** as `unreachable` at the first failing result
that arrives after its *settling time*, measured from its first
failure: 24 hours, or 14 days for a handle that was `verified`. Settling
therefore always rests on at least two failures that far apart with
nothing conclusive in between, since any conclusive answer would have
ended the failure. An `unreachable` handle is then checked again on the
normal weekly cadence; a further failure only updates when it was
checked.

There is no attempt counter. What protects a handle from a short outage
is the settling time, and what protects it from lodestar's own outage
is that checks are not made at all while a pause is in force (below).

## When a handle is checked

### The queue

Work to do ahead of the sweep goes through the `verify_due` queue, in
priority order. A lower number is served first.

| Reason | | Queued by |
|---|---|---|
| 10 | **The claim changed.** A new directory operation or `did:web` document claims a different handle. | ingest, verify |
| 20 | **A consumer looked up an account** whose handle is due (below). | api |
| 21 | **A consumer asked for a refresh** of an account. | api |
| 30 | **A batch lookup** included accounts whose handles are due. | api |
| 31 | **An identity event** named the account. | ingest |
| 32 | **The relay walk** found an unseen `did:web`. | ingest |
| 40 | **A retry** of a failing check. | verify |
| 41 | **A confirming check** of a verified handle. | verify |
| 42 | **Lap overflow**: the sweep reached a handle whose domain had no budget left. | verify |
| 43 | **A re-check** after a control pause or a dropped result. | verify |
| 50 | **A `did:web` document's** regular refresh. | verify |

A consumer's lookup queues a handle by the sweep's own test of being
[due](#the-weekly-sweep), so a handle the current lap has not reached yet
is checked when someone asks about it. A lookup does not queue the same
account again within 10 minutes. Claim changes are never dropped.
Consumer requests and events are each capped (10,000 waiting by
default); past the cap the request is still
answered from what lodestar holds, and the sweep covers the check.

The worker reads the queue in priority order, skipping domains whose
budget is full or that are paused, so one slow domain never holds up the
rest.

### The weekly sweep

Every handle is re-checked once a week. A **lap** reads the whole table
in two passes:

1. **Listed identities first**, read from `listed` in key order and
   looked up in `accounts`: the identities with a repo, which consumers
   are most likely to ask about.
2. **Everything still due**, read from `accounts` in storage order, the
   order rows sit on disk, which is a sequential read.

A handle is **due** when its identity is active, it has a claim that can
be resolved, it is not currently failing (failing handles go by their
retry schedule), and it has not been checked since the current lap
started. Measuring from a lap's start, not from a fixed age, means each
lap checks every handle once. A new lap starts no sooner than 7 days
after the last one started.

The sweep never waits for a domain that is out of budget, since that
would stall every other domain. A handle it cannot check at once goes
to the queue as lap overflow, and the sweep moves on.

### Large domains

One provider, `bsky.social`, holds almost all of the network's handles:
about 47 million, against about 160,000 for the next largest. Its handles
cannot go through the general sweep's overflow; there are too many. So a
**large domain** gets a walker of its own, which reads the table the same
way, checks only that domain's handles, and waits for its budget
instead of passing over them. Its lap lasts as long as its handles take
at its rate. Large domains are named in the configuration;
`bsky.social` is the default.

### Shared handles

About 50 million identities claim one of two handles. A handle claimed
by more identities than a threshold (100 by default) is a **shared
handle**: it is resolved once, and every claimant takes its status from
that one result. The DID it resolves to is `verified`; every other
claimant is `mismatch`, or `unresolved` if it resolves to nothing.

- **Finding them.** At each start, once the PLC source is live, the
  verification role counts claims per handle in a fixed array of
  one-byte counters, 64 MB, and keeps the count current from claim
  changes. The count can over-count, never under-count; a handle wrongly
  treated as shared costs one small row. The threshold cannot exceed
  254.
- **Checking them.** A shared handle's row is resolved when it has never
  been resolved, when a retry or confirmation is due, when it is a week
  old, or when an early refresh was asked for.
- **Early refresh.** A claim change by a claimant, or a consumer's
  refresh request for one, asks for the row to be resolved again, but
  no sooner than an hour after it was last resolved. A request inside
  that hour is kept and served when it ends, never dropped. Without this
  floor, a handle gaining a claimant every second would cost a request
  to its domain every second.
- **Claimants are not written.** The row's checks write only the DID it
  names as verified, which keeps the
  [verified-handle index](data-model.md#the-verified-handle-index) right.
  A live handle lookup, or an operator's `check-account`, verifies the
  one account it resolves to directly, as for any handle.

## Politeness

Lodestar asks other people's servers a great many small questions. These
rules keep it from being a burden.

### Rates

Every handle is its own hostname, so a limit per hostname would not
limit a provider whose users' handles are its subdomains. **Rates are
kept per registrable domain**, in checks a second.

| | `vps` (default) | `hobbyist` |
|---|---|---|
| Any domain, at first | 1 a second, 8 in flight | 1 a second, 4 in flight |
| A domain's adaptive ceiling | 20 a second | 5 a second |
| `bsky.social` | 320 a second, 100 in flight | 20 a second, 8 in flight |
| All domains together | 400 a second, 160 in flight | 40 a second, 24 in flight |

- **Adaptive rates.** A domain starts at the default rate. After a
  second's worth of quick, conclusive answers it climbs a step
  (about a quarter), up to the ceiling. On strain it halves, never below
  the default, and holds for a minute. Strain is an answer slower than
  2 seconds, any inconclusive result, a refusal, or a request to wait.
  Its in-flight cap grows with its rate.
- **Domains with limits of their own**, such as `bsky.social`, keep
  them and do not adapt. Their share of the global limits is set aside
  only while they are in use, so when their sweep is done the other
  domains share the whole.
- **No bursts.** A domain's budget holds at most one second of its
  rate, and starts empty after a restart.
- **The in-flight cap** counts a check from its first DNS query to its
  answer, so a domain that answers slowly or not at all cannot take
  every slot. It is also what makes lodestar back away from a provider
  that stalls: the slots fill within a fraction of a second and nothing
  new is sent until answers come back.
- **`Retry-After`** holds the whole domain for as long as asked, up to an
  hour, so no other request for it can jump the wait.
- **Rates are settings.** `LODESTAR_RATES` sets any domain's rate and
  cap.

The largest provider has been checked at 320 a second for hours at a
time from one address, with no refusal and no slowdown.

### DNS

A check makes one to three DNS queries: the TXT record, and if that
decides nothing, the IPv4 address, and the IPv6 address only if there
is no IPv4 one. At the default rates that is up to about 1,200 queries
a second. A public resolver typically serves one address up to about
1,500 a second and drops or fails the rest, and each failure is an
inconclusive check. The verification role warns at start-up when its
rates could pass 1,400. See [Deployment](deployment.md#dns) for
choosing a resolver.

## Pauses

Failures caused by lodestar's own network, or by one provider having a
bad day, must not unverify anyone. Three kinds of pause stop checks
before they can do harm. While a pause covers a check, the check is not
made: its queue entry stays, nothing it covers settles, and nothing it
covers is demoted.

### 1. Control checks: is the fault lodestar's own?

The operator lists **control handles**: handles known to resolve, each
with the DID it resolves to and the method it uses, DNS or HTTPS. There
must be at least two of each method, each on its own registrable
domain; lodestar refuses to start with fewer. HTTPS controls matter:
nearly every handle on the network is decided over HTTPS.

- Every minute, each control is checked by its own method, with one
  retry. That is a *round*. A control whose domain has asked lodestar to
  wait is not asked, and counts as its last result.
- A method is **failing** in a round when more than half of its counted
  controls failed. **When either method fails, all verification
  pauses**: no handle check of any kind, and no `did:web` fetch. Every
  lap waits where it is.
- It resumes after three rounds in a row in which neither method
  failed.
- **When a pause starts**, the results of the last 10 minutes are
  suspect, since the fault began before a round caught it. Their
  confirmation marks are cleared, the identities are queued to be
  checked again, results still in flight are dropped, and the domain
  samples below are cleared.
- **When a pause ends**, every failure clock moves forward by the
  pause's length, so lodestar's own outage is not charged to any handle.
  The domain pauses' holds stand still while it lasts.
- **A control that has gone bad**, failing every round for 24 hours
  while another control of its method mostly passed, is left out of
  the count until it passes again, and health reports it. A method is
  never counted on fewer than two controls.

A pause survives a restart.

This catches a resolver that is down or wrong, no route out, a broken
clock or certificate store, and a blocked port. It does not catch a
fault between lodestar and one provider only, such as that provider
refusing lodestar's address. That looks like the provider failing, which
is the next pause.

### 2. One domain is failing

For each domain, lodestar keeps a sample of its last 200 **first
attempts** from the last hour: checks of handles that are not already
failing. Retries and confirmations are not in it, so the broken handles
of a working domain cannot pause it by being checked together.

- **Start.** More than 60% of the sample failed, with at least 20 in it.
  An answer that arrives late but inside the time limit is an answer, so
  a short stall does not start a pause.
- **While paused**, no check is made for the domain except a **probe**:
  one check a minute after the pause starts, then every 10 minutes. The
  sweep sends the domain's due handles to the queue and moves on; a
  large domain's walker waits where it is.
- **End.** A probe with a conclusive answer ends the pause, and its
  result is applied. A consumer's lookup can be the probe; its answer
  counts the same way.
- **Hold.** If nothing conclusive has come back after 24 hours, the
  pause ends anyway and cannot start again until something does. The
  domain's queued handles that are not verified are then settled as
  `unreachable` at once, and the domain is held to the default rate
  until it answers conclusively. So a domain that is broken for good
  ends as `unreachable` instead of being retried for weeks.

### 3. One domain's verified handles are resolving to nothing

A second sample per domain: the last 200 checks, from the last hour, of
handles that were `verified`, counting only those that got an answer.

- **Start.** More than half of the sample found nothing, with at least
  20.
- **While paused**, checks go on and handles are still marked as awaiting
  confirmation, but no "nothing" result demotes a handle under the
  domain. A confirming check is probed every 10 minutes.
- **End.** The sample is refilled while the pause lasts, with no limit
  of an hour, and the pause ends when it holds at least 20 and no more
  than half found nothing:
  the opposite of the test that started it. One handle that resolves
  does not end it.
- **Hold.** After 24 hours the pause ends anyway, and the confirmations
  run and demote what still resolves to nothing.

This covers a provider whose endpoint answers 404 for longer than the
confirming delay, while one that has really shut down does not keep its
handles verified for ever.

**What it does not cover.** A `pending` handle has no confirmation step:
a "nothing" answer applies at once. During the first sweep every handle
is `pending`, so a provider wrongly answering 404 then would have its
handles written `unresolved` at that domain's rate, and nothing
automatic stops it. The next lap puts them right, provided the provider
is answering correctly by then.

`did:web` document fetches are in neither sample and held by neither
domain pause; each has its own backoff. The control pause covers them.

## Cadence

| | |
|---|---|
| Re-check interval | 7 days |
| Grace for a verified handle | 14 days |
| Settling time otherwise | 24 hours |
| Confirming delay | 1 hour |
| Shortest retry | 12 hours |
| Longest `Retry-After` honoured | 1 hour |
| Early refresh of a shared handle | at most once an hour (`LODESTAR_SHARED_REFRESH_MINUTES`) |
| Control round | every minute; 3 clear rounds to resume; a 10-minute lookback (`LODESTAR_LOOKBACK_SECONDS`) |
| Domain pauses | samples of 200 over an hour, at least 20; probes every 10 minutes; a 24-hour hold |
| `did:web` refresh | every 24 hours; `unresolvable` after 14 days failing |

The interval is a floor, not a promise: a domain's handles are re-checked
once per lap, and a lap over a domain takes as long as its handles take
at its rate. At the default rates every domain's lap fits inside a week,
the largest in under two days. Between laps, freshness comes from the
queue: a claim change or an identity event re-checks that handle at once.

Most of these values are fixed in 0.1; those with a setting are named.
