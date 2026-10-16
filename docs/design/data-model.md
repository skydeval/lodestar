# Data model

Lodestar keeps everything in Postgres. Disk is the binding constraint
at the small end, so the encodings below are not an option or an
optimisation pass: they are the schema. With them the whole network
fits in about 28 GB; with plain text and one history row per identity,
the same data took 58 GiB.

Columns in each table are ordered widest first, so rows carry no
alignment padding.

## Tables

### `accounts`: one row per identity

| Column | |
|---|---|
| `did` | The DID, binary (below). Primary key. |
| `method` | `plc` or `web`. |
| `status` | `active`, `tombstoned` (PLC), or `unresolvable` (a `did:web` whose document cannot be fetched). |
| `pds_id` | The PDS, as an id into `pds_endpoints`. Nullable. |
| `signing_key` | The atproto signing key, binary. Nullable. |
| `handle_claim` | The handle the identity's document claims, lowercased. Nullable. |
| `handle_status` | `verified`, `mismatch`, `unresolved`, `unreachable`, `pending` or `none` ([Handle verification](handle-verification.md#handle-states)). |
| `handle_checked_at` | When a check last set or confirmed `handle_status`. |
| `created_at` | The first PLC operation's `createdAt`, or when a `did:web` was first seen. |

About 180 bytes an identity, including its key. There is no
`updated_at`: no consumer reads it, and it would cost about a gigabyte.

### The verified-handle index

Handle-to-DID lookups use a partial unique index on
`accounts.handle_claim`, covering only rows whose status is `verified`,
not a table of its own, which would repeat every handle. Three rules
keep it to one holder per handle:

- when an identity's claim changes, its status leaves `verified` in the
  same transaction;
- when a handle is verified for one identity while another holds it,
  the other becomes `mismatch` in the same transaction: the handle
  resolved elsewhere, which is a conclusive answer for it too;
- a resolution names at most one DID.

### `identity_history`: handle and PDS changes

History is kept for every identity, but rows are written **only for
identities whose claim or PDS has changed**. That holds it in about a
tenth of the space, since nearly every identity has never changed.

| Column | |
|---|---|
| `did`, `seq` | The identity, and the row's order within it. Primary key. |
| `changed_at` | The operation's `createdAt` for PLC, the time it was observed for `did:web`. |
| `handle_claim`, `pds_id` | The state from that time. |

See [History](#history) for when rows are written.

### `listed`: identities with a repo

One row per DID a relay lists, or an account event has shown as active.
It is used only to decide what the sweep checks first. It is not hosting
status and is never cleared. It is a table of its own rather than a
column of `accounts`, so marking 20 million identities is 20 million
small inserts, not 20 million rewritten account rows.

### `pds_endpoints`

Every distinct PDS endpoint, once, with an integer id. About 21,000
endpoints serve about 123 million identities. Endpoints can be any
length, so they are unique by their MD5.

### `doc_parts`: DID documents that need more than the columns

For an identity whose DID document cannot be rebuilt exactly from its
claim, PDS and key, such as a labeler with a second key and service,
the document's `alsoKnownAs`, `verificationMethods` and `services` as
the directory gave them, as JSON text. Few identities have a row. See
[Sources](sources.md#what-is-read-from-an-operation).

### `plc_recent_ops`: the recovery window

Every operation from inside the recovery window: its DID, `seq`, CID,
`createdAt`, and the history row it produced, if any. It is what lets a
recovery remove the history of the operations it nullifies
([Sources](sources.md#recoveries)). Pruned continuously; under a million
rows in normal running.

### `shared_handles`

One row for each handle claimed by more identities than the threshold:
the DID its last resolution named, the outcome, when it was checked, its
own failure state, and when it was last refreshed. **Claimants of a
shared handle take their status from this row**, and the row's checks
never write their account rows. Two handles hold about 50 million claims
between them, so this table is tiny and saves that many checks. See
[Handle verification](handle-verification.md#shared-handles).

### Verification state

| Table | Holds |
|---|---|
| `verify_due` | Work to do ahead of the sweep: one row per DID and kind (a handle check or a document fetch), with a reason that sets its priority, a due time, and the handle's registrable domain. Writing a row that exists keeps the higher priority and the earlier time. |
| `check_failures` | One row per DID and kind while a check is failing or awaiting confirmation: when it first failed, when it was last tried, when it is next due, and when it first found nothing. Healthy handles have no row. |
| `web_documents` | For each known `did:web`: when it was last fetched, and its first and last successful fetch. |
| `control_state` | For each control check: since when it has failed every round, how many of those rounds another control passed, and whether it is left out. |
| `resolve_requests` | Live lookups the API is waiting on, and their answers. Short-lived. |
| `role_status` | The verification role's own report, for health. |

### `sync_state`

One row for each source, walk, lap and pause: its position, its phase,
and the times it needs, such as when a source last confirmed the tip
or when a lap started. A position is committed in the same transaction
as the data it covers.

## Encodings

| What | How | Saves |
|---|---|---|
| DIDs | A `did:plc` is one tag byte and its 15 decoded bytes, 16 in all. Any other DID is a different tag byte and its text. | about 12 GiB, two thirds of it in primary keys |
| PDS endpoints | Interned in `pds_endpoints` and referred to by id. | about 7 GiB |
| Signing keys | A `did:key:z…` is stored as its decoded, multicodec-prefixed key bytes. A key that is not a well-formed `did:key` is kept as text behind a tag byte no key starts with. | about 2.5 GiB |
| CIDs | Decoded to bytes from base32 or base58btc, so they compare by value. | |
| History | Only for identities that changed. | about 14 GiB |
| Handle lookup | A partial index, not a table. | about 3 GiB |
| Statuses | Small integers. | |

Every encoding is lossless. Two views, `accounts_readable` and
`identity_history_readable`, show DIDs, endpoints, keys and statuses as
text, for an operator looking at the database by hand, using the SQL
functions `lodestar_did()` and `lodestar_key()`.

The status codes:

| Column | Codes |
|---|---|
| `method` | 0 `plc`, 1 `web` |
| `status` | 0 `active`, 1 `tombstoned`, 2 `unresolvable` |
| `handle_status` | 0 `verified`, 1 `mismatch`, 2 `unresolved`, 3 `unreachable`, 4 `pending`, 5 `none` |

## History

For a `did:plc`:

- **An identity that has never changed has no rows.** Its history is one
  entry, read from `accounts`: its current claim and PDS since
  `created_at`. About 97% of identities are like this.
- **On its first change**, two rows are written. The first is the
  *original-state row*: the claim and PDS it had until now, dated
  `created_at`. The second is the new state. Only the second is
  credited to the operation.
- **Each later change** writes one row.
- **A change of key alone** writes nothing: history holds claims and
  PDSes, not keys.
- **A tombstone** writes no row of its own, but an identity with no rows
  gets its original-state row first, because the tombstone clears the
  account row that would otherwise have been its history.
- **A recovery** deletes the rows its nullified operations produced. The
  original-state row is credited to no operation, so it survives and is
  the state the recovery is compared against. An identity left with only
  an original-state row that equals its current state goes back to
  having no rows.

For a `did:web`, rows are written when a successful fetch finds the
claim or PDS changed since the last one. The original-state row is dated
from its first successful fetch, and new rows from the time of the
fetch.

History holds claims, not verified handles, and the API says so.

## Indexes

Beyond the primary keys, there are only:

- the verified-handle index (above);
- an index on `check_failures` by next attempt, for the timer that
  retries failing checks;
- an index on `verify_due` in the order the verification role reads it;
- the MD5 index on `pds_endpoints`.

Nothing indexes `handle_claim` beyond the verified rows, and nothing
indexes a next-check time: the sweep reads `accounts` sequentially and
does not need one.

The `accounts` primary key is rebuilt when the first sync reaches the
tip, and both `accounts` indexes are rebuilt whenever the weekly laps
have grown them half again past their size after a rebuild
([Deployment](deployment.md#maintenance)).

## Size

On the reference host in October 2026, for about 123 million
identities:

| | On disk |
|---|---|
| After the first sync and first verification | about 28 GB |
| `accounts` with its indexes | about 26 GB of that |
| Peak during the first sync and sweep | about 32 GiB |

The directory grows by roughly 100,000 to 250,000 identities a day,
driven mostly by bulk registration, which is about 1 GiB a month in this
schema. See [Deployment](deployment.md#disk) for what that means for a
disk.
