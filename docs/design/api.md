# API

HTTP and JSON, served by the API role. The reads are meant to be
public, behind a proxy that passes only them
([Deployment](deployment.md#going-public)). Nothing the API serves can
change anything, and there is no authentication.

| Endpoint | Public | Answers |
|---|---|---|
| `GET /account/{did}` | yes | An account's current state |
| `GET /account/{did}/history` | yes | Its handle and PDS changes, newest first |
| `POST /accounts` | yes | The same as the first, for up to 1,000 DIDs |
| `GET /handle/{handle}` | yes | The DID that holds a verified handle |
| `GET /{did}` | yes | A `did:plc`'s DID document, as `plc.directory` gives it |
| `POST /account/{did}/refresh` | no | Re-checks an account now |
| `GET /ops/health` | no | Sync position, verification, storage |
| `GET /ops/metrics` | no | The same, in Prometheus format |

The refresh request makes lodestar contact other servers on demand, and
the ops endpoints name the control handles and internals, so the
proxy rules keep them private.

## Freshness

Every account answer carries the PLC source's position as `source`, so
a consumer can judge how current it is:

```json
"source": {
  "plc": {
    "phase": "live",
    "cursor": 130148076,
    "tip_confirmed_at": "2026-10-16T07:07:32.434Z",
    "last_operation_created_at": "2026-10-16T07:06:02.327Z",
    "fresh": true
  }
}
```

- `phase` is `backfill` during the first sync and `live` after it.
- `tip_confirmed_at` is when the source last confirmed it was at the
  directory's tip ([Sources](sources.md#following-the-stream)). This is
  the measure of how far behind lodestar is.
- `fresh` is true while that confirmation is recent, within 5 minutes
  by default.
- `last_operation_created_at` is context only: on a quiet directory it
  can be old while lodestar is exactly at the tip.

Lodestar answers a `did:plc` it does not have with `not_found` only when
it is `live` and `fresh`. Otherwise the identity may simply not have
arrived yet, and the answer is `syncing`.

## Errors

Errors are `{"error": "<word>"}`, with `source` where it is known. The
word is what a consumer switches on.

| Word | Status | Means |
|---|---|---|
| `invalid_did` | 400 | Not a valid `did:plc` or `did:web`. |
| `invalid_handle` | 400 | Not a valid handle. |
| `batch_too_large` | 400 | More DIDs than the batch limit. |
| `not_found` | 404 | Lodestar is live and fresh and does not have it, or the handle is not verified for anyone. |
| `not_an_account` | 404 | A `did:web` whose document declares no atproto PDS. |
| `syncing` | 503 | Lodestar is not at the directory's tip yet, so it cannot say. |
| `unavailable` | 503 | A live resolution failed or did not finish within its deadline, or verification is paused. Try again later. |
| `internal` | 500 | Something went wrong in lodestar. |
| `unknown` | in a batch | A `did:web` lodestar has not seen; a batch never fetches one. |

`GET /{did}` uses the directory's own error shape instead
([below](#get-did)). A request body that is not valid JSON is refused
by the HTTP framework with its own plain error.

## `GET /account/{did}`

```json
{
  "did": "did:plc:z72i7hdynmk6r22z27h6tvur",
  "status": "active",
  "pds": "https://puffball.us-east.host.bsky.network",
  "signing_key": "zQ3shQo6TF2moaqMTrUZEM1jeuYRQXeHEx4evX9751y2qPqRA",
  "handle": "bsky.app",
  "claimed_handle": "bsky.app",
  "handle_status": "verified",
  "handle_checked_at": "2026-10-13T08:00:36.659Z",
  "created_at": "2023-04-12T04:53:57.057Z",
  "created_at_kind": "plc_genesis",
  "source": { "plc": { … } }
}
```

- `status` is `active`, `tombstoned` or `unresolvable`
  ([Data model](data-model.md#accounts-one-row-per-identity)). A
  tombstoned account is a normal answer.
- `signing_key` is a Multikey multibase string, whatever form the
  source used.
- **`handle` is present only when the handle is `verified`.**
  `claimed_handle` is present whenever the document claims a handle,
  with `handle_status` beside it. A claim is not a handle: for any other
  status it must never be shown as the account's handle.
- `created_at_kind` is `plc_genesis` for a `did:plc` and `first_seen`
  for a `did:web`.
- For a claimant of a [shared handle](handle-verification.md#shared-handles),
  the status and check time come from the shared handle's row.

**A `did:web` lodestar has not seen** is fetched live, on this request,
within a deadline (5 seconds by default). If its document declares an
atproto PDS it is stored and returned. If not, the answer is
`not_an_account` and nothing is stored. A miss is remembered for a
minute, so repeated requests do not repeat the fetch.

**A handle still `pending`** is queued to be checked ahead of the sweep
([Handle verification](handle-verification.md#the-queue)).

## `GET /account/{did}/history`

Handle-claim and PDS changes, newest first, with the account's status.

```json
{
  "did": "did:plc:z72i7hdynmk6r22z27h6tvur",
  "status": "active",
  "entries": [
    {"changed_at": "2023-11-09T21:49:10.793Z", "handle_claim": "bsky.app",
     "pds": "https://puffball.us-east.host.bsky.network"},
    {"changed_at": "2023-04-12T17:26:46.468Z", "handle_claim": "bsky.app",
     "pds": "https://bsky.social"}
  ],
  "next_cursor": 1,
  "note": "handle_claim values are claims, not verified handles",
  "source": { "plc": { … } }
}
```

- `?limit=` sets the page size: 50 by default, at most 200.
  `?cursor=` takes the previous page's `next_cursor`.
- An active identity that has never changed has one entry, built from
  its account row and dated from its creation. A tombstoned or
  unresolvable identity with no changes has none.
- Entries are claims, not verified handles, and the answer says so.
- A recovery removes the entries of the operations it undid
  ([Sources](sources.md#recoveries)).

## `POST /accounts`

A batch lookup, for consumers that resolve pages of accounts at a time.

```json
{"dids": ["did:plc:…", "did:web:…"]}
```

```json
{"accounts": {"did:plc:…": { …account… }, "did:web:…": {"error": "unknown"}},
 "source": { "plc": { … } }}
```

`accounts` maps each DID to its account object, as above, or to
`{"error": "<word>"}`, with one `source` for the whole batch. More than
1,000 DIDs (by default) is `batch_too_large`. A batch never waits on a
live fetch: a `did:web` lodestar has not seen is answered `unknown`.
Accounts whose handles were never checked are queued, up to a cap.

## `GET /handle/{handle}`

The DID that holds a verified handle.

```json
{"handle": "bsky.app", "did": "did:plc:z72i7hdynmk6r22z27h6tvur",
 "checked_at": "2026-10-13T08:00:36.659Z"}
```

The handle is lowercased first. A handle that can never resolve
([Handle verification](handle-verification.md#handles-that-are-never-resolved))
is `not_found` with no request.

If no account holds the handle as verified and lodestar is not yet
live, the answer is `syncing` at once. A
[shared handle](handle-verification.md#shared-handles) resolved in the
last week is `not_found` from its row, with no new resolution: none of
its claimants holds it. Otherwise lodestar resolves the handle now,
within the deadline. If it resolves to an active account that claims it,
that is a two-way verification: it is recorded and returned. If it
resolves to a `did:plc` lodestar does not have yet, or has with another
claim, while lodestar is not live, the answer is `syncing`. Otherwise it
is `not_found`, or `unavailable` if the resolution was inconclusive.
Misses are remembered for a minute.

## `GET /{did}`

The PLC directory's DID document read, answered from what lodestar
stores, in the directory's shape and with its content type,
`application/did+ld+json`. A service that reads DID documents from
`plc.directory` can read them from lodestar by changing one URL.

```json
{
  "@context": [
    "https://www.w3.org/ns/did/v1",
    "https://w3id.org/security/multikey/v1",
    "https://w3id.org/security/suites/secp256k1-2019/v1"
  ],
  "id": "did:plc:z72i7hdynmk6r22z27h6tvur",
  "alsoKnownAs": ["at://bsky.app"],
  "verificationMethod": [{
    "id": "did:plc:z72i7hdynmk6r22z27h6tvur#atproto",
    "type": "Multikey",
    "controller": "did:plc:z72i7hdynmk6r22z27h6tvur",
    "publicKeyMultibase": "zQ3shQo6TF2moaqMTrUZEM1jeuYRQXeHEx4evX9751y2qPqRA"
  }],
  "service": [{
    "id": "#atproto_pds",
    "type": "AtprotoPersonalDataServer",
    "serviceEndpoint": "https://puffball.us-east.host.bsky.network"
  }]
}
```

- The document is built the way the directory builds it: each key adds
  its suite's context (`secp256k1-2019` or `ecdsa-2019`), each
  verification method is a `Multikey`, each service is listed with its
  `#id`. Identities whose documents need more than the stored claim, PDS
  and key, such as labelers, are answered from the parts lodestar keeps
  for them ([Sources](sources.md#what-is-read-from-an-operation)). An
  identity with no repo whose document lists extra `alsoKnownAs`
  entries is answered without them.
- `alsoKnownAs` is the document's own list, which is a claim. Whether
  the handle is verified is a question for `/account/{did}`.
- Object keys come in alphabetical order, and a document with several
  keys or services lists them by name; the directory keeps the order of
  the operation. The content is the same.
- Errors use the directory's form, `{"message": "…"}`: 404 `DID not
  registered` for one lodestar does not have, 404 `DID not available` for
  a tombstoned one, 400 for anything that is not a `did:plc`. A DID
  lodestar does not have while it is not yet at the directory's tip is
  503 instead of 404; one it does have is served as usual.

The directory's log endpoints (`/log`, `/log/audit`, `/data`, `/export`)
are not served: lodestar does not keep the signed operations they return.

## `POST /account/{did}/refresh`

A consumer's signal that what it holds looks stale, for example after a
signature or repo request failed. For a `did:web` the document is
re-fetched now, within the deadline. For any active account with a
claim, the handle is queued to be re-checked ahead of the sweep. The
answer is the account as it now stands, with:

```json
"refresh": {"check_queued": true, "document": "stored"}
```

`document` is the fetch's outcome for a `did:web` (`stored`,
`not_an_account` or `unavailable`) and `null` for a `did:plc`, whose
state comes only from the directory. A `did:plc` caller can compare
`source.plc.cursor` with the change it expects.

Not public by default, because it makes lodestar contact other servers
on demand.

## Live resolution

When a read needs something lodestar does not hold, the API role writes
a request to the database, the verification role does the work under
its usual limits and safety rules, and the API waits up to the deadline,
5 seconds by default. While a pause covers the work, the answer is
`unavailable` at once.

## Not offered

- A stream of changes, or a signal that a re-check has finished. A
  consumer's own firehose shows a handle change before lodestar has
  re-checked it; the refresh request and `handle_checked_at` cover that
  gap, which is about a quarter of a second when the check runs at once.
- An endpoint that lists every DID.
- Handle prefix search.
- Point-in-time queries.

## Ops

`GET /ops/health` reports, as JSON:

- the version;
- the PLC source's position and freshness;
- verification: the worker's own report (checks by result, in flight,
  domains climbed, paused or at their cap), the queue by kind and
  reason, each lap's pass and position, the pauses, each control check
  and whether it passed, the shared handles, and how many checks are
  failing or awaiting confirmation;
- storage: each table's size, the combined size of its indexes, and its
  row estimate.

`GET /ops/metrics` serves the same in Prometheus format, including each
Jetstream subscription's position and how long since it confirmed the
tip.

## The status page

An optional read-only page at `/status/`, with its figures at
`/status/data.json`, served by the API role on an address of its own
when `LODESTAR_STATUS_LISTEN` is set, as it is by default in compose. It
shows the copy of the directory,
the handle totals, the weekly sweep, verification and the sources.

It is built to be public:

- **No names.** Control handles, paused domains and the domains running
  faster than the default appear only as counts.
- **Flat load.** Its figures are read at most once every 30 seconds and
  every viewer gets that reading, so the database's work does not grow
  with the page's audience.
- **Totals in the background.** The handle totals need a pass over every
  account, so they are counted hourly by default
  (`LODESTAR_STATUS_TOTALS_MINUTES`), never for a request. Claimants of a
  shared handle are counted apart, since their own rows never record a
  check.
