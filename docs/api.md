# API

| Endpoint | Answers |
|---|---|
| `GET /account/{did}` | PDS, signing key, handle and its status, account status |
| `GET /account/{did}/history` | handle-claim and PDS changes, newest first |
| `POST /accounts` | the same as the first, for up to 1,000 DIDs: `{"dids": [...]}` |
| `GET /handle/{handle}` | the DID that holds a verified handle |
| `GET /{did}` | a `did:plc`'s DID document, as `plc.directory` gives it |
| `POST /account/{did}/refresh` | re-fetches a `did:web` document and re-checks the handle |
| `GET /ops/health` | sync position, verification, storage |
| `GET /ops/metrics` | the same, in Prometheus format |

The status page is on an address of its own and serves only
`GET /status/` and its figures at `GET /status/data.json`. It names
nothing private.

A handle is reported as an account's `handle` only when lodestar
has verified it: the account claims the handle, and the handle
resolves back to the account. Otherwise it appears as
`claimed_handle`, with a `handle_status` saying why.

Every account answer carries the PLC source's position, so a
consumer can tell how fresh it is. An identity lodestar does not
have is `not_found` only when the source is at the directory's tip;
before that it is `syncing`.

The full reference, with every answer, error word and status code, is
in [the design's API page](design/api.md).
