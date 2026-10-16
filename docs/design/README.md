# Lodestar design

These documents describe how lodestar works and why it is built the way
it is. They describe the code as it stands in the 0.1 series; where the
code and these pages disagree, the code is right and the page is a bug.

For running lodestar, start with the [README](../../README.md). For how
long things take on real hardware, see [measurements](../measurements.md).

## What lodestar is

Lodestar is a **compact, read-only mirror of the PLC directory** for the
ATProto network. It keeps a copy of every identity the directory holds,
small enough to fit on a 75 GB server, and serves it to the services
near it, so that they can ask lodestar instead of asking
`plc.directory` and the network each time.

It answers four questions about any account, quickly:

1. **Where does this account live?** Its current PDS.
2. **What key signs its data?** Its current atproto signing key.
3. **What is its handle, really?** A handle the account claims *and*
   that resolves back to the account.
4. **What has it been called, and where has it lived?** Its history of
   handle and PDS changes.

It also answers the directory's own DID document read, `GET /{did}`, in
the directory's shape, so a service that reads DID documents from
`plc.directory` can read them from lodestar by changing one URL.

It does this for `did:plc` and `did:web` alike, for every identity its
sources show, without fetching anything on first use.

## What it leaves out

Lodestar keeps the *result* of each identity's operations, not the
signed operations themselves. It stores no raw operations, signatures
or rotation keys. That is most of the directory's size, and leaving it
out is what lets lodestar run on a small server. So it does not serve
the directory's log endpoints (`/log`, `/log/audit`, `/data`,
`/export`), and it cannot be used to audit an identity's operation
chain.

It never writes to the directory, and it does not track whether an
account's host considers it active. Consumers read that from their own
firehose.

## Principles

- **Read-only.** Nothing lodestar serves can change anything.
- **Every identity, up front.** Every identity the directory holds is
  indexed before anyone asks, including the tens of millions that are
  not accounts in the ordinary sense. A `did:web` no source has shown
  is resolved on its first request and kept from then on.
- **Handles are verified, not trusted.** An identity's document only
  *claims* a handle. Lodestar resolves every claimed handle itself and
  reports it as the account's handle only when it resolves back.
- **History for everyone.** Handle and PDS history is kept for every
  identity, not only for those someone has looked up.
- **Small first.** The same design runs on a 75 GB VPS and on a much
  larger server. Everything that depends on scale is a setting, with
  defaults that work at the small end.
- **A good neighbour.** Lodestar asks other people's servers a great
  many small questions, and it limits itself, backs away from servers
  that struggle, and stops altogether when its own network looks
  broken.

## Vocabulary

| Term | Meaning |
|---|---|
| Identity | An entry in the PLC directory, or a known `did:web`. Anyone can register one, so most are not accounts in the ordinary sense. |
| Account | An identity a host serves. |
| Claim | The handle an identity's document names. A claim is not a handle until it is verified. |
| Listed | An identity a relay lists as having a repo. Used only to decide what to check first. |
| Registrable domain | A public suffix plus one label, such as `bsky.social` or `example.com`. Rates are kept per registrable domain. |
| Lap | One pass of the background sweep that re-checks every handle, once a week. |
| Large domain | A domain whose handles get a walker of their own, paced at its own rate. `bsky.social` by default. |
| Shared handle | A handle claimed by more identities than a threshold (100 by default), resolved once for all of them. |

## The documents

| Document | Covers |
|---|---|
| [Architecture](architecture.md) | The three roles, how they coordinate, start-up order, packaging |
| [Sources](sources.md) | The PLC directory, Jetstream, the relay walk, `did:web` documents, outbound safety |
| [Data model](data-model.md) | Tables, the compact encodings, history, the verified-handle index |
| [Handle verification](handle-verification.md) | Handle states, how a check works, when checks happen, politeness, pauses |
| [API](api.md) | Endpoints, answers and errors, freshness, the DID document read, the status page |
| [Deployment](deployment.md) | Hardware, disk and transfer budgets, resolvers, maintenance, going public |
| [Decisions](decisions.md) | Why the main choices were made, what is out of scope, known limitations |
