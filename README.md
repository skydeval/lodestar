# lodestar

A compact, read-only mirror of the PLC directory: the whole network in
under 30 GB. Lodestar keeps a copy of who every account on the ATProto
network is, so that services near it can ask it instead of asking
`plc.directory` and the network each time:

- where an account lives (its PDS),
- what key signs its data,
- what its handle is, checked both ways,
- how its handle and PDS have changed.

It mirrors the PLC directory for `did:plc` identities, finds
`did:web` accounts through Jetstream and a relay's listing, and
verifies every handle itself. It keeps each identity's current state
and history, not the signed operations, which is what keeps it
small; it never writes to the directory.

The read API is publicly accessible, behind a reverse proxy that
passes only the read paths (see
[Going public](docs/running.md#going-public)). There is no
authentication, and nothing public can change anything.

**Status:** 0.1.0, the first release. See `CHANGELOG.md`.

## What it needs

- Postgres 16.
- Under 30 GB of disk for the whole network as of October 2026, on an
  SSD, and room for it to grow. The reference host has 4 vCPUs, 8 GB
  of memory and a 75 GB disk.
- Outbound network access for two of its three roles.

On the reference host in October 2026, the first sync of the PLC
directory took 9 hours, and verifying every handle on the network
after it took 40 hours. See [docs/measurements.md](docs/measurements.md).

## Roles

One binary, three roles, each its own process:

| Role | Command | Talks to |
|---|---|---|
| Ingest | `lodestar ingest` | the PLC directory, a Jetstream instance, a relay |
| Verification | `lodestar verify` | any host a handle or `did:web` names |
| API | `lodestar api` | nothing outside; it serves consumers |

They coordinate only through the database.

## Quick start

Clone this repository, then:

```sh
cp .env.example .env    # then fill in its three required settings
docker compose up -d
```

[Running lodestar](docs/running.md) says what the three settings are,
including how to choose the control checks, and how to put it on your
own domain.

## Consumers

- [Farsight](https://github.com/skydeval/farsight), from its 0.7.0,
  reads DID resolution, verified handles and identity history from
  lodestar, and falls back to `plc.directory` for anything lodestar
  cannot answer. With lodestar in the same datacenter, its handle checks
  are about 10 times faster
  ([measurements](docs/measurements.md#serving-a-consumer)). Farsight
  documents its side in its
  [`docs/design/lodestar.md`](https://github.com/skydeval/farsight/blob/main/docs/design/lodestar.md).

## Documentation

| | |
|---|---|
| [Running lodestar](docs/running.md) | Setting up, control checks, going public, politeness |
| [Configuration](docs/configuration.md) | Every setting and its default |
| [API](docs/api.md) | The endpoints, and what their answers mean |
| [Measurements](docs/measurements.md) | How long it takes and how big it gets, on the reference host |
| [Design](docs/design/README.md) | How it works, and why |
| [Development](docs/development.md) | Building, testing and releasing |

## Licence

MIT or Apache-2.0, at your option. See `LICENSE-MIT` and
`LICENSE-APACHE`.
