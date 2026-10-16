# lodestar

A compact, read-only mirror of the PLC directory, small enough for a
75 GB VPS. Lodestar keeps a copy of who every account on the ATProto
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

**Status:** 0.1 is in progress. See `CHANGELOG.md`.

## What it needs

- Postgres 16.
- About 30 GB of disk for the whole network today, on an SSD, and
  room for it to grow. The reference host is a VPS with a 75 GB disk,
  8 GB of memory and 4 vCPUs.
- Outbound network access for two of its three roles.

On the reference host in October 2026, the first sync of the PLC
directory took 9 hours, and verifying every handle on the network
after it took 43 hours. See [docs/measurements.md](docs/measurements.md).

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
