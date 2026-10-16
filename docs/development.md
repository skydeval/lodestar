# Development

## Building

```sh
cargo build --release
cargo test
```

The tests that need Postgres run when `LODESTAR_TEST_DATABASE_URL`
names a database they may create tables in; without it they are
skipped.

## Releasing

Set the version in `Cargo.toml` and `compose.yaml`, give the version
its section in `CHANGELOG.md`, and push a tag `v<version>`. The
`image` workflow builds the image, pushes it, and creates the
release from that section of the changelog.
