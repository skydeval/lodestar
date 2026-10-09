# lodestar-recon

Throwaway tooling for the recon phase. It measures what the design
left as "recon to measure". It is not the implementation and does not
set the implementation's structure.

It is its own Cargo workspace, so the root package does not build it.

Nothing here is built or run on a development machine. Build and run
it on the recon host, against a Postgres that recon owns.

## Subcommands

- `init-db` — create the plain §4 schema and the recon bookkeeping
  tables.
- `plc-backfill` — page the PLC directory's sequenced `/export` from
  the committed cursor to the tip, applying each operation the way the
  design describes (§3.1, §3.1.1), and write a metrics line every few
  hundred pages.
