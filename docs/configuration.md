# Configuration

Everything except the three settings [running](running.md#setting-up)
names has a default that suits the reference host. Any setting can go
in `.env` by its name, or be passed to a role as a flag of the same name
in lower case (`LODESTAR_WALK_EVERY_DAYS` is `--walk-every-days`). A
list is comma-separated.

| Setting | Role | Default | What it does |
|---|---|---|---|
| `LODESTAR_DATABASE_URL` | all | set by `compose.yaml` | Postgres connection string |
| `LODESTAR_USER_AGENT` | all | none, required | sent on every outbound request |
| `LODESTAR_PLC_URL` | ingest | `https://plc.directory` | the PLC directory |
| `LODESTAR_PLC_PAGE_SIZE` | ingest | 1000 | operations asked for a page, at most 1,000 |
| `LODESTAR_PLC_MIN_INTERVAL_MS` | ingest | 250 | least time between export requests in the first sync |
| `LODESTAR_PLC_POLL_SECONDS` | ingest | 5 | how often the export is polled when there is no stream |
| `LODESTAR_PLC_TIP_CHECK_SECONDS` | ingest | 120 | how often the export confirms the tip while streaming |
| `LODESTAR_PLC_WINDOW_HOURS` | ingest | 84 | the recovery window with its margin |
| `LODESTAR_JETSTREAM_URL` | ingest | Bluesky's east and west instances | the Jetstream instances, each subscribed to at once |
| `LODESTAR_JETSTREAM_QUIET_SECONDS` | ingest | 300 | reconnect after this long without a message |
| `LODESTAR_JETSTREAM_EVENT_CAP` | ingest | 10000 | most event checks that may wait in the queue |
| `LODESTAR_RELAY_URL` | ingest | Bluesky's east and west relays | the relays a walk may use; it picks the fastest |
| `LODESTAR_WALK_MIN_INTERVAL_MS` | ingest | 250 | least time between listing requests |
| `LODESTAR_WALK_EVERY_DAYS` | ingest | 7 | how often the relay walk runs again |
| `LODESTAR_CONTROLS` | verify | none, required | the control checks |
| `LODESTAR_PROFILE` | verify | `vps` | starting rates: `vps` or `hobbyist` |
| `LODESTAR_RATES` | verify | none | fixed rates for named domains, as `domain=rate:in_flight` |
| `LODESTAR_RESOLVER` | verify | `system` | the DNS resolver: `system`, or addresses separated by commas, asked in turn when one fails |
| `LODESTAR_LARGE_DOMAINS` | verify | `bsky.social` | domains swept by a walker of their own |
| `LODESTAR_SHARED_THRESHOLD` | verify | 100 | a handle claimed by more identities is resolved once for all |
| `LODESTAR_SHARED_REFRESH_MINUTES` | verify | 60 | least time between early refreshes of a shared handle |
| `LODESTAR_LAP_CHUNK_BLOCKS` | verify | 500 | how much of `accounts` a lap reads at a time |
| `LODESTAR_LOOKBACK_SECONDS` | verify | 600 | results this recent are checked again after a control pause |
| `LODESTAR_VERIFY_CONNECTIONS` | verify | 8 | database connections for applying results |
| `LODESTAR_LISTEN` | api | `127.0.0.1:8080` | where the API listens inside its container |
| `LODESTAR_API_CONNECTIONS` | api | 4 | database connections held |
| `LODESTAR_PLC_FRESH_SECONDS` | api | 300 | the PLC source counts as fresh this long after a tip confirmation |
| `LODESTAR_MAX_BATCH` | api | 1000 | most DIDs in one batch lookup |
| `LODESTAR_LIVE_DEADLINE_SECONDS` | api | 5 | how long a lookup waits on a live check |
| `LODESTAR_CONSUMER_QUEUE_CAP` | api | 10000 | most consumer-requested checks waiting, per kind |
| `LODESTAR_STATUS_LISTEN` | api | `0.0.0.0:8090` in compose, off otherwise | where the status page listens inside its container; empty turns it off |
| `LODESTAR_STATUS_TOTALS_MINUTES` | api | 60 | how often the status page's totals are counted; each count reads all of `accounts` |

`LODESTAR_API_BIND`, `LODESTAR_STATUS_BIND` and `POSTGRES_PASSWORD`
are read by `compose.yaml` itself, as are the bundled Caddy's two
settings: `LODESTAR_DOMAIN`, the domain it serves with HTTPS (plain
HTTP on port 80 if unset), and `LODESTAR_PROXY=0`, which turns it off.

A resolver at one address is asked about three queries per check at
the most, so the default global rate of 400 checks a second can
reach 1,200 a second. Public resolvers serve one address about
1,500 a second and drop the rest; verification warns at start-up
when its rates could pass 1,400. Keep `system` unless you have a
reason not to: a public resolver asked directly can stall under
hours of load at rates it handles in a short test (see
[Deployment](design/deployment.md#dns)).
