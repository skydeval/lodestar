# Running lodestar

## Setting up

Clone the repository (you need `compose.yaml`, `Caddyfile` and
`.env.example` from it), then:

```sh
cp .env.example .env    # then fill it in
docker compose up -d
```

That pulls the released image, `ghcr.io/skydeval/lodestar`, at the
version `compose.yaml` names, and starts Postgres, the three roles and
Caddy. To build the image from the checkout instead, run
`docker compose build` first.

`.env` needs three things:

- `POSTGRES_PASSWORD`: a long random one, such as the output of
  `openssl rand -hex 24`.
- `LODESTAR_USER_AGENT`, sent on every outbound request. Name
  yourself and a way to reach you.
- `LODESTAR_CONTROLS`, the control checks. See below.

Every other setting has a default that suits the reference host; see
[configuration](configuration.md).

The API and the status page listen on the host's loopback
(`127.0.0.1:8080` and `:8090`); the public reaches them through Caddy
on port 80, or 443 once you give it a domain (see
[Going public](#going-public)).

## Control checks

Lodestar resolves a few well-known handles every minute, purely as a
test of its own network: that its DNS resolver answers, that it can
open HTTPS connections, that its clock and certificates are sound. If
most of them suddenly fail, the fault is almost certainly Lodestar's
own, not the handles', so it stops verifying until they pass again,
instead of marking thousands of real handles broken because of its own
outage.

So control handles are canaries, nothing more:

- **They don't need to be yours.** Any handles that resolve reliably
  will do: someone's long-standing handle, a project's account. Lodestar
  only reads them, once a minute, the same way it reads any handle.
- **You need at least two of each kind**, each on its own domain:
  two that resolve by **DNS** (a `_atproto` TXT record) and two that
  resolve by **HTTPS** (`/.well-known/atproto-did`). Each kind tests a
  different path, and Lodestar won't start without them.
- **Pick stable ones.** A control that has failed for a day while the
  others pass is left out of the count and reported in health, so a
  handle that changes hands doesn't stop verification. But it is better
  replaced.

To check a handle and find its DID and kind:

```sh
dig +short TXT _atproto.example.com
# "did=did:plc:…"  →  resolves by DNS

curl -s https://example.com/.well-known/atproto-did
# did:plc:…        →  resolves by HTTPS (only if the DNS record is missing)
```

A handle with a DNS record counts as DNS. Write each as
`dns:handle=did` or `https:handle=did`, comma-separated:

```
LODESTAR_CONTROLS=dns:alice.example.com=did:plc:aaaa…,dns:bob.example.org=did:plc:bbbb…,https:carol.example.net=did:plc:cccc…,https:dave.example.io=did:plc:dddd…
```

## Going public

The API and status page listen on the host's loopback. The public
reaches them through a proxy that passes only the status page and
the reads (see [API](api.md)); `/ops/*` and the refresh request stay
unreachable. [Caddyfile](../Caddyfile) holds those rules, and compose
runs Caddy with them by default: on plain HTTP port 80 until you give it
a domain.

**For HTTPS on your own domain:**

1. Point the domain at the host (A, and AAAA for IPv6). Behind
   Cloudflare's proxy, either use "DNS only" or give Caddy a
   certificate of your own (below).
2. Open ports 80 and 443.
3. In `.env`: `LODESTAR_DOMAIN=lodestar.example.com`.
4. `docker compose up -d`. Caddy fetches its certificate within a
   minute and renews it itself.

**To use a certificate of your own** (a Cloudflare Origin
certificate, with Cloudflare's SSL mode set to Full), put it in
`certs/` beside `compose.yaml`, with a file that tells Caddy to use
it. Nothing tracked is edited, so `git pull` stays clean:

```sh
mkdir -p certs
cp origin.pem certs/lodestar.pem
cp origin.key certs/lodestar.key
chmod 600 certs/lodestar.key
echo 'tls /etc/caddy/certs/lodestar.pem /etc/caddy/certs/lodestar.key' > certs/tls.caddy
docker compose restart caddy
```

Caddy reads every `certs/*.caddy` file into the site, and without one
logs `No files matching import glob pattern` and goes on.

**On a host that already runs a proxy** (one shared with a PDS, say),
set `LODESTAR_PROXY=0` in `.env` to leave the bundled one off, and copy
Caddyfile's rules into yours, with `127.0.0.1:8080` and
`127.0.0.1:8090` in place of `api:8080` and `api:8090`.

## Upgrading

```sh
git pull
docker compose pull
docker compose up -d
```

`compose.yaml` names the image version, so pulling the checkout is
what moves you to a new release; read the [changelog](../CHANGELOG.md)
first. The database is kept across upgrades; each role brings
its schema up to date when it starts. Your `.env` and `certs/` are not
tracked, so a pull leaves them alone. If you changed `compose.yaml` or
`Caddyfile` yourself, `git stash` before the pull and `git stash pop`
after.

## Politeness

Lodestar asks other people's servers a great many small questions.
It limits itself per registrable domain (one check a second by
default), backs away from a host that stalls or asks it to wait, and
stops verifying altogether when its own network looks broken. Rates
are config: `LODESTAR_PROFILE=hobbyist` lowers them, and
`LODESTAR_RATES` sets one domain's.
