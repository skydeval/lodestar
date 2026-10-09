#!/usr/bin/env bash
# Start a local caching recursive resolver for the DNS comparison.
# Listens on 127.0.0.1:5335 only.
set -euo pipefail
DATA="${RECON_DATA:-$HOME/lodestar-recon-data}"
mkdir -p "$DATA/unbound"
cat > "$DATA/unbound/unbound.conf" <<'EOF'
server:
  interface: 0.0.0.0
  port: 53
  access-control: 0.0.0.0/0 allow
  do-ip6: no
  username: ""
  chroot: ""
  logfile: ""
  use-syslog: no
  num-threads: 2
  msg-cache-size: 64m
  rrset-cache-size: 128m
  hide-identity: yes
  hide-version: yes
EOF
docker run -d --name lodestar-recon-unbound --restart unless-stopped \
    -p 127.0.0.1:5335:53/udp -p 127.0.0.1:5335:53/tcp \
    -v "$DATA/unbound/unbound.conf:/etc/unbound/unbound.conf:ro" \
    alpine:3 sh -c "apk add --no-cache unbound >/dev/null && exec unbound -d -c /etc/unbound/unbound.conf"
