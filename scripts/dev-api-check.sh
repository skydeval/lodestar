#!/usr/bin/env bash
# Exercise the API role on the build host. Expects it on 127.0.0.1:$1.
#   dev-api-check.sh <port> <did with history> <did without>
set -uo pipefail
base="http://127.0.0.1:$1"
with="$2"
without="$3"
show() {
    echo "--- $1"
    shift
    curl -sS -m 10 -w '\n[%{http_code}] %{time_total}s\n' "$@"
}
show "account" "$base/account/$with"
show "account, never changed" "$base/account/$without"
show "history" "$base/account/$with/history?limit=2"
show "history, never changed" "$base/account/$without/history"
show "account not in the table" "$base/account/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
show "bad did" "$base/account/did:plc:nope"
show "unsupported method" "$base/account/did:key:zQ3sh"
show "unseen did:web" "$base/account/did:web:example.com"
show "batch" -X POST -H 'content-type: application/json' \
    -d "{\"dids\":[\"$with\",\"$without\",\"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa\",\"did:web:example.com\",\"junk\"]}" "$base/accounts"
show "handle not in the index" "$base/handle/alice.bsky.social"
show "handle that never resolves" "$base/handle/me.test"
show "bad handle" "$base/handle/nodot"
show "health" "$base/ops/health"
