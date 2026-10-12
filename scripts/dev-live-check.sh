#!/usr/bin/env bash
# Exercise the API paths that wait on the verification role. Expects the
# API on 127.0.0.1:$1 and a verification worker running on the same
# database.
set -uo pipefail
base="http://127.0.0.1:$1"
show() {
    echo "--- $1"
    shift
    curl -sS -m 15 -w '\n[%{http_code}] %{time_total}s\n' "$@"
}
show "handle not in the index, resolves to an account lodestar has" "$base/handle/pfrazee.com"
show "the same again: now from the index" "$base/handle/pfrazee.com"
show "handle that resolves to nothing" "$base/handle/thatkruegergirl.bsky.social"
show "the same again: from the miss cache" "$base/handle/thatkruegergirl.bsky.social"
show "handle whose identity lodestar may not have yet" "$base/handle/tre.blacksky.app"
show "unseen did:web that is an account" "$base/account/did:web:sky.charliegleason.com"
show "unseen did:web that cannot be fetched" "$base/account/did:web:le98bf.lnan.site"
show "refresh a did:plc" -X POST "$base/account/did:plc:xb2vhpoddmj7chupbzwn4aq3/refresh"
show "refresh a did:web" -X POST "$base/account/did:web:sky.charliegleason.com/refresh"
