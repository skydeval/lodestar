#!/usr/bin/env bash
# Look handles up through the API on the build host, showing each answer
# and how long it took. Expects the API on 127.0.0.1:$1.
#   dev-handle-demo.sh <port> <handle>...
set -uo pipefail
base="http://127.0.0.1:$1"
shift
for h in "$@"; do
    echo "--- $h"
    curl -sS -m 15 -w '\n[%{http_code}] %{time_total}s\n' "$base/handle/$h"
done
