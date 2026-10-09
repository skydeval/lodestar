#!/usr/bin/env bash
# Show whether a recon job is running and the tail of its log and metrics.
set -euo pipefail
DATA="${RECON_DATA:-$HOME/lodestar-recon-data}"
name="$1"
lines="${2:-3}"
if [ -f "$DATA/$name.pid" ] && kill -0 "$(cat "$DATA/$name.pid")" 2>/dev/null; then
    echo "$name: running (pid $(cat "$DATA/$name.pid"))"
else
    echo "$name: not running"
fi
if [ -f "$DATA/logs/$name.log" ]; then
    echo "--- log"
    tail -n "$lines" "$DATA/logs/$name.log"
fi
if [ -f "$DATA/out/$name.jsonl" ]; then
    echo "--- metrics"
    tail -n "$lines" "$DATA/out/$name.jsonl"
fi
