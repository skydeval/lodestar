#!/usr/bin/env bash
# Block until a recon job exits, then print its status: wait.sh <name> [max-seconds]
set -euo pipefail
DATA="${RECON_DATA:-$HOME/lodestar-recon-data}"
name="$1"
max="${2:-86400}"
start=$(date +%s)
while [ -f "$DATA/$name.pid" ] && kill -0 "$(cat "$DATA/$name.pid")" 2>/dev/null; do
    if [ $(( $(date +%s) - start )) -ge "$max" ]; then
        echo "$name still running after ${max}s"
        break
    fi
    sleep 30
done
bash "$HOME/lodestar-recon/scripts/status.sh" "$name" 2
