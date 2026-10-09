#!/usr/bin/env bash
# Start a recon job in the background: bg.sh <name> <recon args...>
# Output goes to logs/<name>.log and the pid to <name>.pid.
set -euo pipefail
DATA="${RECON_DATA:-$HOME/lodestar-recon-data}"
name="$1"
shift
mkdir -p "$DATA/logs" "$DATA/out"
if [ -f "$DATA/$name.pid" ] && kill -0 "$(cat "$DATA/$name.pid")" 2>/dev/null; then
    echo "$name is already running as pid $(cat "$DATA/$name.pid")" >&2
    exit 1
fi
cd "$DATA"
nohup setsid bash "$HOME/lodestar-recon/scripts/run.sh" "$@" >> "$DATA/logs/$name.log" 2>&1 < /dev/null &
echo $! > "$DATA/$name.pid"
echo "started $name as pid $!"
