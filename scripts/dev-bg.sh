#!/usr/bin/env bash
# Start dev-run.sh detached on the build host, logging to a file.
#   dev-bg.sh <name> <lodestar args...>
# The log is ~/lodestar-dev-logs/<name>.log and the pid beside it.
set -euo pipefail
name="$1"
shift
logs="$HOME/lodestar-dev-logs"
mkdir -p "$logs"
if [ -f "$logs/$name.pid" ] && kill -0 "$(cat "$logs/$name.pid")" 2>/dev/null; then
    echo "$name is already running as pid $(cat "$logs/$name.pid")" >&2
    exit 1
fi
NO_COLOR=1 nohup setsid bash "$(dirname "$0")/dev-run.sh" "$@" >> "$logs/$name.log" 2>&1 < /dev/null &
echo $! > "$logs/$name.pid"
echo "started $name as pid $!"
