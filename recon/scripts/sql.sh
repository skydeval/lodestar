#!/usr/bin/env bash
# Run one of the recon SQL files against the recon database:
#   sql.sh <name> [label]
# The file is read from lodestar-recon/sql/<name>.sql. An optional label
# is passed to the file as the psql variable `label`.
set -euo pipefail
name="$1"
args=()
if [ "$#" -ge 2 ]; then
    args=(-v "label=$2")
fi
if [ "$#" -ge 3 ]; then
    args+=(-v "first=$3")
fi
docker exec -i lodestar-recon-pg psql -U postgres -d recon -X -q -P pager=off -v ON_ERROR_STOP=1 "${args[@]}" \
    < "$HOME/lodestar-recon/sql/$name.sql"
