#!/usr/bin/env bash
# Run one of the recon SQL files against the recon database: sql.sh <name>
# The file is read from lodestar-recon/sql/<name>.sql.
set -euo pipefail
name="$1"
docker exec -i lodestar-recon-pg psql -U postgres -d recon -X -q -P pager=off -v ON_ERROR_STOP=1 \
    < "$HOME/lodestar-recon/sql/$name.sql"
