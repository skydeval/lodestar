#!/usr/bin/env bash
# Run a SQL file against the development database on the build host.
#   dev-sql.sh <file under scripts/sql/, without .sql>
set -euo pipefail
docker exec -i lodestar-recon-pg psql -U postgres -d "${LODESTAR_DEV_DB:-lodestar_dev}" -X -q -P pager=off -v ON_ERROR_STOP=1 \
    < "$(dirname "$0")/sql/$1.sql"
