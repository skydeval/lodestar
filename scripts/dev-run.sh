#!/usr/bin/env bash
# Run the release binary on the build host against the development
# database, which lives beside the recon database in the same Postgres.
#   dev-run.sh <lodestar args...>
# The connection string comes from the recon env file and is never printed.
set -euo pipefail
set -a
. "$HOME/lodestar-recon-data/env"
set +a
export LODESTAR_DATABASE_URL="${RECON_DATABASE_URL%/*}/${LODESTAR_DEV_DB:-lodestar_dev}"
export LODESTAR_USER_AGENT="lodestar/0.1 (operator: skydeval; https://github.com/skydeval/lodestar)"
exec "$(dirname "$0")/../target/release/lodestar" "$@"
