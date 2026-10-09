#!/usr/bin/env bash
# Run the recon binary with the recon host's env file loaded.
# The env file lives outside the repo and holds the database URL, the
# User-Agent and any secret. It is never printed.
set -euo pipefail
DATA="${RECON_DATA:-$HOME/lodestar-recon-data}"
set -a
. "$DATA/env"
set +a
exec "$HOME/lodestar-recon/target/release/lodestar-recon" "$@"
