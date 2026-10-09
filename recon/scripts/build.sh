#!/usr/bin/env bash
# Build the recon binary on the recon host. Prints errors only.
set -uo pipefail
source "$HOME/.cargo/env" 2>/dev/null || true
LOG="$HOME/lodestar-recon-data/logs/build.log"
cd "$HOME/lodestar-recon"
if cargo build --release --quiet --message-format short 2> "$LOG"; then
    echo "build ok"
else
    grep -n 'error' "$LOG" | head -60
    exit 1
fi
