#!/usr/bin/env bash
# Build the recon binary in a container with an older C library, so it
# runs on hosts older than the recon host. Prints errors only. The binary
# lands in lodestar-recon-data/target-portable/release/.
set -uo pipefail
DATA="${RECON_DATA:-$HOME/lodestar-recon-data}"
LOG="$DATA/logs/build-portable.log"
mkdir -p "$DATA/target-portable"
if docker run --rm --user "$(id -u):$(id -g)" \
    -e CARGO_HOME=/target/cargo-home \
    -v "$HOME/lodestar-recon:/src:ro" -v "$DATA/target-portable:/target" -w /src \
    rust:1.88-bookworm cargo build --release --locked --quiet --target-dir /target > "$LOG" 2>&1; then
    echo "build ok"
else
    grep -n 'error' "$LOG" | head -60
    exit 1
fi
