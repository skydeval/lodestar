#!/usr/bin/env bash
# Build and test on the build host. Never run locally.
#   check.sh            tests
#   check.sh release    release build
set -uo pipefail
source "$HOME/.cargo/env" 2>/dev/null || true
cd "$(dirname "$0")/.."
case "${1:-test}" in
    test)
        cargo test 2>&1 | tail -n 60
        exit "${PIPESTATUS[0]}"
        ;;
    release)
        cargo build --release 2>&1 | tail -n 40
        exit "${PIPESTATUS[0]}"
        ;;
    clippy)
        # As CI runs it: the newest stable toolchain, warnings refused.
        cargo +stable clippy --locked --all-targets -- -D warnings 2>&1 | tail -n 80
        exit "${PIPESTATUS[0]}"
        ;;
esac
