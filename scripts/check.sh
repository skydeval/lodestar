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
    db)
        # The tests, with the ones that need Postgres run against a
        # throwaway database beside the recon one.
        set -a
        . "$HOME/lodestar-recon-data/env"
        set +a
        docker exec lodestar-recon-pg dropdb -U postgres --if-exists lodestar_test
        docker exec lodestar-recon-pg createdb -U postgres lodestar_test
        LODESTAR_TEST_DATABASE_URL="${RECON_DATABASE_URL%/*}/lodestar_test" cargo test 2>&1 | tail -n 60
        status="${PIPESTATUS[0]}"
        docker exec lodestar-recon-pg dropdb -U postgres lodestar_test
        exit "$status"
        ;;
    clippy)
        # As CI runs it: formatting first, then the newest stable
        # toolchain with warnings refused.
        if ! cargo +stable fmt --check; then
            echo "not formatted: run cargo fmt"
            exit 1
        fi
        cargo +stable clippy --locked --all-targets -- -D warnings 2>&1 | tail -n 80
        exit "${PIPESTATUS[0]}"
        ;;
esac
