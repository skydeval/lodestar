#!/usr/bin/env bash
# Write .env beside compose.yaml on the host lodestar runs on, generating
# the database password there. Does nothing if .env already exists.
#   deploy-env.sh "<user agent>" "<controls>" [api bind]
set -euo pipefail
cd "$(dirname "$0")"
if [ -e .env ]; then
    echo ".env already exists; left as it is"
    exit 0
fi
umask 077
{
    echo "POSTGRES_PASSWORD=$(openssl rand -hex 24)"
    echo "LODESTAR_USER_AGENT=\"$1\""
    echo "LODESTAR_CONTROLS=$2"
    echo "LODESTAR_API_BIND=${3:-127.0.0.1:8080}"
} > .env
echo ".env written"
