#!/usr/bin/env bash
# Development only. Copy recon's loaded directory into the development
# database and compare lodestar's tables with it. Reads the recon
# database; writes two comparison tables into the development one.
# The report goes to ~/lodestar-dev-logs/compare-recon.log.
set -euo pipefail
DEV="${LODESTAR_DEV_DB:-lodestar_dev}"
LOG="$HOME/lodestar-dev-logs/compare-recon.log"
pg() { docker exec -i lodestar-recon-pg psql -U postgres -X -q -v ON_ERROR_STOP=1 "$@"; }
{
    echo "started $(date -u +%FT%TZ)"
    cursor=$(pg -d recon -At -c "SELECT cursor FROM sync_state WHERE source = 'plc'")
    echo "recon's PLC cursor: $cursor"
    pg -d "$DEV" -c "DROP TABLE IF EXISTS cmp_accounts, cmp_history" \
        -c "CREATE UNLOGGED TABLE cmp_accounts (did bytea, handle_claim text, endpoint text, signing_key bytea, status smallint, created_at timestamptz)" \
        -c "CREATE UNLOGGED TABLE cmp_history (did bytea, changed_at timestamptz, handle_claim text, endpoint text)"
    pg -d recon -c "COPY (SELECT a.did, a.handle_claim, p.endpoint, a.signing_key, a.status, a.created_at FROM c_accounts a LEFT JOIN c_pds p ON p.id = a.pds_id) TO STDOUT (FORMAT binary)" \
        | pg -d "$DEV" -c "COPY cmp_accounts FROM STDIN (FORMAT binary)"
    pg -d recon -c "COPY (SELECT h.did, h.changed_at, h.handle_claim, p.endpoint FROM v_history_changed h LEFT JOIN c_pds p ON p.id = h.pds_id) TO STDOUT (FORMAT binary)" \
        | pg -d "$DEV" -c "COPY cmp_history FROM STDIN (FORMAT binary)"
    pg -d "$DEV" -c "ANALYZE cmp_accounts" -c "ANALYZE cmp_history"
    echo "copied $(date -u +%FT%TZ)"
    pg -d "$DEV" -P pager=off -v "first=$cursor" < "$(dirname "$0")/sql/compare_recon.sql"
    echo "finished $(date -u +%FT%TZ)"
} > "$LOG" 2>&1
