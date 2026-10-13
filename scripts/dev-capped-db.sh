#!/usr/bin/env bash
# Development only. A second Postgres on the build host, held to the
# memory of the reference host, with a copy of the development database
# in which no identity is marked as listed. For timing anything that
# depends on `accounts` not fitting in memory.
#   dev-capped-db.sh setup          create it and copy the data (long)
#   dev-capped-db.sh run <args...>  run the release binary against it
#   dev-capped-db.sh image <tag> <args...>
#                                   run a built image against it
#   dev-capped-db.sh sql            psql, reading stdin
#   dev-capped-db.sh seed [percent] mark a random share of the identities
#                                   as listed (16.4%, about what a relay
#                                   lists)
#   dev-capped-db.sh drop           remove it and its volume
# The password is generated here, kept in the log directory and never
# printed. The development database is only read.
set -euo pipefail
DEV="${LODESTAR_DEV_DB:-lodestar_dev}"
NAME=lodestar-capped-pg
PORT=55447
LOGS="$HOME/lodestar-dev-logs"
ENVFILE="$LOGS/capped-db.env"
UA="lodestar/0.1 (operator: skydeval; https://github.com/skydeval/lodestar)"
src() { docker exec -i lodestar-recon-pg "$@"; }
dst() { docker exec -i "$NAME" psql -U lodestar -d lodestar -X -q -v ON_ERROR_STOP=1 "$@"; }
url() {
    . "$ENVFILE"
    echo "postgres://lodestar:$POSTGRES_PASSWORD@127.0.0.1:$PORT/lodestar"
}

case "${1:-}" in
    setup)
        mkdir -p "$LOGS"
        if [ ! -e "$ENVFILE" ]; then
            (umask 077; echo "POSTGRES_PASSWORD=$(openssl rand -hex 24)" > "$ENVFILE")
        fi
        set -a
        . "$ENVFILE"
        set +a
        # The settings compose.yaml gives Postgres, under a limit that
        # leaves it what the reference host does.
        docker run -d --name "$NAME" --memory 7g --memory-swap 7g --shm-size 1g \
            -e POSTGRES_USER=lodestar -e POSTGRES_DB=lodestar -e POSTGRES_PASSWORD \
            -p "127.0.0.1:$PORT:5432" -v "$NAME:/var/lib/postgresql/data" postgres:16 \
            -c shared_buffers=2GB -c effective_cache_size=5GB -c maintenance_work_mem=512MB \
            -c max_wal_size=3GB -c checkpoint_timeout=15min > /dev/null
        for _ in $(seq 60); do
            if docker exec "$NAME" pg_isready -q -U lodestar -d lodestar 2> /dev/null; then break; fi
            sleep 1
        done
        docker exec "$NAME" pg_isready -q -U lodestar -d lodestar
        sleep 3
        echo "started $(date -u +%FT%TZ)"
        src pg_dump -U postgres -d "$DEV" --section=pre-data --no-owner --no-privileges -T 'cmp_*' | dst
        src pg_dump -U postgres -d "$DEV" --section=data -T 'cmp_*' -T accounts | dst
        echo "small tables copied $(date -u +%FT%TZ)"
        cols=$(src psql -U postgres -d "$DEV" -X -At -c "SELECT string_agg(CASE WHEN column_name = 'listed' THEN 'false' ELSE quote_ident(column_name) END, ', ' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'accounts'")
        # In storage order, so the copy is laid out as the original is.
        src psql -U postgres -d "$DEV" -X -q -c "SET max_parallel_workers_per_gather = 0" \
            -c "COPY (SELECT $cols FROM accounts) TO STDOUT (FORMAT binary)" \
            | dst -c "COPY accounts FROM STDIN (FORMAT binary)"
        echo "accounts copied $(date -u +%FT%TZ)"
        src pg_dump -U postgres -d "$DEV" --section=post-data --no-owner --no-privileges -T 'cmp_*' | dst
        dst -c "DELETE FROM sync_state WHERE source = 'relay-walk'" -c "VACUUM ANALYZE"
        echo "finished $(date -u +%FT%TZ)"
        ;;
    run)
        shift
        LODESTAR_DATABASE_URL="$(url)" LODESTAR_USER_AGENT="$UA" \
            exec "$(dirname "$0")/../target/release/lodestar" "$@"
        ;;
    image)
        tag="$2"
        shift 2
        LODESTAR_DATABASE_URL="$(url)" LODESTAR_USER_AGENT="$UA" \
            exec docker run --rm --network host -e LODESTAR_DATABASE_URL -e LODESTAR_USER_AGENT "$tag" "$@"
        ;;
    sql)
        dst -P pager=off
        ;;
    seed)
        echo "started $(date -u +%FT%TZ)"
        dst -c "TRUNCATE listed" \
            -c "INSERT INTO listed SELECT did FROM accounts TABLESAMPLE BERNOULLI (${2:-16.4})"
        dst -At -c "SELECT count(*) FROM listed"
        echo "finished $(date -u +%FT%TZ)"
        ;;
    drop)
        docker rm -f "$NAME"
        docker volume rm "$NAME"
        ;;
    *)
        echo "usage: dev-capped-db.sh setup | run <args> | image <tag> <args> | sql | seed [percent] | drop" >&2
        exit 2
        ;;
esac
