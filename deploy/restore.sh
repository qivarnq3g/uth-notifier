#!/bin/sh
set -eu

case "${RESTORE_FILE:-}" in
    ''|*[!A-Za-z0-9._-]*)
        echo "RESTORE_FILE must be a backup filename" >&2
        exit 1
        ;;
esac

backup_path="/backups/$RESTORE_FILE"
checksum_path="$backup_path.sha256"
test -f "$backup_path"
test -f "$checksum_path"
cd /backups
sha256sum -c "$(basename "$checksum_path")"
pg_restore --list "$backup_path" >/dev/null
export PGPASSWORD="$(cat /run/secrets/postgres_password)"
pg_restore \
    --host postgres \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --clean \
    --if-exists \
    --no-owner \
    "$backup_path"
