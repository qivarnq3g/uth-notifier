#!/bin/sh
set -eu

umask 077
export PGPASSWORD="$(cat /run/secrets/postgres_password)"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_path="/backups/uth_notifier-$timestamp.dump"
checksum_path="$backup_path.sha256"

pg_dump \
    --host postgres \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --format custom \
    --compress 9 \
    --file "$backup_path"
pg_restore --list "$backup_path" >/dev/null
sha256sum "$backup_path" >"$checksum_path"
find /backups -maxdepth 1 -type f -name 'uth_notifier-*.dump' -mtime "+$BACKUP_RETENTION_DAYS" -delete
find /backups -maxdepth 1 -type f -name 'uth_notifier-*.dump.sha256' -mtime "+$BACKUP_RETENTION_DAYS" -delete
printf '%s\n' "$backup_path"
