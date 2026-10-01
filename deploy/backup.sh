#!/bin/sh
set -eu

BACKUP_MIN_KEEP="${BACKUP_MIN_KEEP:-7}"
for setting in "BACKUP_RETENTION_DAYS=$BACKUP_RETENTION_DAYS" "BACKUP_MIN_KEEP=$BACKUP_MIN_KEEP"; do
    case "${setting#*=}" in
        ''|*[!0-9]*)
            echo "${setting%%=*} must be a non-negative integer" >&2
            exit 1
            ;;
    esac
done

prune_backups() {
    directory="$1"
    now="$(date +%s)"
    kept=0
    for dump in "$directory"/uth_notifier-*.dump; do
        [ -e "$dump" ] && printf '%s\n' "$dump"
    done | LC_ALL=C sort -r | while IFS= read -r dump; do
        # Names embed a UTC timestamp, so the newest BACKUP_MIN_KEEP dumps survive
        # even after a long run of failed backups has aged every one of them out.
        if [ "$kept" -lt "$BACKUP_MIN_KEEP" ]; then
            kept=$((kept + 1))
        elif [ $(((now - $(stat -c %Y "$dump")) / 86400)) -gt "$BACKUP_RETENTION_DAYS" ]; then
            rm -f -- "$dump" "$dump.sha256"
        fi
    done
    for checksum in "$directory"/uth_notifier-*.dump.sha256; do
        if [ -e "$checksum" ] && [ ! -e "${checksum%.sha256}" ] &&
            [ $(((now - $(stat -c %Y "$checksum")) / 86400)) -gt "$BACKUP_RETENTION_DAYS" ]; then
            rm -f -- "$checksum"
        fi
    done
}

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
prune_backups /backups
printf '%s\n' "$backup_path"
