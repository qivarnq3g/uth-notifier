#!/bin/sh
set -u

case "${BACKUP_INTERVAL_SECONDS:-86400}" in
    ''|*[!0-9]*|0)
        echo "BACKUP_INTERVAL_SECONDS must be a positive integer" >&2
        exit 1
        ;;
esac

case "${BACKUP_RETRY_SECONDS:-300}" in
    ''|*[!0-9]*|0)
        echo "BACKUP_RETRY_SECONDS must be a positive integer" >&2
        exit 1
        ;;
esac

while true; do
    if /bin/sh /usr/local/bin/backup; then
        touch /backups/.last-success
        sleep "$BACKUP_INTERVAL_SECONDS"
    else
        exit_code="$?"
        printf 'backup failed with exit code %s; retrying in %s seconds\n' "$exit_code" "$BACKUP_RETRY_SECONDS" >&2
        sleep "$BACKUP_RETRY_SECONDS"
    fi
done
