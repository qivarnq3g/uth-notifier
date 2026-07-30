#!/bin/sh
set -eu

read_secret() {
    secret_path="$1"
    variable_name="$2"
    required="$3"
    if [ -f "$secret_path" ]; then
        secret_value="$(cat "$secret_path")"
        if [ -z "$secret_value" ] && [ "$required" = "true" ]; then
            echo "$secret_path is empty" >&2
            exit 1
        fi
        export "$variable_name=$secret_value"
    elif [ "$required" = "true" ]; then
        echo "$secret_path is missing" >&2
        exit 1
    fi
}

read_secret /run/secrets/database_url DATABASE_URL true
if [ "${1:-}" = "notify" ]; then
    read_secret /run/secrets/telegram_bot_token TELEGRAM_BOT_TOKEN true
    read_secret /run/secrets/telegram_admin_chat_id TELEGRAM_ADMIN_CHAT_ID false
fi
if [ "${1:-}" = "reconcile-edge" ]; then
    read_secret /run/secrets/edge_sync_token EDGE_SYNC_TOKEN true
fi

exec uth-agent "$@"
