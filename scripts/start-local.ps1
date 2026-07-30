$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $projectRoot ".env"
$agentPath = Join-Path $projectRoot "target\release\uth-agent.exe"

if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    throw "Khong tim thay tep .env"
}

foreach ($rawLine in Get-Content -LiteralPath $envPath) {
    $line = $rawLine.Trim()
    if ($line.Length -eq 0) {
        continue
    }

    $separator = $line.IndexOf("=")
    if ($separator -le 0) {
        throw "Tep .env co dong khong hop le"
    }

    $name = $line.Substring(0, $separator).Trim()
    $value = $line.Substring($separator + 1).Trim()
    if ($name -notin @("DATABASE_URL", "TELEGRAM_BOT_TOKEN", "TELEGRAM_ADMIN_CHAT_ID", "TELEGRAM_ADMIN_ONLY", "EDGE_URL", "TELEGRAM_UPDATES_SOURCE", "PORTAL_NOTIFICATIONS_ENABLED", "PORTAL_API_BASE")) {
        throw "Tep .env co ten bien khong duoc ho tro"
    }

    [Environment]::SetEnvironmentVariable($name, $value, "Process")
}

if ([string]::IsNullOrWhiteSpace($env:DATABASE_URL)) {
    throw "DATABASE_URL dang trong"
}

if ([string]::IsNullOrWhiteSpace($env:TELEGRAM_BOT_TOKEN)) {
    throw "TELEGRAM_BOT_TOKEN dang trong"
}

if (-not (Test-Path -LiteralPath $agentPath -PathType Leaf)) {
    throw "Chua co ban chay target\release\uth-agent.exe"
}

$containerName = "uth-notifier-postgres"
$containerState = docker inspect --format "{{.State.Running}}" $containerName 2>$null
if ($LASTEXITCODE -ne 0) {
    throw "Khong tim thay PostgreSQL cua bot"
}

if ($containerState -ne "true") {
    docker start $containerName | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Khong the khoi dong PostgreSQL cua bot"
    }
}

& $agentPath notify
exit $LASTEXITCODE
