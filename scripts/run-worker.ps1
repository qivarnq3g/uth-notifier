param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("scheduler", "classifier", "notify", "edge-reconciler")]
    [string]$Worker,
    [string]$SourceFile = "results\facebook_drl_sources.json"
)

$ErrorActionPreference = "Stop"
$utf8 = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $utf8
[Console]::OutputEncoding = $utf8
$OutputEncoding = $utf8

$projectRoot = Split-Path -Parent $PSScriptRoot
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($projectRoot).TrimEnd('\')
$envPath = Join-Path $projectRoot ".env"
$agentPath = Join-Path $projectRoot "target\release\uth-agent.exe"
$sourcePath = [System.IO.Path]::GetFullPath((Join-Path $projectRoot $SourceFile))
$edgeSyncTokenPath = Join-Path $projectRoot "deploy\secrets\edge_sync_token"
$logDirectory = Join-Path $projectRoot "results\runtime-logs"
$resolvedLogDirectory = [System.IO.Path]::GetFullPath($logDirectory)

if (-not $sourcePath.StartsWith("$resolvedProjectRoot\", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Tep danh sach nguon nam ngoai workspace"
}
if (-not $resolvedLogDirectory.StartsWith("$resolvedProjectRoot\", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Thu muc log nam ngoai workspace"
}
if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    throw "Khong tim thay tep .env"
}
if (-not (Test-Path -LiteralPath $agentPath -PathType Leaf)) {
    throw "Khong tim thay release uth-agent"
}
if ($Worker -eq "scheduler" -and -not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
    throw "Khong tim thay danh sach nguon"
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
if ($Worker -eq "notify" -and [string]::IsNullOrWhiteSpace($env:TELEGRAM_BOT_TOKEN)) {
    throw "TELEGRAM_BOT_TOKEN dang trong"
}
if ($Worker -eq "edge-reconciler") {
    if ([string]::IsNullOrWhiteSpace($env:EDGE_URL)) {
        throw "EDGE_URL dang trong"
    }
    if (-not (Test-Path -LiteralPath $edgeSyncTokenPath -PathType Leaf)) {
        throw "Khong tim thay edge sync token"
    }
    $env:EDGE_SYNC_TOKEN = Get-Content -LiteralPath $edgeSyncTokenPath -Raw
    if ([string]::IsNullOrWhiteSpace($env:EDGE_SYNC_TOKEN)) {
        throw "EDGE_SYNC_TOKEN dang trong"
    }
}

New-Item -ItemType Directory -Path $resolvedLogDirectory -Force | Out-Null
[string[]]$workerArguments = switch ($Worker) {
    "scheduler" { @("crawl-scheduled", $sourcePath, "--concurrency", "2", "--max-backoff", "900", "--poll-interval", "2") }
    "classifier" { @("classify", "--config", (Join-Path $projectRoot "config\classifier-rules.v1.json"), "--poll-interval", "2") }
    "notify" { @("notify") }
    "edge-reconciler" { @("reconcile-edge", "--poll-interval", "2") }
}
$restartDelay = 5

Push-Location $projectRoot
try {
    while ($true) {
        Get-ChildItem -LiteralPath $resolvedLogDirectory -Filter "$Worker-*.log" -File |
            Where-Object { $_.LastWriteTimeUtc -lt [DateTime]::UtcNow.AddDays(-14) } |
            ForEach-Object {
                $resolvedFile = [System.IO.Path]::GetFullPath($_.FullName)
                if ($resolvedFile.StartsWith("$resolvedLogDirectory\", [System.StringComparison]::OrdinalIgnoreCase)) {
                    Remove-Item -LiteralPath $resolvedFile -Force
                }
        }
        $logPath = Join-Path $resolvedLogDirectory ("{0}-{1}.log" -f $Worker, (Get-Date -Format "yyyyMMdd"))
        $startedAt = [DateTime]::UtcNow
        $previousErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & $agentPath @workerArguments 2>&1 | ForEach-Object {
                "{0} {1}" -f ([DateTime]::UtcNow.ToString("o")), $_ | Out-File -LiteralPath $logPath -Append -Encoding utf8
            }
        }
        finally {
            $ErrorActionPreference = $previousErrorActionPreference
        }
        $exitCode = $LASTEXITCODE
        $runtimeSeconds = [int]([DateTime]::UtcNow - $startedAt).TotalSeconds
        $event = [ordered]@{
            schema_version = "runtime-supervisor-event.v1"
            generated_at = [DateTime]::UtcNow.ToString("o")
            worker = $Worker
            event = "process_exited"
            exit_code = $exitCode
            runtime_seconds = $runtimeSeconds
            restart_delay_seconds = $restartDelay
        } | ConvertTo-Json -Compress
        $event | Out-File -LiteralPath $logPath -Append -Encoding utf8
        if ($runtimeSeconds -ge 300) {
            $restartDelay = 5
        } else {
            $restartDelay = [Math]::Min($restartDelay * 2, 300)
        }
        Start-Sleep -Seconds $restartDelay
    }
}
finally {
    Pop-Location
}
