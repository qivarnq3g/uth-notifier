$ErrorActionPreference = "Stop"

$insideWorkTree = & git rev-parse --is-inside-work-tree 2>$null
if ($LASTEXITCODE -ne 0 -or $insideWorkTree -ne "true") {
    throw "Publication check requires a Git worktree"
}

$trackedFiles = @(& git ls-files)
if ($LASTEXITCODE -ne 0) {
    throw "Cannot enumerate tracked files"
}

$forbiddenExact = @(
    ".env",
    "AGENTS.md",
    "AGENTS.local.md",
    "CLAUDE.md",
    "CLAUDE.local.md",
    "skills-lock.json",
    "apps/edge-worker/wrangler.production.toml"
)
$forbiddenPrefixes = @(
    ".agents/",
    "backups/",
    "deploy/secrets/",
    "results/",
    "target/",
    "tmp/"
)

$pathViolations = @(
    $trackedFiles | Where-Object {
        $path = $_.Replace('\', '/')
        $hasForbiddenPrefix = $false
        foreach ($prefix in $forbiddenPrefixes) {
            if ($path.StartsWith($prefix, [StringComparison]::Ordinal)) {
                $hasForbiddenPrefix = $true
                break
            }
        }
        ($forbiddenExact -contains $path) -or
        ($path -match '(^|/)node_modules/') -or
        (($path -match '(^|/)\.env\.') -and ($path -notmatch '(^|/)\.env\.example$')) -or
        $hasForbiddenPrefix
    }
)

if ($pathViolations.Count -gt 0) {
    $pathViolations | Sort-Object -Unique | ForEach-Object { Write-Error "Forbidden tracked path: $_" }
    throw "Public repository boundary contains forbidden paths"
}

$contentPatterns = [ordered]@{
    TelegramBotToken = '\b\d{6,12}:[A-Za-z0-9_-]{30,}\b'
    GitHubToken = '\bgh[pousr]_[A-Za-z0-9]{20,}\b'
    AwsAccessKey = '\b(?:AKIA|ASIA)[A-Z0-9]{16}\b'
    GoogleApiKey = '\bAIza[A-Za-z0-9_-]{30,}\b'
    SlackToken = '\bxox[baprs]-[A-Za-z0-9-]{10,}\b'
    PrivateKey = '-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----'
    TailscaleAddress = '\b100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])(?:\.\d{1,3}){2}\b'
    WindowsUserPath = '(?i)\bC:\\Users\\[^\\\s]+'
    ProductionD1Id = '(?i)database_id\s*=\s*"(?!00000000-0000-0000-0000-000000000000)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"'
}

$contentViolations = [System.Collections.Generic.List[string]]::new()
foreach ($path in $trackedFiles) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        continue
    }
    try {
        $content = [IO.File]::ReadAllText((Resolve-Path -LiteralPath $path))
    } catch {
        continue
    }
    foreach ($entry in $contentPatterns.GetEnumerator()) {
        if ([regex]::IsMatch($content, $entry.Value)) {
            $contentViolations.Add("$($entry.Key): $path")
        }
    }
}

if ($contentViolations.Count -gt 0) {
    $contentViolations | Sort-Object -Unique | ForEach-Object { Write-Error "Sensitive content pattern: $_" }
    throw "Tracked files contain sensitive content patterns"
}

Write-Output "Public repository boundary check passed for $($trackedFiles.Count) tracked files"
