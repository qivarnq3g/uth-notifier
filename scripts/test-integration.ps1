param(
    [string]$PostgresImage = "postgres:17-alpine",
    [ValidateRange(1, 120)]
    [int]$ReadyAttempts = 30
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$containerName = "uth-notifier-postgres-test-$PID"
$previousDatabaseUrl = $env:TEST_DATABASE_URL
$containerCreated = $false
$locationChanged = $false

try {
    docker info --format "{{.ServerVersion}}" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker daemon khong san sang"
    }

    docker run --detach --rm --name $containerName `
        --env POSTGRES_DB=uth_notifier_test `
        --env POSTGRES_USER=postgres `
        --env POSTGRES_PASSWORD=postgres `
        --publish 127.0.0.1::5432 `
        $PostgresImage | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Khong the tao PostgreSQL test container"
    }
    $containerCreated = $true

    $portMapping = docker port $containerName 5432/tcp
    if ($LASTEXITCODE -ne 0 -or $portMapping -notmatch ':(\d+)$') {
        throw "Khong doc duoc cong PostgreSQL test"
    }
    $postgresPort = $Matches[1]

    $ready = $false
    for ($attempt = 1; $attempt -le $ReadyAttempts; $attempt++) {
        docker exec $containerName pg_isready --username postgres --dbname uth_notifier_test | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $ready = $true
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) {
        throw "PostgreSQL test khong san sang sau $ReadyAttempts lan kiem tra"
    }

    $env:TEST_DATABASE_URL = "postgresql://postgres:postgres@127.0.0.1:$postgresPort/uth_notifier_test"
    Push-Location $projectRoot
    $locationChanged = $true
    cargo test -p uth-storage --test postgres_storage -- --ignored --test-threads=1
    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL integration test that bai"
    }
}
finally {
    if ($locationChanged) {
        Pop-Location
    }
    $env:TEST_DATABASE_URL = $previousDatabaseUrl
    if ($containerCreated) {
        docker rm --force $containerName 2>$null | Out-Null
    }
}
