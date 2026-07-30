$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $PSScriptRoot "run-worker.ps1"
$agentPath = Join-Path $projectRoot "target\release\uth-agent.exe"
$taskPrefix = "UTH Notifier"
$userId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

if (-not (Test-Path -LiteralPath $runnerPath -PathType Leaf)) {
    throw "Khong tim thay runtime worker script"
}
if (-not (Test-Path -LiteralPath $agentPath -PathType Leaf)) {
    throw "Khong tim thay release uth-agent"
}

$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $userId
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -StartWhenAvailable

foreach ($worker in @("scheduler", "classifier", "notify", "edge-reconciler")) {
    $taskName = "$taskPrefix $worker"
    $arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$runnerPath`" -Worker $worker"
    $action = New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument $arguments `
        -WorkingDirectory $projectRoot
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Force | Out-Null
    Enable-ScheduledTask -TaskName $taskName | Out-Null
    Start-ScheduledTask -TaskName $taskName
}
