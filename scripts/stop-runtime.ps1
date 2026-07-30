$ErrorActionPreference = "Stop"

$taskPrefix = "UTH Notifier"
foreach ($worker in @("scheduler", "classifier", "notify", "edge-reconciler")) {
    $taskName = "$taskPrefix $worker"
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if ($null -ne $task) {
        Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        Disable-ScheduledTask -TaskName $taskName | Out-Null
    }
}
