# Browser Timeout Process-Tree Design

## Problem

The production scheduler starts the Playwright adapter through Node. Tokio currently applies `kill_on_drop(true)` to Node and wraps `command.output()` in a timeout. When that timeout fires, the direct Node process is killed, but descendant Chromium processes are not guaranteed to terminate. Playwright therefore cannot close Chromium or delete its temporary profile and artifact directories.

Production evidence on 2026-09-01 showed 1,159 browser timeouts, 1,141 abandoned `playwright_chromiumdev_profile-*` directories, 1,140 abandoned `playwright-artifacts-*` directories, 1.78 GB consumed in the scheduler's `PrivateTmp`, full swap, and Chromium blocked in `mem_cgroup_handle_over_high`.

## Goals

- Terminate every process created by one browser fallback when its timeout expires.
- Reap the direct Node child before returning from the timeout path.
- Continue draining stdout and stderr without deadlocking on full pipes.
- Preserve the existing browser attempt contract and timeout error text.
- Keep Windows development builds working.
- Recover production without changing database contracts, crawler presentation logic, authentication, or delivery services.

## Non-Goals

- Bypassing Facebook access controls or changing crawler identities.
- Changing HTTP strategy circuit-breaker thresholds.
- Adding a wildcard cleanup job that can race an active Playwright profile.
- Changing `facebook-post.v1` or `facebook-crawl-report.v1`.

## Design

`apps/core-agent/src/browser.rs` will own browser-process supervision. On Unix it will place Node in a new process group before spawn. It will take the child's stdout and stderr pipes, drain both concurrently, and wait for the child under the configured timeout.

If the timeout expires, the supervisor will send `SIGKILL` to the negative process-group ID, call the direct child's kill operation as a fallback, and then wait for that child. It will await the pipe readers after termination so no reader task or pipe remains live. `ESRCH` is treated as an already-exited process, while other cleanup failures are included in the recorded browser attempt error.

On non-Unix platforms the supervisor will kill and wait for the direct child. Production is Linux, so the process-group guarantee applies to the deployed runtime while Windows remains build-compatible.

The implementation will use `libc` only for Unix process-group signaling and Tokio `io-util` for concurrent pipe reads. No external `kill`, shell, or paid dependency is introduced.

## Testing

A Unix regression test will spawn a shell that creates a long-lived background child. The supervisor must time out, return the existing timeout classification, and leave neither the shell nor its background child alive. A success-path test will verify exit status plus stdout and stderr collection.

The Linux builder will run the focused browser tests and the full workspace tests. The release build will use the pinned Docker `rust-builder` stage and `--locked`.

## Production Recovery and Deployment

Build the Linux amd64 binary locally, verify its SHA-256 before and after upload, copy the active release to a unique new release, replace only `bin/uth-agent`, and atomically switch `/opt/uth-notifier/current`. Restart only `uth-notifier-scheduler`.

The controlled stop uses the existing `KillMode=control-group`, which terminates old Chromium descendants. Once the unit fully stops, systemd tears down the scheduler's private temporary namespace and removes the abandoned Playwright directories. No manual recursive deletion is required unless post-stop verification proves systemd did not remove them.

After cutover, verify the scheduler and all four units are active, `NRestarts` is stable, no lease remains for the previous scheduler owner, Chromium wait channels no longer show cgroup memory throttling, swap pressure falls, new crawler-run records appear, and Playwright temporary directory counts do not grow across bounded timeout canaries. Keep the prior release as the rollback target.

## Rollback

Atomically restore the recorded prior release symlink and restart only the scheduler. Keep the failed release and diagnostic evidence until rollback verification is complete.
