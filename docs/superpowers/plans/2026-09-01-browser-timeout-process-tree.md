# Browser Timeout Process-Tree Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent browser timeouts from leaking Chromium descendants and Playwright temporary directories, then recover and verify production.

**Architecture:** The Rust browser adapter will supervise Node in a dedicated Unix process group, drain output pipes concurrently, kill and reap the group on timeout, and preserve a direct-child fallback elsewhere. Production recovery uses a unique release and controlled scheduler restart so systemd can clear the existing private temporary namespace.

**Tech Stack:** Rust 1.97, Tokio 1, libc 0.2, Playwright/Node, systemd, Docker

**Spec:** `docs/superpowers/specs/2026-09-01-browser-timeout-process-tree-design.md`

## Global Constraints

- Preserve `facebook-post.v1` and `facebook-crawl-report.v1`.
- Do not add authentication, cookies, proxies, alternate egress, or paid services.
- Preserve existing user changes and deploy only explicit allowlisted artifacts.
- Build Linux amd64 with the pinned Docker `rust-builder` stage and Cargo `--locked`.
- Restart only `uth-notifier-scheduler` and retain the previous release for rollback.

---

### Task 1: Add process-tree regression coverage

**Files:**
- Modify: `apps/core-agent/src/browser.rs`

**Interfaces:**
- Consumes: existing `apply_browser_fallback` timeout behavior
- Produces: Unix regression tests for `run_browser_process(&mut Command, Duration)`

- [ ] **Step 1: Write a Unix-only failing timeout test**

Add a Tokio test that runs `sh -c 'sleep 30 & echo $! > "$1"; wait'`, passes a task-owned PID-file path, invokes the not-yet-implemented `run_browser_process`, and asserts that the returned failure is a timeout and the background PID disappears within two seconds.

- [ ] **Step 2: Write a failing success-path test**

Run `sh -c 'printf stdout; printf stderr >&2'` through `run_browser_process` and assert a successful status, stdout `stdout`, and stderr `stderr`.

- [ ] **Step 3: Verify RED on Linux**

Run:

```powershell
docker build --target rust-builder -t uth-notifier-rust-builder:browser-process-red .
```

Expected: build fails because `run_browser_process` and its output/error types do not exist.

### Task 2: Implement bounded browser-process supervision

**Files:**
- Modify: `Cargo.toml`
- Modify: `apps/core-agent/Cargo.toml`
- Modify: `apps/core-agent/src/browser.rs`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: `tokio::process::Command`, browser timeout `Duration`
- Produces: `run_browser_process(&mut Command, Duration) -> Result<BrowserProcessOutput, BrowserProcessError>`

- [ ] **Step 1: Add required dependencies**

Enable Tokio `io-util`, add workspace `libc = "0.2"`, and add `libc.workspace = true` under the core agent's Unix target dependencies.

- [ ] **Step 2: Create the supervisor**

Configure a new process group on Unix before spawn, take stdout and stderr, drain both with Tokio tasks, and wait for the child under `tokio::time::timeout`.

- [ ] **Step 3: Implement timeout cleanup**

On Unix signal `-pgid` with `SIGKILL`; on every platform invoke the direct-child kill fallback, wait for the child, and join both pipe readers. Preserve `browser fallback exceeded <n> seconds` and append a cleanup error only if termination or reap fails.

- [ ] **Step 4: Route browser fallback through the supervisor**

Replace `timeout(browser_timeout, command.output())` with `run_browser_process` while keeping snapshot parsing, outcome classification, and stderr truncation unchanged.

- [ ] **Step 5: Verify GREEN on Linux**

Run:

```powershell
docker build --target rust-builder -t uth-notifier-rust-builder:browser-process-green .
docker run --rm -v "${PWD}/config:/build/config:ro" uth-notifier-rust-builder:browser-process-green cargo test --locked -p uth-agent browser::tests -- --nocapture
```

Expected: focused browser tests pass, including the descendant-process timeout regression.

### Task 3: Verify repository behavior and build the release artifact

**Files:**
- Verify only: workspace source and tests
- Produce: `target/deploy/uth-agent-browser-process-tree`

**Interfaces:**
- Consumes: completed process supervisor
- Produces: tested Linux amd64 release binary and SHA-256

- [ ] **Step 1: Run formatting checks**

Run `cargo fmt --all -- --check` and fix only files touched by this task.

- [ ] **Step 2: Run the full Linux workspace test suite**

Run the pinned builder with `/build/config` mounted read-only:

```powershell
docker run --rm -v "${PWD}/config:/build/config:ro" uth-notifier-rust-builder:browser-process-green cargo test --locked --workspace
```

Expected: all non-ignored workspace tests pass.

- [ ] **Step 3: Build and extract the release binary**

Build the pinned `rust-builder` target with `cargo build --locked --release -p uth-agent`, copy `/build/target/release/uth-agent` from a task-created container to `target/deploy/uth-agent-browser-process-tree`, remove that exact container, and compute SHA-256 locally.

### Task 4: Deploy and recover production

**Files:**
- Remote create: unique `/opt/uth-notifier/releases/<timestamp>-browser-process-tree-<checksum>/`
- Remote replace: new release `bin/uth-agent`

**Interfaces:**
- Consumes: verified Linux binary and current production release
- Produces: atomically switched scheduler runtime with retained rollback release

- [ ] **Step 1: Record pre-cutover state**

Record the resolved current release, scheduler PID/owner, unit status, restart count, active leases owned by that scheduler, private Playwright directory counts, memory, swap, and recent crawl outcome counts without printing sensitive payloads.

- [ ] **Step 2: Upload and verify**

Upload to a unique `/tmp` file, compare remote SHA-256 to the local value, copy the active release into a new unique release, replace only `bin/uth-agent`, and validate the binary with `--help`.

- [ ] **Step 3: Cut over atomically**

Switch `/opt/uth-notifier/current` atomically, restart only `uth-notifier-scheduler`, and verify no source leases remain for the old `uth-agent-<pid>` owner.

- [ ] **Step 4: Verify resource recovery**

Confirm systemd removed the old scheduler `PrivateTmp`, abandoned profile/artifact counts are zero or belong only to active bounded runs, swap and memory pressure decline, no Chromium remains blocked in `mem_cgroup_handle_over_high`, and Chromium crash-report pending remains empty.

- [ ] **Step 5: Verify functional recovery**

Wait for fresh `crawl-scheduler-cycle.v1` records and database crawl runs. Require stable unit activity, non-increasing `NRestarts`, no new timeout-directory accumulation, and at least one healthy presentation when Facebook returns a usable page; if Facebook only returns login walls, require bounded completion without resource leakage.

- [ ] **Step 6: Remove exact staging artifacts**

Delete only the task-created remote `/tmp` upload after checksum and runtime verification. Keep the local release artifact and both production releases.

### Task 5: Document verified operations

**Files:**
- Modify: `docs/server-deployment.md`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: verified production behavior
- Produces: repeatable diagnosis, deployment, and rollback procedure

- [ ] **Step 1: Update deployment guidance**

Document process-group timeout cleanup, private-temp verification, and the bounded post-deployment checks proven in production.

- [ ] **Step 2: Update durable repository memory**

Replace the diagnostic-only browser timeout note with the verified fix and validation procedure, including any measured residual limitations.

- [ ] **Step 3: Run documentation and diff checks**

Run `git diff --check` on the explicit task files and inspect `git status --short` to confirm unrelated user changes remain preserved.
