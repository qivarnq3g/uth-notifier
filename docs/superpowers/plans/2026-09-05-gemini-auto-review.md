# Gemini Auto-Review & Dynamic Few-Shot Learning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate Google Gemini API (model `gemini-3.5-flash-lite`) to autonomously resolve `manual_review` posts, deliver approved campaigns, and learn dynamically from Admin corrections via a Telegram feedback loop.

**Architecture:** A two-tier review pipeline where posts flagged as `manual_review` by the rules classifier are autonomously evaluated by Gemini using Structured JSON output and Dynamic Few-Shot prompting (injecting the latest corrections from PostgreSQL). Telegram commands and inline actions allow the administrator to overturn false positives or false negatives, immediately recording learning examples into PostgreSQL for subsequent evaluations.

**Tech Stack:** Rust (edition 2024), SQLx (PostgreSQL), Reqwest (HTTP client), Serde (JSON serialization/deserialization), Telegram Bot API.

**Spec:** `docs/superpowers/specs/2026-09-05-gemini-auto-review-spec.md`

## Global Constraints
- Do not add code comments, doc comments, TODO comments, explanatory inline comments, or commented-out code to any source files (Rust, SQL, etc.).
- Follow strict Vietnamese communication in Telegram and user-facing messages.
- No emoji in responses, source code, tests, logs, or commit messages.
- TDD: Write failing tests before implementation code for each task.

---

### Task 1: Database Migration and Storage Layer

**Files:**
- Create: `migrations/0022_gemini_auto_review.sql`
- Modify: `crates/storage/src/lib.rs`
- Test: `crates/storage/tests/postgres_storage.rs`

**Interfaces:**
- Produces:
  - `AiLearningExample` struct
  - `AiLearningFeedbackPayload` struct
  - `ManualReviewOverrideOutcome` struct
  - `CrawlStore::latest_ai_learning_examples(&self, limit: i64) -> Result<Vec<AiLearningExample>>`
  - `CrawlStore::record_ai_learning_feedback(&self, payload: AiLearningFeedbackPayload<'_>) -> Result<()>`
  - `CrawlStore::override_manual_review_resolution(&self, classification_id: i64, actor_chat_id: i64, authorized_admin_chat_id: i64, new_action: ManualReviewAction, reason: Option<&str>, notification: Option<ManualReviewNotification<'_>>) -> Result<ManualReviewOverrideOutcome>`

- [ ] **Step 1: Write the failing test for storage methods**
Add test `ai_learning_examples_and_manual_review_override` in `crates/storage/tests/postgres_storage.rs`.

- [ ] **Step 2: Run test to confirm failure**
Run `cargo test -p uth-storage --test postgres_storage ai_learning_examples_and_manual_review_override` to verify it fails compilation or execution.

- [ ] **Step 3: Create migration `migrations/0022_gemini_auto_review.sql`**
Create table `ai_review_learning_examples` and its index.

- [ ] **Step 4: Implement storage models and methods in `crates/storage/src/lib.rs`**
Add `AiLearningExample`, `record_ai_learning_feedback`, `latest_ai_learning_examples`, and `override_manual_review_resolution`.

- [ ] **Step 5: Run tests to verify they pass**
Run `cargo test -p uth-storage --test postgres_storage` and verify all tests pass.

- [ ] **Step 6: Commit changes**
Git commit Task 1 files.

---

### Task 2: Gemini API Client Module

**Files:**
- Create: `apps/core-agent/src/gemini_reviewer.rs`
- Modify: `apps/core-agent/src/main.rs`
- Test: `apps/core-agent/src/gemini_reviewer.rs` (inline test module)

**Interfaces:**
- Consumes:
  - `AiLearningExample` from `uth-storage`
- Produces:
  - `GeminiConfig` struct
  - `GeminiReviewerClient` struct
  - `GeminiReviewDecision` enum (`Send`, `Skip`)
  - `GeminiReviewOutput` struct (`decision`, `reason`, `confidence`)
  - `GeminiReviewerClient::review_post(&self, source_name: &str, post_text: &str, post_url: &str, learning_examples: &[AiLearningExample]) -> Result<GeminiReviewOutput>`

- [ ] **Step 1: Write unit tests with mock server in `gemini_reviewer.rs`**
Test serialization of structured request, injection of few-shot examples, deserialization of JSON response, and error handling for 429/500 responses.

- [ ] **Step 2: Run tests to confirm failure**
Run `cargo test -p uth-agent gemini_reviewer`.

- [ ] **Step 3: Implement `gemini_reviewer.rs`**
Implement client using `reqwest::Client` with timeout (10s), headers, system instructions for UTH student information criteria, JSON schema, and prompt formatting.

- [ ] **Step 4: Run tests to confirm pass**
Run `cargo test -p uth-agent gemini_reviewer` and ensure all tests pass.

- [ ] **Step 5: Commit changes**
Git commit Task 2 files.

---

### Task 3: Notification Worker Integration and Telegram Feedback Loop

**Files:**
- Modify: `apps/core-agent/src/notification_worker.rs`
- Modify: `apps/core-agent/src/main.rs`
- Test: `apps/core-agent/src/notification_worker.rs` (or integration tests)

**Interfaces:**
- Consumes:
  - `GeminiReviewerClient` from `crate::gemini_reviewer`
  - `override_manual_review_resolution` from `uth_storage::CrawlStore`
- Produces:
  - CLI options in `NotifyArgs`: `gemini_api_key`, `gemini_model`, `gemini_api_base`
  - Handling of `ClassificationDecision::ManualReview` with autonomous Gemini evaluation
  - Telegram commands: `/ai_reject_{id}`, `/ai_approve_{id}` (and text command variants)
  - Admin notification formatting with actionable feedback commands

- [ ] **Step 1: Write tests for autonomous review planning and Telegram command parsing**
Add unit tests for `/ai_reject_{id}` and `/ai_approve_{id}` parsing in `notification_worker.rs`.

- [ ] **Step 2: Run tests to confirm failure**
Run `cargo test -p uth-agent parse_ai_override_commands`.

- [ ] **Step 3: Add CLI arguments and update `NotifyArgs`**
Add `gemini_api_key`, `gemini_model`, `gemini_api_base` to `NotifyArgs`. Initialize `GeminiReviewerClient` when key is present.

- [ ] **Step 4: Implement autonomous review handling in `plan_event_notification`**
When `ClassificationDecision::ManualReview` occurs:
- If Gemini is configured: query DB for recent learning examples, call Gemini.
- If `Send`: resolve review as Send, queue campaign, deliver, send admin alert with `/ai_reject_{id}` option.
- If `Skip`: resolve review as Skip, send admin alert with `/ai_approve_{id}` option.
- If Gemini fails: fall back to manual review detail message to Admin.

- [ ] **Step 5: Implement `/ai_reject_{id}` and `/ai_approve_{id}` in `resolve_interaction_command`**
Handle admin correction commands, update database resolution, queue campaigns if approved, and record feedback into `ai_review_learning_examples`.

- [ ] **Step 6: Run tests to verify passing status**
Run `cargo test -p uth-agent` to verify all tests pass.

- [ ] **Step 7: Commit changes**
Git commit Task 3 files.

---

### Task 4: End-to-End Verification and Validation

**Files:**
- Test: Full cargo test suite (`cargo test --workspace`)
- Verify: `cargo clippy --workspace` and `cargo check --workspace`

- [ ] **Step 1: Run full workspace test suite**
Run `cargo test --workspace`.

- [ ] **Step 2: Run clippy and format checks**
Run `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 3: Verify clean workspace and commit history**
Verify git status is clean and all deliverables match specification.
