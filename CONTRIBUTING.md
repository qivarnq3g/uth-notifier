# Contributing

## Development

Use the pinned Rust and Node.js versions documented in
`docs/development.md`. Keep secrets in ignored local files and use synthetic
data in tests, logs and bug reports.

Run the relevant checks before opening a pull request:

```powershell
scripts/check-publication.ps1
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Run `scripts/test-integration.ps1` serially for PostgreSQL integration changes.
Run the browser fixture tests and a policy-compliant live crawl for Facebook
parser changes.

## Pull requests

Keep changes scoped, preserve versioned contracts and document intentional
behavior changes. Do not include `.env`, deployment secrets, production
configuration, raw crawler output, Telegram payloads, payment payloads or
generated build artifacts.

Security vulnerabilities must follow `SECURITY.md` instead of a public issue.
