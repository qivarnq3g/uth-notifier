# Development setup

## Rust

Workspace pin Rust `1.97.1` trong `rust-toolchain.toml`. Trên Windows cần:

- Rustup profile minimal.
- Visual Studio 2022 Build Tools.
- Workload `Microsoft.VisualStudio.Workload.VCTools` và Windows SDK.

Kiểm tra chất lượng đầy đủ:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p uth-agent
cargo install cargo-audit --version 0.22.2 --locked
cargo audit --ignore RUSTSEC-2023-0071
```

## Cloudflare edge worker

Edge runtime viết bằng Rust và đóng gói thành WebAssembly (WASM). Cài tooling đã
khóa phiên bản rồi chạy build triển khai giả lập, không thay đổi tài khoản Cloudflare:

```powershell
rustup target add wasm32-unknown-unknown
cargo install worker-build --version 0.8.5 --locked
cd apps/edge-worker
npm ci --ignore-scripts
npm audit --audit-level=high
npx wrangler deploy --dry-run --outdir build/dry-run
```

`worker-build --no-panic-recovery` được cấu hình trong `wrangler.toml` vì cơ chế
panic recovery của toolchain hiện yêu cầu bảng `externref` không có trong artifact
này. Panic vẫn kết thúc invocation theo hành vi abort; lỗi request bình thường được
trả bằng `Response` và không phụ thuộc cơ chế này.

`Cargo.lock` phải được commit để binary application dùng dependency resolution
có thể tái tạo. `target/` và `results/` là generated output và đã được ignore.
`RUSTSEC-2023-0071` chỉ đi vào lockfile qua `sqlx-mysql` do macro SQLx; `cargo tree
-i rsa --target all` xác nhận nó không thuộc dependency graph được build. Dự án
chỉ bật PostgreSQL nên CI bỏ qua advisory không có bản vá này và vẫn chặn mọi
advisory RustSec khác.

## Browser agent

Browser agent yêu cầu Node.js và Chrome hệ thống:

```powershell
cd apps/browser-agent
npm install --ignore-scripts
npm run typecheck
npm audit --audit-level=high
npm run following -- `
  https://www.facebook.com/example-user/following `
  ../../results/facebook-following.json

npm run post -- `
  https://www.facebook.com/hoisinhvien.com.vn
```

`playwright-core` không tải browser riêng. Đường dẫn Chrome có thể được đặt bằng
biến môi trường `CHROME_PATH`.

Kiểm tra đường batch sau khi build release:

```powershell
target/release/uth-agent crawl-all `
  results/facebook_drl_sources.json `
  --output-dir results/crawl-all `
  --concurrency 4 `
  --timeout 15
```

## PostgreSQL storage

Migration tương thích ngược nằm trong `migrations/` và được nhúng vào
`uth-storage`. Lệnh `crawl-scheduled` tự áp dụng migration trước khi claim nguồn.
`DATABASE_URL` không được ghi vào repository hoặc log.
Production PostgreSQL dùng encoding `SQL_ASCII`; không dùng `LEFT` hoặc `SUBSTRING`
trên text UTF-8 trong SQL vì PostgreSQL có thể cắt theo byte giữa một ký tự. Hãy lấy
text có giới hạn hợp lý rồi cắt theo Unicode scalar trong Rust. Scheduler bắt SIGTERM,
hủy chu kỳ đang chạy và giải phóng lease nguồn thuộc đúng owner trước khi thoát.

Integration test yêu cầu một PostgreSQL database dùng riêng cho test vì test sẽ
xóa toàn bộ bảng thuộc durable crawl schema:

```powershell
$env:TEST_DATABASE_URL = "postgresql://postgres@127.0.0.1/uth_notifier_test"
cargo test -p uth-storage --test postgres_storage -- --ignored
```

Nếu có Docker, script sau tự tạo PostgreSQL trên một cổng trống, chạy integration
test rồi xóa container ngay cả khi test thất bại:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test-integration.ps1
```

Test xác minh migration, lease, insert idempotent, edit revision, outbox dedup,
rules classification, completion event, retry/dead-letter, notification campaign,
delivery retry/success, operational health transition và việc crawl degraded không
làm mất trạng thái post đã lưu.

## Supervisor Windows tùy chọn

`scripts/run-worker.ps1` chỉ đọc các biến được phép từ `.env`, chạy một worker và
khởi động lại với backoff từ 5 đến 300 giây khi process thoát. Mỗi worker có log
theo ngày trong `results/runtime-logs/`; script chỉ xóa log cùng loại do runtime
tạo và cũ hơn 14 ngày.

Runtime Windows thử nghiệm poll PostgreSQL mỗi hai giây. Thử nghiệm concurrency 6
tạo 46 process Chrome, dùng khoảng 4,24 GB RAM cùng 815 MB cho Node nên đã bị loại
bỏ. Sau khi Facebook username page bắt đầu yêu cầu Page Plugin fallback, concurrency
được hạ từ 4 xuống 2 để giảm lỗi browser tạm thời và RAM. Backoff tối đa là 900 giây;
không ép retry dày khi Facebook đang trả login wall.

`scripts/install-runtime.ps1` chỉ được chạy khi máy Windows đó được chọn rõ ràng
làm runtime. Không chạy script trên máy phát triển. Script đăng ký bốn Scheduled
Task cho người dùng hiện tại và khởi động chúng. `scripts/stop-runtime.ps1` dừng
đồng thời vô hiệu hóa các task. Server Linux dùng quy trình trong
`docs/server-deployment.md`.

Browser script `.ts` chạy với Node.js strip-types trong development. Native server
có thể biên dịch script thành `.js`; core agent chỉ thêm
`--experimental-strip-types` khi đường dẫn browser script có đuôi `.ts`.

## Continuous integration

Workflow `.github/workflows/ci.yml` chạy trên push và pull request. Workflow kiểm
tra Rust formatting, Clippy, unit test, release build, TypeScript typecheck và toàn
bộ durable PostgreSQL pipeline bằng PostgreSQL service tạm thời. CI cũng chạy
classifier evaluation với ngưỡng regression của fixture tổng hợp. Workflow không
cần Telegram token và không gửi tin nhắn thật.
