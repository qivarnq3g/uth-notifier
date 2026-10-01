# UTH Notifier

[![CI](https://github.com/qivarnq3g/uth-notifier/actions/workflows/ci.yml/badge.svg)](https://github.com/qivarnq3g/uth-notifier/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Bot Telegram không chính thức giúp sinh viên Trường Đại học Giao thông vận tải TP.HCM (UTH) không bỏ lỡ hoạt động có điểm rèn luyện (ĐRL), học bổng và thông báo từ Cổng đào tạo. Hệ thống theo dõi các trang Facebook công khai và API công khai của Portal UTH, phân loại bài bằng bộ luật có thể giải thích kết hợp Gemini, rồi gửi tới người đăng ký qua Telegram.

Dự án ưu tiên **độ chính xác** và **độ bền**: mọi bước xử lý đi qua hàng đợi bền vững trong PostgreSQL với lease, thử lại có backoff, dead letter và kiểm tra sức khỏe. Toàn bộ hệ thống chạy được trên một máy Linux cấu hình thấp mà không bắt buộc dùng dịch vụ trả phí.

> [!NOTE]
> Đây là dự án cộng đồng, không phải kênh thông tin chính thức của UTH.

## Mục lục

- [Tính năng](#tính-năng)
- [Kiến trúc](#kiến-trúc)
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Bắt đầu nhanh](#bắt-đầu-nhanh)
- [Cấu hình](#cấu-hình)
- [Lệnh CLI `uth-agent`](#lệnh-cli-uth-agent)
- [Lệnh Telegram](#lệnh-telegram)
- [Triển khai](#triển-khai)
- [Vận hành](#vận-hành)
- [Phát triển và kiểm thử](#phát-triển-và-kiểm-thử)
- [Bảo mật và quyền riêng tư](#bảo-mật-và-quyền-riêng-tư)
- [Đóng góp](#đóng-góp)
- [Giấy phép](#giấy-phép)

## Tính năng

### Cho sinh viên

- **Tin hoạt động và ĐRL** từ các trang Facebook công khai trong [`config/facebook-sources.v1.json`](config/facebook-sources.v1.json), cùng các trang do sinh viên đề xuất và được quản trị viên duyệt.
- **Thông báo Cổng đào tạo** luôn được gửi ngay, kèm tệp đính kèm chính thức từ `daotao.ut.edu.vn`. Loại thông báo này không phụ thuộc cài đặt, giờ yên lặng hay trạng thái tạm dừng. Thông báo cũ hơn 48 giờ chỉ được lưu vào lịch sử, không gửi đi.
- **Cài đặt cá nhân:** chỉ nhận tin có ĐRL hoặc mọi hoạt động phù hợp; nhận ngay từng tin hoặc gom thành bản tin lúc 07:30; giờ yên lặng 22:00–07:00 (giờ Việt Nam).
- **Tra cứu:** hoạt động và học bổng còn mở trong 14 ngày qua, bài mới nhất, lịch sử Portal (tải lại tệp đính kèm), danh sách trang đang theo dõi.
- **Tương tác:** đề xuất trang mới, gửi góp ý (tối đa 2.000 ký tự), đánh giá từng thông báo, ủng hộ tự nguyện qua payOS/VietQR.

### Cho quản trị viên

- Duyệt trang đề xuất, rà soát bài phân loại chưa chắc chắn và đảo ngược quyết định của Gemini. Các quyết định đảo ngược được dùng làm ví dụ cho những lần duyệt sau.
- Thống kê người dùng, đọc góp ý, xem lịch sử từng lần crawl và xuất báo cáo vận hành dạng Markdown.
- Nhận cảnh báo qua Telegram khi trạng thái sức khỏe hệ thống thay đổi.

### Cho người vận hành

- **Crawl không cần đăng nhập:** đọc dữ liệu nhúng trong HTML công khai, không dùng tài khoản, cookie hay token. Khi HTTP chỉ trả về ít bài (ví dụ gặp login wall), hệ thống chuyển sang Playwright Chromium.
- **Lịch crawl thích ứng** theo mức độ hoạt động của từng trang, kèm circuit breaker cho từng chiến lược crawl (mặc định ngắt sau 10 lần lỗi, thử lại sau 15 phút).
- **Chống gửi trùng:** bài được định danh bằng ID số và `content_hash`; mọi worker dùng outbox, lease và thao tác idempotent.
- **Vận hành gọn:** báo cáo health dạng JSON, sao lưu tự động có kiểm tra checksum, gói release dựng sẵn cho Linux và tự áp dụng migration khi khởi động.

## Kiến trúc

```mermaid
flowchart LR
    FB[Trang Facebook công khai] -->|HTTP / Playwright| SCH[crawl-scheduled]
    SCH --> PG[(PostgreSQL)]
    PG <--> CLS[classify]
    PG <--> NTF[notify]
    PORTAL[Portal UTH API] --> NTF
    NTF -->|bài cần xét| GEM[Gemini API]
    NTF -->|Bot API| TG[Telegram]
    TG -.->|webhook| EDGE[Cloudflare Worker + D1]
    PAY[payOS] -.->|webhook| EDGE
    EDGE -.->|pull / ack| REC[reconcile-edge]
    REC -.-> PG
```

Đường nét đứt là tầng edge tùy chọn.

### Thành phần

| Thành phần | Thư mục | Vai trò |
|---|---|---|
| `uth-agent` | [`apps/core-agent`](apps/core-agent) | CLI Rust chứa mọi worker (`crawl-scheduled`, `classify`, `notify`, `reconcile-edge`), kiểm tra sức khỏe và công cụ đánh giá bộ phân loại. |
| Browser agent | [`apps/browser-agent`](apps/browser-agent) | TypeScript + Playwright (Chromium headless). `post.ts` quét lịch sử bài khi HTTP không đủ dữ liệu; `following.ts` liệt kê các trang mà một trang đang theo dõi để tìm nguồn mới; `verify-runtime.ts` kiểm tra Chromium đóng gói kèm. |
| Edge worker | [`apps/edge-worker`](apps/edge-worker) | Cloudflare Worker (Rust/WASM) nhận webhook Telegram và payOS, ghi vào D1 để `reconcile-edge` kéo về, tránh mất sự kiện khi máy chủ chính gián đoạn. |
| Thư viện | [`crates/`](crates) | `domain` (mô hình dữ liệu), `crawler` (phân tích trang Facebook), `classifier` (bộ luật và đánh giá), `delivery` (client Telegram), `storage` (lớp PostgreSQL và migration). |

### Luồng dữ liệu

1. **`crawl-scheduled`** nhận các nguồn đến hạn bằng lease, crawl và lưu bài mới hoặc bài đã sửa, đồng thời ghi sự kiện `facebook_post.discovered` / `facebook_post.updated` vào outbox.
2. **`classify`** áp dụng bộ luật trong [`config/classifier-rules.v1.json`](config/classifier-rules.v1.json). Bộ luật trích các tín hiệu như `explicit_drl`, `registration_call`, `form_link`, `future_deadline` rồi kết luận: gửi, bỏ qua hoặc cần rà soát. Kết quả được ghi thành sự kiện `classification.completed`.
3. **`notify`** lập kế hoạch gửi theo cài đặt của từng người và nhờ Gemini xét các bài cần rà soát (bài quá 3 ngày bị bỏ qua). Worker này gửi Telegram có giới hạn tốc độ, xử lý lệnh người dùng (polling hoặc qua edge) và quét Portal theo chu kỳ thích ứng: 5 phút, rút xuống 1 phút trong 15 phút sau khi có thông báo mới.
4. **`reconcile-edge`** (tùy chọn) kéo sự kiện Telegram/payOS từ D1 vào PostgreSQL rồi xác nhận để edge xoá.

Mỗi worker tự áp dụng migration trong [`migrations/`](migrations) khi khởi động.

## Cấu trúc thư mục

```text
.
├── apps/
│   ├── core-agent/      # Binary uth-agent (Rust)
│   ├── browser-agent/   # Playwright fallback (TypeScript)
│   └── edge-worker/     # Cloudflare Worker + migration D1
├── crates/              # domain, crawler, classifier, delivery, storage
├── config/              # Danh sách nguồn Facebook và bộ luật phân loại
├── migrations/          # Migration PostgreSQL (sqlx)
├── deploy/              # Unit systemd, script sao lưu/khôi phục, cấu hình máy chủ
├── scripts/             # Script PowerShell: chạy trên Windows, integration test, kiểm tra công khai
├── tests/fixtures/      # HTML Facebook mẫu và tập nhãn đánh giá bộ phân loại
├── compose.server.yml   # Triển khai bằng Docker Compose
└── Dockerfile
```

## Bắt đầu nhanh

### Yêu cầu

| Công cụ | Phiên bản | Ghi chú |
|---|---|---|
| Rust | 1.97.1 | Ghim trong [`rust-toolchain.toml`](rust-toolchain.toml); `rustup` tự cài. |
| Node.js | 24 | Phiên bản CI dùng. Chạy trực tiếp file `.ts` cần tính năng type stripping của Node. |
| PostgreSQL | 17 | CI và Docker Compose dùng `postgres:17-alpine`. |
| Docker | tùy chọn | Cho Docker Compose và integration test. |

### Chạy trên máy phát triển

```bash
# 1. PostgreSQL tạm thời
docker run -d --name uth-notifier-postgres \
  -e POSTGRES_USER=uth_agent -e POSTGRES_PASSWORD=dev -e POSTGRES_DB=uth_notifier \
  -p 5432:5432 postgres:17-alpine

# 2. Build
cargo build --release -p uth-agent
(cd apps/browser-agent && npm ci --ignore-scripts)

# 3. Biến môi trường
export DATABASE_URL=postgresql://uth_agent:dev@127.0.0.1:5432/uth_notifier
export TELEGRAM_BOT_TOKEN=<token từ @BotFather>
export TELEGRAM_ADMIN_CHAT_ID=<chat ID của bạn>
export PORTAL_NOTIFICATIONS_ENABLED=true

# 4. Chạy ba worker, mỗi lệnh trong một terminal, từ thư mục gốc repo
target/release/uth-agent crawl-scheduled config/facebook-sources.v1.json --no-browser-fallback
target/release/uth-agent classify
target/release/uth-agent notify
```

Muốn bật Playwright fallback, hãy cài Chromium đi kèm rồi bỏ cờ `--no-browser-fallback`:

```bash
cd apps/browser-agent
PLAYWRIGHT_BROWSERS_PATH=0 node node_modules/playwright-core/cli.js install --only-shell chromium
# Chạy scheduler với PLAYWRIGHT_BROWSERS_PATH=0 để dùng bản Chromium này
```

Kiểm tra nhanh trạng thái:

```bash
target/release/uth-agent health
```

## Cấu hình

Mọi tham số đều có thể truyền qua cờ dòng lệnh. Bảng dưới liệt kê các tham số đọc được từ biến môi trường; xem [`.env.example`](.env.example) và [`deploy/server.env.example`](deploy/server.env.example) để có mẫu.

| Biến | Dùng bởi | Mặc định | Ý nghĩa |
|---|---|---|---|
| `DATABASE_URL` | mọi worker | *(bắt buộc)* | Chuỗi kết nối PostgreSQL. |
| `TELEGRAM_BOT_TOKEN` | `notify` | *(bắt buộc)* | Token bot Telegram. |
| `TELEGRAM_ADMIN_CHAT_ID` | `notify`, `review-send` | — | Chat nhận lệnh quản trị và cảnh báo vận hành. Nếu đặt thì không được để chuỗi rỗng. |
| `TELEGRAM_ADMIN_ONLY` | `notify` | `false` | Chế độ thử nghiệm chỉ phục vụ chat quản trị. **Khi bật, mọi người đăng ký khác bị hủy kích hoạt.** |
| `TELEGRAM_UPDATES_SOURCE` | `notify` | `polling` | `polling` (gọi `getUpdates`) hoặc `edge` (đọc update do `reconcile-edge` kéo về). |
| `PORTAL_NOTIFICATIONS_ENABLED` | `notify` | `false` | Bật quét và gửi thông báo Portal. |
| `PORTAL_API_BASE` | `notify` | `https://portal.ut.edu.vn/api/v1/` | Địa chỉ API Portal. |
| `PORTAL_MAX_NOTICE_AGE_HOURS` | `notify` | `48` | Thông báo cũ hơn ngưỡng này chỉ được lưu vào lịch sử, không gửi. |
| `GEMINI_API_KEY`, `GEMINI_FALLBACK_API_KEY` | `notify` | — | Khóa Gemini chính và dự phòng. Không đặt khóa thì bài cần xét chờ quản trị viên duyệt tay. |
| `GEMINI_MODEL` | `notify` | `gemini-3.5-flash-lite` | Mô hình Gemini. |
| `GEMINI_API_BASE` | `notify` | `https://generativelanguage.googleapis.com` | Địa chỉ API Gemini. |
| `PAYOS_CLIENT_ID`, `PAYOS_API_KEY`, `PAYOS_CHECKSUM_KEY` | `notify` | — | Đặt đủ cả ba để bật ủng hộ qua payOS; để trống cả ba để tắt. |
| `PAYOS_RETURN_URL`, `PAYOS_CANCEL_URL` | `notify` | — | Bắt buộc khi bật payOS; thường là `<EDGE_URL>/donate/return` và `/donate/cancel`. |
| `PAYOS_API_BASE` | `notify` | `https://api-merchant.payos.vn` | Địa chỉ API payOS. |
| `DONATE_VIETQR_URL`, `DONATE_MESSAGE`, `DONATE_BANK_ACCOUNT` | `notify` | — | Thông tin VietQR tĩnh, hiển thị khi không dùng payOS. |
| `EDGE_URL`, `EDGE_SYNC_TOKEN` | `reconcile-edge` | *(bắt buộc)* | Địa chỉ Cloudflare Worker và token đồng bộ. |
| `FACEBOOK_BROWSER_NETWORK_MODE` | browser agent | `system` | `system` hoặc `prefer_ipv4`. |
| `CHROME_PATH` | browser agent | — | Dùng Chromium chỉ định thay cho bản Playwright quản lý. |
| `PLAYWRIGHT_BROWSERS_PATH` | browser agent | — | Đặt `0` để dùng Chromium cài trong `node_modules`, như gói release. |

## Lệnh CLI `uth-agent`

Chạy `uth-agent <lệnh> --help` để xem đầy đủ tham số và giá trị mặc định.

| Lệnh | Mục đích |
|---|---|
| `crawl <URL>` | Crawl thử một trang Facebook công khai và xuất báo cáo JSON. |
| `crawl-all <INPUT>` | Crawl mọi nguồn trong tệp danh sách nguồn (không cần database). |
| `crawl-scheduled <INPUT>` | Worker crawl theo lịch, lưu kết quả vào PostgreSQL. |
| `classify` | Worker phân loại bài bằng bộ luật. |
| `notify` | Worker lập kế hoạch gửi, gọi Gemini, gửi Telegram, xử lý lệnh người dùng và quét Portal. |
| `reconcile-edge` | Worker kéo sự kiện từ Cloudflare Worker vào PostgreSQL. |
| `health` | In báo cáo sức khỏe dạng JSON; `--require-healthy` trả mã lỗi nếu trạng thái khác `healthy`. |
| `review-send <ID>` | Duyệt một bài đang chờ rà soát và đưa vào hàng đợi gửi. |
| `subscriber add\|remove\|list` | Quản lý người nhận thủ công. |
| `suggestion list\|approve\|reject` | Duyệt trang Facebook do người dùng đề xuất. |
| `evaluate-classifier` | Đo precision/recall của bộ luật trên tập nhãn. |
| `prepare-classifier-review` | Tạo gói rà soát từ một báo cáo crawl. |
| `finalize-classifier-review` | Kết hợp nhãn của người rà soát thành tập đánh giá. |

Ví dụ:

```bash
# Crawl thử một trang, thử mọi chiến lược HTTP
uth-agent crawl https://www.facebook.com/ITClubUTH/ --probe-all --limit 20 --output report.json

# Chạy một vòng duy nhất rồi thoát (hữu ích khi gỡ lỗi)
uth-agent crawl-scheduled config/facebook-sources.v1.json --once
uth-agent classify --once
uth-agent notify --once

# Duyệt trang đề xuất từ dòng lệnh
uth-agent suggestion list
uth-agent suggestion approve --id 12 --name "CLB Tình nguyện"
```

## Lệnh Telegram

Lệnh dạng `/lenh_ID` có thể bấm trực tiếp trong tin nhắn của bot. Hầu hết lệnh cũng có nút bấm tương ứng trên bàn phím của bot.

### Sinh viên

| Lệnh | Chức năng |
|---|---|
| `/start` | Bắt đầu dùng bot, chọn loại hoạt động muốn nhận. |
| `/settings` | Đổi loại hoạt động, cách nhận (ngay hoặc 07:30) và giờ yên lặng. |
| `/events` | Hoạt động và học bổng còn mở trong 14 ngày qua. |
| `/latest` | Bài Facebook mới nhất; `/latest_post_ID` xem chi tiết một bài. |
| `/portal_history` | Lịch sử thông báo Portal; `/portal_notice_ID` xem lại một thông báo kèm tệp. |
| `/pages` | Danh sách trang đang được theo dõi. |
| `/suggest <link>` | Đề xuất trang Facebook mới. |
| `/feedback <nội dung>` | Gửi góp ý cho ban quản trị (tối đa 2.000 ký tự). |
| `/donate [số tiền]` | Ủng hộ tự nguyện, từ 10.000 đến 10.000.000 VND. |
| `/status` | Xem trạng thái đăng ký. |
| `/stop` | Tạm dừng tin hoạt động (vẫn nhận thông báo Portal). |
| `/contact` | Liên hệ quản trị viên. |
| `/cancel` | Hủy thao tác đang nhập dở. |
| `/help` | Hướng dẫn sử dụng. |

### Quản trị viên

| Lệnh | Chức năng |
|---|---|
| `/admin` | Bảng điều khiển quản trị. |
| `/pending` | Trang đề xuất đang chờ duyệt. |
| `/approve <id> <tên>` · `/reject <id> <lý do>` | Duyệt hoặc từ chối trang đề xuất. |
| `/reviews` | Bài đang chờ rà soát; `/review <id>` xem chi tiết. |
| `/review_send_ID` · `/review_skip_ID` | Gửi hoặc bỏ qua bài đang chờ rà soát. |
| `/ai_approve_ID` · `/ai_reject_ID` | Đảo ngược quyết định của Gemini và lưu làm ví dụ học. |
| `/metrics` | Thống kê người dùng, tương tác và phản hồi 7 ngày. |
| `/feedbacks` | Góp ý của sinh viên. |
| `/crawl_history` · `/crawl_run_ID` | Lịch sử crawl và chi tiết một lần crawl. |
| `/report` | Xuất báo cáo vận hành dạng Markdown. |

## Triển khai

Có ba cách triển khai. Cả ba dùng chung binary `uth-agent` và cùng một database schema.

### 1. Linux + systemd (production)

Workflow [**Build Release**](.github/workflows/build-release.yml) chạy mỗi khi `main` thay đổi code, cấu hình hoặc thư mục `deploy/`. Nó tạo artifact `uth-notifier-linux-amd64` (giữ 7 ngày) gồm:

- `uth-notifier-runtime-linux-amd64.tar.gz` và tệp `.sha256` tương ứng. Gói chứa `bin/uth-agent`, `config/`, `deploy/` và `apps/browser-agent/` (bản build `dist/` cùng `node_modules` có sẵn Chromium).
- `uth-agent` và `uth-agent.sha256` (binary riêng lẻ).

Bố cục trên máy chủ mà các unit trong [`deploy/systemd`](deploy/systemd) mặc định:

| Đường dẫn | Nội dung |
|---|---|
| `/opt/uth-notifier/releases/<bản>/` | Mỗi bản phát hành giải nén vào một thư mục riêng. |
| `/opt/uth-notifier/current` | Symlink trỏ tới bản đang chạy. |
| `/etc/uth-notifier/runtime.env` | Biến môi trường cho mọi worker (quyền đọc hạn chế). |
| `/var/lib/uth-notifier` | Thư mục làm việc của user hệ thống `uth-notifier`. |
| `/var/backups/uth-notifier` | Bản sao lưu PostgreSQL. |

| Unit | Vai trò |
|---|---|
| `uth-notifier-scheduler.service` | `crawl-scheduled` với Playwright fallback (cần Node.js tại `/usr/bin/node`). |
| `uth-notifier-classifier.service` | `classify`. |
| `uth-notifier-notify.service` | `notify`. |
| `uth-notifier-edge-reconciler.service` | `reconcile-edge` (chỉ khi dùng edge). |
| `uth-notifier-backup.timer` / `.service` | `pg_dump` hằng ngày, giữ 14 ngày. |
| `uth-notifier-browser-crash-clean.timer` / `.service` | Dọn báo cáo crash của Chromium. |

Các tệp cấu hình đi kèm: [`deploy/tmpfiles.d`](deploy/tmpfiles.d) (vào `/etc/tmpfiles.d/`), [`deploy/journald.conf.d`](deploy/journald.conf.d) (giới hạn dung lượng log) và [`deploy/postgresql-low-memory.conf`](deploy/postgresql-low-memory.conf) (tinh chỉnh PostgreSQL cho máy ít RAM).

**Cập nhật lên bản mới:**

1. Tải artifact và kiểm tra: `sha256sum -c uth-notifier-runtime-linux-amd64.tar.gz.sha256`.
2. Sao lưu database: `sudo systemctl start uth-notifier-backup.service`.
3. Giải nén vào `/opt/uth-notifier/releases/<bản-mới>/`, giữ nguyên bản cũ.
4. Nếu unit trong `deploy/systemd/` thay đổi, chép vào `/etc/systemd/system/` rồi chạy `systemctl daemon-reload`.
5. Đổi symlink `current` sang bản mới rồi khởi động lại worker:
   `systemctl restart uth-notifier-scheduler uth-notifier-classifier uth-notifier-notify uth-notifier-edge-reconciler`.
6. Kiểm tra `systemctl status`, `journalctl` và `uth-agent health`.

> [!WARNING]
> Migration được áp dụng ngay khi worker khởi động. Binary cũ sẽ từ chối chạy trên database đã có migration mới hơn, nên muốn quay về bản cũ thì phải khôi phục database từ bản sao lưu ở bước 2.

### 2. Docker Compose

[`compose.server.yml`](compose.server.yml) chạy PostgreSQL 17 cùng các worker. Container worker chạy ở chế độ chỉ đọc và bỏ mọi capability; các giá trị bí mật được truyền qua Docker secrets.

```bash
mkdir -p deploy/secrets
printf '%s' 'postgresql://uth_agent:<mật-khẩu>@postgres:5432/uth_notifier' > deploy/secrets/database_url
printf '%s' '<mật-khẩu>'          > deploy/secrets/postgres_password
printf '%s' '<bot-token>'         > deploy/secrets/telegram_bot_token
printf '%s' '<admin-chat-id>'     > deploy/secrets/telegram_admin_chat_id
printf '%s' '<edge-sync-token>'   > deploy/secrets/edge_sync_token   # chỉ cần khi dùng edge
cp deploy/server.env.example deploy/server.env                       # rồi điền giá trị

docker compose --env-file deploy/server.env -f compose.server.yml up -d --build
```

- Dịch vụ mặc định: `postgres`, `scheduler`, `classifier`, `notify`, `backup-scheduler` (sao lưu vào `./backups` theo chu kỳ `BACKUP_INTERVAL_SECONDS`).
- Profile `edge`: thêm `edge-reconciler` (`--profile edge`).
- Profile `maintenance`: `backup` (sao lưu một lần) và `restore`.

Khôi phục một bản sao lưu (dừng các worker trước):

```bash
docker compose --env-file deploy/server.env -f compose.server.yml stop scheduler classifier notify
docker compose --env-file deploy/server.env -f compose.server.yml --profile maintenance \
  run --rm -e RESTORE_FILE=<tên-tệp.dump> restore
```

### 3. Tầng edge trên Cloudflare (tùy chọn)

Tầng edge nhận webhook ngay cả khi máy chủ chính gián đoạn, và là điều kiện để xác nhận giao dịch payOS.

| Endpoint | Mô tả |
|---|---|
| `POST /telegram/webhook` | Webhook Telegram, xác thực bằng header `X-Telegram-Bot-Api-Secret-Token`. |
| `POST /payos/webhook` | Webhook payOS, xác thực bằng chữ ký `PAYOS_CHECKSUM_KEY`. |
| `GET /donate/return`, `GET /donate/cancel` | Trang kết quả sau khi thanh toán. |
| `GET /internal/events`, `POST /internal/ack` | `reconcile-edge` kéo và xác nhận sự kiện (Bearer `EDGE_SYNC_TOKEN`). |
| `GET /health` | Kiểm tra sống. |

```bash
cd apps/edge-worker
npm ci --ignore-scripts
rustup target add wasm32-unknown-unknown
cargo install worker-build --version 0.8.5 --locked

npx wrangler d1 create uth-notifier-edge
cp wrangler.toml wrangler.production.toml   # điền database_id; tệp này không được commit
npx wrangler d1 migrations apply uth-notifier-edge --remote --config wrangler.production.toml
npx wrangler secret put TELEGRAM_WEBHOOK_SECRET --config wrangler.production.toml
npx wrangler secret put EDGE_SYNC_TOKEN --config wrangler.production.toml
npx wrangler secret put PAYOS_CHECKSUM_KEY --config wrangler.production.toml
npm run deploy
```

Sau khi deploy:

1. Đăng ký webhook Telegram: `https://api.telegram.org/bot<TOKEN>/setWebhook?url=<EDGE_URL>/telegram/webhook&secret_token=<TELEGRAM_WEBHOOK_SECRET>`.
2. Khai báo `<EDGE_URL>/payos/webhook` trong trang quản lý payOS.
3. Đặt `TELEGRAM_UPDATES_SOURCE=edge` cho `notify`, rồi chạy `reconcile-edge` với `EDGE_URL` và `EDGE_SYNC_TOKEN`.

### Windows (máy cá nhân)

- [`start-local.cmd`](start-local.cmd) chạy `notify` với tệp `.env` ở thư mục gốc. Script này cần container PostgreSQL tên `uth-notifier-postgres` và binary `target\release\uth-agent.exe`.
- [`scripts/install-runtime.ps1`](scripts/install-runtime.ps1) đăng ký bốn Scheduled Task `UTH Notifier <worker>` chạy khi đăng nhập; [`scripts/stop-runtime.ps1`](scripts/stop-runtime.ps1) dừng và vô hiệu hóa các task đó.
- Các script Windows chỉ đọc 8 biến trong `.env`: `DATABASE_URL`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ADMIN_CHAT_ID`, `TELEGRAM_ADMIN_ONLY`, `EDGE_URL`, `TELEGRAM_UPDATES_SOURCE`, `PORTAL_NOTIFICATIONS_ENABLED`, `PORTAL_API_BASE`.
- Scheduler mặc định đọc `results\facebook_drl_sources.json`; có thể chép [`config/facebook-sources.v1.json`](config/facebook-sources.v1.json) sang đó. `edge-reconciler` đọc token từ `deploy\secrets\edge_sync_token`.

## Vận hành

### Kiểm tra sức khỏe

`uth-agent health` in báo cáo `operational-health.v1` với một trong ba trạng thái:

| Trạng thái | Khi nào |
|---|---|
| `failed` | Có dead letter, lượt gửi hoặc bản tin thất bại, sự kiện edge bị loại bỏ, giao dịch ủng hộ thất bại, hoặc lỗi nguồn mang tính hệ thống (từ 3 nguồn và ít nhất 1/4 tổng số nguồn đang cảnh báo). |
| `degraded` | Không có nguồn nào, có nguồn chưa từng crawl, crawl trễ lịch, có nguồn đang cảnh báo, hàng đợi tồn quá 15 phút, hoặc có người đăng ký nhưng worker Telegram không hoạt động. |
| `healthy` | Không có điều kiện nào ở trên. |

`notify` cũng gửi cảnh báo tới `TELEGRAM_ADMIN_CHAT_ID` khi trạng thái chuyển sang xấu hơn hoặc hồi phục.

### Sao lưu và khôi phục

- **systemd:** [`deploy/backup-native.sh`](deploy/backup-native.sh) tạo bản `pg_dump` định dạng custom kèm `.sha256`, kiểm tra bằng `pg_restore --list`, rồi xoá bản cũ hơn `BACKUP_RETENTION_DAYS` (mặc định 14 ngày).
- **Docker Compose:** [`deploy/backup.sh`](deploy/backup.sh) và [`deploy/restore.sh`](deploy/restore.sh). Bước khôi phục kiểm tra checksum trước khi chạy `pg_restore --clean`.

### Thời gian lưu dữ liệu

| Dữ liệu | Mặc định | Tham số |
|---|---|---|
| Lịch sử crawl | 30 ngày | `crawl-scheduled --run-retention-days` |
| Sự kiện đã xử lý, dead letter | 30 ngày | `classify --processed-event-retention-days`, `--dead-letter-retention-days` |
| Lượt gửi thành công / thất bại | 90 / 30 ngày | `notify --sent-delivery-retention-days`, `--failed-delivery-retention-days` |
| Người đăng ký đã ngừng | 90 ngày | `notify --inactive-subscriber-retention-days` |
| Góp ý, đánh giá, sự kiện sử dụng | 180 ngày | cố định |
| Sự kiện edge đã xử lý | 30 ngày | `reconcile-edge --processed-retention-days` |

## Phát triển và kiểm thử

```bash
# Rust
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# Integration test với PostgreSQL thật (chạy tuần tự)
TEST_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/uth_notifier_test \
  cargo test -p uth-storage --test postgres_storage -- --ignored --test-threads=1
# Trên Windows: scripts/test-integration.ps1 tự dựng container postgres:17-alpine

# Browser agent
cd apps/browser-agent && npm ci --ignore-scripts && npm test && npm run typecheck && npm run build

# Kiểm tra hồi quy bộ phân loại (CI yêu cầu precision và recall 100%)
cargo run -p uth-agent -- evaluate-classifier \
  --minimum-precision-basis-points 10000 --minimum-recall-basis-points 10000
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) chạy các bước trên, cùng với:

- `npm audit` và `cargo audit`;
- build thử edge worker (`wrangler deploy --dry-run`);
- kiểm tra cấu hình Docker Compose và script shell;
- [`scripts/check-publication.ps1`](scripts/check-publication.ps1), đảm bảo không commit tệp bí mật hay cấu hình production.

### Cập nhật bộ phân loại

1. Crawl dữ liệu thật: `uth-agent crawl <URL> --output crawl-report.json`.
2. Tạo gói rà soát: `uth-agent prepare-classifier-review crawl-report.json --output review.json --markdown-output review.md`.
3. Gán nhãn thủ công, rồi chốt tập đánh giá: `uth-agent finalize-classifier-review review.json human-labels.json --output-review review-final.json --output-dataset evaluation.json --markdown-output review-final.md`.
4. Chỉnh [`config/classifier-rules.v1.json`](config/classifier-rules.v1.json) và chạy `evaluate-classifier --dataset evaluation.json` cho tới khi đạt ngưỡng.

## Bảo mật và quyền riêng tư

- **Không thu thập thông tin đăng nhập:** hệ thống không dùng tài khoản, mật khẩu, cookie hay token Facebook/Portal; chỉ đọc nội dung công khai.
- **Xác thực ở edge:** webhook Telegram dùng secret token, webhook payOS dùng chữ ký checksum, endpoint nội bộ dùng Bearer token so sánh thời gian hằng. Kích thước webhook giới hạn 256 KiB.
- **Tối thiểu hóa dữ liệu:** góp ý giới hạn 2.000 ký tự; góp ý, đánh giá và sự kiện sử dụng tự xoá sau 180 ngày; log không in thông tin tài khoản thanh toán.
- **Cô lập khi chạy:** unit systemd bật `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome` và giới hạn bộ nhớ; container worker trong Compose chạy chỉ đọc và bỏ mọi capability.
- **Báo lỗ hổng:** làm theo [SECURITY.md](SECURITY.md), không mở issue công khai.

## Đóng góp

Xem [CONTRIBUTING.md](CONTRIBUTING.md). Tóm tắt: giữ thay đổi gọn, chạy các bước kiểm tra ở trên trước khi mở pull request, dùng dữ liệu giả lập trong test và log, và không commit `.env`, bí mật, cấu hình production hay dữ liệu crawl thô.

## Giấy phép

Phát hành theo [Giấy phép MIT](LICENSE).
