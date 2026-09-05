# UTH Notifier

Bot theo dõi hoạt động Điểm rèn luyện (ĐRL) và thông báo công khai từ Cổng đào tạo (Portal) UTH, sau đó gửi qua Telegram. Dự án ưu tiên độ chính xác, khả năng chịu lỗi và hiệu quả tài nguyên trong ràng buộc chi phí bắt buộc bằng 0 đồng.

---

## 1. Kiến trúc và Tính năng Nổi bật

### Kiến trúc Đa tầng (Multi-tier Architecture)

* **Core Agent (`apps/core-agent` - `uth-agent`):** Viết bằng Rust tối ưu tài nguyên, đảm nhiệm crawl HTTP byte-level, bộ phân loại luật kết hợp Gemini AI, điều phối Telegram delivery worker và quản lý trạng thái bền vững trong PostgreSQL.
* **Browser Agent (`apps/browser-agent`):** Ứng dụng Node.js TypeScript và Playwright headless Chromium, hoạt động như tầng fallback thứ cấp khi Facebook kích hoạt login wall.
* **Edge Ingress (`apps/edge-worker`):** Cloudflare Worker tiếp nhận Telegram webhook và payOS payment webhook, ghi nhận vào D1 ledger chống mất mát sự kiện trước khi Core Agent đối soát.
* **Cơ sở dữ liệu PostgreSQL:** Lưu trữ trạng thái bền vững với encoding `SQL_ASCII`, cắt chuỗi theo Unicode scalar trong Rust và áp đặt ràng buộc byte `octet_length <= 16384`.

### Tính năng Cốt lõi

1. **Thu thập Facebook Công khai:**
   * Quét trực tiếp JSON nhúng trong HTML công khai bằng byte scanner, không cần đăng nhập tài khoản, cookie hay token.
   * Phân cấp route thông minh: Ưu tiên `/people/<alias>/<id>/` (ổn định ~99.5%), fallback sang `profile.php?id=<id>`.
   * Định danh bài viết bằng ID số và `content_hash`, ngăn chặn gửi trùng lặp giữa các revision của cùng một bài.
   * Circuit breaker tự động ngắt sau 10 lần lỗi liên tiếp và cho phép 1 lần thăm dò sau cooldown.
2. **Thu thập Cổng đào tạo UTH (Portal Ingestion):**
   * Quét API công khai của `portal.ut.edu.vn` theo chu kỳ thích ứng (5 phút mặc định, tăng tốc 1 phút trong 15 phút khi có bài mới).
   * Cơ chế Zero-Spam: Tự động lưu trữ thông báo cũ hơn 48 giờ mà không gửi ra ngoài, ngăn ngừa spam dữ liệu lịch sử.
   * Tự động giải quyết và tải tệp đính kèm PDF chính thức từ `daotao.ut.edu.vn`, tải lên Telegram một lần và tái sử dụng `file_id`.
3. **Phân loại Thông minh 2 Tầng (Hybrid Classifier):**
   * **Tầng 1 (Explainable Rules Engine):** Trích xuất các tín hiệu `explicit_drl`, `registration_call`, `form_link`, `future_deadline`, `future_event_time`, `target_students`. Áp dụng quy tắc `risk.restricted_audience` để giữ lại các bài giới hạn khoa/khóa cho quản trị viên duyệt.
   * **Tầng 2 (Gemini AI Auto-Reviewer):** Sử dụng mô hình `gemini-3.5-flash-lite` với cơ chế dynamic few-shot prompt. Lọc triệt để bài đăng quá 3 ngày hoặc sự kiện đã diễn ra; trả về lý do bằng tiếng Việt chuẩn có dấu; tự động học hỏi từ các quyết định điều chỉnh của quản trị viên (`/ai_approve`, `/ai_reject`).
4. **Trải nghiệm Telegram Toàn diện:**
   * **Dành cho Sinh viên:**
     * Bật/tắt nhận tin hoạt động (`/start`, `/stop`). Lưu ý: Thông báo Portal là thông báo quan trọng, luôn được gửi ngay lập tức.
     * Cài đặt linh hoạt (`/settings`): Chọn chỉ nhận tin có ĐRL hoặc toàn bộ hoạt động; chọn nhận tức thì (instant) hoặc nhận bản tin tổng hợp lúc 07:30 sáng; bật giờ yên lặng (22:00 - 07:00).
     * Tra cứu sự kiện và học bổng đang mở (`/events`): Xem danh sách các hoạt động còn hạn trong 14 ngày qua kèm link đăng ký trực tiếp.
     * Xem bài viết mới nhất (`/latest`, `/latest_post_ID`).
     * Xem lịch sử Portal và tải lại file đính kèm (`/portal_history`, `/portal_notice_ID`).
     * Xem danh sách trang đang theo dõi (`/pages`) và đề xuất trang mới (`/suggest <link>`).
     * Gửi phản hồi, góp ý tự do cho ban quản trị (`/feedback`).
     * Tự nguyện ủng hộ chi phí vận hành qua VietQR Pro hybrid payOS (`/donate`).
   * **Dành cho Quản trị viên:**
     * Menu quản trị trung tâm (`/admin`).
     * Duyệt trang đề xuất (`/pending`, `/approve <id> <tên>`, `/reject <id> <lý do>`).
     * Rà soát bài phân loại chưa rõ ràng (`/reviews`, `/review_send_<id>`, `/review_skip_<id>`).
     * Can thiệp và cập nhật bộ nhớ học cho Gemini AI (`/ai_approve <id>`, `/ai_reject <id>`).
     * Theo dõi chỉ số tăng trưởng và mức độ hài lòng (`/metrics`).
     * Xem và phản hồi ý kiến sinh viên (`/feedbacks`).
     * Tra cứu lịch sử vận hành crawler (`/crawl_history`, `/crawl_run_<id>`).
     * Xuất tệp báo cáo vận hành hệ thống định dạng Markdown (`/report`).

---

## 2. Lệnh Thao tác trong Telegram Bot

| Lệnh | Đối tượng | Mô tả chức năng |
|---|---|---|
| `/start` | Sinh viên | Bắt đầu sử dụng bot, chọn loại hoạt động và cách nhận tin |
| `/settings` | Sinh viên | Thay đổi chế độ nhận tin (tức thì / 07:30), giờ yên lặng, phạm vi ĐRL |
| `/events` | Sinh viên | Xem danh sách các hoạt động, học bổng đang mở trong 14 ngày qua |
| `/latest` | Sinh viên | Xem các bài viết Facebook mới nhất được hệ thống thu thập |
| `/portal_history` | Sinh viên | Xem lịch sử thông báo Portal UTH và tải lại tệp đính kèm |
| `/pages` | Sinh viên | Xem danh sách các fanpage đang được theo dõi trong hệ thống |
| `/suggest <link>` | Sinh viên | Đề xuất fanpage Facebook mới để quản trị viên xét duyệt |
| `/feedback <nội dung>` | Sinh viên | Gửi ý kiến góp ý cho ban quản trị (tối đa 2.000 ký tự) |
| `/donate` | Sinh viên | Ủng hộ chi phí duy trì máy chủ tự nguyện qua VietQR Pro |
| `/help` | Sinh viên | Xem hướng dẫn sử dụng bot |
| `/status` | Sinh viên | Kiểm tra trạng thái đăng ký nhận tin của tài khoản |
| `/stop` | Sinh viên | Tạm dừng nhận thông báo hoạt động Facebook (vẫn nhận tin Portal) |
| `/admin` | Quản trị viên | Bảng điều khiển quản trị viên |
| `/pending` | Quản trị viên | Xem danh sách các fanpage sinh viên đề xuất đang chờ duyệt |
| `/approve <id> <tên>` | Quản trị viên | Duyệt và kích hoạt fanpage vào danh sách theo dõi |
| `/reject <id> <lý do>` | Quản trị viên | Từ chối fanpage được đề xuất |
| `/reviews` | Quản trị viên | Danh sách bài viết Facebook đang chờ con người rà soát |
| `/review_send_<id>` | Quản trị viên | Duyệt và phát thông báo bài viết cho sinh viên |
| `/review_skip_<id>` | Quản trị viên | Bỏ qua bài viết |
| `/ai_approve <id>` | Quản trị viên | Đảo ngược quyết định AI thành duyệt và ghi vào bộ nhớ học |
| `/ai_reject <id>` | Quản trị viên | Đảo ngược quyết định AI thành bỏ qua và ghi vào bộ nhớ học |
| `/metrics` | Quản trị viên | Xem thống kê người dùng, tỉ lệ tương tác và phản hồi 7 ngày |
| `/feedbacks` | Quản trị viên | Xem danh sách góp ý của sinh viên kèm link liên hệ trực tiếp |
| `/crawl_history` | Quản trị viên | Xem nhật ký tất cả các lần crawl của các nguồn |
| `/crawl_run_<id>` | Quản trị viên | Xem chi tiết lượt thử và thông số kỹ thuật của 1 lần crawl |
| `/report` | Quản trị viên | Xuất toàn bộ báo cáo trạng thái hệ thống thành tệp Markdown |

---

## 3. Cài đặt và Lệnh Vận hành Kỹ thuật

### Yêu cầu Môi trường

* **Rust:** Phiên bản được định nghĩa trong `rust-toolchain.toml`, khóa phụ thuộc trong `Cargo.lock`.
* **Node.js:** Phiên bản 20+ (dành cho `apps/browser-agent`).
* **PostgreSQL:** Phiên bản 15+ (tương thích môi trường production `SQL_ASCII`).

### Biên dịch và Kiểm tra Chất lượng Mã nguồn

Biên dịch nhị phân release:

```powershell
cargo build --release -p uth-agent
```

Kiểm tra định dạng, cảnh báo và kiểm thử đơn vị:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Chạy kiểm thử tích hợp cơ sở dữ liệu:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test-integration.ps1
```

### Các Lệnh CLI của `uth-agent`

Crawl thử nghiệm một trang Facebook công khai:

```powershell
target/release/uth-agent crawl `
  https://www.facebook.com/hoisinhvien.com.vn `
  --probe-all `
  --limit 20 `
  --output results/facebook-report.json
```

Crawl toàn bộ danh sách nguồn:

```powershell
target/release/uth-agent crawl-all `
  results/facebook_drl_sources.json `
  --output-dir results/crawl-all `
  --concurrency 4 `
  --timeout 15 `
  --limit 10
```

Khởi chạy Durable Crawl Scheduler với lịch thích ứng:

```powershell
$env:DATABASE_URL = "postgresql://uth_agent@localhost/uth_notifier"
target/release/uth-agent crawl-scheduled `
  results/facebook_drl_sources.json `
  --concurrency 4 `
  --schedule-interval 300 `
  --active-schedule-interval 120 `
  --idle-schedule-interval 480 `
  --lease-duration 600
```

Khởi chạy Rules Classifier Worker:

```powershell
$env:DATABASE_URL = "postgresql://uth_agent@localhost/uth_notifier"
target/release/uth-agent classify `
  --config config/classifier-rules.v1.json `
  --concurrency 4 `
  --lease-duration 120
```

Khởi chạy Telegram Notification Worker (hỗ trợ tích hợp Gemini AI Auto-Reviewer):

```powershell
$env:DATABASE_URL = "postgresql://uth_agent@localhost/uth_notifier"
$env:TELEGRAM_BOT_TOKEN = "your_bot_token"
$env:TELEGRAM_ADMIN_CHAT_ID = "your_admin_chat_id"
$env:GEMINI_API_KEY = "your_gemini_api_key"

target/release/uth-agent notify
```

Kiểm tra trạng thái sức khỏe vận hành toàn diện:

```powershell
$env:DATABASE_URL = "postgresql://uth_agent@localhost/uth_notifier"
target/release/uth-agent health --require-healthy
```

---

## 4. Đánh giá Classifier và Rà soát Bài đăng

Đánh giá hồi quy của bộ phân loại luật trên tập dữ liệu kiểm thử:

```powershell
target/release/uth-agent evaluate-classifier `
  --minimum-precision-basis-points 10000 `
  --minimum-recall-basis-points 10000 `
  --output results/classifier-evaluation.json
```

Chuẩn bị dữ liệu bài đăng thực tế từ báo cáo crawl để người rà soát:

```powershell
target/release/uth-agent prepare-classifier-review `
  results/classifier-review/crawl-report.json `
  --output results/classifier-review/review.json `
  --markdown-output results/classifier-review/review.md
```

Kết sổ nhãn con người thành tập dữ liệu đánh giá tái lập:

```powershell
target/release/uth-agent finalize-classifier-review `
  results/classifier-review/review.json `
  results/classifier-review/human-labels.json `
  --output-review results/classifier-review/review-final.json `
  --output-dataset results/classifier-review/evaluation.v1.json `
  --markdown-output results/classifier-review/review-final.md
```

---

## 5. Bảo mật, Quyền riêng tư và Chính sách

* **Không lưu trữ dữ liệu nhạy cảm:** Tuyệt đối không lưu trữ tài khoản, mật khẩu sinh viên, cookie phiên đăng nhập hay thông tin danh tính cá nhân (PII).
* **Bảo mật giao dịch:** Dữ liệu thanh toán ngân hàng đối soát được kiểm tra theo khóa lược đồ và số lượng, không in trực tiếp thông tin tài khoản đối ứng trong log.
* **Quyền riêng tư phản hồi:** Tin nhắn góp ý từ sinh viên được giới hạn 2.000 ký tự và tuân theo chính sách tự động xóa sau 180 ngày.
* **Chính sách Không Chi phí Bắt buộc (Zero Mandatory Cost):** Hệ thống được thiết kế để vận hành hoàn toàn trên hạ tầng tự lưu trữ (self-hosted Linux) và các gói miễn phí hợp lệ (Cloudflare Workers free tier), không phụ thuộc dịch vụ tính phí.

---

## 6. Giấy phép

Mã nguồn dự án được phát hành theo [MIT License](LICENSE).
