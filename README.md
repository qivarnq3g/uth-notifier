# UTH Notifier

Bot theo dõi hoạt động Điểm rèn luyện (ĐRL) và thông báo công khai từ Portal UTH,
sau đó gửi qua Telegram. Dự án ưu tiên độ chính xác, khả năng chịu lỗi và hiệu quả
tài nguyên trong ràng buộc chi phí bắt buộc bằng 0 đồng.

## Tài liệu

- [Kiến trúc hệ thống](docs/architecture.md): quyết định kiến trúc hybrid multi-cloud, các tầng fallback, mô hình dữ liệu và chính sách zero-cost.
- [Facebook crawler reliability](docs/facebook-crawler.md): contract, health outcome, fallback và kết quả kiểm chứng.
- [Rules classifier](docs/classifier.md): decision contract, feature, worker và giới hạn hiện tại.
- [Telegram delivery](docs/telegram-delivery.md): người nhận, gửi lại, giới hạn tốc độ và vận hành.
- [Cloudflare Telegram ingress](docs/edge-ingress.md): D1, secrets, webhook, reconciler và rollback.
- [Server deployment](docs/server-deployment.md): Docker Compose, secrets, healthcheck, backup tự động và restore.
- [Cách dùng bot](docs/user-guide.md): hướng dẫn ngắn dành cho người dùng Telegram.
- [Development setup](docs/development.md): Rust toolchain và các lệnh kiểm tra bắt buộc.

> Trạng thái hiện tại: Rust core crawler cho Facebook Page công khai đã chạy
> live và có fixture bảo vệ contract. PostgreSQL durable crawl scheduler đã được
> triển khai. Explainable rules classifier và durable outbox worker đã được triển
> khai. Telegram notification planner và delivery worker đã được triển khai. Các
> revision của cùng một bài Facebook chỉ tạo tối đa một campaign; link Telegram ưu
> tiên page ID số và post ID số, hoặc page ID số với `pfbid` hợp lệ khi ID bài số chưa có.
> Bản tin 07:30 gộp các campaign cùng một bài theo `post_id`, dùng revision mới nhất để
> hiển thị nhưng vẫn giữ toàn bộ campaign ID trong lịch sử audit; việc cắt nội dung Unicode
> được thực hiện trong Rust để tương thích PostgreSQL production `SQL_ASCII`.
> Cloudflare Telegram ingress, D1 ledger và PostgreSQL reconciler đã được triển khai
> live. Runtime production hiện chạy native trên Debian 13 LXC với 2 CPU, 2 GB RAM,
> PostgreSQL và bốn systemd worker. Bốn Scheduled Task Windows được giữ disabled chỉ
> để rollback. Người dùng Telegram có thể tự đăng ký nhận thông báo; chat ID cấu hình
> trong `TELEGRAM_ADMIN_CHAT_ID` là quản trị viên duy nhất và có quyền cao nhất.
> Notifier lưu trạng thái health trong PostgreSQL, chủ động báo admin khi lỗi nghiêm
> trọng, khi degraded kéo dài và khi hệ thống duy trì phục hồi đủ ngưỡng mà không gửi
> lặp sau restart.
> Notifier kiểm tra API công khai của Portal UTH mỗi 60 giây. Lần chạy đầu chỉ
> ghi nhận thông báo mới nhất làm baseline; từ đó mỗi ID mới tạo một campaign bắt
> buộc cho cả người đang nhận tin hoạt động lẫn người đã dùng `/stop`. Nếu Portal
> công bố tệp, bot tải tối đa 50 MiB từ đúng endpoint chính thức, upload một lần
> lên Telegram rồi tái sử dụng `file_id` cho các người nhận còn lại. Tối đa 20
> thông báo gần nhất được lưu làm lịch sử ban đầu mà không gửi lại cho người dùng.
> Classifier tự gửi form đăng ký sinh viên từ host tin cậy khi nguồn đã duyệt và
> bài có đủ bằng chứng đăng ký, đối tượng cùng ngữ cảnh hoạt động; form yếu vẫn chờ duyệt.
> Crawl scheduler dùng lịch thích ứng bền vững 120/300/480 giây để ưu tiên page vừa có
> post mới mà không vượt tải máy chủ; migration và đường live đã được xác minh ngày
> 2026-07-23.
> HTTP crawler có circuit breaker theo presentation, khôi phục trạng thái từ lịch sử
> attempt trong PostgreSQL và chỉ cho một probe sau cooldown; health crawler được tính
> theo từng nguồn và quorum thay vì coi một Page lỗi là toàn hệ thống lỗi.
> Browser fallback production bounded-sweep Page Plugin và page route, thu response
> GraphQL có giới hạn ngay trong phiên Playwright công khai rồi hợp nhất với cửa sổ HTTP;
> final URL `/login/` không được phép trở thành false `healthy` qua DOM hint. Hệ thống vẫn
> giữ DOM làm fallback và không dùng cookie, session hay phát lại request nội bộ. Scheduler
> và batch crawler ưu tiên `/people/<alias>/<page-id>/` cho nguồn alias, dùng
> `profile.php?id=<page-id>` làm fallback hữu hạn, đồng thời giữ nguyên `/people/` đã xác
> minh trong cấu hình; URL cấu hình thân thiện vẫn được giữ trong contract và giao diện.
> Donate hybrid payOS đã được triển khai: bot đưa ba mức gợi ý cùng lựa chọn nhập
> số tiền tùy tâm bằng trạng thái PostgreSQL có TTL, tạo
> link thanh toán riêng, gửi ảnh VietQR Pro trực tiếp trong Telegram với QR cục bộ
> làm fallback, webhook
> được xác minh chữ ký tại Cloudflare, D1 chống mất sự kiện và PostgreSQL đối soát
> idempotent; caption chỉ hiển thị STK gốc cấu hình tại runtime, không hiển thị số
> định danh payOS; VietQR tĩnh vẫn là fallback khi payOS không sẵn sàng.
> Trải nghiệm tăng trưởng đã được triển khai: deep-link `/start` lưu nguồn chiến
> dịch, người mới xem tin mẫu và chọn chỉ ĐRL hoặc mọi hoạt động trước khi bật
> nhận tin. Nội dung bắt đầu và trợ giúp mời người dùng tham gia nhóm hỗ trợ tại
> `https://t.me/uth_notifier_group`. Người dùng có thể chọn nhận từng tin ngay khi phát hiện hoặc một bản tin lúc 07:30; giờ
> yên lặng 22:00–07:00. Tin mới hiển thị nguồn, ngày liên quan, trạng thái biểu mẫu,
> nút đăng ký, bài gốc và phản hồi mức phù hợp. Product event tối thiểu được lưu
> trong PostgreSQL; admin xem kết quả 7 ngày bằng `/metrics` và lịch sử feedback kèm liên kết tới người gửi bằng `/feedbacks`. Mọi người dùng có thể xem lịch sử
> Portal bằng `/portal_history`, mở `/portal_notice_ID` để nhận lại tệp đính kèm nếu có, và xem bài Facebook đã crawl bằng `/latest`. Quản trị viên xem toàn bộ lịch sử hoạt động crawl bằng `/crawl_history` và chi tiết từng lần bằng `/crawl_run_ID`. Người dùng có thể chủ
> động chọn nút `Ủng hộ` hoặc lệnh `/donate`; lời mời theo ngữ cảnh chỉ xuất hiện
> một lần sau phản hồi hữu ích. Người dùng có thể gửi góp ý cho quản trị viên bằng
> `/feedback nội dung` hoặc gửi `/feedback` rồi nhắn nội dung trong 10 phút.
> Failover và local ML vẫn là kiến trúc đích; chủ hệ thống đã đưa backup
> off-site ra ngoài phạm vi triển khai hiện tại.

## Portal attachments

Portal notice ingestion resolves a missing official attachment from its public `daotao.ut.edu.vn` article before planning delivery. The resolver uses a bounded no-cookie HTTP request and only accepts a direct HTTPS WordPress upload PDF. Administrators can open `/portal_notice_ID` to resolve and persist an older missing PDF before Telegram sends it.

## Thử nghiệm crawl Facebook công khai

Build release binary:

```powershell
cargo build --release -p uth-agent
```

Crawl một lần, không dùng tài khoản, cookie hay access token:

```powershell
target/release/uth-agent crawl `
  https://www.facebook.com/hoisinhvien.com.vn `
  --probe-all `
  --limit 20 `
  --output results/facebook-report.json
```

So sánh với lần chạy trước để phân biệt bài mới, bài sửa và bài đã thấy:

```powershell
target/release/uth-agent crawl `
  https://www.facebook.com/hoisinhvien.com.vn `
  --baseline results/facebook-report.json `
  --limit 20 `
  --output results/facebook-report-next.json
```

Rust crawler đọc payload JSON được nhúng trong HTML công khai bằng byte scanner
không dựng DOM, chuẩn hóa post theo contract version hóa, tính content hash, thử
fallback và ghi health diagnostics. Không cần tài khoản, cookie hoặc access token.

Chạy test:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test-integration.ps1
```

Đo regression của rules classifier trên bộ dữ liệu gán nhãn có phiên bản:

```powershell
target/release/uth-agent evaluate-classifier `
  --minimum-precision-basis-points 10000 `
  --minimum-recall-basis-points 10000 `
  --output results/classifier-evaluation.json
```

Bộ fixture mặc định là dữ liệu tổng hợp để chặn thay đổi ngoài ý muốn, không phải
bằng chứng precision hoặc recall trên bài đăng thực tế.

Chuẩn bị bài thật từ một healthy crawl report để con người gán nhãn:

```powershell
target/release/uth-agent prepare-classifier-review `
  results/classifier-review/giadinhkynang-crawl.json `
  --output results/classifier-review/giadinhkynang-review.json `
  --markdown-output results/classifier-review/giadinhkynang-review.md
```

JSON giữ dữ liệu có cấu trúc để nhập lại vào evaluation pipeline. Markdown đánh số
từng bài, hiển thị dự đoán và để nhãn con người là `pending`.

Kết sổ nhãn con người thành dataset đánh giá tái lập:

```powershell
target/release/uth-agent finalize-classifier-review `
  results/classifier-review/giadinhkynang-review.json `
  results/classifier-review/giadinhkynang-human-labels.json `
  --output-review results/classifier-review/giadinhkynang-review-final.json `
  --output-dataset results/classifier-review/giadinhkynang-evaluation.v1.json `
  --markdown-output results/classifier-review/giadinhkynang-review-final.md
```

Thiết kế, contract và giới hạn vận hành được mô tả tại
[Facebook crawler reliability](docs/facebook-crawler.md). Đây vẫn là công cụ
kiểm chứng khả năng truy cập, chưa phải crawler production.

Quản trị bài phân loại mơ hồ ngay trong Telegram:

```text
/reviews
/review_ID
/review_send_ID
/review_skip_ID
/latest
/latest_post_ID
```

Các lệnh này chỉ chấp nhận chat ID cấu hình trong `TELEGRAM_ADMIN_CHAT_ID`. Quyết
định được lưu trong PostgreSQL và thao tác lặp không tạo campaign hoặc delivery trùng.

## Khám phá nguồn đang theo dõi

Browser agent dùng Chrome hệ thống, không đăng nhập, để paginate danh sách
following công khai:

```powershell
cd apps/browser-agent
npm install --ignore-scripts
npm run following -- `
  https://www.facebook.com/example-user/following `
  ../../results/facebook-following.json
```

## Crawl toàn bộ nguồn

Lệnh batch chạy HTTP trước, chỉ mở Chrome khi HTTP không có post hoặc khi URL dạng
`/people/` cần xác minh bài đầu feed:

```powershell
target/release/uth-agent crawl-all `
  results/facebook_drl_sources.json `
  --output-dir results/crawl-all `
  --concurrency 4 `
  --timeout 15 `
  --limit 10
```

Mỗi nguồn có một `facebook-crawl-report.v1` riêng. `summary.json` dùng contract
`facebook-crawl-batch-report.v1` và lệnh trả exit code khác 0 nếu còn nguồn không
có post khả dụng. Browser fallback không dùng tài khoản, cookie hoặc access token.

## Durable crawl scheduler

Scheduler tự áp dụng migration, đồng bộ danh sách nguồn, claim nguồn đến hạn bằng
lease PostgreSQL và chạy với concurrency hữu hạn:

```powershell
$env:DATABASE_URL = "postgresql://uth_agent@localhost/uth_notifier"
target/release/uth-agent crawl-scheduled `
  results/facebook_drl_sources.json `
  --concurrency 4 `
  --schedule-interval 300 `
  --active-schedule-interval 120 `
  --idle-schedule-interval 480 `
  --active-unchanged-crawls 3 `
  --idle-after-unchanged-crawls 6 `
  --base-backoff 60 `
  --max-backoff 3600 `
  --lease-duration 600 `
  --alert-after-failures 3 `
  --run-retention-days 30 `
  --retention-interval 86400
```

Lịch thích ứng được bật mặc định. Khi phát hiện post mới, nguồn được kiểm tra lại sau 120
giây và tiếp tục ở tầng nhanh trong ba lần crawl không đổi. Sau đó lịch trở về 300 giây,
rồi giãn tối đa 480 giây từ lần không đổi thứ sáu. Trạng thái này nằm trong PostgreSQL nên
không bị mất khi tiến trình restart. Dùng `--no-adaptive-schedule` để quay lại lịch cố định.
Các lần crawl `degraded` hoặc `failed` không được tính là nguồn im và vẫn dùng backoff lỗi.

Lần crawl `healthy` đầu tiên của mỗi nguồn được lưu làm dữ liệu nền nhưng không tạo
outbox event, vì vậy bot không gửi lại các bài lịch sử. Chỉ dùng
`--notify-existing-posts` khi chủ động muốn phát cả các bài đã tồn tại trước lúc
nguồn được thêm vào hệ thống. Khi không bật cờ này, các revision về sau của bài
có thời gian đăng trước baseline vẫn được lưu nhưng không tạo outbox event.

Mỗi cycle ghi một JSON line `crawl-scheduler-cycle.v1` để log collector theo dõi
health, số post mới/sửa/không đổi, số outbox event và retention. Chỉ report
`healthy` được upsert vào `posts`. Report `degraded` hoặc `failed` vẫn được ghi
audit và retry bằng exponential backoff có jitter, nhưng không được diễn giải là
post đã biến mất. Mỗi nguồn phát cờ `alert=true` khi đạt ngưỡng lỗi liên tiếp.

Kiểm tra toàn bộ trạng thái vận hành từ PostgreSQL:

```powershell
$env:DATABASE_URL = "postgresql://uth_agent@localhost/uth_notifier"
target/release/uth-agent health --require-healthy
```

Lệnh xuất `operational-health.v1` và trả exit code khác 0 khi trạng thái là
`degraded` hoặc `failed`. Snapshot bao phủ nguồn chưa crawl/quá hạn, lỗi liên tiếp,
backlog classifier/notification, dead letter, delivery và Telegram worker lease.

### Supervisor Windows tùy chọn

Các script Windows chỉ dành cho host Windows được chọn làm runtime, không được cài
tự động trên máy phát triển. Triển khai server chuẩn dùng
[Docker Compose](docs/server-deployment.md). Nếu chủ động dùng Windows server, tệp
`.env` phải chứa `DATABASE_URL`, `TELEGRAM_BOT_TOKEN` và
`TELEGRAM_ADMIN_CHAT_ID`:

```powershell
cargo build --release -p uth-agent
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/install-runtime.ps1
```

Ba task `UTH Notifier scheduler`, `UTH Notifier classifier` và
`UTH Notifier notify` tự khởi động lại worker với backoff hữu hạn. Log được giữ
14 ngày tại `results/runtime-logs/`. Dừng và vô hiệu hóa runtime bằng:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/stop-runtime.ps1
```

## Rules classifier worker

Worker claim các event `facebook_post.discovered` và `facebook_post.updated`, chạy
hard validation cùng explainable rules rồi ghi classification và completion event
trong cùng transaction:

```powershell
$env:DATABASE_URL = "postgresql://uth_agent@localhost/uth_notifier"
target/release/uth-agent classify `
  --config config/classifier-rules.v1.json `
  --concurrency 4 `
  --lease-duration 120 `
  --max-attempts 5 `
  --dead-letter-retention-days 30 `
  --processed-event-retention-days 30
```

Thêm `--once` để xử lý tối đa một batch rồi thoát. Event lỗi được retry với
exponential backoff và chuyển `dead_letters` khi đạt giới hạn attempt.

## Gửi thông báo Telegram

Thêm một người nhận bằng mã cuộc trò chuyện Telegram:

```powershell
$env:DATABASE_URL = "postgresql://uth_agent@localhost/uth_notifier"
target/release/uth-agent subscriber add --chat-id 123456789 --name "Admin"
```

Đặt khóa bot trong biến môi trường của terminal rồi chạy worker:

```powershell
$secureToken = Read-Host "Telegram bot token" -AsSecureString
$credential = [PSCredential]::new("telegram", $secureToken)
$env:TELEGRAM_BOT_TOKEN = $credential.GetNetworkCredential().Password

target/release/uth-agent notify --once
```

Bỏ `--once` khi chạy liên tục trên server. Khóa bot không được ghi vào repository,
log hoặc lịch sử lệnh. Máy chủ phải dùng kho bí mật thay cho tệp `.env`.

Tiến trình `notify` đồng thời nhận lệnh Telegram trong cuộc trò chuyện riêng.
Người dùng có thể bật hoặc tắt tin hoạt động, xem các trang đang theo dõi và gửi
link đề xuất trang mới để quản trị viên xét duyệt, hoặc gửi feedback tự do bằng
`/feedback`. `/stop`, phạm vi ĐRL, digest và
giờ yên lặng không tắt thông báo Portal bắt buộc. Danh sách nguồn hiển thị trực
tiếp từ PostgreSQL, không được ghi cứng trong bot.

### Chạy thử cục bộ bằng `.env`

Sao chép `.env.example` thành `.env`, điền token thử nghiệm và nhấp đúp
`start-local.cmd`. Tệp `.env` bị loại khỏi Git và chỉ dùng cho thử nghiệm
cục bộ. Hủy token sau khi thử xong. Không dùng cách này trên máy chủ.

`TELEGRAM_ADMIN_CHAT_ID` xác định cuộc trò chuyện nhận đề xuất trang mới. Quản trị
viên có thể dùng `/pending`, `/approve` và `/reject` ngay trong Telegram; các lệnh
này bị từ chối với người dùng thông thường.

## Bảo mật và quyền riêng tư

Không commit `.env`, `deploy/secrets/`, `results/`, `target/`, `tmp/`, cấu hình
production cục bộ hoặc dữ liệu Telegram. Chỉ tạo bản phát hành từ file được Git
theo dõi; không tải trực tiếp toàn bộ thư mục làm việc lên dịch vụ lưu trữ.

`apps/edge-worker/wrangler.toml` dùng D1 ID vô hiệu để build và kiểm tra an toàn.
Sao chép file này thành `wrangler.production.toml`, đặt D1 ID thật trong bản sao
cục bộ rồi dùng `npm run deploy`. File production bị Git và Docker loại trừ.

Báo cáo lỗ hổng theo [SECURITY.md](SECURITY.md). Dự án chỉ crawl nguồn công khai,
không dùng tài khoản, cookie, session, proxy hay cơ chế vượt kiểm soát truy cập.
Người triển khai chịu trách nhiệm tuân thủ điều khoản nền tảng và pháp luật áp dụng.

## Giấy phép

Mã nguồn được phát hành theo [MIT License](LICENSE).
