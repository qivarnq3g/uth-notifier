# Kiến trúc UTH Activity Notifier

- **Trạng thái:** Được chọn
- **Ngày cập nhật:** 2026-07-27
- **Ngày kiểm chứng quota:** 2026-07-19
- **Phạm vi:** Kiến trúc đích và các ràng buộc triển khai

Hiện trạng triển khai bao phủ data plane một server và nền tảng Telegram ingress
gồm Rust Cloudflare Worker, D1 ledger và PostgreSQL reconciler. Worker đã qua build
WASM và Wrangler dry-run nhưng chưa được triển khai vào tài khoản Cloudflare thật.
payOS, off-site backup và failover trong tài liệu này vẫn là kiến trúc đích, chưa
phải kết quả đã triển khai hoặc đo kiểm.

## 1. Bối cảnh và mục tiêu

Kiến trúc này thay thế định hướng `một Go process + PostgreSQL local`. Phương án cũ dễ xây nhưng không còn phù hợp với yêu cầu thực tế:

> Hệ thống phải mạnh, chính xác, chịu lỗi và tối ưu tài nguyên nhất có thể; có thể rất phức tạp, nhưng chi phí bắt buộc phải bằng 0 đồng.

Thứ tự ưu tiên:

1. Không bỏ sót hoạt động ĐRL.
2. Không gửi rác hoặc gửi trùng.
3. Crawler vẫn hoạt động khi Facebook thay đổi một đường truy cập.
4. Bot vẫn nhận lệnh khi máy chủ crawler chết.
5. Không mất job hoặc webhook khi deploy/restart.
6. Tự động khôi phục và đối soát.
7. Dùng ít CPU, RAM và băng thông nhất có thể.
8. Không phát sinh hóa đơn.
9. Độ đơn giản không phải tiêu chí tối ưu.

Chỉ thêm độ phức tạp khi nó cải thiện ít nhất một chỉ số đo được. Không dùng Kubernetes hoặc tách microservice chỉ để hệ thống trông chuyên nghiệp.

## 2. Quyết định kiến trúc

Chọn kiến trúc **hybrid multi-cloud, event-driven, active edge với các execution path warm/cold standby**. Đây không phải PostgreSQL high availability active-standby: khi OCI mất hoàn toàn, hệ thống chuyển sang chế độ degraded operation rồi disaster recovery từ backup.

```text
                         INTERNET
                             │
              ┌──────────────┴──────────────┐
              │                             │
       Telegram Webhook              payOS Webhook
              │                             │
              └──────────────┬──────────────┘
                             ▼
┌─────────────────────────────────────────────────────────┐
│                 CLOUDFLARE CONTROL PLANE                │
│                                                         │
│  Workers                                                │
│  ├── Telegram ingress                                   │
│  ├── Donation ingress                                   │
│  ├── API/admin endpoints                                │
│  ├── Request authentication                             │
│  └── Emergency Telegram delivery                       │
│                                                         │
│  D1                  Queues              R2             │
│  ├── durable inbox    ├── crawl jobs     ├── backups    │
│  ├── event ledger    ├── classify jobs  ├── fixtures   │
│  ├── edge cache      └── delivery jobs  └── raw pages  │
│  └── health state                                      │
│                                                         │
│  Cron Triggers   Browser Run   Workers AI               │
│  └── scheduling  └── fallback  └── optional fallback   │
└──────────────────────────┬──────────────────────────────┘
                           │ authenticated pull/tunnel
                           ▼
┌─────────────────────────────────────────────────────────┐
│             ORACLE CLOUD ALWAYS FREE DATA PLANE         │
│                                                         │
│  Rust Core                                              │
│  ├── crawler orchestration                              │
│  ├── Portal notice polling                              │
│  ├── normalization                                      │
│  ├── rule classifier                                    │
│  ├── Telegram delivery                                  │
│  └── reconciliation                                     │
│                                                         │
│  PostgreSQL                                             │
│  ├── source of truth                                    │
│  ├── posts/classifications                              │
│  ├── subscribers/deliveries                             │
│  ├── transactional outbox                              │
│  └── audit/history                                      │
│                                                         │
│  Node.js + Playwright          Local ONNX model          │
│  └── browser fallback          └── ambiguous posts       │
└──────────────────────────┬──────────────────────────────┘
                           │ encrypted backups
                           ▼
                     Cloudflare R2

                     TERTIARY FALLBACK
                     GitHub Actions
                     ├── CI/security
                     ├── emergency crawl/classify
                     └── restore validation
```

Cloudflare là control plane hoạt động độc lập với OCI. OCI là data plane mạnh nhưng có thể được dựng lại. PostgreSQL là nguồn sự thật nghiệp vụ. D1, Queues và R2 tạo lớp đệm để ingress tiếp tục nhận event, job có thể được tái tạo và PostgreSQL có thể được phục hồi trong các mục tiêu RPO/RTO ở phần 9. Không thành phần cloud nào được coi là luôn sẵn sàng tuyệt đối.

## 3. Vì sao không đặt toàn bộ trên một máy chủ?

Một máy chủ duy nhất tạo ra nhiều điểm lỗi chung:

- Restart máy làm Telegram ngừng nhận lệnh.
- PostgreSQL lỗi khiến crawler, bot và donate cùng ngừng.
- Chromium hết RAM có thể ảnh hưởng toàn process.
- Oracle thu hồi VM có thể làm mất toàn bộ dịch vụ.
- IP của máy chủ bị Facebook hạn chế khiến mọi crawler cùng thất bại.

OCI Always Free hiện cung cấp tổng mức Ampere A1 tương đương **2 OCPU và 12 GB RAM**, bên cạnh tối đa hai micro-instance AMD. Oracle có thể thu hồi instance Always Free bị coi là idle nếu CPU, network và, với A1, RAM đều dưới ngưỡng trong cửa sổ đánh giá bảy ngày. Vì vậy OCI không được là điểm vào duy nhất của bot. Xem [Oracle Always Free Resources][oracle-free].

Cloudflare Workers phù hợp làm control plane độc lập với VM. Workers Free hiện giới hạn 100.000 request/ngày, 128 MB RAM, 10 ms CPU cho mỗi HTTP invocation, 50 external subrequest/request và tối đa năm Cron Trigger. Xem [Cloudflare Workers limits][workers-limits].

## 4. Phân chia ngôn ngữ

Không ép toàn hệ thống dùng một ngôn ngữ.

| Thành phần | Ngôn ngữ | Lý do |
| --- | --- | --- |
| Cloudflare Workers | **Rust/WASM** | Đồng nhất contract và runtime production; Workers SDK hỗ trợ D1 |
| Crawler HTTP, classifier, worker | **Rust** | RAM thấp, không GC, kiểm soát concurrency tốt |
| Browser automation | **TypeScript + Playwright** | Hệ sinh thái Playwright chính thức và đầy đủ |
| Database | PostgreSQL SQL | Transaction, outbox và queue claim |
| ML inference | Rust; ONNX Runtime ở production | Model cục bộ, không tốn API |
| Infrastructure | OpenTofu/Terraform + Wrangler | Tái tạo được toàn bộ hạ tầng |
| Contract | JSON Schema/OpenAPI/Protobuf | Tránh việc các service hiểu payload khác nhau |

Rust dùng cho toàn bộ runtime production, gồm Workers. TypeScript chỉ dùng với
Playwright trong browser automation. Đây là polyglot có kiểm soát: contract phải
được version hóa, consumer phải được kiểm thử, và không được thay contract mà
không cập nhật toàn bộ consumer liên quan.

## 5. Cloudflare control plane

### 5.1. Telegram webhook

Chuyển từ long polling sang webhook:

```text
Telegram
   ↓ HTTPS
Cloudflare Worker
   ↓ xác thực + ghi event
D1 durable inbox
   ↓
Processing worker
```

Worker phải kiểm tra header `X-Telegram-Bot-Api-Secret-Token`. Telegram hỗ trợ `secret_token` khi gọi `setWebhook`; `update_id` được dùng làm khóa idempotency để bỏ qua update lặp hoặc xử lý update đến sai thứ tự. Xem [Telegram Bot API][telegram-api].

Lợi ích:

- Ingress vẫn nhận và ghi bền `/start`, `/stop`, `/status` khi OCI restart.
- Không có hai poller tranh cùng token.
- Không cần mở port trên VM.
- Ingress có thể phản hồi nhanh rồi xử lý bất đồng bộ.

Khi PostgreSQL không sẵn sàng, Worker chỉ xác nhận rằng command đã được tiếp nhận:

- `/start` và `/stop` được ghi vào D1 với `update_id` và trạng thái `pending_sync`; thay đổi subscription chỉ có hiệu lực sau khi PostgreSQL commit và trạng thái chuyển thành `applied`.
- `/status` trả dữ liệu cache kèm `as_of` và cờ `degraded=true`; không được trình bày cache cũ như trạng thái hiện tại.
- Reconciler đồng bộ command theo thứ tự trên từng `chat_id`; unique key trong PostgreSQL ngăn áp dụng lại cùng update.

### 5.2. D1 là event inbox, không phải database chính

D1 Free hiện có 5 triệu row read/ngày, 100.000 row write/ngày và 5 GB tổng dung lượng. Xem [D1 pricing][d1-pricing].

D1 chỉ giữ:

- Telegram update chưa đồng bộ.
- payOS webhook.
- Job ledger.
- Cache trạng thái subscriber.
- Health/failover state.
- Idempotency key ở edge.

Không lưu toàn bộ lịch sử `deliveries` dài hạn trong D1. PostgreSQL vẫn là nguồn dữ liệu nghiệp vụ chính.

Trước production phải lập budget D1 từ workload đo được, tối thiểu theo `telegram_updates + payment_events + ledger_state_transitions + retries + index_write_amplification`. Guard nội bộ phải từ chối workload không thiết yếu trước giới hạn chính thức; không suy quota chỉ từ số subscriber.

### 5.3. Cloudflare Queues

Tách queue theo workload:

```text
crawl-jobs
classify-jobs
delivery-jobs
payment-events
dead-letter
```

Workers Free hiện có 10.000 Queue operation/ngày tính trên tổng read, write và delete; message chỉ được giữ tối đa 24 giờ. Queue không được là nguồn dữ liệu duy nhất. Xem [Queues Free changelog][queues-free] và [Queues overview][queues-overview].

Ownership của job được phân định rõ:

- D1 sở hữu ingress event và edge job cho đến khi chúng được import vào PostgreSQL.
- PostgreSQL `outbox_events` sở hữu mọi job nghiệp vụ sau import, gồm crawl, classify, campaign và delivery.
- Mọi event có một `event_id` ổn định xuyên suốt D1, Queue và PostgreSQL.

D1 và Queue không có distributed transaction. Edge publisher dùng state machine `pending → publishing → published → acknowledged`; reconciler đưa lease `publishing` quá hạn về `pending` và publish lại với cùng `event_id`. Consumer phải idempotent. Sau khi PostgreSQL commit bản import, edge event chuyển thành `acknowledged`; từ thời điểm đó PostgreSQL outbox là ledger duy nhất cho downstream job.

Queues hỗ trợ batching, retry, delay, dead-letter queue và pull consumer từ hạ tầng ngoài Cloudflare. Rust worker trên OCI có thể kéo job mà không cần public endpoint nhận push.

### 5.4. R2

R2 lưu:

- Bản sao lưu PostgreSQL đã mã hóa.
- Raw HTML/JSON của crawler.
- Fixture test.
- Model ONNX và checksum.
- Audit export.
- QR donate.

Free tier của R2 Standard hiện gồm 10 GB-tháng, một triệu Class A operation/tháng, 10 triệu Class B operation/tháng và egress Internet miễn phí. Không dùng Infrequent Access vì free tier không áp dụng cho storage class này. Xem [R2 pricing][r2-pricing].

## 6. Crawler nhiều tầng

Không phụ thuộc một scraper duy nhất:

```text
Strategy 0: feed/API chính thức nếu nguồn có
        ↓ thất bại
Strategy 1: lightweight conditional HTTP fetch
        ↓
Strategy 2: public HTML/parser
        ↓
Strategy 3: Cloudflare Browser Run
        ↓
Strategy 4: OCI Playwright
        ↓
Strategy 5: authenticated session — chỉ khi thực sự cần
        ↓
Strategy 6: GitHub Actions emergency crawler
```

### 6.1. Chọn strategy theo health score

Mỗi nguồn và strategy có các chỉ số:

```text
success_rate
median_latency
parse_yield
duplicate_rate
rate_limit_count
last_success_at
```

Scheduler chọn strategy khỏe có chi phí tài nguyên thấp nhất. Ví dụ:

```text
public HTTP thành công 98%
→ không khởi động Chromium

public HTTP thất bại 3 lần liên tiếp
→ bật Browser Run hoặc Playwright

Playwright ổn định
→ vẫn probe public HTTP định kỳ để quay lại đường nhẹ
```

Ngưỡng chuyển strategy phải là cấu hình version hóa, không hard-code rải rác.

### 6.2. Adaptive polling

Không quét mọi page cùng tần suất:

- Quét nhanh trong khung giờ nguồn thường đăng.
- Giảm tần suất khi nguồn nhiều ngày không đăng.
- Quét lại sớm sau khi phát hiện bài mới.
- Dùng exponential backoff và jitter khi gặp 429 hoặc lỗi tạm thời.

### 6.3. Browser fallback miễn phí

Browser Run Free hiện giới hạn 10 phút browser/ngày và ba browser đồng thời. Nó chỉ là fallback ngắn, không phải crawler chính liên tục. Khi hết hạn mức ngày, request tiếp theo bị từ chối đến kỳ quota tiếp theo. Playwright trên OCI là browser fallback chính; Browser Run dùng khi OCI hoặc IP OCI gặp vấn đề. Xem [Browser Run pricing][browser-run-pricing].

## 7. Classifier nhiều tầng

Không chỉ dùng rule đơn giản và không để AI quyết định tất cả.

### 7.1. Tầng A — hard validation

Loại ngay:

- Bài quá cũ.
- Deadline đã qua.
- Hoạt động đã kết thúc.
- Tuyển dụng, bán hàng hoặc quảng cáo.
- Bài tổng kết không còn hành động.
- Nội dung không thuộc nguồn đã duyệt.

### 7.2. Tầng B — explainable rules

Trích các feature:

```text
explicit_drl
registration_call
form_link
future_event_time
future_deadline
location
target_students
approved_source
negative_commercial
past_event
```

Mỗi kết quả phải lưu điểm, rule đã match, dữ liệu trích xuất, phiên bản classifier và hash cấu hình.

### 7.3. Tầng C — local ML classifier

Trường hợp mơ hồ chạy qua model phân loại văn bản tiếng Việt:

- Fine-tune từ tập bài thật đã được gán nhãn.
- Quantize INT8.
- Export ONNX.
- Chạy CPU trên OCI.
- Không gọi API trả phí.
- Lưu checksum, version và tập đánh giá của model.

Model chỉ cung cấp xác suất và feature bổ sung; không tự tạo nội dung notification.

### 7.4. Tầng D — AI fallback

Workers AI có mức cấp phát miễn phí hằng ngày và chỉ đóng vai trò “second opinion” cho một phần nhỏ bài mơ hồ. Quota tính theo Neurons và có thể thay đổi theo model; triển khai phải đọc quota hiện hành từ [Workers AI pricing][workers-ai-pricing] thay vì giả định cố định. Workers AI không phải nguồn quyết định duy nhất.

Khi OCI mất lâu hơn ngưỡng failover, GitHub Actions emergency worker tải đúng Rust binary, rule config và ONNX model đã ký checksum từ release/R2 để xử lý một lượng backlog giới hạn. Đường này ưu tiên bài gần deadline, không cam kết latency real-time và không thay thế data plane chính. Nếu cả OCI lẫn emergency worker không chạy được, event vẫn ở ledger để xử lý sau; hệ thống phải báo degraded thay vì tự hạ tiêu chuẩn phân loại.

### 7.5. Quyết định cuối

```text
Hard reject
→ rejected

Rules rất chắc chắn
→ matched_explicit

Rules + local model đồng ý
→ matched_probable

Mâu thuẫn hoặc confidence thấp
→ manual_review
→ báo riêng cho admin
→ admin gửi hoặc bỏ qua bằng quyết định được audit

Workers AI lỗi hoặc hết quota
→ vẫn dùng rules + local model
```

## 8. Notification và idempotency

### 8.1. Transactional outbox

Trong cùng một transaction:

```text
INSERT post
INSERT classification
INSERT delivery campaign
INSERT outbox event
COMMIT
```

Dispatcher chỉ tạo batch delivery sau commit. Không đánh dấu bài đã xử lý trước khi tạo đầy đủ campaign.

### 8.2. Rate limiter

Telegram khuyến nghị tránh vượt khoảng một message/giây trong một chat và khoảng 30 message/giây khi broadcast nếu không dùng paid broadcast. Xem [Telegram Bot FAQ][telegram-faq].

Cấu hình mặc định:

```text
target rate: 25 msg/s
burst: 5
per-chat: 1 msg/s
```

Không đặt chính xác 30 msg/s để chừa biên cho retry và jitter. Khi gặp 429, phải tôn trọng `retry_after`.

### 8.3. Batch fanout

Không tạo một queue operation cho từng subscriber ở edge:

```text
1 campaign
   ├── shard 000: chat 1–100
   ├── shard 001: chat 101–200
   └── ...
```

Rust delivery worker đọc shard rồi gửi từng message theo rate limit. PostgreSQL vẫn lưu delivery theo người để tránh gửi lại người đã thành công, deactivate người block bot, retry lỗi tạm thời và audit chính xác.

### 8.4. Giới hạn “exactly once”

Telegram không cung cấp idempotency key cho `sendMessage`. Nếu process chết sau khi Telegram nhận message nhưng trước khi database ghi `sent`, hệ thống không thể chứng minh exactly-once tuyệt đối.

Mục tiêu kỹ thuật là:

> **Exactly-once campaign creation + effectively-once delivery + cửa sổ duplicate cực nhỏ và đo được.**

## 9. PostgreSQL

PostgreSQL chạy trên OCI và không public ra Internet.

Kỹ thuật bắt buộc:

- `INSERT ... ON CONFLICT`.
- `FOR UPDATE SKIP LOCKED`.
- Partial index cho queue pending.
- Partition bảng delivery/audit theo tháng khi dữ liệu đủ lớn để có lợi.
- Chỉ dùng `JSONB` cho dữ liệu không ổn định; cột quan trọng phải typed.
- Pool kết nối nhỏ hoặc PgBouncer khi số process yêu cầu.
- Theo dõi WAL, dung lượng, checkpoint và autovacuum.
- Đưa raw crawler payload sang R2 thay vì giữ toàn bộ trong PostgreSQL.

Các bảng chính:

```text
sources
source_strategies
crawler_runs
posts
post_artifacts
classifications
classification_features
manual_review_resolutions
portal_notice_state
portal_notices
subscribers
source_suggestions
campaigns
delivery_shards
deliveries
digest_items
digest_batches
notification_feedback
user_feedback_messages
product_events
outbox_events
dead_letters
donation_intents
donation_transactions
system_health
```

Migration phải tương thích ngược trong suốt quá trình rolling deploy và phải có restore test.

`subscribers` giữ attribution đầu tiên, trạng thái onboarding, phạm vi tin, chế độ
gửi và quiet hours. `product_events` là event log tối thiểu cho funnel, không thay
thế bảng nghiệp vụ và không lưu nội dung Telegram thô. `notification_feedback`
giữ một phản hồi hiện tại trên mỗi subscriber/campaign. Digest dùng hai bảng riêng
để việc gom nhóm và gửi lại vẫn bền vững, không làm thay đổi contract delivery tức
thời đã tồn tại.

`user_feedback_messages` lưu feedback tự do do người dùng gửi qua `/feedback`, có
giới hạn 2.000 ký tự, hàng đợi gửi cho `TELEGRAM_ADMIN_CHAT_ID` và retention 180 ngày.
Nội dung không được đưa vào log chu kỳ hoặc event metadata; `product_events` chỉ ghi
ID feedback để thống kê số lượt gửi.

`portal_notice_state` giữ cursor singleton để baseline và quét tăng dần theo ID.
`portal_notices` lưu metadata công khai; campaign Portal dùng chung ledger delivery
nhưng không đi qua classifier hoặc digest. Quyền nhận bắt buộc được xác định từ
trạng thái onboarding và lý do deactivation có kiểu, không từ tùy chọn nội dung.
Tệp không được lưu dài hạn trong PostgreSQL: Telegram `file_id` trên campaign là
khóa tái sử dụng sau lần upload thành công đầu tiên.

### 9.1. Backup và disaster recovery

Backup phải dùng công cụ hỗ trợ PostgreSQL-aware backup và point-in-time recovery, ví dụ WAL-G hoặc pgBackRest với R2 qua S3-compatible API:

- Archive WAL liên tục; uploader retry cục bộ khi R2 tạm lỗi.
- Tạo encrypted base backup hằng ngày.
- Giữ local spool có dung lượng giới hạn để không làm đầy volume khi object storage lỗi.
- Áp dụng retention theo storage budget; không xóa base backup cuối cùng đã qua restore validation.
- Kiểm tra checksum sau upload và chạy restore validation tối thiểu hằng tuần.
- Backup credential chỉ có quyền trên prefix/bucket cần thiết; restore credential tách biệt khi khả thi.

Mục tiêu ban đầu, phải được đo bằng failure drill:

| Chỉ số | Mục tiêu | Giới hạn |
| --- | --- | --- |
| PostgreSQL RPO | ≤ 5 phút | Phụ thuộc lần WAL archive thành công cuối |
| Event ingress RPO khi OCI lỗi | 0 sau khi D1 commit | Không bao phủ outage đồng thời của Cloudflare |
| RTO khi OCI VM còn tồn tại | ≤ 30 phút | Restart service và replay backlog |
| RTO khi phải cấp VM mới | ≤ 6 giờ | Best effort; có thể lâu hơn nếu OCI hết Always Free capacity |

D1 không phải bản sao đầy đủ của PostgreSQL và không thể phục hồi posts, classifications, deliveries hoặc audit. Khôi phục đầy đủ luôn cần base backup + WAL từ R2. Nếu không đạt RPO/RTO qua restore drill, tài liệu vận hành phải ghi số đo thực tế thay cho mục tiêu.

## 10. Chịu lỗi nhiều lớp

| Lỗi | Phản ứng |
| --- | --- |
| OCI restart | Cloudflare vẫn nhận Telegram/payOS event |
| PostgreSQL tạm ngừng | D1 inbox giữ event; command chỉ đọc dữ liệu cache còn hợp lệ |
| Cloudflare Queue hết retention | Reconciler đọc D1 edge ledger hoặc PostgreSQL outbox theo ownership và tạo lại job |
| Public crawler lỗi | Chuyển browser fallback |
| Playwright OCI lỗi | Browser Run hoặc GitHub Actions |
| AI lỗi/hết quota | Rules + local ONNX |
| payOS lỗi | VietQR tĩnh |
| R2 lỗi | Backup cục bộ; OCI Object Storage là tùy chọn nếu vẫn trong Always Free |
| Telegram 429 | Retry theo `retry_after` và rate limiter |
| Worker gửi trùng job | Unique key + PostgreSQL upsert |
| VM bị Oracle thu hồi | Dựng lại bằng IaC; phục hồi PostgreSQL từ R2 rồi replay ingress còn pending trong D1 |

Reconciler phải đối soát tối thiểu:

- Inbox chưa đồng bộ.
- Outbox chưa publish.
- Campaign thiếu shard.
- Delivery pending quá SLA.
- Job ledger không còn message tương ứng.
- Backup chưa hoàn tất hoặc chưa qua restore validation.

Giới hạn đã chấp nhận:

- Cloudflare là shared failure domain của Telegram ingress, payOS ingress, D1, Queues và R2. Khi Cloudflare outage, Telegram có thể giữ update tối đa 24 giờ; payment recovery phụ thuộc API/retry contract của adapter payOS và phải được kiểm chứng trước production.
- Không có zero-cost synchronous ingress failover tự động. Runbook có thể chuyển Telegram webhook sang endpoint OCI khi OCI còn khỏe, nhưng đây là thao tác disaster recovery và không bảo đảm không gián đoạn.
- Việc cấp lại OCI Always Free có thể bị trì hoãn vì hết host capacity. Hệ thống giữ backlog và chạy emergency worker giới hạn; không tự cấp paid VM để đạt RTO.

## 11. Repository, CI và quy tắc cho coding agent

Nếu chấp nhận được về dữ liệu và vận hành, repository nên public để dùng GitHub-hosted standard runner miễn phí và secret scanning cho public repository. GitHub Actions scheduled workflow chỉ là CI và tertiary fallback: schedule có thể bị trễ, bị bỏ khi tải cao và bị tự động vô hiệu hóa sau thời gian dài repository không hoạt động. Xem [GitHub Actions workflow syntax][github-workflow] và [GitHub Actions limits][github-actions-limits].

Secret không được nằm trong repository:

```text
Cloudflare Secrets
GitHub Actions Secrets
OCI Vault hoặc file root-only
SOPS + age cho config mã hóa
```

Cấu trúc monorepo đích:

```text
uth-activity-notifier/
├── apps/
│   ├── edge-worker/          # TypeScript
│   ├── core-agent/           # Rust
│   ├── browser-agent/        # TypeScript + Playwright
│   └── admin-cli/            # Rust
├── crates/
│   ├── domain/
│   ├── classifier/
│   ├── crawler/
│   ├── delivery/
│   └── contracts/
├── packages/
│   ├── telegram-contracts/
│   └── cloudflare-contracts/
├── models/
│   ├── metadata/
│   └── evaluation/
├── migrations/
├── fixtures/
├── infrastructure/
│   ├── cloudflare/
│   ├── oracle/
│   └── compose/
├── observability/
├── scripts/
├── docs/
└── AGENTS.md
```

Coding agent phải tuân thủ:

- Không sửa contract mà không cập nhật consumer.
- Luôn thêm migration tương thích ngược.
- Test idempotency, retry và failure injection.
- Chạy Rust Clippy, Rust tests, TypeScript typecheck, migration test và integration test.
- Không tự thêm dịch vụ hoặc tính năng trả phí.
- Mọi dịch vụ ngoài phải có adapter và fallback.
- Không commit secret, raw session, cookie hoặc dữ liệu người dùng.

## 12. Chính sách chi phí bằng 0

Không thể bảo đảm nhà cung cấp giữ chính sách miễn phí vĩnh viễn. Hệ thống phải bảo đảm không chủ động nâng cấp hoặc tiếp tục workload khi vượt ngân sách zero-cost:

1. Chỉ provision resource Free/Always Free đã được allowlist.
2. Không bật Workers Paid hoặc paid Telegram broadcast.
3. Không thêm payment method nếu nhà cung cấp không bắt buộc.
4. Với dịch vụ có metered overage, chỉ bật khi tài khoản có hard spending cap bằng 0 hoặc có cơ chế từ chối request khi hết free allocation; nếu không có, adapter đó không đủ điều kiện production.
5. Đặt usage guard thấp hơn quota chính thức 10–20%.
6. Khi gần quota, giảm tần suất, chuyển provider hoặc tắt tính năng phụ; tuyệt đối không tự nâng gói.
7. Mọi adapter phải khai báo `cost_policy=ZERO_ONLY`.
8. CI kiểm tra IaC và config không chứa paid feature ngoài allowlist.
9. Cảnh báo admin khi đạt 70%, 85% và 95% hạn mức nội bộ.
10. VietQR tĩnh là fallback cuối cho donate.
11. Quota trong tài liệu là snapshot có ngày; pipeline vận hành phải kiểm tra lại định kỳ và trước mỗi thay đổi hạ tầng.

Donate dùng payOS làm adapter chính. Mỗi yêu cầu tạo một `donation_intents` trước
khi gọi API để có `orderCode` ổn định. Webhook đi qua Cloudflare Worker, xác minh
HMAC-SHA256 trên toàn bộ `data`, ghi D1 idempotent rồi mới trả `2xx`. Edge
reconciler chuyển event về PostgreSQL; worker chỉ đánh dấu `paid` khi `orderCode`,
số tiền và `paymentLinkId` cùng khớp, đồng thời khóa trùng bằng `reference`.
Không lưu khóa payOS trong D1 hoặc PostgreSQL; checksum key của webhook là
Cloudflare secret, còn ba khóa API là runtime secret trên máy chủ.
Intent lưu bản chụp ngân hàng nhận, số tài khoản, tên người nhận và nội dung chuyển
khoản do payOS trả về để hỗ trợ đối soát về sau. Caption Telegram dùng STK gốc từ
runtime và không trình bày số định danh payOS như STK người nhận.

Lựa chọn donate tùy tâm dùng `donation_amount_input_state` trong PostgreSQL với TTL
10 phút. Callback chỉ mở trạng thái; số tiền hợp lệ mới tạo `donation_intents`.
Trạng thái được giữ khi nhập sai hoặc khi gửi Telegram lỗi, xóa sau khi QR được gửi
thành công, và được retention dọn khi hết hạn. Giới hạn 10.000–10.000.000 VND được
thực thi ở cả parser và ràng buộc database.

Ảnh donate ưu tiên adapter `img.vietqr.io` với mẫu `vietqr_pro`, cùng đường tạo ảnh
mà checkout payOS công bố. Adapter chỉ gửi BIN, số định danh payOS, số tiền và nội
dung chuyển khoản; không gửi khóa API. Request có timeout 8 giây, tối đa hai lần
thử, giới hạn response 2 MiB và kiểm tra chữ ký PNG. QR dựng cục bộ từ payload
payOS là fallback không phụ thuộc dịch vụ ảnh.

## 13. Bounded component và runtime

Kiến trúc chia theo bounded component:

```text
edge_ingress
scheduler
crawl_orchestrator
lightweight_crawler
browser_crawler
normalizer
classifier_rules
classifier_ml
campaign_planner
delivery_worker
payment_adapter
event_ledger
storage
observability
admin_cli
```

Không bắt buộc mỗi component là một container. Các component Rust có thể compile vào cùng binary với command khác nhau:

```text
uth-agent crawl
uth-agent classify
uth-agent deliver
uth-agent reconcile
```

Cách này giữ ranh giới kiến trúc rõ mà runtime vẫn nhẹ.

## 14. Stack được chọn

| Lớp | Công nghệ |
| --- | --- |
| Telegram ingress | Cloudflare Worker webhook |
| Edge durability | D1 inbox |
| Event transport | Cloudflare Queues |
| Main database | PostgreSQL trên OCI Always Free |
| Raw storage/backup | Cloudflare R2 Standard |
| Core runtime | Rust |
| Browser fallback | TypeScript + Playwright |
| ML | Rules + local quantized ONNX |
| AI fallback | Workers AI free allocation |
| Main compute | OCI Ampere Always Free |
| Emergency compute | Browser Run + GitHub Actions |
| Secure connection | Cloudflare Tunnel hoặc pull queue |
| Payment | payOS + VietQR fallback |
| CI/CD | GitHub Actions + Wrangler + OpenTofu |
| Interface | Telegram commands/buttons cho đăng ký, danh sách nguồn và đề xuất nguồn; chưa cần Mini App |

Đây là kiến trúc đích cho yêu cầu tối ưu hiệu năng, độ chính xác, khả năng chịu lỗi, tự động hóa và chi phí bắt buộc bằng 0. Mọi bước triển khai sau này phải giữ bốn invariant:

1. PostgreSQL là nguồn sự thật nghiệp vụ.
2. Ingress không phụ thuộc OCI còn sống.
3. Crawler và classifier có fallback không trả phí; fallback có thể chạy degraded và không cam kết latency real-time.
4. Không workload nào được tự động chuyển sang paid tier.

## Tài liệu tham khảo

[oracle-free]: https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm
[workers-limits]: https://developers.cloudflare.com/workers/platform/limits/
[telegram-api]: https://core.telegram.org/bots/api
[d1-pricing]: https://developers.cloudflare.com/d1/platform/pricing/
[queues-free]: https://developers.cloudflare.com/changelog/post/2026-02-04-queues-free-plan/
[queues-overview]: https://developers.cloudflare.com/queues/
[r2-pricing]: https://developers.cloudflare.com/r2/pricing/
[browser-run-pricing]: https://developers.cloudflare.com/browser-run/pricing/
[workers-ai-pricing]: https://developers.cloudflare.com/workers-ai/platform/pricing/
[telegram-faq]: https://core.telegram.org/bots/faq
[github-workflow]: https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax
[github-actions-limits]: https://docs.github.com/en/actions/reference/limits
