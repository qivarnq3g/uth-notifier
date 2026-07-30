# Cloudflare Telegram ingress

## Trạng thái và ranh giới

Rust Cloudflare Worker, D1 migration, hợp đồng `edge-event.v1`, PostgreSQL inbox,
reconciler, Telegram webhook và payOS webhook đã chạy live tại Cloudflare Workers. Production dùng
`TELEGRAM_UPDATES_SOURCE=edge`; không có `getUpdates` chạy song song. Smoke test đã
xác nhận Worker health, reconciler pull/ack và bot xử lý lệnh admin qua cùng đường
xử lý bền vững trong PostgreSQL.

PostgreSQL vẫn là nguồn sự thật. D1 chỉ là inbox và ledger giữ event cho đến khi
PostgreSQL commit. Không bật webhook và `getUpdates` đồng thời.

## Luồng và bảo đảm

```text
Telegram webhook
  -> kiểm tra secret header
  -> ghi D1 theo update_id
  -> reconciler claim bằng lease hữu hạn
  -> PostgreSQL import idempotent trong transaction
  -> xác nhận D1
  -> notify xử lý lệnh theo thứ tự update_id

payOS webhook
  -> xác minh HMAC-SHA256 trên data
  -> ghi D1 theo paymentLinkId và reference
  -> reconciler import idempotent vào PostgreSQL
  -> notify đối chiếu orderCode, amount và paymentLinkId
  -> ghi transaction và xác nhận cho người ủng hộ
```

Webhook giới hạn body 256 KiB. Pull giới hạn 100 event, lease tối đa 300 giây và
HTTP client thử lại tối đa ba lần. ACK là idempotent nên phản hồi mạng bị mất không
làm reconciler kẹt. Event chỉ được đánh dấu xử lý trong PostgreSQL sau khi lệnh và
phản hồi Telegram hoàn tất. Nếu process chết giữa lúc Telegram nhận phản hồi và
lúc đánh dấu hoàn tất, phản hồi có thể lặp; Bot API không có idempotency key cho
`sendMessage` nên không thể loại bỏ hoàn toàn cửa sổ này.

Payload Telegram hoặc payOS không xử lý được sẽ được thử lại tối đa ba lần với
backoff hữu hạn. Sau đó PostgreSQL ghi `dead_lettered_at` và bỏ qua event đó để
không chặn toàn bộ inbox. Chi tiết lỗi được cắt ở 1.000 ký tự và chỉ hiển thị trong
health dành cho quản trị viên.

Reconciler production poll mỗi hai giây. Mức này tạo tối đa khoảng 43.200 request
mỗi ngày khi chạy liên tục và giữ độ trễ do polling dưới hai giây. Không giảm xuống
một giây trên Workers Free vì riêng reconciler sẽ dùng 86.400 request mỗi ngày,
không còn đủ biên cho webhook và vận hành.

Event đã xác nhận trong D1 và event đã xử lý trong PostgreSQL được xóa theo lô sau
30 ngày. Event `pending` hoặc đang lease không thuộc retention này.

## Provision và triển khai

Các lệnh sau thay đổi tài khoản Cloudflare thật. Chỉ chạy sau khi đăng nhập đúng
tài khoản và xác nhận vẫn dùng gói miễn phí:

```powershell
cd apps/edge-worker
npx wrangler login
npx wrangler d1 create uth-notifier-edge
Copy-Item wrangler.toml wrangler.production.toml
```

Thay `database_id` vô hiệu trong `wrangler.production.toml` bằng ID vừa tạo. File
production này bị Git và Docker loại trừ. Sinh hai secret
độc lập, tối thiểu 32 ký tự, rồi lưu cùng giá trị sync token vào secret server:

```powershell
$webhookSecret = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
$syncToken = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
$webhookSecret | npx wrangler secret put TELEGRAM_WEBHOOK_SECRET --config wrangler.production.toml
$syncToken | npx wrangler secret put EDGE_SYNC_TOKEN --config wrangler.production.toml
$payosChecksumKey | npx wrangler secret put PAYOS_CHECKSUM_KEY --config wrangler.production.toml
npx wrangler d1 migrations apply uth-notifier-edge --remote --config wrangler.production.toml
npm run deploy
```

Không ghi các giá trị này vào repository, log hoặc tham số command. Ghi `$syncToken`
vào `deploy/secrets/edge_sync_token` trên server bằng kênh quản trị an toàn. Đặt
`EDGE_URL` thành URL Worker thật nhưng vẫn giữ `TELEGRAM_UPDATES_SOURCE=polling`.
Đăng ký URL `/payos/webhook` bằng API `POST /confirm-webhook` của payOS sau khi
Worker đã deploy và secret checksum đã được đặt. Request xác nhận của payOS chứa
webhook mẫu; Worker vẫn xác minh chữ ký và ghi ledger như một event bình thường,
nhưng PostgreSQL đánh dấu lỗi đối soát nếu không có intent tương ứng.

## Chuyển traffic

Trước khi chuyển, kiểm tra Worker health và chạy reconciler một lần:

```powershell
Invoke-WebRequest "$env:EDGE_URL/health"
docker compose --env-file .env.server -f compose.server.yml --profile edge run --rm edge-reconciler reconcile-edge --once
```

Gọi Bot API `setWebhook` với URL `/telegram/webhook`, `secret_token` đúng secret
Worker, `allowed_updates=["message","callback_query"]` và
`drop_pending_updates=false`. Thiếu `callback_query` khiến nút nội tuyến hiển thị
nhưng Telegram không gửi update khi người dùng bấm. Sau khi Telegram xác nhận webhook, đổi
`TELEGRAM_UPDATES_SOURCE=edge`, rồi chạy:

```powershell
docker compose --env-file .env.server -f compose.server.yml --profile edge up -d edge-reconciler
docker compose --env-file .env.server -f compose.server.yml up -d --no-deps notify
```

Gửi lần lượt `/status`, `/stop`, `/start` từ một chat thử. Xác minh D1 nhận event,
reconciler báo `acknowledged`, bảng `edge_inbox_events.processed_at` được điền và bot
trả lời đúng. Chỉ coi cutover thành công khi toàn bộ kiểm tra này qua.

## Rollback

Gọi Bot API `deleteWebhook` với `drop_pending_updates=false`, đặt
`TELEGRAM_UPDATES_SOURCE=polling`, khởi động lại `notify`, rồi dừng `edge-reconciler`.
Không xóa D1 hoặc PostgreSQL inbox trong rollback. Event còn lại được giữ để đối
soát; `update_id` và khóa idempotency ngăn import trùng.
