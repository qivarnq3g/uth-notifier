# Triển khai server bằng Docker Compose

## Phạm vi

`compose.server.yml` triển khai data plane hiện có trên một server Linux:

- PostgreSQL 17 là nguồn dữ liệu chính.
- `scheduler` crawl 43 nguồn Facebook và ghi transactional outbox.
- `classifier` xử lý sự kiện bài viết.
- `notify` nhận lệnh và gửi Telegram.
- `backup-scheduler` tạo backup hằng ngày; `backup` và `restore` vẫn có thể chạy thủ công qua profile `maintenance`.

Máy phát triển chỉ dùng để build và test. Các lệnh trong tài liệu này phải chạy
trên server đích. Compose không mở PostgreSQL ra cổng host.

## Chuẩn bị server

Cần Docker Engine và Docker Compose v2. Từ thư mục repository trên server:

```sh
cp deploy/server.env.example .env.server
mkdir -p deploy/secrets backups
chmod 700 deploy/secrets backups
openssl rand -hex 32 > deploy/secrets/postgres_password
password="$(cat deploy/secrets/postgres_password)"
printf 'postgresql://uth_agent:%s@postgres:5432/uth_notifier' "$password" > deploy/secrets/database_url
printf '%s' 'TELEGRAM_BOT_TOKEN_THAT' > deploy/secrets/telegram_bot_token
printf '%s' 'TELEGRAM_ADMIN_CHAT_ID_THAT' > deploy/secrets/telegram_admin_chat_id
openssl rand -hex 32 > deploy/secrets/edge_sync_token
chmod 600 deploy/secrets/*
```

Nếu đổi `POSTGRES_USER` hoặc `POSTGRES_DB` trong `.env.server`, phải cập nhật cùng
giá trị trong secret `database_url`. Mật khẩu mẫu sinh bằng hexadecimal nên không
cần URL-encode. Không commit `.env.server`, `deploy/secrets/` hoặc `backups/`.

Kiểm tra cấu hình trước khi tạo container:

```sh
docker compose --env-file .env.server -f compose.server.yml config --quiet
docker compose --env-file .env.server -f compose.server.yml build --pull
```

## Khởi động lần đầu

```sh
docker compose --env-file .env.server -f compose.server.yml up -d postgres
docker compose --env-file .env.server -f compose.server.yml up -d scheduler classifier notify backup-scheduler
docker compose --env-file .env.server -f compose.server.yml ps
```

Lần crawl `healthy` đầu tiên tạo baseline và không gửi bài lịch sử. Không thêm
`--notify-existing-posts` vào command scheduler nếu chưa chủ động duyệt rủi ro.

Theo dõi quá trình baseline:

```sh
docker compose --env-file .env.server -f compose.server.yml logs -f --tail 100 scheduler classifier notify
docker compose --env-file .env.server -f compose.server.yml exec scheduler sh -c 'DATABASE_URL="$(cat /run/secrets/database_url)" uth-agent health --require-healthy'
```

Health chỉ chuyển sang `healthy` khi toàn bộ nguồn đã có crawl khỏe, backlog không
quá hạn, không có dead letter hoặc delivery thất bại và Telegram worker còn lease.

## Cập nhật và rollback

Tạo backup trước khi cập nhật:

```sh
docker compose --env-file .env.server -f compose.server.yml --profile maintenance run --rm backup
docker compose --env-file .env.server -f compose.server.yml build --pull
docker compose --env-file .env.server -f compose.server.yml up -d --no-deps scheduler classifier notify
```

Giữ image cũ bằng tag bất biến trước khi thay thế để rollback không phụ thuộc việc
build lại source. Đặt `UTH_AGENT_IMAGE` trong `.env.server` thành tag phát hành cụ
thể thay vì dùng `latest`. Không chạy hai container `notify` dùng cùng token
Telegram.

## Backup và khôi phục

`backup-scheduler` chạy ngay khi khởi động, sau đó lặp theo `BACKUP_INTERVAL_SECONDS`, mặc
định 86400 giây. Khi backup thất bại, worker ghi lỗi và thử lại sau
`BACKUP_RETRY_SECONDS`, mặc định 300 giây. Healthcheck chuyển sang unhealthy nếu không có
lần backup thành công trong 48 giờ. Theo dõi riêng trạng thái này sau khi khởi động:

```sh
docker compose --env-file .env.server -f compose.server.yml ps backup-scheduler
docker compose --env-file .env.server -f compose.server.yml logs --tail 100 backup-scheduler
```

Tạo backup PostgreSQL custom-format, kiểm tra catalog và ghi SHA-256:

```sh
docker compose --env-file .env.server -f compose.server.yml --profile maintenance run --rm backup
```

Mặc định file quá 14 ngày bị xóa. Backup trong thư mục cùng server chưa phải
disaster recovery; cần đồng bộ bản đã mã hóa sang một nơi lưu trữ độc lập và định
kỳ thử restore trên database tạm.

Khôi phục là thao tác phá hủy dữ liệu hiện tại. Dừng worker, chọn đúng tên file và
chỉ chạy sau khi đã xác minh checksum:

```sh
docker compose --env-file .env.server -f compose.server.yml stop scheduler classifier notify
docker compose --env-file .env.server -f compose.server.yml --profile maintenance run --rm -e RESTORE_FILE=uth_notifier-YYYYMMDDTHHMMSSZ.dump restore
docker compose --env-file .env.server -f compose.server.yml up -d scheduler classifier notify
```

## Giới hạn hiện tại

- Compose là data plane một server, chưa phải high availability.
- Long polling là mặc định cho đến khi Cloudflare Worker và webhook thật qua smoke test.
- Profile `edge` chỉ chạy reconciler; phải đổi `TELEGRAM_UPDATES_SOURCE=edge` cùng lúc bật webhook.
- Backup off-site mã hóa và diễn tập restore định kỳ cần được cấu hình trên server.
- Chủ hệ thống hiện chấp nhận rủi ro mất toàn bộ máy cá nhân và đưa backup off-site
  ra ngoài phạm vi triển khai; đây là quyết định vận hành, không phải bảo đảm kỹ thuật.
- Resource limit là giới hạn bảo vệ ban đầu, phải điều chỉnh bằng số đo trên server đích.

## LXC bộ nhớ thấp

Máy Debian 13 LXC có 2 CPU, 2 GB RAM và 1 GB swap chạy native bằng PostgreSQL,
Chromium, Node.js và systemd thay vì Docker. Cấu hình này chỉ dùng một crawl browser
tại một thời điểm. Dùng các unit trong `deploy/systemd/` và nạp
`deploy/postgresql-low-memory.conf` bằng `include_dir` của PostgreSQL.

Scheduler có `MemoryHigh=1100M`, `MemoryMax=1400M`, classifier có trần 128 MB và
đều chạy bằng user `uth-notifier`. Browser script production là JavaScript đã biên
dịch từ TypeScript, cho phép Debian Node.js 20 chạy mà không cần strip TypeScript
lúc runtime. Scheduler phải đặt `CHROME_PATH=/usr/bin/chromium`; nếu thiếu biến này,
browser adapter sẽ dùng đường dẫn Chrome mặc định của Windows và fallback thất bại.
Build release phải dùng một job, không chạy đồng thời production:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo build --locked --release -p uth-agent -j 1
```

Sau khi copy binary vào `bin/uth-agent` trong release directory, đối chiếu
`ExecStart` và executable của PID với artifact đó, rồi xác minh rollback target, xóa source
build, Cargo target, registry cache và Rust toolchain khỏi server. Không chạy
Telegram worker trên máy shadow; chỉ bật một notifier sau cutover để tránh nhận và
gửi trùng.

`uth-notifier-notify` luôn bật `--admin-only`. `uth-notifier-edge-reconciler`
nhận lệnh Telegram từ edge. Timer `uth-notifier-backup` tạo PostgreSQL
custom-format backup hằng ngày và giữ 14 ngày. Runtime secret nằm trong
`/etc/uth-notifier/runtime.env` với quyền `0640`, không nằm trong release hay
source tree.
The production scheduler also sets `FACEBOOK_BROWSER_NETWORK_MODE=prefer_ipv4`. The browser
resolves the current Facebook A record at process start, uses a bounded Chromium resolver
rule with QUIC disabled, and falls back to the system resolver when IPv4 resolution or
navigation cannot be used. Do not replace this with a hard-coded IP, proxy, cookie,
authenticated session, or address rotation.
The scheduler release currently uses bounded `--concurrency 2`. This was enabled only
after a production observation of approximately 575 MiB scheduler cgroup peak with one
browser and is guarded by `MemoryMax=1400M`; revert to 1 if the cgroup peak approaches
the limit or browser failures increase.
