# Gửi thông báo Telegram

- **Trạng thái:** Bộ tạo thông báo và gửi Telegram đã có kiểm thử cục bộ cùng PostgreSQL integration test

Link bài Facebook gửi cho người dùng ưu tiên dạng
`https://www.facebook.com/<page-id>/posts/<numeric-post-id>` khi cả hai ID số đã được
xác minh. Dạng này được dùng cho nội dung cũ, nút mở bài, duyệt thủ công và `/latest`;
canonical URL trong crawler và PostgreSQL vẫn được giữ nguyên.
Khi crawler chưa tìm được ID bài số nhưng đã có page ID số và một `pfbid` hợp lệ, bot
dùng `https://www.facebook.com/<page-id>/posts/<pfbid>` thay vì giữ đường dẫn theo alias.

Mỗi bài Facebook trong PostgreSQL chỉ được tạo một chiến dịch thông báo. Revision mới
của bài đã có chiến dịch vẫn được lưu và phân loại để audit nhưng không tạo thêm lượt
gửi hoặc mục digest, kể cả khi Facebook luân phiên giữa presentation đầy đủ và rút gọn.

## Tương tác với bot

Menu công khai hiển thị sáu lệnh chính:

- `/start`: đăng ký hoặc bật lại thông báo.
- `/settings`: xem tin mẫu, chọn loại hoạt động, nhận từng tin ngay khi phát hiện hoặc một bản tin lúc 07:30, giờ yên lặng và tạm dừng tin hoạt động.
- `/pages`: xem các trang Facebook đang được theo dõi.
- `/help`: xem hướng dẫn.
- `/donate`: chủ động mở lựa chọn ủng hộ chi phí vận hành.
- `/feedback`: gửi feedback tự do cho quản trị viên.

Feedback có thể gửi trực tiếp bằng `/feedback nội dung`, hoặc gửi `/feedback` rồi
nhắn nội dung ở tin tiếp theo. Trạng thái chờ hết hạn sau 10 phút; nội dung tối đa
2.000 ký tự và được lưu trong hàng đợi có retention 180 ngày trước khi chuyển cho
`TELEGRAM_ADMIN_CHAT_ID`.
Tin gửi cho quản trị viên luôn kèm Telegram chat ID và nút liên hệ trực tiếp với người
gửi, kể cả khi tài khoản đó không có username công khai.

Phản hồi của `/start`, các nút bật hoặc bật lại thông báo, `/help` và nút
**Trợ giúp** cùng hiển thị lời mời tham gia nhóm hỗ trợ và nhận cập nhật mới nhất
tại `https://t.me/uth_notifier_group`.

Các lệnh tương thích như `/suggest`, `/status` và `/stop` vẫn được xử
lý nhưng không xuất hiện trong menu. Menu riêng của quản trị viên gồm `/admin`,
`/pending`, `/reviews`, `/latest`, `/feedbacks`, `/crawl_history`, `/portal_history` và `/metrics`; `/feedbacks`
hiển thị lịch sử feedback đã lưu theo từng trang và tạo liên kết Telegram trực tiếp
tới từng người gửi từ chat ID, còn `/crawl_history`
hiển thị mọi lần crawl đã ghi, kể cả lần không có bài; `/crawl_run_ID` mở chi tiết các lượt thử
và lỗi. Các lệnh thao tác chi tiết như
`/approve`, `/reject`, `/review_send_ID` và `/review_skip_ID` vẫn được dùng từ
nút hoặc lệnh được bot hiển thị trong nội dung tương ứng.

Khi đủ `PAYOS_CLIENT_ID`, `PAYOS_API_KEY`, `PAYOS_CHECKSUM_KEY`,
`PAYOS_RETURN_URL` và `PAYOS_CANCEL_URL`, `/donate` hiển thị các nút nội tuyến
10.000, 20.000 và 50.000 VND cùng nút `Tùy tâm` ngay dưới tin nhắn. Bot xác nhận
callback query trước khi xử lý. `Tùy tâm` tạo trạng thái chờ 10 phút trong
PostgreSQL; tin nhắn tiếp theo chấp nhận số từ 10.000 đến 10.000.000 VND với dạng
chữ số, phân cách hàng nghìn hoặc hậu tố `k`. `/cancel` hủy trạng thái. Trạng thái
chỉ được xóa sau khi Telegram nhận QR thành công và các hàng hết hạn được dọn trong
chu kỳ retention. Worker ghi intent vào
PostgreSQL trước khi gọi payOS và tạo link hết hạn sau 30 phút. Adapter ảnh lấy mẫu
`vietqr_pro` từ `img.vietqr.io` bằng BIN, số định danh payOS, số tiền và nội dung
chuyển khoản rồi gửi PNG hoặc JPEG trực tiếp trong Telegram. Mỗi lần lấy ảnh có timeout 8 giây,
tối đa hai lần thử và giới hạn 2 MiB. Nếu dịch vụ ảnh lỗi hoặc trả dữ liệu không phải
ảnh hợp lệ, bot ghi outcome đã rút gọn vào journal và tự dựng QR đen trắng từ payload payOS.
Caption hiển thị ngân hàng,
người nhận, STK gốc lấy từ `DONATE_BANK_ACCOUNT`, số tiền và nội dung chuyển khoản.
Mã BIN và số định danh payOS trong payload QR không được hiển thị cho người dùng.
Bot vẫn lưu dữ liệu payOS trả về để đối soát và tự xác nhận khi webhook
hợp lệ được đồng bộ từ D1. Sai chữ ký bị từ chối trước khi ghi; trùng `reference` được xử lý
idempotent; sai số tiền hoặc `paymentLinkId` bị ghi lỗi và không đánh dấu đã trả.
Tin xác nhận sau thanh toán gồm số tiền, nội dung, mã giao dịch và thời gian payOS.

`DONATE_BANK_ACCOUNT` chỉ được cấu hình trong runtime, gồm 6 đến 20 chữ số. Không
đưa STK thật vào mã nguồn, tài liệu, fixture hoặc test. Giá trị này chỉ phục vụ hiển
thị; không được dùng để sửa payload VietQR do payOS trả về vì sẽ phá vỡ đường đối
soát tự động.

`DONATE_VIETQR_URL` và `DONATE_MESSAGE` là fallback tĩnh. URL phải dùng HTTPS và
không chứa credentials. Nếu payOS lỗi, bot chỉ hiển thị fallback khi URL này đã
được cấu hình; nếu chưa có, bot báo tính năng tạm thời chưa sẵn sàng.

Các nút `Cài đặt`, `Trang đang theo dõi`, `Đề xuất trang`, `Trợ giúp` và `Ủng hộ` thực hiện
cùng chức năng. Vị trí xử lý tin
nhắn được lưu trong PostgreSQL để khi khởi động lại bot không đọc lại toàn bộ tin
nhắn cũ.

Đề xuất trang có ba trạng thái `pending`, `approved` và `rejected`. Bot báo cho
quản trị viên khi có đề xuất mới. Chỉ đề xuất đã duyệt mới được thêm vào bảng
`sources` và xuất hiện trong danh sách theo dõi.

Người dùng thường chỉ thấy hệ thống `ổn định`, `cần kiểm tra` hoặc `có lỗi`. Nếu
chat ID khớp `TELEGRAM_ADMIN_CHAT_ID`, `/status` hiển thị thêm số nguồn chưa được
kiểm tra, nguồn quá lâu chưa cập nhật, việc đang chờ, lượt gửi thất bại và đề xuất
đang chờ bằng tiếng Việt dễ đọc. Cách này không lộ chi tiết vận hành cho người dùng thường.

Notifier kiểm tra health mỗi 30 giây và lưu trạng thái quan sát tại
`operational_alert_state`. Trạng thái `failed` được báo ngay cho
`TELEGRAM_ADMIN_CHAT_ID`; `degraded` chỉ được báo khi kéo dài quá 900 giây để tránh
spam do lỗi crawl thoáng qua. Sau khi đã báo lỗi, trạng thái phải duy trì `healthy`
đủ 900 giây mới tạo đúng một tin phục hồi; nếu suy giảm trở lại trong cửa sổ này thì
không báo phục hồi. Trạng thái đã gửi được lưu sau khi Telegram chấp nhận tin nên
restart worker không làm gửi lại cùng một trạng thái; vẫn tồn tại cửa sổ lặp rất nhỏ
nếu process chết sau khi Telegram nhận tin nhưng trước khi PostgreSQL commit.

Lỗi gửi cuối cùng được phân loại trước khi tính operational health. Trường hợp người nhận
chặn bot hoặc chat không còn khả dụng được lưu là `recipient_unavailable`, người nhận bị
vô hiệu hóa và bản ghi vẫn được giữ để audit, nhưng không làm health toàn hệ thống thành
`failed`. Các lỗi hết retry, request bị Telegram từ chối vì lý do khác và lỗi chưa phân loại
vẫn được tính là lỗi nghiêm trọng. Cùng quy tắc này áp dụng cho cả thông báo tức thời và bản
tin hằng ngày.

Lỗi crawl được đánh giá theo từng nguồn bằng số lỗi liên tiếp và cửa sổ 900 giây.
Một nguồn lỗi thoáng qua không làm health tổng suy giảm; một nguồn đạt ngưỡng chỉ làm
health tổng `degraded`. Crawler chỉ làm health tổng `failed` khi ít nhất ba nguồn và
tối thiểu 25% nguồn cùng đạt ngưỡng. Các lỗi bền vững của hàng đợi, delivery, edge
hoặc donation vẫn giữ mức `failed`.

Đặt `TELEGRAM_ADMIN_CHAT_ID` để bot gửi đề xuất mới đến quản trị viên. Quản trị
viên có thể duyệt ngay trong Telegram:

```text
/pending
/approve 12 Tên trang
/reject 12 Lý do
```

Hoặc xét bằng dòng lệnh:

```powershell
target/release/uth-agent suggestion list
target/release/uth-agent suggestion approve --id 12 --name "Tên trang"
target/release/uth-agent suggestion reject --id 12 --reason "Lý do"
```

- **Phạm vi:** Gửi bài `matched_explicit`; nhận lệnh bằng `getUpdates` hoặc Cloudflare webhook, chỉ bật một nguồn tại một thời điểm

Cloudflare webhook ghi update vào D1 trước khi trả thành công. Reconciler kéo event
bằng lease hữu hạn, commit idempotent vào PostgreSQL rồi mới xác nhận D1. Worker
`notify --telegram-updates-source edge` dùng cùng bộ xử lý lệnh với long polling.
Webhook Telegram phải đăng ký cả hai loại update `message` và `callback_query`.
Chi tiết triển khai và rollback nằm trong [Cloudflare Telegram ingress](edge-ingress.md).

Đặt `TELEGRAM_ADMIN_ONLY=true` trong giai đoạn thử nghiệm để chỉ chat khớp
`TELEGRAM_ADMIN_CHAT_ID` được xử lý lệnh và duy trì subscription hoạt động. Khi
tắt chế độ này, người dùng khác có thể đăng ký bằng `/start` như bình thường.
Production hiện tắt `TELEGRAM_ADMIN_ONLY`: người dùng thường được dùng các lệnh
công khai và nhận bài phù hợp sau khi đăng ký. Các lệnh duyệt đề xuất, duyệt bài,
xem bài mới nhất và tình trạng chi tiết vẫn bắt buộc khớp `TELEGRAM_ADMIN_CHAT_ID`.

`/start` chấp nhận tham số chiến dịch Telegram tối đa 64 ký tự base64url và lưu
nguồn đầu tiên. Người dùng mới chưa nhận tin cho tới khi chọn phạm vi thông báo.
Chế độ **Nhận ngay** được hoãn tới 07:00 nếu giờ yên lặng đang bật và thời điểm lập tin nằm
trong 22:00–07:00 theo `Asia/Bangkok`. Chế độ `daily` đưa campaign vào
`digest_items`; notifier gom các mục đến hạn thành `digest_batches` bền vững rồi
gửi dưới tên **Bản tin lúc 07:30** theo `Asia/Bangkok`, tôn trọng giờ yên lặng và giới hạn 4096 ký tự.
Độ dài được tính trên toàn bộ chuỗi cuối cùng gồm tiêu đề, từng mục, link và dòng
báo còn tin; không dùng biên dự phòng ước lượng. Mục chưa vừa bản tin được giữ lại
cho lần kế tiếp và không bị đánh dấu đã gửi. Các campaign cùng bài, cùng phần tóm
tắt hiển thị và cùng link được gộp thành một mục, nhưng toàn bộ campaign ID vẫn
được gắn với batch để giữ audit và ngăn xuất hiện lại trong bản tin sau.

Thông báo Portal là một nguồn delivery riêng và bắt buộc đối với mọi subscriber
đã hoàn tất onboarding, gồm cả người đã dùng `/stop`. Chúng bỏ qua phạm vi ĐRL,
digest và giờ yên lặng; người bị quản trị viên loại hoặc Telegram từ chối vì đã
chặn bot không được coi là người nhận. Retention giữ lại subscriber do chính người
dùng `/stop` để họ tiếp tục nhận Portal. Lần chạy đầu chỉ lưu ID mới nhất làm
baseline nhằm không phát lại toàn bộ lịch sử Portal.

Notifier poll API công khai `portal.ut.edu.vn/api/v1/notification`, retry tối đa ba
lần mỗi request và lưu cursor, notice, campaign cùng delivery trong PostgreSQL.
Tệp chỉ được tải từ endpoint HTTPS chính thức
`/api/v1/notification/getFile/{id}`, có timeout riêng và giới hạn 50 MiB. Campaign
chỉ cho một delivery upload khi chưa có `telegram_file_id`; sau khi Telegram trả
`file_id`, các delivery còn lại dùng lại ID đó và không tải lại tệp Portal. Lỗi
poll xuất hiện trong trường `portal.error` của `notification-worker-cycle.v1`; lỗi
tải hoặc gửi đi theo retry hữu hạn của delivery.

### Poll Portal thích ứng

Đây là hành vi production từ release `20260809-portal-adaptive-polling-v2`.
Mục tiêu là giảm request từ IP đầu ra của máy chủ mà vẫn giữ độ trễ ngắn khi Portal đăng
nhiều thông báo liên tiếp:

- Ở trạng thái ổn định, poll mỗi 300 giây với jitter hữu hạn và chỉ yêu cầu mục mới nhất
  bằng `page=1&size=1`.
- Khi ID mới nhất lớn hơn cursor, tải các trang có kích thước nhỏ theo thứ tự giới hạn cho
  đến khi gặp cursor, xử lý mọi ID còn thiếu theo thứ tự tăng dần, rồi poll mỗi 60 giây trong
  15 phút. Mỗi ID chỉ được lấy chi tiết và tệp khi chưa được lưu.
- Sau 15 phút không có ID mới, tự trở lại chu kỳ 300 giây. Trạng thái burst và thời điểm poll
  kế tiếp phải quan sát được và an toàn qua restart; PostgreSQL cursor vẫn là mốc đầy đủ.
- Với HTTP `403`, không retry tức thời: mở circuit 6 giờ và báo quản trị viên. Với `429`, tôn
  trọng `Retry-After`; nếu header thiếu hoặc không hợp lệ thì nghỉ tối thiểu 30 phút. Với `5xx`
  hoặc lỗi mạng, chỉ retry hữu hạn bằng exponential backoff có jitter rồi nghỉ 15 phút.
- Dùng `ETag` hoặc `Last-Modified` khi endpoint thực sự cung cấp và xử lý đúng conditional
  request; đây chỉ là tối ưu băng thông, không được dùng thay cursor hoặc tính là giảm số request.
- Log phải ghi chế độ `steady`/`burst`/`cooldown`, HTTP outcome, lý do cooldown, thời điểm poll
  kế tiếp và số ID mới, nhưng không ghi payload riêng tư hoặc địa chỉ hạ tầng.
- Test phải chứng minh không bỏ sót nhiều ID giữa hai poll, restart không phát lại lịch sử,
  `403`/`429` không tạo retry burst, và rollback về scheduler cố định không làm hỏng cursor.

IPv6 không phải cơ chế giảm tải hoặc né chặn. Không đổi họ địa chỉ, proxy, exit node hay egress
để vượt `403`/`429`; phải giảm request và tôn trọng cooldown. Tại lần kiểm tra production ngày
2026-08-09, máy chủ có default IPv6 route nhưng DNS công khai của `portal.ut.edu.vn` không có
bản ghi AAAA, nên request Portal vẫn dùng IPv4.

Tương tác Telegram và callback payOS lỗi được thử lại hữu hạn trong PostgreSQL.
Sau ba lần, sự kiện được chuyển sang trạng thái lỗi cách ly để một payload hỏng
không thể chặn các tin nhắn phía sau; trạng thái này xuất hiện trong health admin.

Campaign lưu link bài gốc, link hành động và cờ ĐRL. Nút đăng ký dùng callback để
ghi `notification_cta_clicked` trước khi trả link, còn hai nút phản hồi ghi duy
nhất một giá trị `useful` hoặc `irrelevant` cho mỗi subscriber và campaign. Lời
mời donate theo ngữ cảnh chỉ xuất hiện ở phản hồi hữu ích đầu tiên; người dùng vẫn
có thể chủ động chọn nút **Ủng hộ** hoặc lệnh `/donate` bất cứ lúc nào. Admin dùng
`/metrics` để xem funnel 7 ngày mà không cần truy vấn chat ID hoặc dữ liệu cá nhân.

## Luồng xử lý

```text
classification.completed
    → matched_explicit: tạo một campaign duy nhất
    → manual_review: báo riêng cho admin và chờ quyết định
    → tạo một delivery cho mỗi người đang hoạt động
    → gửi Telegram
    → lưu message ID và lịch sử attempt
```

`rejected` không tạo thông báo. `manual_review` được lưu trong hàng đợi PostgreSQL
và bot gửi chi tiết cho `TELEGRAM_ADMIN_CHAT_ID`. Admin dùng `/reviews`, `/review_ID`,
`/review_send_ID` hoặc `/review_skip_ID`. Mọi lệnh đọc hoặc thay đổi hàng đợi
đều kiểm tra chat ID; quyết định được audit trong `manual_review_resolutions`.
`/review_send_ID` tạo campaign và delivery trong cùng transaction. Khóa duy nhất trên
resolution và campaign ngăn xử lý hoặc gửi trùng khi lệnh được lặp lại.
Nếu một classification mới có cùng nguồn, thời điểm đăng và `content_hash` với bài đã
được admin xử lý, notifier ghi một resolution `skip` kế thừa để giữ audit và không hỏi
duyệt hoặc tạo campaign lần nữa.

Admin dùng `/latest [trang]` để xem feed post mới nhất đã lưu từ toàn bộ nguồn đang
bật và `/latest_post_ID` để xem nội dung đầy đủ. Hai lệnh này kiểm tra
`TELEGRAM_ADMIN_CHAT_ID` trước khi truy vấn dữ liệu.

Admin dùng `/portal_history [trang]` để xem lịch sử thông báo Portal đã crawl và
`/portal_notice_ID` để xem một thông báo cụ thể. Nếu bản ghi có tệp đính kèm, bot
tải lại tệp từ endpoint HTTPS chính thức của Portal và gửi kèm cho admin. Hai lệnh
này kiểm tra `TELEGRAM_ADMIN_CHAT_ID` trước khi truy vấn hoặc tải tệp.
Khi bảng lịch sử còn trống, notifier lưu tối đa 20 thông báo gần nhất làm lịch sử
tham chiếu nhưng không tạo campaign hoặc gửi lại các thông báo cũ.

Khi notifier khởi động, bot đặt menu mặc định gồm năm lệnh công khai và đặt menu
riêng cho `TELEGRAM_ADMIN_CHAT_ID` gồm các lệnh quản trị thường dùng. Các lệnh có
mã bản ghi vẫn hoạt động nhưng không làm menu dấu `/` bị rối. Việc ẩn menu chỉ hỗ
trợ giao diện; kiểm tra chat ID trong worker vẫn là lớp phân quyền bắt buộc.

## Cấu hình bot an toàn

Chỉ cần khóa bot khi gửi thật. Không đặt khóa trong tham số dòng lệnh vì hệ điều
hành có thể hiển thị tham số trong danh sách process. Trên máy cá nhân có thể nhập
khóa vào biến môi trường tạm của terminal:

```powershell
$secureToken = Read-Host "Telegram bot token" -AsSecureString
$credential = [PSCredential]::new("telegram", $secureToken)
$env:TELEGRAM_BOT_TOKEN = $credential.GetNetworkCredential().Password
```

Trên server, dùng secret store hoặc environment file chỉ tài khoản chạy service có
quyền đọc. Không commit khóa bot.

## Người nhận

```powershell
target/release/uth-agent subscriber add --chat-id 123456789 --name "Admin"
target/release/uth-agent subscriber list
target/release/uth-agent subscriber remove --chat-id 123456789
```

`chat_id` là mã cuộc trò chuyện Bot API, không phải username và không nên suy ra từ
đường dẫn Telegram Web. Bot phải được người dùng mở và nhấn Start trước khi có thể
gửi tin nhắn riêng.

## Worker

```powershell
target/release/uth-agent notify `
  --concurrency 5 `
  --messages-per-second 25 `
  --max-attempts 5
```

Worker không bật paid broadcast. Mỗi người nhận được giới hạn tối đa một lần gửi
mỗi giây. Toàn hệ thống mặc định không vượt 25 yêu cầu gửi mỗi giây. Khi Telegram
trả `retry_after`, delivery được dời đúng số giây đó. Lỗi mạng và lỗi máy chủ được
thử lại với khoảng chờ tăng dần. Người chặn bot được ngừng nhận tự động.

Tin nhắn được cắt theo ký tự để không vượt giới hạn 4096 ký tự của `sendMessage`.
Không dùng Markdown hoặc HTML nên nội dung bài đăng không thể làm hỏng định dạng.
Lỗi riêng ở bước chuẩn bị digest được ghi trong `notification-worker-cycle.v1`
và không làm dừng xử lý tương tác. Worker đăng ký cả `SIGINT` và `SIGTERM`, hoàn tất
chu kỳ đang chạy rồi chủ động nhả lease PostgreSQL khi dừng có kiểm soát. Chế độ
chạy một lần và lỗi chu kỳ cũng nhả lease. Lease có hạn vẫn là lớp an toàn cho
trường hợp process bị kill cưỡng bức hoặc mất máy đột ngột.

## Giới hạn chống gửi trùng

Campaign và hàng đợi delivery được tạo đúng một lần. Tuy nhiên Telegram không nhận
khóa chống trùng cho `sendMessage`. Nếu Telegram đã nhận tin nhưng process chết
trước khi PostgreSQL ghi `sent`, lần thử sau có thể gửi lại. Đây là giới hạn không
thể loại bỏ hoàn toàn; mọi attempt và Telegram message ID đều được lưu để đo cửa sổ
rủi ro này.

Delivery thành công được giữ mặc định 90 ngày, delivery lỗi 30 ngày và người nhận
đã ngừng hoạt động 90 ngày sau khi không còn delivery tham chiếu.

Tài liệu Bot API chính thức: [sendMessage](https://core.telegram.org/bots/api#sendmessage)
và [ResponseParameters](https://core.telegram.org/bots/api#responseparameters).
