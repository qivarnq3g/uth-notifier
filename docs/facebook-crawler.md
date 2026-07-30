# Facebook crawler reliability

- **Trạng thái:** Rust implementation đã kiểm chứng live
- **Ngày kiểm chứng:** 2026-07-19
- **Phạm vi:** Facebook Page công khai, không login/cookie/access token

## Mục tiêu

Crawler không được coi response rỗng là “không có bài mới”. Mỗi lần chạy phải
phân biệt được ít nhất các trạng thái:

| Outcome | Ý nghĩa |
| --- | --- |
| `healthy` | HTTP thành công và số bài đạt `minimum_yield` |
| `sparse` | Chỉ thấy một phần nhỏ timeline, thường là bài ghim/nổi bật |
| `login_wall` | Response không có bài và chứa checkpoint/login wall |
| `parse_failure` | Có JSON script nhưng toàn bộ payload liên quan bị lỗi parse |
| `empty` | Response hợp lệ nhưng không tìm thấy post |
| `http_error` | HTTP status từ 400 trở lên |
| `network_error` | Timeout, DNS, TLS hoặc lỗi kết nối |

Scheduler chỉ được đánh dấu crawl thành công khi report có `health=healthy`.
`degraded` và `failed` phải retry với backoff và phát cảnh báo nếu lặp lại.

Durable scheduler hiện claim nguồn bằng PostgreSQL lease có hạn và
`FOR UPDATE SKIP LOCKED`. Crawl run và attempt luôn được lưu để quan sát failure;
chỉ report `healthy` mới được phép cập nhật bảng `posts` và transactional outbox.
Backoff tăng theo số lần lỗi, có jitter ổn định theo nguồn và bị chặn bởi giới hạn
cấu hình.

Scheduler dùng lịch thích ứng ba tầng nhưng không bỏ theo dõi nguồn im. Một post mới đưa
nguồn về tầng nhanh; các lần crawl `healthy` không đổi mới tăng bộ đếm để chuyển dần sang
tầng thường và tầng im. Bộ đếm được cập nhật trong cùng transaction với post, outbox và
`next_crawl_at`. Baseline dùng tầng thường; report không healthy không được tăng bộ đếm.
Mặc định hiện tại là 120/300/480 giây, với ba lần không đổi ở tầng nhanh và chuyển sang
tầng im từ lần thứ sáu. Các ngưỡng đều cấu hình được và bị kiểm tra quan hệ
`active <= normal <= idle` trước khi scheduler chạy.

Scheduler duy trì circuit breaker riêng cho từng HTTP presentation. Mặc định, 10 attempt
liên tiếp không `healthy` mở circuit trong 900 giây; presentation đang mở bị bỏ qua và
browser fallback vẫn chạy. Hết cooldown chỉ một probe được phép chạy đồng thời. Probe
khỏe đóng circuit, còn mọi outcome khác mở lại circuit. Khi khởi động, scheduler đọc tối
đa 10 attempt gần nhất trong cửa sổ 86.400 giây từ PostgreSQL để không quên circuit chỉ vì
process restart. Báo cáo `crawl-scheduler-cycle.v1` xuất trạng thái circuit và từng
presentation đã thử hoặc bỏ qua.

Lần `healthy` đầu tiên của một nguồn là baseline: post và revision vẫn được lưu,
nhưng transactional outbox không phát event. Crawl `degraded` hoặc `failed` không
hoàn tất baseline. Cờ `--notify-existing-posts` là opt-in rõ ràng để bỏ cơ chế
chặn này. Ở chế độ mặc định, một bài có `published_at` trước thời điểm baseline
đầu tiên tiếp tục được lưu revision nhưng các thay đổi về sau không phát outbox
event; thay đổi do số tương tác hoặc bình luận vì thế không hồi sinh bài lịch sử.

Khóa nguồn trong source-selection được chuẩn hóa về `facebook:<id>` trước khi
upsert, tránh tạo hai nguồn cho cùng một page khi file đầu vào dùng ID số thuần.
Scheduler và batch crawler chỉ tạo numeric crawl presentation khi khóa nguồn chứa ID
Facebook số đã xác minh và URL cấu hình là HTTPS Facebook không có thông tin xác thực.
Nguồn alias ưu tiên presentation
`https://www.facebook.com/people/<configured-alias>/<numeric-page-id>/` và chỉ thử
`profile.php?id=<numeric-page-id>` khi presentation ưu tiên không `healthy`. URL
`/people/.../<numeric-page-id>/` đã xác minh trong cấu hình được giữ nguyên. Mỗi presentation
đều bị ràng buộc với numeric source ID đã xác minh; report khôi phục URL cấu hình để giữ
tương thích contract và phần hiển thị. Report hoặc post trả về source ID khác ID đã xác minh
bị từ chối trước khi ghi PostgreSQL.

Khi fallback chạy, report giữ toàn bộ attempt của presentation ưu tiên trước rồi đến attempt
của `profile.php`; health và post window được lấy từ report có chất lượng cao hơn. Vì vậy
`crawler_attempts` và `/crawl_run_ID` cho biết fallback đã thực sự chạy, thay vì chỉ lưu
presentation đầu tiên.

## Contract `facebook-post.v1`

```json
{
  "schema_version": "facebook-post.v1",
  "source_id": "facebook:hoisinhvien.com.vn",
  "platform": "facebook",
  "external_post_id": "1448197277342081",
  "canonical_url": "https://www.facebook.com/.../posts/...",
  "published_at": "2026-07-19T02:00:17+00:00",
  "text": "...",
  "media": [
    {"kind": "image", "url": "https://...", "alt_text": "..."}
  ],
  "outbound_links": ["https://example.edu.vn/dang-ky"],
  "content_hash": "sha256:...",
  "crawl_strategy": "bingbot",
  "fetched_at": "2026-07-19T03:00:00+00:00"
}
```

`external_post_id` là khóa dedup chính. `content_hash` được tính từ nội dung,
media ổn định và outbound link; metadata của lần fetch không tham gia hash. Vì
vậy cùng post/cùng nội dung là `unchanged`, còn post đã sửa là `updated`.
Với media trên `*.fbcdn.net`, định danh dùng để tính hash giữ đường dẫn tệp nhưng
bỏ host CDN và query có thời hạn. URL media đầy đủ vẫn được lưu để hiển thị.
Storage đối chiếu cùng quy tắc khi chuyển từ hash cũ sang hash mới; nếu chữ,
outbound link và định danh media không đổi thì cập nhật revision nhưng không phát
`facebook_post.updated`.

`pfbid` từ presentation Facebook ẩn danh chỉ được xem là locator, không phải định danh ổn
định tuyệt đối. Browser ưu tiên numeric post ID khi payload có giá trị này. Khi chỉ có
`pfbid`, storage đối chiếu thêm nguồn, thời điểm đăng chính xác và `content_hash`; một
locator mới có cùng ba thuộc tính được cập nhật như alias của bài hiện có và không phát
sự kiện khám phá hoặc cập nhật trùng.

`missing_from_current_window` không đồng nghĩa post đã bị xóa: Facebook chỉ trả
một cửa sổ timeline giới hạn. Chỉ kết luận bị xóa sau khi kiểm tra permalink
riêng và lặp lại qua nhiều lần crawl.

## Fallback hiện tại

```text
standard browser HTTP
    ↓ không healthy
polite project user-agent
    ↓ không healthy
experimental search-crawler presentation
    ↓ tổng cửa sổ HTTP dưới 20 post hoặc URL dạng /people/
Playwright system Chrome bounded history sweep, không login, hợp nhất với HTTP
    ↓ vẫn không có post khả dụng sau retry hữu hạn
report failed
```

Các tên strategy `bingbot`/`googlebot` được giữ để tương thích cấu hình cũ nhưng
dùng user-agent trình duyệt tiêu chuẩn, không mạo danh crawler tìm kiếm. Khi HTTP chỉ
trả một cửa sổ nhỏ, browser fallback vẫn chạy để quét bù hữu hạn; kết quả được hợp nhất
với HTTP thay vì ghi đè. URL dạng `/people/` luôn được browser xác minh vì
HTTP có thể chỉ trả một video cũ thay vì bài đầu feed. Browser tìm permalink thuộc
đúng owner, mở permalink, tương quan `pfbid` hoặc video ID với `post_id`, timestamp
và caption rồi chuyển snapshot cho Rust chuẩn hóa. Mỗi process có timeout, mặc định
retry tối đa hai lần và không dùng login, cookie hoặc access token.

Kiểm tra cấu hình scheduler tính lease theo trường hợp xấu nhất gồm cả presentation `/people/`
và fallback `profile.php`. Với mặc định HTTP 35 giây, browser 60 giây và hai browser retry,
lease tối thiểu là 550 giây; cấu hình production 600 giây vẫn nằm trong giới hạn an toàn.

Ba presentation HTTP không bị tắt vĩnh viễn. Circuit breaker chỉ bỏ qua presentation đã
được đo là lỗi liên tiếp, giảm request không hữu ích và tự probe lại sau cooldown. Nếu
toàn bộ HTTP circuit đang mở, scheduler tạo report HTTP rỗng có kiểm soát rồi đi thẳng
đến Playwright; không diễn giải việc bỏ qua HTTP là không có bài mới.

HTTP response bị giới hạn 8 MiB. Trình duyệt JSON dừng ở độ sâu 128 hoặc 200.000
node. Link Facebook bỏ tham số theo dõi; link ngoài giữ tham số nghiệp vụ và chỉ
loại các khóa theo dõi phổ biến như `utm_*`, `fbclid`, `gclid` và `ref`.

Playwright thu response `/api/graphql/` trong đúng browser context khi phải mở page route.
Mỗi response bị giới hạn 2 MiB; snapshot dành tối đa 3 MiB cho payload gốc và 1 MiB cho
bản ghi post đã chuẩn hóa. Adapter không phát lại request GraphQL, không hard-code `doc_id`
và không tạo session khác với trang đang tải. Browser bounded-sweep Page Plugin và trang
nguồn, thu thêm các response GraphQL đang phát sinh và dùng tối đa 20 bài hoàn chỉnh
trong một crawl. Payload được escape trước khi gắn thành
`script[type=application/json]`; Rust tiếp tục xác minh owner, chuẩn hóa URL, dedup, tính
content hash và quyết định health. Nếu không có GraphQL hợp lệ, Page Plugin và DOM vẫn là
fallback không đổi.

## Implementation

Production path hiện nằm trong Rust workspace:

```text
apps/core-agent/       uth-agent CLI, Tokio current-thread runtime
apps/browser-agent/    TypeScript, Playwright, system Chrome
crates/domain/         contract được version hóa
crates/crawler/        HTTP strategy, parser, validation, diff
```

HTTP dùng một `reqwest::Client` tái sử dụng connection, TLS qua Rustls, timeout và
redirect policy hữu hạn. Tokio dùng `current_thread` vì một agent có thể multiplex
I/O mà không cần mặc định tạo worker pool nhiều thread.

Parser không dùng DOM. Một scanner byte-level tìm đúng thẻ
`script[type=application/json]` và trả borrowed slice cho `serde_json`. Quyết định
này tránh dựng cây HTML hơn 2 MB chỉ để lấy JSON, đồng thời giảm release binary từ
5.330.432 xuống 4.585.984 byte trong build Windows x86-64 ngày 2026-07-19.

## Kết quả live gần nhất

Audit toàn bộ lịch sử từ `2026-07-22 15:17:22+00` đến `2026-07-29 10:28:57+00`
gồm 39.278 run của 43 nguồn và 111.276 attempt. Có 31.212 run `healthy`, 8.041
`failed` và 25 `degraded`, tương ứng tỷ lệ `healthy` toàn kỳ 79,46%. Ba nguồn đã cấu hình
`/people/.../<numeric-id>/` đạt trung bình 99,47% `healthy`; 40 nguồn alias đạt trung bình
77,91%. Trong 100 cửa sổ 15 phút mà toàn bộ nguồn alias thất bại, ba nguồn `/people/` vẫn
khỏe 374/375 lượt. Đây là tương quan theo presentation, không phải bằng chứng rằng mọi thời
điểm Facebook đều trả cùng một nội dung.

Từ lúc production chuyển alias sang `profile.php?id=<numeric-id>` lúc
`2026-07-29 06:07:56+00`, 188/203 run là `healthy`, tăng lên 92,61%; 15 run còn lại đều
dừng ở browser `login_wall`. Trong 11 trường hợp đã có lần khỏe kế tiếp, trung vị thời gian
phục hồi là 870,3 giây, p90 là 1.810,7 giây. Phần lớn độ trễ này đến từ hàng đợi scheduler
`concurrency=1`, không phải browser retry tức thời. Không tăng concurrency production vì một
Chromium đã dùng khoảng 696 MiB RSS trong khi unit có `MemoryMax=1400M`.

Canary ghép cặp ngày `2026-07-29` dùng đúng bốn Facebook numeric ID đã xác minh. Cả bốn
presentation `profile.php` và bốn presentation `/people/` đều `healthy`; `/people/` lấy được
nhiều post hơn ở 3/4 cặp và không kém ở cặp còn lại. Kết hợp với lịch sử production, alias
được chuyển sang `/people/<configured-alias>/<numeric-id>/` làm presentation ưu tiên, giữ
`profile.php` làm fallback hữu hạn. Thay đổi không dùng cookie, phiên đăng nhập, proxy hoặc
request GraphQL phát lại.

Release canary v3 sau đó chạy 89 lượt production: 79 `healthy` và 10 `degraded`. Mười lượt
không khỏe đều gặp `login_wall` ở presentation `/people/` và fallback `profile.php`, nên
union hai route không thể phục hồi trong các thời điểm đó. Tỷ lệ ngắn hạn 88,76% thấp hơn
đoạn `profile.php` 92,61% ở khung giờ trước và chưa chứng minh riêng `/people/` làm tăng tỷ
lệ dài hạn; khung giờ và trạng thái chặn của Facebook là biến gây nhiễu lớn.

Release `20260729-people-route-v5` lưu cả hai chuỗi attempt. Bảy run đầu đều `healthy`; một
run ghi rõ `/people/` gặp `login_wall` rồi `profile.php` fallback `healthy`, chứng minh fallback
có tăng kết quả thành công trong trường hợp cụ thể này. Cửa sổ bảy run quá nhỏ để suy rộng
tỷ lệ dài hạn. Cutover không tạo post, revision, classification, campaign hoặc delivery giả;
toàn bộ backlog vẫn bằng 0 tại thời điểm xác minh.

Chẩn đoán production ngày 2026-07-29 ghi nhận 3.085/3.524 run trong 24 giờ là `failed`.
Trong giờ gần nhất, 133/149 browser attempt bị Facebook chuyển alias sang `/login/`, còn
HTTP trả 400 giống nhau trên IPv4 và IPv6. Ba nguồn đã dùng `/people/.../<numeric-id>/`
khỏe 100% trong 24 giờ. Route `/<numeric-id>` từng khôi phục hai alias trong probe nhưng
canary sau đó thất bại 8/8 và chính hai probe cũng quay lại login wall, nên route này không
được coi là ổn định. `profile.php?id=<numeric-id>` khôi phục 2/3 probe tiếp theo, trong đó
một nguồn vừa thất bại bằng route numeric trực tiếp; kết quả production cuối cùng phải được
đánh giá bằng canary nhiều nguồn và không bảo đảm mọi presentation hoặc thời điểm đều khỏe.

Canary production sau khi parser nhận numeric `id` của `profile.php` chạy lại đúng tám
nguồn từng thất bại 8/8 bằng route `/<numeric-id>` và đạt 7/8 `healthy`. Bảy lượt khỏe đều
là post đã lưu không đổi; không tạo post, revision, outbox, classification, campaign hoặc
delivery mới. Một nguồn còn lại vẫn là `login_wall`, vì vậy presentation mới làm giảm mạnh
lỗi quan sát được nhưng không loại bỏ quyền kiểm soát presentation của Facebook.
Sau khi mở lại scheduler, 6/6 run liên tục đầu tiên và 10/10 run gần nhất đều `healthy`;
không có warning scheduler, outbox tồn hoặc delivery tồn tại thời điểm xác minh.

Audit production tiếp theo trong cửa sổ 24 giờ ghi nhận 3.460 run, gồm 484 `healthy` và
2.976 không thành công; phần lớn attempt lỗi là `login_wall`, còn HTTP presentation trả
`400`. Ba nguồn dùng route `/people/.../<numeric-id>/` vẫn đạt 100% `healthy` trong cửa
sổ này. Phân bố `post_count` của các lượt khỏe là 70 lượt có 1 bài, 43 có 4 bài, 7 có
6 bài, 273 có 7 bài, 3 có 9 bài, 98 có 10 bài và 1 có 11 bài. Trong số lượt khỏe có
431 lượt mà bài mới nhất đã lưu cũ hơn 24 giờ; đây là tín hiệu presentation có thể chỉ
trả cửa sổ xếp hạng cũ, không phải bằng chứng chắc chắn rằng crawler bỏ sót bài. Để kết
luận độ trễ phát hiện phải đối chiếu `published_at`, `first_seen_at` và các run trung gian.

Một lần browser fallback gặp `document.body` chưa tồn tại khi sweep lịch sử. Agent nay
dùng chiều cao dự phòng từ `document.documentElement`; sau release kiểm chứng không còn
lỗi `scrollHeight`. Scheduler release `20260729-scheduler-lease-v2` cũng đã được restart
khi đang giữ một lease: lease của owner cũ được giải phóng ngay, không phải chờ hết 600
giây. Đây là xử lý shutdown có kiểm soát, không thay đổi cơ chế lease bảo vệ khi process
crash hoặc host mất kết nối.

Đo production ngày 2026-07-28 xác định `healthy` một post không bảo đảm timeline đầy đủ:
năm bài trễ 31 đến 64 giờ có tổng cộng 1.461 run khỏe trước khi được phát hiện, trong đó
1.460 run chỉ trả đúng một post. Browser agent sau đó được đổi sang bounded history sweep
Page Plugin và page route, còn Rust chạy sweep khi HTTP trả dưới 20 post và hợp nhất hai tập.
Live probe từ máy phát triển trả một post qua HTTP và sáu post bổ sung qua browser, tổng cộng
bảy post khỏe. Canary production cùng ngày gặp `/login/` trên egress máy chủ; bản mới không
còn dùng DOM hint khi final URL là login wall nên ghi đúng `failed`, `post_count=0` thay vì
false `healthy`, `post_count=1`. Kết quả production này xác minh outcome an toàn, chưa xác minh
multi-post trên chính egress máy chủ trong thời gian Facebook còn chặn presentation.

Đo production trong 24 giờ kết thúc ngày 2026-07-25 ghi nhận 7.044 crawl run, trong đó
6.711 `healthy` (95,27%). `browser_playwright` tạo 6.708 run khỏe; `standard` nhận HTTP
400 ở 7.044/7.044 attempt, còn `bingbot` và `googlebot` không tạo attempt khỏe nào.
Đây là baseline trước circuit breaker, không phải cam kết tỷ lệ khỏe sau triển khai.

Canary production cùng ngày khởi tạo ngay circuit `open` cho `standard`, `bingbot` và
`googlebot` từ lịch sử PostgreSQL. Sau 10 login wall liên tiếp, `polite` cũng mở circuit.
Hai run kế tiếp bỏ qua toàn bộ HTTP presentation, chỉ chạy `browser_playwright` và vẫn
`healthy`, `unchanged=1`, `outbox_events=0`. Toàn canary có 14/14 run `healthy`, không có
backlog, dead letter, delivery hoặc classification mới. Một post lịch sử lần đầu xuất
hiện được lưu revision có text nhưng không phát event, đúng quy tắc baseline lịch sử.

Kiểm tra production ngày 2026-07-25 xác định query có thời hạn trong URL ảnh
Facebook làm cùng một bài nhận hash mới sau mỗi crawl. Trong cửa sổ hai giờ có
89 sự kiện `facebook_post.updated`, 4 bài mới và 24 classification
`manual_review` chỉ thuộc 4 post. Sau khi chuẩn hóa định danh media và triển khai
lớp tương thích storage, live crawl trên `vnuhcm.info` đạt `health=healthy` với
một post qua `browser_playwright`. Các lượt production kế tiếp trên nguồn từng
bị ảnh hưởng trả `unchanged=1`, `outbox_events=0`; notifier không còn backlog
classification hoặc delivery sau cutover.

Kiểm tra production ngày 2026-07-28 xác định cùng một bài của `Lớp trưởng UIT`, cùng thời
điểm đăng và cùng `content_hash`, đã được Facebook trình bày bằng bốn `pfbid` khác nhau
trong bốn ngày liên tiếp. Storage vì vậy không được dùng `pfbid` đơn lẻ để quyết định bài
mới; regression test bảo vệ trường hợp locator đổi nhưng nội dung và thời điểm đăng giữ
nguyên.

Đo production ngày 2026-07-23 trên máy chủ 2 CPU, 2 GB RAM với 43 nguồn cho thấy 43/43
lần crawl gần nhất phải chọn `browser_playwright`. Trong cửa sổ 30 phút có 124 run;
khoảng cách trung bình thực tế giữa hai run của cùng nguồn là 504 giây dù lịch cố định là
300 giây. Latency attempt có trung vị 774 ms, p90 3.985 ms và p99 xấp xỉ 11.430 ms.
Kết quả này là baseline trước khi bật lịch thích ứng, không phải cam kết latency sau triển
khai.

Sau khi triển khai cùng ngày, nguồn `Nhà Văn hóa Sinh viên TP. Hồ Chí Minh` phát hiện một
post mới, tạo đúng một outbox event và đặt lần crawl kế tiếp ở 120 giây. Hai crawl
`healthy` không đổi tiếp theo vẫn giữ tầng 120 giây; khoảng cách thực tế giữa hai lần sau
là 147,8 giây. Phần chênh 27,8 giây gồm thời gian chờ hàng đợi và crawl. Lần kiểm tra này
xác minh đường chuyển tầng và transaction trên production, nhưng chưa đủ để kết luận phân
bố latency dài hạn; cần số liệu ít nhất 24 giờ sau khi hàng đợi triển khai ổn định.

Kiểm tra hồi quy live ngày 2026-07-22 với `https://www.facebook.com/CongdoanUTH/`
đã xác nhận browser fallback giữ snapshot trang nguồn trước khi mở permalink. Kết quả
`health=healthy`, một post hợp lệ, `external_post_id=967987786300312`, thời gian đăng
`2026-07-22T09:21:05+00:00` và canonical URL thuộc đúng `CongdoanUTH`. Trước thay đổi,
Facebook chuyển lần mở permalink về trang chủ khiến adapter thay mất HTML nguồn và
không thể xác thực post.

Kiểm tra live tiếp theo cùng ngày xác nhận username page có thể bị chuyển thẳng sang
`/login/` ở mọi HTTP presentation và Playwright. Browser fallback hiện thử Facebook
Page Plugin công khai khi page route không còn permalink. Adapter chỉ chấp nhận post
có permalink thuộc đúng username page, `data-utime` hợp lệ, `pfbid` ổn định và text
không rỗng. Với `viencokhi.uthcmc`, fallback trả `health=healthy`, post
`pfbid0BC616t42MqkfZeSTDyqUj8kYyu4XYdi2w6zMxj7yqYuY3Gq3Atf5FVQH8BU6AbrBl` và thời
gian `2026-03-25T12:55:13+00:00`. Khi nguồn cũ dùng numeric post ID, storage đổi alias
theo canonical URL trong transaction và không phát sự kiện cập nhật giả.

Batch ngày 2026-07-19 trên 43 nguồn giáo dục:

- 43/43 nguồn có `health=healthy`.
- 21 nguồn hoàn tất bằng HTTP.
- 22 nguồn dùng `browser_playwright`.
- 0 bài mới nhất bị rỗng text.
- URL dạng `/people/`, permalink thường và Reel đều xuất cùng contract.

Report kiểm chứng nằm tại `results/crawl-all-final/summary.json` và được xem là
generated output, không nên commit cùng source code.

Canary production ngày 2026-07-27 gọi browser agent qua symlink
`/opt/uth-notifier/current` dưới user `uth-notifier` và truyền stdout qua pipe như
scheduler thật. Probe xuất snapshot JSON hợp lệ 483.436 byte. Trong 17 attempt đầu
sau cutover, 6 attempt `healthy`, 11 attempt `login_wall`, 0 lỗi parse stdout `EOF`;
các lượt khỏe đều là `unchanged` và không tạo outbox event. Kết quả này xác minh
đường response-capture và lỗi hồi quy symlink/stdout đã được chặn, nhưng không chứng
minh login wall từ Facebook đã biến mất.

## Test coverage

Rust fixture tests hiện bao phủ:

- Parse và dedup payload lặp.
- Chuẩn hóa permalink và outbound link.
- Trích xuất media.
- Content hash không phụ thuộc thời điểm/strategy fetch.
- Content hash bỏ host và query có thời hạn của Facebook CDN nhưng vẫn phát hiện
  thay đổi đường dẫn media hoặc query nghiệp vụ bên ngoài.
- Storage không phát update khi chỉ chuyển từ URL CDN cũ sang định danh media ổn định.
- Loại post thuộc fanpage khác.
- Phát hiện login wall.
- Phát hiện malformed JSON.
- Fallback qua nhiều outcome.
- Tương quan username và numeric owner bằng `pfbid` hoặc video ID.
- Chọn GraphQL post mới nhất đúng owner, bỏ payload khác page và chuyển
  `subscription_target_id` thành bản ghi Rust parser hiểu được.
- Bổ sung timestamp và caption cho Reel từ browser snapshot có ràng buộc post ID.
- Diff `new`, `updated`, `unchanged`, `missing_from_current_window`.
- Byte scanner với attribute order, quote style và tag casing khác nhau.

Rust sở hữu HTTP crawler, chuẩn hóa contract, hash, diff và batch orchestration.
TypeScript chỉ là adapter Playwright cho browser fallback. Repository và production
runtime không phụ thuộc Python.

Browser fallback thử Page Plugin trước vì các username page công khai thường chuyển
đến login wall. Adapter chặn image, media và font nhưng vẫn cho document, script,
stylesheet và request dữ liệu chạy. Nếu Page Plugin không tạo post hợp lệ, adapter
quay lại source page và scroll một lần. Một live validation ngày 2026-07-22 trên
`VovinamUTH` giảm latency adapter từ khoảng 9,4 giây xuống 5,1 giây, đồng thời vẫn
trả permalink, `pfbid`, publication time và text; đây là một phép đo đơn lẻ, không
phải cam kết latency cho mọi nguồn.

Một số source page vẫn trả nội dung công khai nhưng đồng thời hiển thị hộp thoại yêu
cầu đăng nhập. Browser adapter đóng hộp thoại bằng nút visible có nhãn `Đóng` hoặc
`Close`, sau đó mới sweep DOM và GraphQL; nếu không có nút đóng thì thử `Escape` một
lần. Khi URL đã chuyển sang `/login/` hoặc `/checkpoint/`, adapter không cố đóng giao
diện để giả lập thành công mà giữ nguyên tín hiệu `login_wall`. Canary live ngày
2026-07-29 trên `utengclub` có final URL là source page, status 200, permalink bài,
thời gian đăng và text không rỗng sau bước đóng hộp thoại.

Chẩn đoán production tiếp theo ngày 2026-07-29 xác nhận Chromium mặc định kết nối
`www.facebook.com` qua IPv6; thêm `--disable-ipv6` không đổi family theo
`Response.serverAddr()`. Cùng source ILASS trả full `/login/`, không dialog, không
article và không post link qua IPv6. Một canary chỉ định đúng A record vừa resolve cho
hostname, vẫn giữ TLS hostname và không dùng proxy, trả source page công khai qua IPv4.
Cả sáu nguồn đang có `failure_count >= 2` đều trả status 200, một dialog đóng được và
ít nhất một post link trong canary IPv4. Trên egress máy phát triển, cả 18 tổ hợp alias,
`/people/` và `profile.php` của sáu nguồn cũng trả nội dung công khai; hai route numeric
đều redirect về alias. Kết quả cô lập khác biệt theo egress/family tại thời điểm đo,
không chứng minh IPv4 sẽ luôn khỏe và không cho phép hard-code hoặc xoay địa chỉ.

Source page có thể hiển thị bài DOM mới hơn các bài hoàn chỉnh thu được từ GraphQL.
Browser adapter so sánh hai ứng viên, mở permalink của ứng viên DOM khác biệt để bổ
sung thời gian, ID số và toàn bộ nội dung, rồi chọn bài có thời gian đăng mới hơn. Nội
dung permalink được chụp ngay sau navigation vì Facebook ẩn danh có thể render bài
trong chốc lát rồi SPA redirect về trang chủ. Nếu bài DOM mới nhất không thể xác định
đủ metadata, attempt được ghi `sparse` thay vì báo `healthy` sai. Canary production
ngày 2026-07-29 trên `doanhoiuth` lấy đúng bài ID `1503388841807379`, origin `dom`,
`newest_dom_post_unresolved=false` trong khoảng 16,6 giây; chu kỳ scheduler kế tiếp
đã lưu cùng bài với thời gian đăng `2026-07-29T15:04:54Z`.

## Giới hạn và điều kiện production

Facebook có thể thay đổi HTML và payload nội bộ mà không báo trước. Việc triển khai
phải duy trì hồ sơ cấp phép phù hợp với phạm vi crawl. Scheduler đã có bounded
concurrency, timeout, lease, backoff và crawl-run retention; production vẫn cần
rate limit theo nguồn, raw failure artifact có retention giới hạn, cảnh báo parse
regression và browser fallback độc lập.

Operational health giữ `failure_count` theo từng nguồn và đánh giá thêm cửa sổ gần nhất
mặc định 900 giây. Một lỗi nguồn thoáng qua chỉ xuất hiện trong số liệu chi tiết. Nguồn
đạt ba lỗi liên tiếp, hoặc có ít nhất ba lỗi chiếm tối thiểu một nửa số run trong cửa sổ,
được tính là `sources_alerting`. Một nguồn alert chỉ làm health tổng `degraded`; health
tổng chỉ `failed` do crawler khi có ít nhất ba nguồn và tối thiểu 25% nguồn đang alert.
Lỗi hàng đợi, delivery, edge hoặc donation vẫn có thể làm health `failed` ngay.

PostgreSQL lưu contract đã chuẩn hóa, crawl diagnostics, revision và outbox event;
không lưu raw HTML. Vì vậy crawler không làm tăng vô hạn dữ liệu raw. Khi bổ sung
failure artifact adapter, artifact phải đặt ngoài PostgreSQL và có cả giới hạn dung
lượng lẫn thời hạn xóa.
Browser production network mode is configured with `FACEBOOK_BROWSER_NETWORK_MODE=prefer_ipv4`.
The adapter resolves a fresh DNS A record for `www.facebook.com`, maps only that hostname
for the current Chromium process, disables QUIC, and records network family plus login and
overlay telemetry without storing the resolved IP. DNS or runtime navigation failure falls
back once to the system resolver. A full `/login/` or `/checkpoint/` result remains
`login_wall`; only a public-page overlay may be closed.
