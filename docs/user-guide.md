# Cách dùng UTH Notifier Bot

UTH Notifier Bot theo dõi các trang Facebook đăng hoạt động và các thông báo công
khai trên Portal UTH.

## Bắt đầu

Mở bot và chọn **Start**. Bot cho xem một tin mẫu nếu bạn muốn, sau đó bạn chọn
chỉ hoạt động có nhắc tới điểm rèn luyện hoặc mọi hoạt động phù hợp.

Tham gia nhóm hỗ trợ và nhận cập nhật mới nhất tại:
https://t.me/uth_notifier_group

## Các nút chính

- **Trang đang theo dõi:** xem những trang bot đang kiểm tra.
- **Đề xuất trang:** gửi một trang Facebook mới cho quản trị viên xem xét.
- **Gửi phản hồi:** gửi góp ý tự do cho quản trị viên.
- **Cài đặt:** xem tin mẫu, chọn loại hoạt động, nhận từng tin ngay khi phát hiện hoặc một bản tin lúc 07:30, bật giờ yên lặng và tạm dừng hoặc bật lại tin hoạt động.
- **Trợ giúp:** xem lại hướng dẫn ngắn.
- **Ủng hộ:** chủ động hỗ trợ chi phí vận hành. Việc ủng hộ hoàn toàn tự nguyện và không ảnh hưởng quyền dùng bot.

## Đề xuất thêm trang

Gửi link theo mẫu:

```text
/suggest https://www.facebook.com/ten.trang
```

Bot sẽ xác nhận đã nhận. Trang chỉ được theo dõi sau khi quản trị viên kiểm tra và
duyệt.

## Lệnh nhanh

```text
/start   Bật thông báo
/settings Chọn loại hoạt động và cách nhận
/pages   Xem các trang đang theo dõi
/help    Xem hướng dẫn
/donate  Ủng hộ chi phí vận hành
/feedback Gửi phản hồi cho quản trị viên
```

Gửi feedback trực tiếp bằng `/feedback nội dung góp ý`, hoặc gửi `/feedback` trước rồi
nhắn nội dung ở tin nhắn tiếp theo. Bot giữ trạng thái chờ tối đa 10 phút và chuyển
feedback cho quản trị viên khi Telegram sẵn sàng; nội dung được giới hạn 2.000 ký tự.

Trong **Cài đặt**, hai cách nhận tin hoạt động như sau:

- **Nhận ngay:** bot gửi từng hoạt động phù hợp khi phát hiện. Nếu giờ yên lặng đang bật, tin từ 22:00 đến 07:00 được giữ lại đến sau 07:00.
- **Bản tin lúc 07:30:** bot gom các hoạt động mới phù hợp và gửi một lần mỗi ngày. Bot không gửi bản tin vào ngày không có tin mới.

Thông báo mới từ Portal UTH là thông báo bắt buộc đối với người đã dùng bot. Bot
gửi ngay, không phụ thuộc phạm vi ĐRL, bản tin 07:30, giờ yên lặng hoặc `/stop`.
Nếu Portal có tệp đính kèm, bot gửi tệp cùng nội dung thông báo. `/stop` chỉ tạm
dừng tin hoạt động Facebook; người chặn hẳn bot trên Telegram thì Telegram không
cho bot gửi bất kỳ loại tin nào.

Thông báo hoạt động có nút mở biểu mẫu, xem bài gốc và phản hồi **Hữu ích** hoặc
**Không phù hợp**. Phản hồi được dùng để đo và cải thiện độ liên quan. Khi bạn
đánh dấu một thông báo hữu ích, bot có thể hiển thị một lời mời ủng hộ duy nhất;
bỏ qua lời mời không ảnh hưởng bất kỳ chức năng nào.

Bot tạo link riêng có thời hạn và tự xác nhận sau khi payOS báo thanh toán thành
công. Khoản ủng hộ là tự nguyện và không ảnh hưởng đến quyền nhận thông báo.
Bạn chọn một trong ba nút 10.000, 20.000 hoặc 50.000 VND. Nếu chọn `Tùy tâm`, bot
chờ 10 phút để bạn gửi số tiền từ 10.000 đến 10.000.000 VND, chẳng hạn `35000`,
`35.000` hoặc `35k`. Gửi `/cancel` để hủy nhập; việc ủng hộ không ảnh hưởng các
chức năng khác của bot.

Khi danh sách có nhiều trang, chọn lệnh màu xanh như `/pages_2` để chuyển sang
trang tiếp theo.

Quản trị viên còn nhận cảnh báo riêng khi crawler hoặc hàng đợi lỗi kéo dài và một
tin xác nhận khi hệ thống phục hồi. Người dùng khác không nhận chi tiết vận hành này.

Quản trị viên thấy thêm menu riêng gồm `/admin`, `/pending`, `/reviews`, `/latest`,
`/feedbacks`, `/crawl_history`, `/portal_history` và `/metrics`. `/feedbacks` hiển thị
lịch sử feedback đã lưu theo từng trang, gồm người gửi, chat ID, thời gian, trạng thái
chuyển cho admin, nội dung đã rút gọn và liên kết mở trực tiếp cuộc trò chuyện với
người gửi. `/crawl_history` hiển thị mọi lần
crawl đã ghi, kể cả lần không tìm thấy bài; dùng `/crawl_run_ID` để xem các lượt thử
và lỗi của một lần. `/portal_history` hiển thị lịch sử crawl Portal;
chọn `/portal_notice_ID` để xem chi tiết và nhận tệp đính kèm nếu có. Các lệnh quản trị có mã bài được bot hiển thị dưới dạng một token
có thể bấm trực tiếp, ví dụ `/review_send_38`.
