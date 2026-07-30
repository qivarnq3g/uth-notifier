# Rules classifier

- **Trạng thái:** Explainable rules implementation đã có unit và PostgreSQL integration test
- **Phạm vi:** Hard validation và rules; chưa có local ONNX hoặc AI fallback

## Contract `classification.v1`

Mỗi kết quả chứa:

```json
{
  "schema_version": "classification.v1",
  "post_source_id": "facebook:source.a",
  "external_post_id": "post-1",
  "input_content_hash": "sha256:...",
  "decision": "matched_explicit",
  "score": 12,
  "confidence_basis_points": 8750,
  "matched_rules": ["feature.explicit_drl", "decision.explicit_threshold"],
  "features": {},
  "extracted": {},
  "classifier_version": "rules-2026-07-29-online-participation",
  "config_hash": "sha256:...",
  "classified_at": "2026-07-19T12:00:00+00:00"
}
```

`confidence_basis_points` là độ chắc chắn heuristic của rule decision, không phải
xác suất đã hiệu chuẩn. Không được dùng giá trị này như metric chất lượng model.

## Decision

| Decision | Ý nghĩa |
| --- | --- |
| `rejected` | Hard validation đã loại bài hoặc không có bằng chứng hành động |
| `matched_explicit` | Có bằng chứng ĐRL cùng lời kêu gọi đăng ký hoặc hướng dẫn tham gia trực tuyến rõ ràng, hoặc form đăng ký sinh viên từ nguồn đã duyệt có đủ ngữ cảnh và score |
| `manual_review` | Có dấu hiệu ĐRL, đăng ký hoặc form nhưng chưa đủ điều kiện tự gửi |

Rules hiện trích `explicit_drl`, `registration_call`, `form_link`,
`future_event_time`, `future_deadline`, `location`, `target_students`,
`approved_source`, `negative_commercial` và `past_event`. Cấu hình, weight,
keyword và form host nằm trong `config/classifier-rules.v1.json`; mọi kết quả lưu
classifier version và SHA-256 của toàn bộ config bytes.

Cụm `tham gia trực tuyến trên hệ thống` được coi là lời kêu gọi hành động. Cụm này
chỉ tự gửi theo nhánh ĐRL khi bài đồng thời có bằng chứng điểm rèn luyện, đạt score
và không vi phạm hard reject; nếu đứng riêng, bài vẫn không được tự gửi.

Ngoài nhánh ĐRL rõ ràng, classifier tự gửi form đăng ký khi đồng thời có form thuộc
host tin cậy, lời kêu gọi đăng ký, đối tượng sinh viên, nguồn đã duyệt, ít nhất một
ngữ cảnh thời gian/địa điểm và score đạt `registration_form_match_score`. Form đơn
lẻ, bài không nhắm tới sinh viên hoặc thiếu ngữ cảnh hoạt động vẫn vào review hoặc
bị từ chối; hard reject luôn được áp dụng trước nhánh này.

## Durable worker

```text
facebook_post.discovered / facebook_post.updated
    → PostgreSQL lease + FOR UPDATE SKIP LOCKED
    → validate event payload và persisted post revision
    → classify
    → classifications + classification_features
    → classification.completed
    → mark input event processed
```

Toàn bộ bước ghi kết quả, feature, completion event và mark input event nằm trong
một transaction. Unique key ngăn tạo classification hoặc completion event trùng.
Lỗi tạm thời được retry với backoff hữu hạn. Poison event chuyển sang
`dead_letters` cùng payload, attempt count và error bị giới hạn độ dài. Dead letter
và processed input event có retention hữu hạn, mặc định 30 ngày; pending event
không bị retention xóa.

## Giới hạn

- Date parser hiểu ngày dạng `dd/mm/yyyy`, `dd-mm-yyyy`, `yyyy-mm-dd`, ngày không năm
  trong ngữ cảnh phù hợp và các biến thể dấu phân cách tương đương. Ranh giới chữ số ngăn
  niên khóa như `2025-2026` bị đọc một phần thành ngày.
- Ngữ cảnh deadline gồm cả `hạn chót`, `hạn cuối`, `trước ngày` và `chậm nhất`; bài chỉ có
  deadline đã qua bị từ chối cứng.
- Keyword rules có thể bỏ sót cách diễn đạt mới hoặc tạo false positive. Bài không
  có dấu hiệu ĐRL, đăng ký hoặc form bị từ chối; bài có dấu hiệu nhưng chưa đủ bằng
  chứng vào `manual_review`.
- Dataset thật đầu tiên có 20 bài từ một fanpage và không có bài nào mang nhãn
  `matched_explicit`. Dataset này đo được exact decision agreement và false
  notification, nhưng chưa thể đo recall cho bài thực sự cần gửi.
- `matched_explicit` tạo Telegram campaign qua durable outbox; `rejected` không gửi.
  `manual_review` chỉ gửi yêu cầu duyệt riêng cho admin, không tạo campaign cho người
  nhận cho đến khi admin dùng `/review_send ID`.

## Đánh giá chất lượng

Lệnh `evaluate-classifier` đọc contract `classifier-evaluation-dataset.v1`, chạy
cùng một rules implementation như worker và xuất `classifier-evaluation-report.v1`:

```powershell
target/release/uth-agent evaluate-classifier `
  --dataset tests/fixtures/classifier/rules_cases.v1.json `
  --minimum-precision-basis-points 10000 `
  --minimum-recall-basis-points 10000 `
  --output results/classifier-evaluation.json
```

Report ghi exact decision accuracy, precision, recall, F1, confusion counts và từng
ca phân loại sai. Basis point nằm trong khoảng 0–10000; 10000 tương ứng 100%.
Lệnh trả exit code khác 0 khi metric thấp hơn ngưỡng nhưng vẫn ghi report để điều
tra nguyên nhân. Dataset lịch sử có thể đặt `evaluated_at` riêng cho từng case để
chạy lại classifier tại thời điểm bài được đăng, tránh dùng hạn đăng ký đã qua ở
thời điểm review làm sai backtest.

Fixture trong repository gồm 13 ca tổng hợp để bảo vệ regression của rules và CLI.
Kết quả trên fixture này không được trình bày như chất lượng đo từ dữ liệu thật.
Đánh giá production cần một dataset riêng gồm bài đăng thực tế đã được người có
thẩm quyền gán nhãn và rà soát định kỳ.

## Chuẩn bị dữ liệu review từ bài thật

Lệnh `prepare-classifier-review` chỉ nhận một healthy `facebook-crawl-report.v1`.
Lệnh chạy classifier hiện tại trên từng bài rồi ghi đồng thời JSON có cấu trúc và
Markdown dễ đọc:

```powershell
target/release/uth-agent prepare-classifier-review `
  results/classifier-review/giadinhkynang-crawl.json `
  --output results/classifier-review/giadinhkynang-review.json `
  --markdown-output results/classifier-review/giadinhkynang-review.md
```

Mỗi bài có số thứ tự, URL gốc, nội dung, predicted decision, score, matched rules,
`human_decision=null` và `reviewer_note=null`. Người review chọn một trong ba nhãn
`matched_explicit`, `manual_review`, `rejected`. Dự đoán của rules chỉ là gợi ý và
không được tự sao chép thành nhãn con người.

Sau khi lưu nhãn và lý do theo contract `classifier-human-labels.v1`, kết sổ review
thành dataset tái lập:

```powershell
target/release/uth-agent finalize-classifier-review `
  results/classifier-review/giadinhkynang-review.json `
  results/classifier-review/giadinhkynang-human-labels.json `
  --output-review results/classifier-review/giadinhkynang-review-final.json `
  --output-dataset results/classifier-review/giadinhkynang-evaluation.v1.json `
  --markdown-output results/classifier-review/giadinhkynang-review-final.md
```

Baseline sau safety gate trên 20 nhãn đầu tiên đạt 19/20 exact decision, không tạo
false notification và để ba bài vào `manual_review`. Precision và recall của nhãn
gửi vẫn chưa xác định vì dataset này không có positive case.
