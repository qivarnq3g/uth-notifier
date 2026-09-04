# Gemini Auto-Review & Dynamic Few-Shot Learning Specification

## 1. Mục tiêu
Tự động hóa quy trình duyệt các bài viết Facebook rơi vào trạng thái `manual_review` bằng cách tích hợp Gemini API (model `gemini-3.5-flash-lite`), đồng thời thiết lập cơ chế phản hồi từ Admin (Telegram Feedback Loop) để tích lũy tri thức thông qua Dynamic Few-Shot In-Context Learning.

## 2. Lựa chọn Model
Dựa trên phân tích tệp HAR từ Google AI Studio (`aistudio.google.com.har`):
- Model chính: `gemini-3.5-flash-lite` (hoặc `gemini-flash-lite-latest`).
- Hạn mức Free Tier: 15 RPM, 500 RPD, 250k TPM.
- Thời gian phản hồi: < 1s, hỗ trợ `responseMimeType: "application/json"` và `responseSchema`.
- Cấu hình qua CLI/Env: `GEMINI_API_KEY`, `GEMINI_MODEL`, `GEMINI_API_BASE`.

## 3. Kiến trúc Cơ sở dữ liệu (Migration 0022)
Bảng `ai_review_learning_examples`:
- `id`: BIGSERIAL PRIMARY KEY
- `classification_id`: BIGINT REFERENCES classifications(id) ON DELETE SET NULL
- `post_id`: BIGINT REFERENCES posts(id) ON DELETE SET NULL
- `post_text`: TEXT NOT NULL
- `source_name`: TEXT NOT NULL
- `ai_decision`: TEXT NOT NULL CHECK (ai_decision IN ('send', 'skip'))
- `ai_reason`: TEXT NOT NULL
- `admin_decision`: TEXT NOT NULL CHECK (admin_decision IN ('send', 'skip'))
- `admin_notes`: TEXT
- `created_at`: TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP

Index: `ai_review_learning_examples_created_idx` trên `created_at DESC`.

## 4. Giao thức Tương tác Gemini API
Endpoint: `POST {GEMINI_API_BASE}/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}`

Cấu hình Generation:
- `responseMimeType: "application/json"`
- `responseSchema`:
  - `decision`: enum `["send", "skip"]`
  - `reason`: string tiếng Việt
  - `confidence`: float [0.0, 1.0]

Prompt Structure:
1. System Instruction: Tiêu chuẩn tin tức sinh viên UTH (điểm rèn luyện, học bổng, việc làm, hội thảo, tình nguyện cấp trường/khoa; từ chối quảng cáo ngoài trường, spam).
2. Dynamic Few-Shot: 3-5 mẫu sửa sai mới nhất từ `ai_review_learning_examples`.
3. Input: Nguồn bài viết, nội dung bài viết, link bài viết.

## 5. Tích hợp Notification Worker & Telegram Commands
Quy trình:
1. Khi phát hiện `ClassificationDecision::ManualReview`:
   - Nếu `GEMINI_API_KEY` có cấu hình: gọi Gemini API.
   - Nếu Gemini duyệt `send`:
     - Tự động gọi `store.resolve_manual_review(id, ..., Send)`.
     - Tạo campaign gửi bài cho subscribers.
     - Báo cho Admin kèm lệnh / nút: `/ai_reject_{id}` (Báo sai: Bỏ qua).
   - Nếu Gemini duyệt `skip`:
     - Tự động gọi `store.resolve_manual_review(id, ..., Skip)`.
     - Báo cho Admin kèm lệnh / nút: `/ai_approve_{id}` (Đảo ngược: Duyệt).
   - Nếu Gemini lỗi (mạng, timeout, 429) hoặc không có key:
     - Giữ nguyên trạng thái `pending_manual_reviews`, gửi tin duyệt thủ công như cũ cho Admin.
2. Admin tương tác:
   - `/ai_reject_{id} [lý do]`: Ghi nhận bài đã duyệt sai ('send' -> 'skip'), lưu vào `ai_review_learning_examples`.
   - `/ai_approve_{id} [lý do]`: Đảo ngược bài bị bỏ qua ('skip' -> 'send'), chuyển trạng thái `send`, tạo campaign gửi cho sinh viên, lưu vào `ai_review_learning_examples`.

## 6. Ràng buộc Kỹ thuật
- Không thêm code comments trong Rust / SQL.
- Tuân thủ quy chuẩn UTF-8 SQL_ASCII.
- Timeout gọi Gemini API: 10 giây.
- Fallback an toàn 100% không làm mất bài viết.
