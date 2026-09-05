use std::time::Duration;

use chrono::{DateTime, FixedOffset};
use reqwest::{StatusCode, redirect};
use serde::{Deserialize, Serialize};
use url::Url;
use uth_domain::{FacebookPost, TELEGRAM_MESSAGE_LIMIT, TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT};

const TELEGRAM_API_BASE: &str = "https://api.telegram.org";
const TELEGRAM_RESPONSE_LIMIT: u64 = 65_536;
const TELEGRAM_DOCUMENT_LIMIT: usize = 50 * 1024 * 1024;
pub const TELEGRAM_DOCUMENT_TIMEOUT_SECONDS: u64 = 120;
const TELEGRAM_DOCUMENT_TIMEOUT: Duration = Duration::from_secs(TELEGRAM_DOCUMENT_TIMEOUT_SECONDS);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramSendOutcome {
    Sent {
        message_id: i64,
        file_id: Option<String>,
    },
    RetryAfter {
        seconds: u64,
        detail: String,
    },
    ChatMigrated {
        new_chat_id: i64,
        detail: String,
    },
    PermanentFailure {
        deactivate: bool,
        detail: String,
    },
    TransientFailure {
        detail: String,
    },
    AuthenticationFailure {
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramUpdatesOutcome {
    Received { updates: Vec<TelegramUpdate> },
    RetryAfter { seconds: u64, detail: String },
    TransientFailure { detail: String },
    AuthenticationFailure { detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramConfigurationOutcome {
    Applied,
    RetryAfter { seconds: u64, detail: String },
    PermanentFailure { detail: String },
    TransientFailure { detail: String },
    AuthenticationFailure { detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TelegramUpdate {
    pub update_id: i64,
    pub message: Option<TelegramIncomingMessage>,
    pub callback_query: Option<TelegramCallbackQuery>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TelegramCallbackQuery {
    pub id: String,
    pub message: Option<TelegramIncomingMessage>,
    pub data: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TelegramIncomingMessage {
    pub chat: TelegramChat,
    pub text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TelegramChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramUserLink {
    pub offset: usize,
    pub length: usize,
    pub user_chat_id: i64,
}

#[derive(Clone)]
pub struct TelegramClient {
    client: reqwest::Client,
    endpoint: String,
}

impl TelegramClient {
    pub fn new(token: &str, timeout: Duration) -> Result<Self, String> {
        Self::with_base_url(token, timeout, TELEGRAM_API_BASE)
    }

    fn with_base_url(token: &str, timeout: Duration, base_url: &str) -> Result<Self, String> {
        if timeout.is_zero() {
            return Err("Telegram timeout must be at least 1 millisecond".to_owned());
        }
        if token.len() < 20
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-'))
        {
            return Err("Telegram bot token has an invalid format".to_owned());
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(redirect::Policy::none())
            .build()
            .map_err(|_| "failed to create Telegram HTTP client".to_owned())?;
        Ok(Self {
            client,
            endpoint: format!("{}/bot{token}", base_url.trim_end_matches('/')),
        })
    }

    pub async fn send_message(&self, chat_id: i64, text: &str) -> TelegramSendOutcome {
        self.send_message_with_markup(chat_id, text, MessageReplyMarkup::Reply(command_keyboard()))
            .await
    }

    pub async fn send_message_with_user_links(
        &self,
        chat_id: i64,
        text: &str,
        links: &[TelegramUserLink],
    ) -> TelegramSendOutcome {
        let text_length = text.encode_utf16().count();
        if links.is_empty()
            || links.len() > 100
            || links.iter().any(|link| {
                link.user_chat_id == 0
                    || link.length == 0
                    || link
                        .offset
                        .checked_add(link.length)
                        .is_none_or(|end| end > text_length)
            })
        {
            return TelegramSendOutcome::PermanentFailure {
                deactivate: false,
                detail: "invalid Telegram user links".to_owned(),
            };
        }
        let entities = links
            .iter()
            .map(|link| TelegramMessageEntity {
                kind: "text_link",
                offset: link.offset,
                length: link.length,
                url: format!("tg://user?id={}", link.user_chat_id),
            })
            .collect();
        self.send_message_with_entities(
            chat_id,
            text,
            MessageReplyMarkup::Reply(command_keyboard()),
            Some(entities),
        )
        .await
    }

    pub async fn send_admin_feedback(
        &self,
        admin_chat_id: i64,
        text: &str,
        user_chat_id: i64,
    ) -> TelegramSendOutcome {
        if user_chat_id == 0 || text.is_empty() {
            return TelegramSendOutcome::PermanentFailure {
                deactivate: false,
                detail: "invalid Telegram feedback contact".to_owned(),
            };
        }
        let contact_label = "Mở cuộc trò chuyện với người gửi";
        let message = format!("{text}\n\n{contact_label}");
        if message.chars().count() > TELEGRAM_MESSAGE_LIMIT {
            return TelegramSendOutcome::PermanentFailure {
                deactivate: false,
                detail: "Telegram feedback message is too long".to_owned(),
            };
        }
        let entity = TelegramMessageEntity {
            kind: "text_link",
            offset: text.encode_utf16().count() + 2,
            length: contact_label.encode_utf16().count(),
            url: format!("tg://user?id={user_chat_id}"),
        };
        self.send_message_with_entities(
            admin_chat_id,
            &message,
            MessageReplyMarkup::Reply(command_keyboard()),
            Some(vec![entity]),
        )
        .await
    }

    pub async fn send_donation_prompt(&self, chat_id: i64, text: &str) -> TelegramSendOutcome {
        self.send_message_with_markup(
            chat_id,
            text,
            MessageReplyMarkup::Inline(donation_keyboard()),
        )
        .await
    }

    pub async fn send_onboarding_prompt(&self, chat_id: i64, text: &str) -> TelegramSendOutcome {
        self.send_message_with_markup(
            chat_id,
            text,
            MessageReplyMarkup::Inline(onboarding_keyboard()),
        )
        .await
    }

    pub async fn send_settings_prompt(&self, chat_id: i64, text: &str) -> TelegramSendOutcome {
        self.send_message_with_markup(
            chat_id,
            text,
            MessageReplyMarkup::Inline(settings_keyboard()),
        )
        .await
    }

    pub async fn send_notification(
        &self,
        chat_id: i64,
        text: &str,
        campaign_id: i64,
        action_url: Option<&str>,
        post_url: Option<&str>,
    ) -> TelegramSendOutcome {
        self.send_message_with_markup(
            chat_id,
            text,
            MessageReplyMarkup::DynamicInline(notification_keyboard(
                campaign_id,
                action_url,
                post_url,
            )),
        )
        .await
    }

    pub async fn send_portal_notification(
        &self,
        chat_id: i64,
        text: &str,
        source_url: Option<&str>,
    ) -> TelegramSendOutcome {
        let reply_markup = match portal_message_markup(source_url) {
            Ok(value) => value,
            Err(detail) => {
                return TelegramSendOutcome::PermanentFailure {
                    deactivate: false,
                    detail,
                };
            }
        };
        self.send_message_with_markup(chat_id, text, reply_markup)
            .await
    }

    pub async fn send_portal_document_by_id(
        &self,
        chat_id: i64,
        caption: &str,
        file_id: &str,
        source_url: Option<&str>,
    ) -> TelegramSendOutcome {
        if chat_id == 0
            || caption.is_empty()
            || caption.chars().count() > 1_024
            || file_id.is_empty()
            || file_id.chars().count() > 512
        {
            return TelegramSendOutcome::PermanentFailure {
                deactivate: false,
                detail: "invalid Telegram document request".to_owned(),
            };
        }
        let reply_markup = match portal_dynamic_keyboard(source_url) {
            Ok(value) => value,
            Err(detail) => {
                return TelegramSendOutcome::PermanentFailure {
                    deactivate: false,
                    detail,
                };
            }
        };
        let response = match self
            .client
            .post(format!("{}/sendDocument", self.endpoint))
            .timeout(TELEGRAM_DOCUMENT_TIMEOUT)
            .json(&SendDocumentRequest {
                chat_id,
                document: file_id,
                caption,
                allow_paid_broadcast: false,
                reply_markup,
            })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => return document_request_failure(error),
        };
        classify_send_response(response, "document").await
    }

    pub async fn upload_portal_document(
        &self,
        chat_id: i64,
        caption: &str,
        file_name: &str,
        content_type: &str,
        document: Vec<u8>,
        source_url: Option<&str>,
    ) -> TelegramSendOutcome {
        if chat_id == 0
            || caption.is_empty()
            || caption.chars().count() > 1_024
            || file_name.is_empty()
            || file_name.chars().count() > 255
            || document.is_empty()
            || document.len() > TELEGRAM_DOCUMENT_LIMIT
        {
            return TelegramSendOutcome::PermanentFailure {
                deactivate: false,
                detail: "invalid Telegram document upload".to_owned(),
            };
        }
        let reply_markup = match portal_dynamic_keyboard(source_url).and_then(|value| {
            value
                .map(|markup| serde_json::to_string(&markup))
                .transpose()
                .map_err(|_| "failed to encode Telegram document markup".to_owned())
        }) {
            Ok(value) => value,
            Err(detail) => {
                return TelegramSendOutcome::PermanentFailure {
                    deactivate: false,
                    detail,
                };
            }
        };
        let part = match reqwest::multipart::Part::bytes(document)
            .file_name(file_name.to_owned())
            .mime_str(content_type)
        {
            Ok(value) => value,
            Err(_) => {
                return TelegramSendOutcome::PermanentFailure {
                    deactivate: false,
                    detail: "invalid Telegram document content type".to_owned(),
                };
            }
        };
        let mut form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .text("caption", caption.to_owned())
            .text("allow_paid_broadcast", "false")
            .part("document", part);
        if let Some(reply_markup) = reply_markup {
            form = form.text("reply_markup", reply_markup);
        }
        let response = match self
            .client
            .post(format!("{}/sendDocument", self.endpoint))
            .timeout(TELEGRAM_DOCUMENT_TIMEOUT)
            .multipart(form)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => return document_request_failure(error),
        };
        classify_send_response(response, "document").await
    }

    async fn send_message_with_markup(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: MessageReplyMarkup,
    ) -> TelegramSendOutcome {
        self.send_message_with_entities(chat_id, text, reply_markup, None)
            .await
    }

    async fn send_message_with_entities(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: MessageReplyMarkup,
        entities: Option<Vec<TelegramMessageEntity>>,
    ) -> TelegramSendOutcome {
        if chat_id == 0 || text.is_empty() || text.chars().count() > TELEGRAM_MESSAGE_LIMIT {
            return TelegramSendOutcome::PermanentFailure {
                deactivate: false,
                detail: "invalid Telegram chat ID or message length".to_owned(),
            };
        }
        let response = match self
            .client
            .post(format!("{}/sendMessage", self.endpoint))
            .json(&SendMessageRequest {
                chat_id,
                text,
                allow_paid_broadcast: false,
                link_preview_options: LinkPreviewOptions { is_disabled: false },
                reply_markup,
                entities: entities.as_deref(),
            })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let detail = if error.is_timeout() {
                    "Telegram request timed out"
                } else if error.is_connect() {
                    "failed to connect to Telegram"
                } else {
                    "Telegram request failed"
                };
                return TelegramSendOutcome::TransientFailure {
                    detail: detail.to_owned(),
                };
            }
        };
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|size| size > TELEGRAM_RESPONSE_LIMIT)
        {
            return TelegramSendOutcome::TransientFailure {
                detail: "Telegram response exceeded size limit".to_owned(),
            };
        }
        let bytes = match response.bytes().await {
            Ok(bytes)
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= TELEGRAM_RESPONSE_LIMIT =>
            {
                bytes
            }
            Ok(_) => {
                return TelegramSendOutcome::TransientFailure {
                    detail: "Telegram response exceeded size limit".to_owned(),
                };
            }
            Err(_) => {
                return TelegramSendOutcome::TransientFailure {
                    detail: "failed to read Telegram response".to_owned(),
                };
            }
        };
        let parsed = match serde_json::from_slice::<TelegramResponse>(&bytes) {
            Ok(parsed) => parsed,
            Err(_) => {
                return classify_unparseable_response(status);
            }
        };
        classify_response(status, parsed)
    }

    pub async fn answer_callback_query(
        &self,
        callback_query_id: &str,
    ) -> TelegramConfigurationOutcome {
        if callback_query_id.is_empty() || callback_query_id.chars().count() > 256 {
            return TelegramConfigurationOutcome::PermanentFailure {
                detail: "Telegram callback query ID is invalid".to_owned(),
            };
        }
        let response = match self
            .client
            .post(format!("{}/answerCallbackQuery", self.endpoint))
            .json(&AnswerCallbackQueryRequest { callback_query_id })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let detail = if error.is_timeout() {
                    "Telegram callback acknowledgement timed out"
                } else if error.is_connect() {
                    "failed to connect to Telegram"
                } else {
                    "Telegram callback acknowledgement failed"
                };
                return TelegramConfigurationOutcome::TransientFailure {
                    detail: detail.to_owned(),
                };
            }
        };
        let status = response.status();
        let bytes = match response.bytes().await {
            Ok(bytes)
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= TELEGRAM_RESPONSE_LIMIT =>
            {
                bytes
            }
            Ok(_) => {
                return TelegramConfigurationOutcome::TransientFailure {
                    detail: "Telegram callback response exceeded size limit".to_owned(),
                };
            }
            Err(_) => {
                return TelegramConfigurationOutcome::TransientFailure {
                    detail: "failed to read Telegram callback response".to_owned(),
                };
            }
        };
        let parsed = match serde_json::from_slice::<TelegramBasicResponse>(&bytes) {
            Ok(parsed) => parsed,
            Err(_) => return classify_unparseable_configuration_response(status),
        };
        classify_configuration_response(status, parsed)
    }

    pub async fn send_photo(
        &self,
        chat_id: i64,
        caption: &str,
        image: &[u8],
    ) -> TelegramSendOutcome {
        let image_format = if image.starts_with(b"\x89PNG\r\n\x1a\n") {
            Some(("payos-qr.png", "image/png"))
        } else if image.starts_with(b"\xff\xd8\xff") {
            Some(("payos-qr.jpg", "image/jpeg"))
        } else {
            None
        };
        if chat_id == 0
            || caption.is_empty()
            || caption.chars().count() > 1024
            || image.len() < 8
            || image.len() > 10 * 1024 * 1024
            || image_format.is_none()
        {
            return TelegramSendOutcome::PermanentFailure {
                deactivate: false,
                detail: "invalid Telegram photo request".to_owned(),
            };
        }
        let reply_markup = match serde_json::to_string(&command_keyboard()) {
            Ok(value) => value,
            Err(_) => {
                return TelegramSendOutcome::TransientFailure {
                    detail: "failed to encode Telegram reply markup".to_owned(),
                };
            }
        };
        let (file_name, mime_type) = image_format.unwrap_or_default();
        let photo = match reqwest::multipart::Part::bytes(image.to_vec())
            .file_name(file_name)
            .mime_str(mime_type)
        {
            Ok(value) => value,
            Err(_) => {
                return TelegramSendOutcome::TransientFailure {
                    detail: "failed to prepare Telegram photo".to_owned(),
                };
            }
        };
        let form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .text("caption", caption.to_owned())
            .text("allow_paid_broadcast", "false")
            .text("reply_markup", reply_markup)
            .part("photo", photo);
        let response = match self
            .client
            .post(format!("{}/sendPhoto", self.endpoint))
            .multipart(form)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let detail = if error.is_timeout() {
                    "Telegram photo request timed out"
                } else if error.is_connect() {
                    "failed to connect to Telegram"
                } else {
                    "Telegram photo request failed"
                };
                return TelegramSendOutcome::TransientFailure {
                    detail: detail.to_owned(),
                };
            }
        };
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|size| size > TELEGRAM_RESPONSE_LIMIT)
        {
            return TelegramSendOutcome::TransientFailure {
                detail: "Telegram response exceeded size limit".to_owned(),
            };
        }
        let bytes = match response.bytes().await {
            Ok(bytes)
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= TELEGRAM_RESPONSE_LIMIT =>
            {
                bytes
            }
            Ok(_) => {
                return TelegramSendOutcome::TransientFailure {
                    detail: "Telegram response exceeded size limit".to_owned(),
                };
            }
            Err(_) => {
                return TelegramSendOutcome::TransientFailure {
                    detail: "failed to read Telegram response".to_owned(),
                };
            }
        };
        let parsed = match serde_json::from_slice::<TelegramResponse>(&bytes) {
            Ok(parsed) => parsed,
            Err(_) => return classify_unparseable_response(status),
        };
        classify_response(status, parsed)
    }

    pub async fn get_updates(&self, offset: i64, limit: u8) -> TelegramUpdatesOutcome {
        if offset < 0 || !(1..=100).contains(&limit) {
            return TelegramUpdatesOutcome::TransientFailure {
                detail: "invalid Telegram update offset or limit".to_owned(),
            };
        }
        let response = match self
            .client
            .post(format!("{}/getUpdates", self.endpoint))
            .json(&GetUpdatesRequest {
                offset,
                limit,
                timeout: 0,
                allowed_updates: ["message", "callback_query"],
            })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let detail = if error.is_timeout() {
                    "Telegram update request timed out"
                } else if error.is_connect() {
                    "failed to connect to Telegram"
                } else {
                    "Telegram update request failed"
                };
                return TelegramUpdatesOutcome::TransientFailure {
                    detail: detail.to_owned(),
                };
            }
        };
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|size| size > TELEGRAM_RESPONSE_LIMIT)
        {
            return TelegramUpdatesOutcome::TransientFailure {
                detail: "Telegram update response exceeded size limit".to_owned(),
            };
        }
        let bytes = match response.bytes().await {
            Ok(bytes)
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= TELEGRAM_RESPONSE_LIMIT =>
            {
                bytes
            }
            Ok(_) => {
                return TelegramUpdatesOutcome::TransientFailure {
                    detail: "Telegram update response exceeded size limit".to_owned(),
                };
            }
            Err(_) => {
                return TelegramUpdatesOutcome::TransientFailure {
                    detail: "failed to read Telegram update response".to_owned(),
                };
            }
        };
        let parsed = match serde_json::from_slice::<TelegramUpdatesResponse>(&bytes) {
            Ok(parsed) => parsed,
            Err(_) => {
                return classify_unparseable_updates_response(status);
            }
        };
        classify_updates_response(status, parsed)
    }

    pub async fn configure_commands(&self) -> TelegramConfigurationOutcome {
        self.configure_command_scope(&default_commands(), None)
            .await
    }

    pub async fn configure_admin_commands(&self, chat_id: i64) -> TelegramConfigurationOutcome {
        if chat_id == 0 {
            return TelegramConfigurationOutcome::PermanentFailure {
                detail: "Telegram admin chat ID is invalid".to_owned(),
            };
        }
        self.configure_command_scope(
            &admin_commands(),
            Some(BotCommandScopeChat {
                kind: "chat",
                chat_id,
            }),
        )
        .await
    }

    async fn configure_command_scope(
        &self,
        commands: &[BotCommand],
        scope: Option<BotCommandScopeChat>,
    ) -> TelegramConfigurationOutcome {
        let response = match self
            .client
            .post(format!("{}/setMyCommands", self.endpoint))
            .json(&SetMyCommandsRequest { commands, scope })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let detail = if error.is_timeout() {
                    "Telegram command configuration timed out"
                } else if error.is_connect() {
                    "failed to connect to Telegram"
                } else {
                    "Telegram command configuration failed"
                };
                return TelegramConfigurationOutcome::TransientFailure {
                    detail: detail.to_owned(),
                };
            }
        };
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|size| size > TELEGRAM_RESPONSE_LIMIT)
        {
            return TelegramConfigurationOutcome::TransientFailure {
                detail: "Telegram command response exceeded size limit".to_owned(),
            };
        }
        let bytes = match response.bytes().await {
            Ok(bytes)
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= TELEGRAM_RESPONSE_LIMIT =>
            {
                bytes
            }
            Ok(_) => {
                return TelegramConfigurationOutcome::TransientFailure {
                    detail: "Telegram command response exceeded size limit".to_owned(),
                };
            }
            Err(_) => {
                return TelegramConfigurationOutcome::TransientFailure {
                    detail: "failed to read Telegram command response".to_owned(),
                };
            }
        };
        let parsed = match serde_json::from_slice::<TelegramBasicResponse>(&bytes) {
            Ok(parsed) => parsed,
            Err(_) => return classify_unparseable_configuration_response(status),
        };
        classify_configuration_response(status, parsed)
    }
}

pub fn render_notification(post: &FacebookPost) -> String {
    let header = "Thông báo hoạt động sinh viên UTH\n\n";
    let post_url = delivery_post_url(post);
    let footer = format!(
        "\n\nĐăng lúc: {}\nBài gốc: {}",
        format_vietnam_datetime(&post.published_at),
        post_url
    );
    let reserved = header
        .chars()
        .count()
        .saturating_add(footer.chars().count());
    let body_limit = TELEGRAM_MESSAGE_LIMIT.saturating_sub(reserved);
    let body = truncate_chars(post.text.trim(), body_limit);
    let message = format!("{header}{body}{footer}");
    debug_assert!(message.len() <= TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT);
    message
}

pub fn delivery_post_url(post: &FacebookPost) -> String {
    if post.platform != "facebook" || !is_facebook_post_locator(&post.external_post_id) {
        return post.canonical_url.clone();
    }
    let source_owner_id = post
        .source_id
        .strip_prefix("facebook:")
        .filter(|value| !value.is_empty())
        .filter(|value| value.chars().all(|character| character.is_ascii_digit()));
    let canonical_owner_id = Url::parse(&post.canonical_url)
        .ok()
        .filter(|url| {
            url.scheme() == "https"
                && url.host_str().is_some_and(|host| {
                    let host = host.to_ascii_lowercase();
                    host == "facebook.com" || host.ends_with(".facebook.com")
                })
                && url.path() == "/permalink.php"
        })
        .and_then(|url| {
            url.query_pairs()
                .find_map(|(key, value)| (key == "id").then(|| value.into_owned()))
        })
        .filter(|value| !value.is_empty())
        .filter(|value| value.chars().all(|character| character.is_ascii_digit()));
    let Some(owner_id) = source_owner_id.map(str::to_owned).or(canonical_owner_id) else {
        return post.canonical_url.clone();
    };
    format!(
        "https://www.facebook.com/{owner_id}/posts/{}",
        post.external_post_id
    )
}

fn is_facebook_post_locator(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && (value.chars().all(|character| character.is_ascii_digit())
            || (value.starts_with("pfbid")
                && value
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric())))
}

pub fn humanize_notification_sample(message: &str) -> String {
    message
        .lines()
        .map(|line| {
            ["Thời gian đăng bài: ", "Thời gian đăng: "]
                .into_iter()
                .find_map(|prefix| line.strip_prefix(prefix))
                .map(|value| format!("Đăng lúc: {}", format_vietnam_datetime(value)))
                .or_else(|| {
                    line.strip_prefix("Xem bài viết: ")
                        .map(|value| format!("Bài gốc: {value}"))
                })
                .unwrap_or_else(|| line.to_owned())
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_vietnam_datetime(value: &str) -> String {
    let Some(offset) = FixedOffset::east_opt(7 * 60 * 60) else {
        return value.to_owned();
    };
    DateTime::parse_from_rfc3339(value)
        .map(|date| {
            date.with_timezone(&offset)
                .format("%H:%M ngày %d/%m/%Y")
                .to_string()
        })
        .unwrap_or_else(|_| value.to_owned())
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    if limit <= 1 {
        return "…".chars().take(limit).collect();
    }
    let mut output = value.chars().take(limit - 1).collect::<String>();
    output.push('…');
    output
}

fn document_request_failure(error: reqwest::Error) -> TelegramSendOutcome {
    let detail = if error.is_timeout() {
        "Telegram document request timed out"
    } else if error.is_connect() {
        "failed to connect to Telegram for document delivery"
    } else {
        "Telegram document request failed"
    };
    TelegramSendOutcome::TransientFailure {
        detail: detail.to_owned(),
    }
}

async fn classify_send_response(
    response: reqwest::Response,
    response_kind: &str,
) -> TelegramSendOutcome {
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|size| size > TELEGRAM_RESPONSE_LIMIT)
    {
        return TelegramSendOutcome::TransientFailure {
            detail: format!("Telegram {response_kind} response exceeded size limit"),
        };
    }
    let bytes = match response.bytes().await {
        Ok(bytes) if u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= TELEGRAM_RESPONSE_LIMIT => {
            bytes
        }
        Ok(_) => {
            return TelegramSendOutcome::TransientFailure {
                detail: format!("Telegram {response_kind} response exceeded size limit"),
            };
        }
        Err(_) => {
            return TelegramSendOutcome::TransientFailure {
                detail: format!("failed to read Telegram {response_kind} response"),
            };
        }
    };
    match serde_json::from_slice::<TelegramResponse>(&bytes) {
        Ok(parsed) => classify_response(status, parsed),
        Err(_) => classify_unparseable_response(status),
    }
}

fn classify_response(status: StatusCode, response: TelegramResponse) -> TelegramSendOutcome {
    if response.ok {
        return match response.result {
            Some(message) => TelegramSendOutcome::Sent {
                message_id: message.message_id,
                file_id: message.document.map(|document| document.file_id),
            },
            None => TelegramSendOutcome::TransientFailure {
                detail: "Telegram success response omitted message ID".to_owned(),
            },
        };
    }
    let code = response.error_code.unwrap_or(i32::from(status.as_u16()));
    let detail = response
        .description
        .as_deref()
        .map(sanitize_detail)
        .unwrap_or_else(|| format!("Telegram returned error {code}"));
    if let Some(new_chat_id) = response
        .parameters
        .as_ref()
        .and_then(|parameters| parameters.migrate_to_chat_id)
    {
        return TelegramSendOutcome::ChatMigrated {
            new_chat_id,
            detail,
        };
    }
    if code == 429 || status == StatusCode::TOO_MANY_REQUESTS {
        let seconds = response
            .parameters
            .and_then(|parameters| parameters.retry_after)
            .unwrap_or(1)
            .clamp(1, 86_400);
        return TelegramSendOutcome::RetryAfter { seconds, detail };
    }
    if code == 401 || code == 404 {
        return TelegramSendOutcome::AuthenticationFailure { detail };
    }
    if code == 403 {
        return TelegramSendOutcome::PermanentFailure {
            deactivate: true,
            detail,
        };
    }
    if status.is_server_error() || code >= 500 {
        return TelegramSendOutcome::TransientFailure { detail };
    }
    if status.is_client_error() || (400..500).contains(&code) {
        return TelegramSendOutcome::PermanentFailure {
            deactivate: detail.to_lowercase().contains("chat not found"),
            detail,
        };
    }
    TelegramSendOutcome::TransientFailure { detail }
}

fn classify_unparseable_response(status: StatusCode) -> TelegramSendOutcome {
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::NOT_FOUND) {
        TelegramSendOutcome::AuthenticationFailure {
            detail: "Telegram authentication failed".to_owned(),
        }
    } else {
        TelegramSendOutcome::TransientFailure {
            detail: "Telegram returned an invalid response".to_owned(),
        }
    }
}

fn classify_updates_response(
    status: StatusCode,
    response: TelegramUpdatesResponse,
) -> TelegramUpdatesOutcome {
    if response.ok {
        return TelegramUpdatesOutcome::Received {
            updates: response.result.unwrap_or_default(),
        };
    }
    let code = response.error_code.unwrap_or(i32::from(status.as_u16()));
    let detail = response
        .description
        .as_deref()
        .map(sanitize_detail)
        .unwrap_or_else(|| format!("Telegram returned error {code}"));
    if code == 429 || status == StatusCode::TOO_MANY_REQUESTS {
        let seconds = response
            .parameters
            .and_then(|parameters| parameters.retry_after)
            .unwrap_or(1)
            .clamp(1, 86_400);
        return TelegramUpdatesOutcome::RetryAfter { seconds, detail };
    }
    if code == 401 || code == 404 {
        return TelegramUpdatesOutcome::AuthenticationFailure { detail };
    }
    TelegramUpdatesOutcome::TransientFailure { detail }
}

fn classify_unparseable_updates_response(status: StatusCode) -> TelegramUpdatesOutcome {
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::NOT_FOUND) {
        TelegramUpdatesOutcome::AuthenticationFailure {
            detail: "Telegram authentication failed".to_owned(),
        }
    } else {
        TelegramUpdatesOutcome::TransientFailure {
            detail: "Telegram returned an invalid update response".to_owned(),
        }
    }
}

fn classify_configuration_response(
    status: StatusCode,
    response: TelegramBasicResponse,
) -> TelegramConfigurationOutcome {
    if response.ok && response.result == Some(true) {
        return TelegramConfigurationOutcome::Applied;
    }
    let code = response.error_code.unwrap_or(i32::from(status.as_u16()));
    let detail = response
        .description
        .as_deref()
        .map(sanitize_detail)
        .unwrap_or_else(|| format!("Telegram returned error {code}"));
    if code == 429 || status == StatusCode::TOO_MANY_REQUESTS {
        let seconds = response
            .parameters
            .and_then(|parameters| parameters.retry_after)
            .unwrap_or(1)
            .clamp(1, 86_400);
        return TelegramConfigurationOutcome::RetryAfter { seconds, detail };
    }
    if code == 401 || code == 404 {
        return TelegramConfigurationOutcome::AuthenticationFailure { detail };
    }
    if status.is_client_error() || (400..500).contains(&code) {
        return TelegramConfigurationOutcome::PermanentFailure { detail };
    }
    TelegramConfigurationOutcome::TransientFailure { detail }
}

fn classify_unparseable_configuration_response(status: StatusCode) -> TelegramConfigurationOutcome {
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::NOT_FOUND) {
        TelegramConfigurationOutcome::AuthenticationFailure {
            detail: "Telegram authentication failed".to_owned(),
        }
    } else {
        TelegramConfigurationOutcome::TransientFailure {
            detail: "Telegram returned an invalid command response".to_owned(),
        }
    }
}

fn sanitize_detail(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control())
        .take(1_000)
        .collect()
}

#[derive(Serialize)]
struct SendMessageRequest<'a> {
    chat_id: i64,
    text: &'a str,
    allow_paid_broadcast: bool,
    link_preview_options: LinkPreviewOptions,
    reply_markup: MessageReplyMarkup,
    #[serde(skip_serializing_if = "Option::is_none")]
    entities: Option<&'a [TelegramMessageEntity]>,
}

#[derive(Serialize)]
struct TelegramMessageEntity {
    #[serde(rename = "type")]
    kind: &'static str,
    offset: usize,
    length: usize,
    url: String,
}

#[derive(Serialize)]
struct SendDocumentRequest<'a> {
    chat_id: i64,
    document: &'a str,
    caption: &'a str,
    allow_paid_broadcast: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<DynamicInlineKeyboardMarkup>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum MessageReplyMarkup {
    Reply(ReplyKeyboardMarkup),
    Inline(InlineKeyboardMarkup),
    DynamicInline(DynamicInlineKeyboardMarkup),
}

#[derive(Serialize)]
struct LinkPreviewOptions {
    is_disabled: bool,
}

#[derive(Serialize)]
struct ReplyKeyboardMarkup {
    keyboard: Vec<Vec<KeyboardButton>>,
    resize_keyboard: bool,
    is_persistent: bool,
}

#[derive(Serialize)]
struct KeyboardButton {
    text: &'static str,
}

fn command_keyboard() -> ReplyKeyboardMarkup {
    ReplyKeyboardMarkup {
        keyboard: vec![
            vec![
                KeyboardButton {
                    text: "Hoạt động"
                },
                KeyboardButton {
                    text: "Cổng đào tạo",
                },
            ],
            vec![
                KeyboardButton {
                    text: "Cài đặt"
                },
                KeyboardButton {
                    text: "Trang theo dõi",
                },
            ],
            vec![
                KeyboardButton {
                    text: "Trợ giúp"
                },
                KeyboardButton { text: "Ủng hộ" },
            ],
        ],
        resize_keyboard: true,
        is_persistent: true,
    }
}

#[derive(Serialize)]
struct InlineKeyboardMarkup {
    inline_keyboard: Vec<Vec<InlineKeyboardButton>>,
}

#[derive(Serialize)]
struct InlineKeyboardButton {
    text: &'static str,
    callback_data: &'static str,
}

fn donation_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![
            vec![
                InlineKeyboardButton {
                    text: "10.000 VND",
                    callback_data: "/donate_10000",
                },
                InlineKeyboardButton {
                    text: "20.000 VND",
                    callback_data: "/donate_20000",
                },
            ],
            vec![
                InlineKeyboardButton {
                    text: "50.000 VND",
                    callback_data: "/donate_50000",
                },
                InlineKeyboardButton {
                    text: "Tùy tâm",
                    callback_data: "/donate_custom",
                },
            ],
        ],
    }
}

fn onboarding_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![
            vec![InlineKeyboardButton {
                text: "Chỉ hoạt động có điểm rèn luyện",
                callback_data: "/onboard_drl",
            }],
            vec![InlineKeyboardButton {
                text: "Mọi hoạt động phù hợp",
                callback_data: "/onboard_all",
            }],
            vec![InlineKeyboardButton {
                text: "Xem một tin mẫu",
                callback_data: "/onboard_sample",
            }],
        ],
    }
}

fn settings_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![
            vec![
                InlineKeyboardButton {
                    text: "Chỉ tin có điểm rèn luyện",
                    callback_data: "/settings_scope_drl",
                },
                InlineKeyboardButton {
                    text: "Mọi hoạt động",
                    callback_data: "/settings_scope_all",
                },
            ],
            vec![
                InlineKeyboardButton {
                    text: "Nhận ngay",
                    callback_data: "/settings_mode_instant",
                },
                InlineKeyboardButton {
                    text: "Bản tin lúc 07:30",
                    callback_data: "/settings_mode_daily",
                },
            ],
            vec![
                InlineKeyboardButton {
                    text: "Bật giờ yên lặng",
                    callback_data: "/settings_quiet_on",
                },
                InlineKeyboardButton {
                    text: "Tắt giờ yên lặng",
                    callback_data: "/settings_quiet_off",
                },
            ],
            vec![
                InlineKeyboardButton {
                    text: "Bật lại thông báo",
                    callback_data: "/start",
                },
                InlineKeyboardButton {
                    text: "Tạm dừng tin hoạt động",
                    callback_data: "/stop",
                },
            ],
            vec![InlineKeyboardButton {
                text: "Xem một tin mẫu",
                callback_data: "/settings_sample",
            }],
        ],
    }
}

#[derive(Serialize)]
struct DynamicInlineKeyboardMarkup {
    inline_keyboard: Vec<Vec<DynamicInlineKeyboardButton>>,
}

#[derive(Serialize)]
struct DynamicInlineKeyboardButton {
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    callback_data: Option<String>,
}

fn notification_keyboard(
    campaign_id: i64,
    action_url: Option<&str>,
    post_url: Option<&str>,
) -> DynamicInlineKeyboardMarkup {
    let mut actions = Vec::new();
    if action_url.is_some() {
        actions.push(DynamicInlineKeyboardButton {
            text: "Đăng ký".to_owned(),
            url: None,
            callback_data: Some(format!("/open_{campaign_id}")),
        });
    }
    if let Some(url) = post_url {
        actions.push(DynamicInlineKeyboardButton {
            text: "Xem bài gốc".to_owned(),
            url: Some(url.to_owned()),
            callback_data: None,
        });
    }
    let mut rows = Vec::new();
    if !actions.is_empty() {
        rows.push(actions);
    }
    rows.push(vec![
        DynamicInlineKeyboardButton {
            text: "Hữu ích".to_owned(),
            url: None,
            callback_data: Some(format!("/useful_{campaign_id}")),
        },
        DynamicInlineKeyboardButton {
            text: "Không phù hợp".to_owned(),
            url: None,
            callback_data: Some(format!("/irrelevant_{campaign_id}")),
        },
    ]);
    DynamicInlineKeyboardMarkup {
        inline_keyboard: rows,
    }
}

fn portal_message_markup(source_url: Option<&str>) -> Result<MessageReplyMarkup, String> {
    Ok(match portal_dynamic_keyboard(source_url)? {
        Some(markup) => MessageReplyMarkup::DynamicInline(markup),
        None => MessageReplyMarkup::Reply(command_keyboard()),
    })
}

fn portal_dynamic_keyboard(
    source_url: Option<&str>,
) -> Result<Option<DynamicInlineKeyboardMarkup>, String> {
    let Some(source_url) = source_url else {
        return Ok(None);
    };
    let parsed = reqwest::Url::parse(source_url)
        .map_err(|_| "Portal notification link is invalid".to_owned())?;
    if source_url.chars().count() > 2_048
        || parsed.scheme() != "https"
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err("Portal notification link is invalid".to_owned());
    }
    Ok(Some(DynamicInlineKeyboardMarkup {
        inline_keyboard: vec![vec![DynamicInlineKeyboardButton {
            text: "Xem thông báo gốc".to_owned(),
            url: Some(parsed.to_string()),
            callback_data: None,
        }]],
    }))
}

#[derive(Serialize)]
struct AnswerCallbackQueryRequest<'a> {
    callback_query_id: &'a str,
}

#[derive(Serialize)]
struct SetMyCommandsRequest<'a> {
    commands: &'a [BotCommand],
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<BotCommandScopeChat>,
}

#[derive(Clone, Copy, Serialize)]
struct BotCommand {
    command: &'static str,
    description: &'static str,
}

#[derive(Serialize)]
struct BotCommandScopeChat {
    #[serde(rename = "type")]
    kind: &'static str,
    chat_id: i64,
}

fn default_commands() -> [BotCommand; 6] {
    [
        BotCommand {
            command: "start",
            description: "Bắt đầu hoặc bật lại thông báo",
        },
        BotCommand {
            command: "events",
            description: "Hoạt động và học bổng đang mở",
        },
        BotCommand {
            command: "portal",
            description: "Thông báo mới từ Cổng Đào tạo",
        },
        BotCommand {
            command: "settings",
            description: "Cài đặt loại tin và thời gian",
        },
        BotCommand {
            command: "donate",
            description: "Ủng hộ chi phí duy trì bot",
        },
        BotCommand {
            command: "help",
            description: "Xem hướng dẫn và hỗ trợ",
        },
    ]
}

fn admin_commands() -> [BotCommand; 8] {
    let public = default_commands();
    [
        public[0],
        public[1],
        public[2],
        public[3],
        public[4],
        public[5],
        BotCommand {
            command: "admin",
            description: "Bảng điều khiển quản trị",
        },
        BotCommand {
            command: "report",
            description: "Xuất tệp báo cáo vận hành hệ thống",
        },
    ]
}

#[derive(Serialize)]
struct GetUpdatesRequest {
    offset: i64,
    limit: u8,
    timeout: u8,
    allowed_updates: [&'static str; 2],
}

#[derive(Deserialize)]
struct TelegramResponse {
    ok: bool,
    result: Option<SentMessage>,
    error_code: Option<i32>,
    description: Option<String>,
    parameters: Option<ResponseParameters>,
}

#[derive(Deserialize)]
struct TelegramUpdatesResponse {
    ok: bool,
    result: Option<Vec<TelegramUpdate>>,
    error_code: Option<i32>,
    description: Option<String>,
    parameters: Option<ResponseParameters>,
}

#[derive(Deserialize)]
struct TelegramBasicResponse {
    ok: bool,
    result: Option<bool>,
    error_code: Option<i32>,
    description: Option<String>,
    parameters: Option<ResponseParameters>,
}

#[derive(Deserialize)]
struct SentMessage {
    message_id: i64,
    document: Option<SentDocument>,
}

#[derive(Deserialize)]
struct SentDocument {
    file_id: String,
}

#[derive(Deserialize)]
struct ResponseParameters {
    migrate_to_chat_id: Option<i64>,
    retry_after: Option<u64>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use uth_domain::{FacebookPost, POST_SCHEMA_VERSION, TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT};

    use super::{
        TelegramClient, TelegramConfigurationOutcome, TelegramSendOutcome, TelegramUpdatesOutcome,
        TelegramUserLink, delivery_post_url, humanize_notification_sample, render_notification,
    };

    const TOKEN: &str = "123456789:test_token_for_local_mock";

    #[tokio::test]
    async fn sends_zero_cost_json_and_reads_message_id() {
        let (base_url, server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":42}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.send_message(123, "Xin chào").await;
        let request = server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 42,
                file_id: None
            }
        );
        assert!(request.contains("\"allow_paid_broadcast\":false"));
        assert!(request.contains("\"chat_id\":123"));
        assert!(request.contains("Hoạt động"));
        assert!(request.contains("Cổng đào tạo"));
        assert!(request.contains("Cài đặt"));
        assert!(request.contains("Trang theo dõi"));
        assert!(request.contains("Trợ giúp"));
        assert!(request.contains("Ủng hộ"));
    }

    #[tokio::test]
    async fn sends_donation_prompt_with_amount_buttons() {
        let (base_url, server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":44}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.send_donation_prompt(123, "Chọn mức ủng hộ").await;
        let request = server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 44,
                file_id: None
            }
        );
        assert!(request.contains("10.000 VND"));
        assert!(request.contains("20.000 VND"));
        assert!(request.contains("50.000 VND"));
        assert!(request.contains("Tùy tâm"));
        assert!(request.contains("\"inline_keyboard\""));
        assert!(request.contains("\"callback_data\":\"/donate_10000\""));
        assert!(request.contains("\"callback_data\":\"/donate_custom\""));
        assert!(!request.contains("\"keyboard\""));
    }

    #[tokio::test]
    async fn sends_onboarding_and_settings_choices() {
        let (base_url, onboarding_server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":45}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.send_onboarding_prompt(123, "Chọn loại tin").await;
        let request = onboarding_server.await.unwrap();
        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 45,
                file_id: None
            }
        );
        assert!(request.contains("Chỉ hoạt động có điểm rèn luyện"));
        assert!(request.contains("Mọi hoạt động phù hợp"));
        assert!(request.contains("Xem một tin mẫu"));

        let (base_url, settings_server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":46}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.send_settings_prompt(123, "Cài đặt").await;
        let request = settings_server.await.unwrap();
        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 46,
                file_id: None
            }
        );
        assert!(request.contains("Bản tin lúc 07:30"));
        assert!(request.contains("Nhận ngay"));
        assert!(request.contains("Bật giờ yên lặng"));
        assert!(request.contains("Tạm dừng tin hoạt động"));
        assert!(request.contains("Xem một tin mẫu"));
        assert!(request.contains("/settings_sample"));
    }

    #[tokio::test]
    async fn sends_actionable_notification_with_feedback() {
        let (base_url, server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":47}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client
            .send_notification(
                123,
                "Hoạt động mới",
                17,
                Some("https://forms.gle/test"),
                Some("https://www.facebook.com/test/posts/1"),
            )
            .await;
        let request = server.await.unwrap();
        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 47,
                file_id: None
            }
        );
        assert!(request.contains("Đăng ký"));
        assert!(request.contains("/open_17"));
        assert!(request.contains("Xem bài gốc"));
        assert!(request.contains("/useful_17"));
        assert!(request.contains("/irrelevant_17"));
    }

    #[tokio::test]
    async fn uploads_portal_document_and_captures_reusable_file_id() {
        let (base_url, server) = mock_server(
            200,
            r#"{"ok":true,"result":{"message_id":48,"document":{"file_id":"portal-file-id"}}}"#,
        )
        .await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client
            .upload_portal_document(
                123,
                "Thông báo Portal",
                "thong-bao.pdf",
                "application/pdf",
                b"%PDF-1.7 mock".to_vec(),
                Some("https://daotao.ut.edu.vn/thong-bao/1"),
            )
            .await;
        let request = server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 48,
                file_id: Some("portal-file-id".to_owned())
            }
        );
        assert!(request.starts_with("POST /bot123456789:test_token_for_local_mock/sendDocument"));
        assert!(request.contains("name=\"document\"; filename=\"thong-bao.pdf\""));
        assert!(request.contains("Content-Type: application/pdf"));
        assert!(request.contains("%PDF-1.7 mock"));
        assert!(request.contains("allow_paid_broadcast"));
        assert!(request.contains("false"));
        assert!(request.contains("Xem thông báo gốc"));
    }

    #[tokio::test]
    async fn reuses_portal_document_file_id_without_uploading_bytes() {
        let (base_url, server) = mock_server(
            200,
            r#"{"ok":true,"result":{"message_id":49,"document":{"file_id":"portal-file-id"}}}"#,
        )
        .await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client
            .send_portal_document_by_id(
                123,
                "Thông báo Portal",
                "portal-file-id",
                Some("https://daotao.ut.edu.vn/thong-bao/1"),
            )
            .await;
        let request = server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 49,
                file_id: Some("portal-file-id".to_owned())
            }
        );
        assert!(request.contains(r#""document":"portal-file-id""#));
        assert!(request.contains(r#""allow_paid_broadcast":false"#));
        assert!(!request.contains("multipart/form-data"));
    }

    #[tokio::test]
    async fn acknowledges_callback_query() {
        let (base_url, server) = mock_server(200, r#"{"ok":true,"result":true}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.answer_callback_query("callback-123").await;
        let request = server.await.unwrap();

        assert_eq!(outcome, TelegramConfigurationOutcome::Applied);
        assert!(request.contains("/answerCallbackQuery"));
        assert!(request.contains("\"callback_query_id\":\"callback-123\""));
    }

    #[tokio::test]
    async fn sends_inline_png_with_caption() {
        let (base_url, server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":43}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client
            .send_photo(123, "Mã QR ủng hộ", b"\x89PNG\r\n\x1a\nmock")
            .await;
        let request = server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 43,
                file_id: None
            }
        );
        assert!(request.starts_with("POST /bot123456789:test_token_for_local_mock/sendPhoto"));
        assert!(request.contains("name=\"photo\"; filename=\"payos-qr.png\""));
        assert!(request.contains("Content-Type: image/png"));
        assert!(request.contains("Mã QR ủng hộ"));
    }

    #[tokio::test]
    async fn sends_branded_jpeg_with_matching_mime_type() {
        let (base_url, server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":43}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client
            .send_photo(123, "Mã QR ủng hộ", b"\xff\xd8\xffmock-image")
            .await;
        let request = server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 43,
                file_id: None
            }
        );
        assert!(request.contains("filename=\"payos-qr.jpg\""));
        assert!(request.contains("Content-Type: image/jpeg"));
    }

    #[tokio::test]
    async fn sends_admin_feedback_with_clickable_contact_link() {
        let (base_url, server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":44}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client
            .send_admin_feedback(123, "Phản hồi người dùng #1", 456)
            .await;
        let request = server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 44,
                file_id: None
            }
        );
        assert!(request.contains(r#""type":"text_link""#));
        assert!(request.contains("tg://user?id=456"));
        assert!(request.contains("Mở cuộc trò chuyện với người gửi"));
    }

    #[tokio::test]
    async fn sends_feedback_history_with_multiple_user_links() {
        let (base_url, server) =
            mock_server(200, r#"{"ok":true,"result":{"message_id":45}}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let text = "Mở người gửi #1\nMở người gửi #2";
        let first_length = "Mở người gửi #1".encode_utf16().count();
        let outcome = client
            .send_message_with_user_links(
                123,
                text,
                &[
                    TelegramUserLink {
                        offset: 0,
                        length: first_length,
                        user_chat_id: 456,
                    },
                    TelegramUserLink {
                        offset: first_length + 1,
                        length: "Mở người gửi #2".encode_utf16().count(),
                        user_chat_id: 789,
                    },
                ],
            )
            .await;
        let request = server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::Sent {
                message_id: 45,
                file_id: None
            }
        );
        assert_eq!(request.matches(r#""type":"text_link""#).count(), 2);
        assert!(request.contains("tg://user?id=456"));
        assert!(request.contains("tg://user?id=789"));
    }

    #[tokio::test]
    async fn replaces_telegram_command_menu_with_current_features() {
        let (base_url, server) = mock_server(200, r#"{"ok":true,"result":true}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.configure_commands().await;
        let request = server.await.unwrap();

        assert_eq!(outcome, TelegramConfigurationOutcome::Applied);
        assert!(request.contains("/setMyCommands"));
        assert!(request.contains("Hoạt động và học bổng đang mở"));
        assert!(request.contains("Cài đặt loại tin và thời gian"));
        assert!(!request.contains("\"scope\""));
        assert!(request.contains(r#""command":"events""#));
        assert!(request.contains(r#""command":"portal""#));
        assert!(request.contains(r#""command":"donate""#));
        assert!(!request.contains("\"suggest\""));
    }

    #[tokio::test]
    async fn configures_chat_scoped_admin_command_menu() {
        let (base_url, server) = mock_server(200, r#"{"ok":true,"result":true}"#).await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.configure_admin_commands(123).await;
        let request = server.await.unwrap();

        assert_eq!(outcome, TelegramConfigurationOutcome::Applied);
        assert!(request.contains(r#""scope":{"type":"chat","chat_id":123}"#));
        assert!(request.contains(r#""command":"admin""#));
        assert!(request.contains(r#""command":"report""#));
        assert!(request.contains(r#""command":"events""#));
        assert!(request.contains(r#""command":"portal""#));
        assert!(!request.contains(r#""command":"review_send""#));
    }

    #[tokio::test]
    async fn receives_private_text_updates() {
        let (base_url, server) = mock_server(
            200,
            r#"{"ok":true,"result":[{"update_id":17,"message":{"chat":{"id":123,"type":"private","first_name":"Test User"},"text":"/start"}}]}"#,
        )
        .await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.get_updates(10, 100).await;
        let request = server.await.unwrap();

        let TelegramUpdatesOutcome::Received { updates } = outcome else {
            panic!("expected Telegram updates");
        };
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].update_id, 17);
        assert_eq!(
            updates[0].message.as_ref().unwrap().text.as_deref(),
            Some("/start")
        );
        assert!(request.starts_with("POST /bot123456789:test_token_for_local_mock/getUpdates"));
    }

    #[tokio::test]
    async fn receives_callback_query_updates() {
        let (base_url, server) = mock_server(
            200,
            r#"{"ok":true,"result":[{"update_id":18,"callback_query":{"id":"callback-123","message":{"chat":{"id":123,"type":"private","first_name":"Test User"}},"data":"/donate_20000"}}]}"#,
        )
        .await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.get_updates(11, 100).await;
        server.await.unwrap();

        let TelegramUpdatesOutcome::Received { updates } = outcome else {
            panic!("expected Telegram updates");
        };
        let callback = updates[0].callback_query.as_ref().unwrap();
        assert_eq!(callback.id, "callback-123");
        assert_eq!(callback.data.as_deref(), Some("/donate_20000"));
        assert_eq!(callback.message.as_ref().unwrap().chat.id, 123);
    }

    #[tokio::test]
    async fn respects_retry_after() {
        let (base_url, server) = mock_server(
            429,
            r#"{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":7}}"#,
        )
        .await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.send_message(123, "Xin chào").await;
        server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::RetryAfter {
                seconds: 7,
                detail: "Too Many Requests".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn deactivates_recipient_when_bot_is_blocked() {
        let (base_url, server) = mock_server(
            403,
            r#"{"ok":false,"error_code":403,"description":"Forbidden: bot was blocked by the user"}"#,
        )
        .await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.send_message(123, "Xin chào").await;
        server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::PermanentFailure {
                deactivate: true,
                detail: "Forbidden: bot was blocked by the user".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn reports_invalid_token_without_exposing_it() {
        let (base_url, server) = mock_server(
            401,
            r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#,
        )
        .await;
        let client =
            TelegramClient::with_base_url(TOKEN, Duration::from_secs(2), &base_url).unwrap();
        let outcome = client.send_message(123, "Xin chào").await;
        server.await.unwrap();

        assert_eq!(
            outcome,
            TelegramSendOutcome::AuthenticationFailure {
                detail: "Unauthorized".to_owned()
            }
        );
    }

    #[test]
    fn notification_never_exceeds_telegram_limit() {
        let mut post = post();
        post.text = "ă".repeat(5_000);
        let message = render_notification(&post);

        assert!(message.chars().count() <= 4_096);
        assert!(message.len() <= TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT);
        assert!(message.contains(&post.canonical_url));
        assert!(message.contains("09:00 ngày 19/07/2026"));
        assert!(!message.contains("2026-07-19T02:00:17+00:00"));
    }

    #[test]
    fn uses_stable_numeric_facebook_route_for_delivery() {
        let mut post = post();
        post.source_id = "facebook:61590435791495".to_owned();
        post.external_post_id = "122125722063347859".to_owned();
        post.canonical_url =
            "https://www.facebook.com/permalink.php?story_fbid=pfbid-example&id=61590435791495"
                .to_owned();

        let url = delivery_post_url(&post);
        let message = render_notification(&post);

        assert_eq!(
            url,
            "https://www.facebook.com/61590435791495/posts/122125722063347859"
        );
        assert!(message.contains(&url));
        assert!(!message.contains("permalink.php"));
    }

    #[test]
    fn derives_numeric_owner_from_verified_facebook_permalink() {
        let mut post = post();
        post.external_post_id = "122125722063347859".to_owned();
        post.canonical_url =
            "https://www.facebook.com/permalink.php?story_fbid=pfbid-example&id=61590435791495"
                .to_owned();

        assert_eq!(
            delivery_post_url(&post),
            "https://www.facebook.com/61590435791495/posts/122125722063347859"
        );
    }

    #[test]
    fn uses_numeric_owner_with_pfbid_when_numeric_post_id_is_not_yet_available() {
        let mut post = post();
        post.source_id = "facebook:100064289305513".to_owned();
        post.external_post_id =
            "pfbid02DkqH4LacFBQgoQCwHRUpn6H4hJDsM2N1VLEHho6dnxAoywk3z7vAqHDo3uhWPZbRl".to_owned();
        post.canonical_url = format!(
            "https://www.facebook.com/nvhsvtphcm/posts/{}",
            post.external_post_id
        );

        assert_eq!(
            delivery_post_url(&post),
            format!(
                "https://www.facebook.com/100064289305513/posts/{}",
                post.external_post_id
            )
        );
    }

    #[test]
    fn keeps_canonical_url_without_verified_numeric_identity() {
        let post = post();

        assert_eq!(delivery_post_url(&post), post.canonical_url);
    }

    #[test]
    fn humanizes_legacy_notification_samples() {
        let sample = "Hoạt động mới\n\nThời gian đăng bài: 2026-07-24T12:00:02+00:00\nXem bài viết: https://example.com";
        let message = humanize_notification_sample(sample);

        assert!(message.contains("Đăng lúc: 19:00 ngày 24/07/2026"));
        assert!(message.contains("Bài gốc: https://example.com"));
        assert!(!message.contains("T12:00:02+00:00"));
    }

    async fn mock_server(
        status: u16,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4_096];
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request_complete(&request) {
                    break;
                }
            }
            let reason = if status == 200 { "OK" } else { "Error" };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });
        (format!("http://{address}"), task)
    }

    fn request_complete(request: &[u8]) -> bool {
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_lowercase()
                    .strip_prefix("content-length: ")
                    .map(str::to_owned)
            })
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        request.len() >= header_end + 4 + content_length
    }

    fn post() -> FacebookPost {
        FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: "facebook:source.a".to_owned(),
            platform: "facebook".to_owned(),
            external_post_id: "post-1".to_owned(),
            canonical_url: "https://www.facebook.com/source.a/posts/post-1".to_owned(),
            published_at: "2026-07-19T02:00:17+00:00".to_owned(),
            text: "Nội dung hoạt động".to_owned(),
            media: Vec::new(),
            outbound_links: Vec::new(),
            content_hash: "sha256:test".to_owned(),
            crawl_strategy: "standard".to_owned(),
            fetched_at: "2026-07-19T03:00:17+00:00".to_owned(),
        }
    }
}
