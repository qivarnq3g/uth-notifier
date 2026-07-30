use hmac::{Hmac, Mac};
use js_sys::Date;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use uth_domain::{EDGE_EVENT_SCHEMA_VERSION, EdgeEvent, telegram_edge_event};
use wasm_bindgen::JsValue;
use worker::{Context, Env, Method, Request, Response, Result, event};

const MAX_WEBHOOK_BYTES: usize = 262_144;
const MAX_PULL_BATCH: usize = 100;
const MAX_LEASE_SECONDS: u64 = 300;

#[derive(Debug, Deserialize)]
struct StoredEvent {
    event_id: String,
    schema_version: String,
    event_type: String,
    aggregate_key: String,
    sequence: i64,
    occurred_at: String,
    payload: String,
}

#[derive(Debug, Serialize)]
struct PullResponse {
    events: Vec<EdgeEvent>,
}

#[derive(Debug, Deserialize)]
struct AckRequest {
    owner: String,
    event_ids: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AckResponse {
    acknowledged: usize,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    count: i64,
}

#[derive(Debug, Deserialize)]
struct PayOsWebhook {
    code: String,
    success: bool,
    data: Value,
    signature: String,
}

#[event(fetch)]
pub async fn main(mut request: Request, env: Env, _context: Context) -> Result<Response> {
    match (request.method(), request.path().as_str()) {
        (Method::Post, "/telegram/webhook") => telegram_webhook(&mut request, &env).await,
        (Method::Post, "/payos/webhook") => payos_webhook(&mut request, &env).await,
        (Method::Get, "/donate/return") => donation_result_page(true),
        (Method::Get, "/donate/cancel") => donation_result_page(false),
        (Method::Get, "/internal/events") => pull_events(&request, &env).await,
        (Method::Post, "/internal/ack") => acknowledge_events(&mut request, &env).await,
        (Method::Get, "/health") => Response::ok("ok"),
        _ => Response::error("not found", 404),
    }
}

async fn payos_webhook(request: &mut Request, env: &Env) -> Result<Response> {
    if request
        .headers()
        .get("Content-Length")?
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|value| value > MAX_WEBHOOK_BYTES)
    {
        return Response::error("payload too large", 413);
    }
    let bytes = request.bytes().await?;
    if bytes.len() > MAX_WEBHOOK_BYTES {
        return Response::error("payload too large", 413);
    }
    let webhook: PayOsWebhook = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => return Response::error("invalid JSON", 400),
    };
    let checksum_key = env.secret("PAYOS_CHECKSUM_KEY")?.to_string();
    let signature_data = match signature_data(&webhook.data) {
        Ok(value) => value,
        Err(error) => return Response::error(error, 400),
    };
    if !valid_hmac(
        checksum_key.as_bytes(),
        signature_data.as_bytes(),
        &webhook.signature,
    ) {
        return Response::error("invalid signature", 401);
    }
    if webhook.code != "00" || !webhook.success {
        return Response::ok("ignored");
    }
    let order_code = match webhook.data.get("orderCode").and_then(Value::as_i64) {
        Some(value) if value > 0 => value,
        _ => return Response::error("invalid orderCode", 400),
    };
    let reference = match webhook.data.get("reference").and_then(Value::as_str) {
        Some(value) if !value.is_empty() && value.len() <= 120 => value,
        _ => return Response::error("invalid reference", 400),
    };
    let payment_link_id = match webhook.data.get("paymentLinkId").and_then(Value::as_str) {
        Some(value) if !value.is_empty() && value.len() <= 120 => value,
        _ => return Response::error("invalid paymentLinkId", 400),
    };
    let occurred_at = Date::new_0()
        .to_iso_string()
        .as_string()
        .unwrap_or_default();
    let event = EdgeEvent {
        schema_version: EDGE_EVENT_SCHEMA_VERSION.to_owned(),
        event_id: format!("payos:{payment_link_id}:{reference}"),
        event_type: "payos.payment".to_owned(),
        aggregate_key: format!("payos-order:{order_code}"),
        sequence: order_code,
        occurred_at,
        payload: webhook.data,
    };
    if let Err(error) = event.validate() {
        return Response::error(error, 400);
    }
    persist_event(env, &event).await?;
    Response::from_json(&json!({"success": true}))
}

fn donation_result_page(success: bool) -> Result<Response> {
    let (title, body) = if success {
        (
            "Thanh toán đã được ghi nhận",
            "Bạn có thể quay lại Telegram. Bot sẽ xác nhận sau khi nhận webhook từ payOS.",
        )
    } else {
        (
            "Thanh toán đã hủy",
            "Không có giao dịch nào được ghi nhận. Bạn có thể quay lại Telegram.",
        )
    };
    Response::from_html(format!(
        "<!doctype html><html lang=\"vi\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"><title>{title}</title><body><main><h1>{title}</h1><p>{body}</p></main></body></html>"
    ))
}

async fn telegram_webhook(request: &mut Request, env: &Env) -> Result<Response> {
    let expected = env.secret("TELEGRAM_WEBHOOK_SECRET")?.to_string();
    let provided = request
        .headers()
        .get("X-Telegram-Bot-Api-Secret-Token")?
        .unwrap_or_default();
    if !constant_time_equal(expected.as_bytes(), provided.as_bytes()) {
        return Response::error("unauthorized", 401);
    }
    if request
        .headers()
        .get("Content-Length")?
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|value| value > MAX_WEBHOOK_BYTES)
    {
        return Response::error("payload too large", 413);
    }
    let bytes = request.bytes().await?;
    if bytes.len() > MAX_WEBHOOK_BYTES {
        return Response::error("payload too large", 413);
    }
    let payload: Value = match serde_json::from_slice(&bytes) {
        Ok(payload) => payload,
        Err(_) => return Response::error("invalid JSON", 400),
    };
    let occurred_at = Date::new_0()
        .to_iso_string()
        .as_string()
        .unwrap_or_default();
    let event = match telegram_edge_event(payload, occurred_at) {
        Ok(event) => event,
        Err(error) => return Response::error(error, 400),
    };
    persist_event(env, &event).await?;
    Response::ok("accepted")
}

async fn persist_event(env: &Env, event: &EdgeEvent) -> Result<()> {
    let database = env.d1("EDGE_DB")?;
    let payload = serde_json::to_string(&event.payload)?;
    let result = database
        .prepare(
            "INSERT OR IGNORE INTO edge_events \
             (event_id, schema_version, event_type, aggregate_key, sequence, occurred_at, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&[
            JsValue::from_str(&event.event_id),
            JsValue::from_str(&event.schema_version),
            JsValue::from_str(&event.event_type),
            JsValue::from_str(&event.aggregate_key),
            JsValue::from_f64(event.sequence as f64),
            JsValue::from_str(&event.occurred_at),
            JsValue::from_str(&payload),
        ])?
        .run()
        .await?;
    if result.meta()?.and_then(|meta| meta.changes).unwrap_or(0) == 0 {
        let matching = database
            .prepare(
                "SELECT COUNT(*) AS count FROM edge_events \
                 WHERE event_id = ? AND schema_version = ? AND event_type = ? \
                   AND aggregate_key = ? AND sequence = ? AND payload = ?",
            )
            .bind(&[
                JsValue::from_str(&event.event_id),
                JsValue::from_str(&event.schema_version),
                JsValue::from_str(&event.event_type),
                JsValue::from_str(&event.aggregate_key),
                JsValue::from_f64(event.sequence as f64),
                JsValue::from_str(&payload),
            ])?
            .first::<CountRow>(None)
            .await?;
        if matching.is_none_or(|row| row.count != 1) {
            return Err(worker::Error::RustError("event ID conflict".to_owned()));
        }
    }
    Ok(())
}

async fn pull_events(request: &Request, env: &Env) -> Result<Response> {
    if !authorized_sync_request(request, env)? {
        return Response::error("unauthorized", 401);
    }
    let url = request.url()?;
    let owner = query_value(&url, "owner").unwrap_or_default();
    if !valid_owner(&owner) {
        return Response::error("invalid owner", 400);
    }
    let limit = query_value(&url, "limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(MAX_PULL_BATCH)
        .clamp(1, MAX_PULL_BATCH);
    let lease_seconds = query_value(&url, "lease_seconds")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60)
        .clamp(1, MAX_LEASE_SECONDS);
    let database = env.d1("EDGE_DB")?;
    database
        .prepare(
            "DELETE FROM edge_events WHERE event_id IN (\
                SELECT event_id FROM edge_events \
                WHERE state = 'acknowledged' \
                  AND acknowledged_at < datetime('now', '-30 days') \
                ORDER BY acknowledged_at LIMIT 100\
             )",
        )
        .run()
        .await?;
    let candidates = database
        .prepare(
            "SELECT event_id, schema_version, event_type, aggregate_key, sequence, \
                    occurred_at, payload \
             FROM edge_events \
             WHERE state = 'pending' \
                OR (state = 'publishing' AND lease_expires_at <= CURRENT_TIMESTAMP) \
             ORDER BY sequence, created_at LIMIT ?",
        )
        .bind(&[JsValue::from_f64(limit as f64)])?
        .all()
        .await?
        .results::<StoredEvent>()?;
    let mut events = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let result = database
            .prepare(
                "UPDATE edge_events SET state = 'publishing', lease_owner = ?, \
                        lease_expires_at = datetime('now', '+' || ? || ' seconds'), \
                        attempts = attempts + 1 \
                 WHERE event_id = ? AND (state = 'pending' \
                    OR (state = 'publishing' AND lease_expires_at <= CURRENT_TIMESTAMP))",
            )
            .bind(&[
                JsValue::from_str(&owner),
                JsValue::from_f64(lease_seconds as f64),
                JsValue::from_str(&candidate.event_id),
            ])?
            .run()
            .await?;
        if result.meta()?.and_then(|meta| meta.changes).unwrap_or(0) != 1 {
            continue;
        }
        let event = EdgeEvent {
            schema_version: candidate.schema_version,
            event_id: candidate.event_id,
            event_type: candidate.event_type,
            aggregate_key: candidate.aggregate_key,
            sequence: candidate.sequence,
            occurred_at: candidate.occurred_at,
            payload: serde_json::from_str(&candidate.payload)?,
        };
        event.validate().map_err(worker::Error::RustError)?;
        events.push(event);
    }
    Response::from_json(&PullResponse { events })
}

async fn acknowledge_events(request: &mut Request, env: &Env) -> Result<Response> {
    if !authorized_sync_request(request, env)? {
        return Response::error("unauthorized", 401);
    }
    let ack: AckRequest = match request.json().await {
        Ok(value) => value,
        Err(_) => return Response::error("invalid JSON", 400),
    };
    if !valid_owner(&ack.owner) || ack.event_ids.is_empty() || ack.event_ids.len() > MAX_PULL_BATCH
    {
        return Response::error("invalid acknowledgement", 400);
    }
    let database = env.d1("EDGE_DB")?;
    let mut acknowledged = 0;
    for event_id in ack.event_ids {
        if event_id.is_empty() || event_id.len() > 200 {
            return Response::error("invalid event ID", 400);
        }
        let result = database
            .prepare(
                "UPDATE edge_events SET state = 'acknowledged', acknowledged_at = CURRENT_TIMESTAMP, \
                        lease_owner = NULL, lease_expires_at = NULL \
                 WHERE event_id = ? AND state = 'publishing' AND lease_owner = ?",
            )
            .bind(&[
                JsValue::from_str(&event_id),
                JsValue::from_str(&ack.owner),
            ])?
            .run()
            .await?;
        let changed = result
            .meta()?
            .and_then(|meta| meta.changes)
            .unwrap_or_default();
        if changed == 1 {
            acknowledged += 1;
            continue;
        }
        let existing = database
            .prepare(
                "SELECT COUNT(*) AS count FROM edge_events \
                 WHERE event_id = ? AND state = 'acknowledged'",
            )
            .bind(&[JsValue::from_str(&event_id)])?
            .first::<CountRow>(None)
            .await?;
        if existing.is_some_and(|row| row.count == 1) {
            acknowledged += 1;
        }
    }
    Response::from_json(&AckResponse { acknowledged })
}

fn authorized_sync_request(request: &Request, env: &Env) -> Result<bool> {
    let expected = env.secret("EDGE_SYNC_TOKEN")?.to_string();
    let provided = request
        .headers()
        .get("Authorization")?
        .and_then(|value| value.strip_prefix("Bearer ").map(str::to_owned))
        .unwrap_or_default();
    Ok(constant_time_equal(
        expected.as_bytes(),
        provided.as_bytes(),
    ))
}

fn query_value(url: &url::Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

fn valid_owner(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

fn signature_data(data: &Value) -> std::result::Result<String, String> {
    let object = data
        .as_object()
        .ok_or_else(|| "payOS data must be an object".to_owned())?;
    let mut fields = object.iter().collect::<Vec<_>>();
    fields.sort_by(|left, right| left.0.cmp(right.0));
    fields
        .into_iter()
        .map(|(key, value)| Ok(format!("{key}={}", signature_value(value)?)))
        .collect::<std::result::Result<Vec<_>, String>>()
        .map(|fields| fields.join("&"))
}

fn signature_value(value: &Value) -> std::result::Result<String, String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) if value == "undefined" || value == "null" => Ok(String::new()),
        Value::String(value) => Ok(value.clone()),
        Value::Array(values) => {
            let sorted = values
                .iter()
                .map(|value| match value {
                    Value::Object(object) => {
                        let sorted = object
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect::<std::collections::BTreeMap<_, _>>();
                        Ok(serde_json::to_value(sorted)?)
                    }
                    _ => Ok(value.clone()),
                })
                .collect::<std::result::Result<Vec<_>, serde_json::Error>>()
                .map_err(|error| error.to_string())?;
            serde_json::to_string(&sorted).map_err(|error| error.to_string())
        }
        Value::Object(_) => Err("nested payOS objects are unsupported".to_owned()),
    }
}

fn valid_hmac(key: &[u8], data: &[u8], signature: &str) -> bool {
    let Ok(signature) = decode_hex(signature) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(key) else {
        return false;
    };
    mac.update(data);
    mac.verify_slice(&signature).is_ok()
}

fn decode_hex(value: &str) -> std::result::Result<Vec<u8>, ()> {
    if !value.len().is_multiple_of(2) {
        return Err(());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).ok_or(())?;
            let low = (pair[1] as char).to_digit(16).ok_or(())?;
            Ok(((high << 4) | low) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{constant_time_equal, signature_data, valid_hmac, valid_owner};
    use serde_json::json;

    #[test]
    fn secret_comparison_rejects_different_values_and_lengths() {
        assert!(constant_time_equal(b"same", b"same"));
        assert!(!constant_time_equal(b"same", b"diff"));
        assert!(!constant_time_equal(b"same", b"same-longer"));
    }

    #[test]
    fn owner_validation_is_bounded() {
        assert!(valid_owner("oci-worker_1"));
        assert!(!valid_owner(""));
        assert!(!valid_owner("invalid owner"));
    }

    #[test]
    fn verifies_official_payos_webhook_signature_sample() {
        let data = json!({
            "orderCode": 123,
            "amount": 3000,
            "description": "VQRIO123",
            "accountNumber": "12345678",
            "reference": "TF230204212323",
            "transactionDateTime": "2023-02-04 18:25:00",
            "currency": "VND",
            "paymentLinkId": "124c33293c43417ab7879e14c8d9eb18",
            "code": "00",
            "desc": "Thành công",
            "counterAccountBankId": "",
            "counterAccountBankName": "",
            "counterAccountName": "",
            "counterAccountNumber": "",
            "virtualAccountName": "",
            "virtualAccountNumber": ""
        });
        let canonical = signature_data(&data).unwrap();
        assert!(valid_hmac(
            b"1a54716c8f0efb2744fb28b6e38b25da7f67a925d98bc1c18bd8faaecadd7675",
            canonical.as_bytes(),
            "412e915d2871504ed31be63c8f62a149a4410d34c4c42affc9006ef9917eaa03"
        ));
    }
}
