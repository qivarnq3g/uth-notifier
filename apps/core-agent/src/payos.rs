use std::time::Duration;
use std::{io::Cursor, num::NonZeroU32};

use anyhow::{Context, Result, bail};
use hmac::{Hmac, Mac};
use qrcode::{Color, QrCode};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use url::Url;

const VIETQR_IMAGE_BASE: &str = "https://img.vietqr.io/";
const VIETQR_IMAGE_ATTEMPTS: u8 = 2;
const VIETQR_IMAGE_TIMEOUT: Duration = Duration::from_secs(8);
const VIETQR_IMAGE_MAX_BYTES: usize = 2 * 1024 * 1024;
const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";
const JPEG_SIGNATURE: &[u8] = b"\xff\xd8\xff";

#[derive(Clone)]
pub struct PayOsClient {
    client: Client,
    vietqr_images: VietQrImageClient,
    client_id: String,
    api_key: String,
    checksum_key: String,
    api_base: Url,
    return_url: Url,
    cancel_url: Url,
}

#[derive(Clone)]
struct VietQrImageClient {
    client: Client,
}

#[derive(Debug, Clone)]
pub struct PaymentLink {
    pub bank_bin: String,
    pub account_number: String,
    pub account_name: String,
    pub amount: i64,
    pub description: String,
    pub payment_link_id: String,
    pub checkout_url: String,
    pub qr_code: String,
    pub qr_png: Vec<u8>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreatePaymentRequest {
    order_code: i64,
    amount: i64,
    description: String,
    cancel_url: String,
    return_url: String,
    expired_at: i64,
    signature: String,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    code: String,
    desc: String,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreatePaymentData {
    bin: String,
    account_number: String,
    account_name: String,
    amount: i64,
    description: String,
    payment_link_id: String,
    checkout_url: String,
    qr_code: String,
}

impl PayOsClient {
    pub fn new(
        client_id: String,
        api_key: String,
        checksum_key: String,
        api_base: Url,
        return_url: Url,
        cancel_url: Url,
        timeout: Duration,
    ) -> Result<Self> {
        for (name, value) in [
            ("PAYOS_CLIENT_ID", client_id.as_str()),
            ("PAYOS_API_KEY", api_key.as_str()),
            ("PAYOS_CHECKSUM_KEY", checksum_key.as_str()),
        ] {
            if value.trim().is_empty() || value.len() > 512 {
                bail!("{name} must contain between 1 and 512 characters");
            }
        }
        for (name, value) in [
            ("PAYOS_API_BASE", &api_base),
            ("PAYOS_RETURN_URL", &return_url),
            ("PAYOS_CANCEL_URL", &cancel_url),
        ] {
            if value.scheme() != "https" || value.host_str().is_none() {
                bail!("{name} must be a valid HTTPS URL");
            }
        }
        let client = Client::builder()
            .timeout(timeout)
            .user_agent("uth-notifier-payos/0.1")
            .build()?;
        Ok(Self {
            vietqr_images: VietQrImageClient {
                client: client.clone(),
            },
            client,
            client_id,
            api_key,
            checksum_key,
            api_base,
            return_url,
            cancel_url,
        })
    }

    pub async fn create_payment_link(
        &self,
        order_code: i64,
        amount: i64,
        expired_at: i64,
    ) -> Result<PaymentLink> {
        let description = format!("UTH{:06}", order_code.rem_euclid(1_000_000));
        let cancel_url = self.cancel_url.as_str().to_owned();
        let return_url = self.return_url.as_str().to_owned();
        let signature_data = format!(
            "amount={amount}&cancelUrl={cancel_url}&description={description}&orderCode={order_code}&returnUrl={return_url}"
        );
        let request = CreatePaymentRequest {
            order_code,
            amount,
            description,
            cancel_url,
            return_url,
            expired_at,
            signature: hmac_hex(self.checksum_key.as_bytes(), signature_data.as_bytes())?,
        };
        let endpoint = self.api_base.join("/v2/payment-requests")?;
        let response = self
            .client
            .post(endpoint)
            .header("x-client-id", &self.client_id)
            .header("x-api-key", &self.api_key)
            .json(&request)
            .send()
            .await
            .context("payOS create payment request failed")?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            bail!("payOS rejected the configured credentials");
        }
        if !status.is_success() {
            bail!("payOS create payment request returned HTTP {status}");
        }
        let response = response
            .json::<ApiResponse<CreatePaymentData>>()
            .await
            .context("payOS returned an invalid create payment response")?;
        if response.code != "00" {
            bail!("payOS rejected payment request: {}", response.desc);
        }
        let data = response
            .data
            .context("payOS create payment response did not contain data")?;
        let checkout_url =
            Url::parse(&data.checkout_url).context("payOS returned an invalid checkout URL")?;
        if checkout_url.scheme() != "https" || checkout_url.host_str() != Some("pay.payos.vn") {
            bail!("payOS returned an unexpected checkout URL");
        }
        if data.amount != amount
            || data.bin.is_empty()
            || data.bin.len() > 20
            || data.account_number.is_empty()
            || data.account_number.len() > 100
            || data.account_name.is_empty()
            || data.account_name.chars().count() > 200
            || data.description.is_empty()
            || data.description.chars().count() > 200
            || data.payment_link_id.is_empty()
            || data.payment_link_id.len() > 200
            || data.qr_code.is_empty()
            || data.qr_code.len() > 4096
        {
            bail!("payOS returned invalid payment link data");
        }
        let local_qr_png = render_qr_png(&data.qr_code)?;
        let qr_png = match self.vietqr_images.fetch(&data).await {
            Ok(image) => image,
            Err(outcome) => {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "schema_version": "payos-qr-image-fallback.v1",
                        "outcome": outcome,
                        "attempts": VIETQR_IMAGE_ATTEMPTS
                    })
                );
                local_qr_png
            }
        };
        Ok(PaymentLink {
            bank_bin: data.bin,
            account_number: data.account_number,
            account_name: data.account_name,
            amount: data.amount,
            description: data.description,
            payment_link_id: data.payment_link_id,
            checkout_url: checkout_url.to_string(),
            qr_code: data.qr_code,
            qr_png,
        })
    }
}

impl VietQrImageClient {
    async fn fetch(&self, data: &CreatePaymentData) -> std::result::Result<Vec<u8>, &'static str> {
        let url = build_vietqr_image_url(data).map_err(|_| "url_build_error")?;
        let mut last_outcome = "network_error";
        for attempt in 1..=VIETQR_IMAGE_ATTEMPTS {
            let response = self
                .client
                .get(url.clone())
                .timeout(VIETQR_IMAGE_TIMEOUT)
                .send()
                .await;
            if let Ok(mut response) = response {
                if !response.status().is_success() {
                    last_outcome = "http_error";
                } else if response
                    .content_length()
                    .is_some_and(|length| length > VIETQR_IMAGE_MAX_BYTES as u64)
                {
                    last_outcome = "image_too_large";
                } else {
                    let mut image = Vec::new();
                    let mut read_failed = false;
                    loop {
                        match response.chunk().await {
                            Ok(Some(chunk)) => {
                                if image.len().saturating_add(chunk.len()) > VIETQR_IMAGE_MAX_BYTES
                                {
                                    last_outcome = "image_too_large";
                                    read_failed = true;
                                    break;
                                }
                                image.extend_from_slice(&chunk);
                            }
                            Ok(None) => break,
                            Err(_) => {
                                last_outcome = "network_error";
                                read_failed = true;
                                break;
                            }
                        }
                    }
                    if !read_failed {
                        if image.starts_with(PNG_SIGNATURE) || image.starts_with(JPEG_SIGNATURE) {
                            return Ok(image);
                        }
                        last_outcome = "invalid_image";
                    }
                }
            }
            if attempt < VIETQR_IMAGE_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
            }
        }
        Err(last_outcome)
    }
}

fn build_vietqr_image_url(data: &CreatePaymentData) -> Result<Url> {
    let mut url = Url::parse(VIETQR_IMAGE_BASE)?;
    let image_name = format!("{}-{}-vietqr_pro.jpg", data.bin, data.account_number);
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("VietQR image base cannot be a base URL"))?
        .extend(["image", image_name.as_str()]);
    url.query_pairs_mut()
        .append_pair("addInfo", &data.description)
        .append_pair("amount", &data.amount.to_string());
    Ok(url)
}

fn render_qr_png(payload: &str) -> Result<Vec<u8>> {
    let code = QrCode::new(payload.as_bytes()).context("payOS QR payload is invalid")?;
    let module_count = u32::try_from(code.width())?;
    let scale = NonZeroU32::new(8).context("QR scale must be non-zero")?;
    let quiet_zone = 4_u32;
    let image_size = module_count
        .checked_add(quiet_zone * 2)
        .and_then(|value| value.checked_mul(scale.get()))
        .context("QR image dimensions overflow")?;
    let pixel_count = usize::try_from(
        image_size
            .checked_mul(image_size)
            .context("QR image is too large")?,
    )?;
    let mut pixels = vec![255_u8; pixel_count];
    for y in 0..module_count {
        for x in 0..module_count {
            if code[(usize::try_from(x)?, usize::try_from(y)?)] != Color::Dark {
                continue;
            }
            let start_x = (x + quiet_zone) * scale.get();
            let start_y = (y + quiet_zone) * scale.get();
            for pixel_y in start_y..start_y + scale.get() {
                for pixel_x in start_x..start_x + scale.get() {
                    let index = usize::try_from(pixel_y * image_size + pixel_x)?;
                    pixels[index] = 0;
                }
            }
        }
    }
    let mut output = Vec::new();
    {
        let mut encoder = png::Encoder::new(Cursor::new(&mut output), image_size, image_size);
        encoder.set_color(png::ColorType::Grayscale);
        encoder.set_depth(png::BitDepth::Eight);
        encoder
            .write_header()?
            .write_image_data(&pixels)
            .context("failed to encode payOS QR PNG")?;
    }
    Ok(output)
}

fn hmac_hex(key: &[u8], data: &[u8]) -> Result<String> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key).map_err(|_| anyhow::anyhow!("invalid HMAC key"))?;
    mac.update(data);
    Ok(mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::{CreatePaymentData, build_vietqr_image_url, hmac_hex, render_qr_png};

    #[test]
    fn creates_stable_hmac_sha256() {
        assert_eq!(
            hmac_hex(b"key", b"data").unwrap(),
            "5031fe3d989c6d1537a013fa6e739da23463fdaec3b70137d828e36ace221bd0"
        );
    }

    #[test]
    fn builds_vietqr_pro_image_url() {
        let url = build_vietqr_image_url(&CreatePaymentData {
            bin: "970418".to_owned(),
            account_number: "VIRTUAL123".to_owned(),
            account_name: "TEST USER".to_owned(),
            amount: 10_000,
            description: "UTH000001".to_owned(),
            payment_link_id: "payment-1".to_owned(),
            checkout_url: "https://pay.payos.vn/web/payment-1".to_owned(),
            qr_code: "000201010212".to_owned(),
        })
        .unwrap();

        assert_eq!(url.host_str(), Some("img.vietqr.io"));
        assert_eq!(url.path(), "/image/970418-VIRTUAL123-vietqr_pro.jpg");
        assert!(url.query().unwrap().contains("addInfo=UTH000001"));
        assert!(url.query().unwrap().contains("amount=10000"));
    }

    #[test]
    fn renders_qr_as_png() {
        let png = render_qr_png("00020101021238570010A000000727").unwrap();
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
        assert!(png.len() > 100);
    }
}
