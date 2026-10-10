// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Measures the R2 behaviours the immutable store relies on (spec §3.2), against the bucket the
//! environment names.
//!
//! The store writes a fragment as `x-amz-meta-lore-fragment` on the object, reads a payload from
//! the middle of it, and relies on `DeleteObject` being the whole of an obliteration. Each of
//! those is an R2 detail rather than an S3-guaranteed one, so they are measured against the real
//! endpoint before the store is trusted with a bucket. The plan's auxiliary items (endpoint,
//! region, path-style, and a repeated PUT) are measured alongside, because they cost nothing once
//! the client exists.
//!
//! ```text
//! LORE_R2_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com \
//! LORE_R2_BUCKET=haikizoku-lore-r2-test \
//! LORE_R2_ACCESS_KEY_ID=... LORE_R2_SECRET_ACCESS_KEY=... \
//!   cargo run -p lore-cloudflare --example r2_probe
//! ```
//!
//! One markdown table goes to stdout, for the PR body. Nothing an attacker could use is printed:
//! the leftmost host label of a public endpoint becomes `<account>`, and the keys are only ever
//! named, never shown. Every object this run creates is deleted before it exits.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::RequestChecksumCalculation;
use aws_sdk_s3::config::SharedCredentialsProvider;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use bytes::Bytes;
use lore_aws::SdkError;
use lore_aws::aws_error::AwsError;
use lore_aws::clients::AwsClientBuilder;
use lore_aws::clients::TimeoutConfig;
use lore_aws::s3::S3Impl;
use lore_cloudflare::R2Credentials;
use lore_cloudflare::R2Settings;

/// The metadata key the store writes its fragment under.
///
/// Duplicated from `lore-aws/src/store/object_metadata.rs` rather than re-exported: the module is
/// private to `lore-aws`, and the probe has to state the key it is checking by name anyway.
const FRAGMENT_KEY: &str = "lore-fragment";

/// A well-formed metadata value, `flags:size_payload:size_content`, with the payload flags clear.
const FRAGMENT_VALUE: &str = "0:100:120";

/// More than the range asks for, so a partial read is visible in the body length.
const PAYLOAD_LEN: usize = 2048;

/// Where the half-open range ends; the request turns it into `bytes=0-1023`.
const RANGE_END: usize = 1024;

/// The region a deployment configures for R2. Cloudflare's own value.
const REGION: &str = "auto";

/// How long the probe waits on any one request. The store's default is 5 s.
const REQUEST_TIMEOUT_MILLIS: u64 = 5_000;

#[tokio::main]
async fn main() {
    if let Err(message) = run().await {
        eprintln!("r2_probe: {message}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let settings = settings_from_environment()?;
    let bucket = settings.bucket().to_string();
    let client = build_client(&settings).await?;

    let (base, checksum_key, duplicate_key) = probe_keys();

    println!("# R2 probe");
    println!();
    println!("- bucket: {bucket}");
    println!(
        "- endpoint: {}",
        redact_endpoint(settings.endpoint_url().unwrap_or("<未設定>"))
    );
    println!(
        "- region: {REGION}, path-style: {}, timeout: {REQUEST_TIMEOUT_MILLIS} ms",
        settings.force_path_style()
    );
    println!("- keys: {base}, {checksum_key}, {duplicate_key}");
    println!();

    let mut table = Table::new();

    item_1_metadata(&client, &bucket, &base, &mut table).await;
    item_2_range(&client, &bucket, &base, &mut table).await;
    item_3_checksum(&client, &bucket, &checksum_key, &mut table).await;
    item_4_versioning(&client, &bucket, &base, &mut table).await;
    let bucket_state = item_5_placement(&client, &settings, &bucket, &mut table).await;
    item_6_duplicate_put(&client, &bucket, &duplicate_key, &mut table).await;

    cleanup(
        &client,
        &bucket,
        &[&base, &checksum_key, &duplicate_key],
        &mut table,
    )
    .await;

    println!("{table}");

    // The table is printed either way: it says what the endpoint answered. But a bucket that is
    // not there makes every row above a 404, so the run is not a measurement and must not look
    // like one to whatever is reading the exit code.
    if bucket_state == Some(false) {
        return Err(format!(
            "the bucket {bucket} is not there (head_bucket answered 404), so nothing above was \
             measured"
        ));
    }

    Ok(())
}

/// Item 1: does a `HeadObject` return the `x-amz-meta-*` the object was written with?
///
/// If it does not, `get_metadata` cannot read a fragment from a HEAD and has to fall back to a
/// `GetObject` — which reads the payload to learn its shape, the cost the metadata avoids.
async fn item_1_metadata(client: &S3Impl, bucket: &str, key: &str, table: &mut Table) {
    let metadata = HashMap::from([(FRAGMENT_KEY.to_string(), FRAGMENT_VALUE.to_string())]);

    let put = client
        .put_object(
            bucket,
            key,
            Bytes::from(vec![7u8; PAYLOAD_LEN]),
            Some(metadata),
        )
        .await;

    if let Err(error) = put {
        table.push(
            ITEM_1,
            "put_object (metadata 付き) → head_object",
            describe_aws_error(&error),
            "PUT が失敗したので未確認",
        );
        return;
    }

    match client.head_object(bucket, key).await {
        Ok(head) => {
            let returned = head
                .metadata()
                .and_then(|metadata| metadata.get(FRAGMENT_KEY))
                .cloned();
            let count = head.metadata().map_or(0, HashMap::len);

            let value = match &returned {
                Some(value) => format!("{FRAGMENT_KEY} = {value} (metadata {count} 件)"),
                None => format!("metadata {count} 件、{FRAGMENT_KEY} を含まない"),
            };

            let verdict = if returned.as_deref() == Some(FRAGMENT_VALUE) {
                "返る (想定どおり)".to_string()
            } else {
                "返らない → get_metadata は GET にフォールバックが要る".to_string()
            };

            table.push(ITEM_1, "put_object → head_object", value, verdict);
        }
        Err(error) => {
            let verdict = if status_of(&error) == Some(403) {
                "403 (権限) なので未確認。token の権限を見る"
            } else {
                "HeadObject が失敗したので未確認"
            };

            table.push(
                ITEM_1,
                "put_object → head_object",
                describe_aws_error(&error),
                verdict,
            );
        }
    }
}

/// Item 2: does a `Range` GET answer 206 with the bytes asked for?
///
/// The store reads a payload in pieces, so a `Range` that is ignored (or answered 200 with the
/// whole object) would make every read the size of the payload it is trying to avoid.
async fn item_2_range(client: &S3Impl, bucket: &str, key: &str, table: &mut Table) {
    match client.get_object(bucket, key, Some(0..RANGE_END)).await {
        Ok(output) => {
            let content_range = output.content_range().unwrap_or("<無し>").to_string();
            let content_length = output.content_length();
            let body = output.body.collect().await.map(|bytes| bytes.into_bytes());

            let body_length = match &body {
                Ok(bytes) => bytes.len().to_string(),
                Err(error) => format!("body の読み取りに失敗: {error}"),
            };

            let value = format!(
                "{content_range}; Content-Length={content_length:?}; body={body_length} bytes"
            );

            let verdict = match (&body, content_length) {
                (Ok(bytes), Some(length))
                    if bytes.len() == RANGE_END && length == RANGE_END as i64 =>
                {
                    if content_range.starts_with("bytes 0-1023/") {
                        "206 で 1024 bytes (想定どおり)".to_string()
                    } else {
                        "部分 body だが Content-Range が想定と違う".to_string()
                    }
                }
                (Ok(bytes), _) if bytes.len() == PAYLOAD_LEN => {
                    "Range が無視され全 body が返った → 途中読みができない".to_string()
                }
                _ => "部分 body になっていない".to_string(),
            };

            table.push(ITEM_2, "get_object (bytes=0-1023)", value, verdict);
        }
        Err(error) => table.push(
            ITEM_2,
            "get_object (bytes=0-1023)",
            describe_aws_error(&error),
            "Range GET が失敗した",
        ),
    }
}

/// Item 3: does the SDK's default checksum survive R2?
///
/// `WhenSupported` makes aws-sdk-s3 attach a CRC-32 over the whole object. Cloudflare's table
/// lists CRC-32 as a composite checksum and gives full-object support only to CRC-64/NVME, so this
/// may come back refused. The fallback — the same service config with `WhenRequired` — is measured
/// with a client derived from the first, so a difference between the two rows is the checksum and
/// nothing else.
async fn item_3_checksum(client: &S3Impl, bucket: &str, key: &str, table: &mut Table) {
    let payload = Bytes::from(vec![5u8; 256]);

    match client.put_object(bucket, key, payload.clone(), None).await {
        Ok(_) => table.push(
            ITEM_3,
            "put_object (SDK 既定 = WhenSupported)",
            "成功",
            "通る (CRC-32 の FULL_OBJECT を R2 が受ける)",
        ),
        Err(error) => {
            let described = describe_aws_error(&error);

            let requiring = client_requiring_checksums(client);
            let fallback = requiring.put_object(bucket, key, payload, None).await;

            let (value, verdict) = match fallback {
                Ok(_) => (
                    format!("既定 (WhenSupported): {described}; WhenRequired: 成功"),
                    "既定は拒否され、WhenRequired で通る → store の client に設定が要る"
                        .to_string(),
                ),
                Err(error) => (
                    format!(
                        "既定 (WhenSupported): {described}; WhenRequired: {}",
                        describe_aws_error(&error)
                    ),
                    "両方失敗 → checksum 以外の原因を先に見る".to_string(),
                ),
            };

            table.push(ITEM_3, "put_object", value, verdict);
        }
    }
}

/// Item 4: what does R2 answer for `ListObjectVersions`, and does one `DeleteObject` remove the
/// object for good?
///
/// The store's `delete_payload` drops the version sweep the AWS one does, on the assumption that
/// R2 has no versioning. A `ListObjectVersions` that succeeds would be that assumption breaking,
/// and a delete that leaves a readable object would be worse.
async fn item_4_versioning(client: &S3Impl, bucket: &str, key: &str, table: &mut Table) {
    let versions_before = client.list_versions(bucket, key).await;
    let before = match &versions_before {
        Ok(output) => format!("成功 ({} versions)", output.versions().len()),
        Err(error) => format!("失敗: {}", describe_aws_error(error)),
    };

    let deleted = client.delete_object(bucket, key, None).await;
    let delete = match &deleted {
        Ok(_) => "成功".to_string(),
        Err(error) => format!("失敗: {}", describe_aws_error(error)),
    };

    let head_after = client.head_object(bucket, key).await;
    let after_head = match &head_after {
        Ok(_) => "まだ読める".to_string(),
        Err(error) => describe_aws_error(error),
    };

    let versions_after = match client.list_versions(bucket, key).await {
        Ok(output) => format!("成功 ({} versions)", output.versions().len()),
        Err(error) => format!("失敗: {}", describe_aws_error(&error)),
    };

    let value = format!(
        "list_object_versions: {before} / delete_object: {delete} / 削除後の head_object: \
         {after_head} / 削除後の list_object_versions: {versions_after}"
    );

    let refusal = versions_before.as_ref().err().and_then(status_of);

    let verdict = if versions_before.is_ok() {
        "list_object_versions が通った → versioning がある。第 3.3 節の前提が崩れる".to_string()
    } else if deleted.is_ok() && is_not_found(&head_after) {
        match refusal {
            Some(403) => "削除 1 回で消えたが、第 3.3 節の前提 (versioning が無い) を list では \
                          確かめられていない: list_object_versions が 403"
                .to_string(),
            _ => "versioning は無い。DeleteObject 1 回で消える (第 3.3 節の前提どおり)".to_string(),
        }
    } else {
        "想定と違う結果。値の列を見る".to_string()
    };

    table.push(
        ITEM_4,
        "list_object_versions → delete_object → head_object",
        value,
        verdict,
    );
}

/// Item 5: do the endpoint, the region and path-style addressing the plan gives actually work, and
/// can the region be left out?
///
/// The store refuses to build without an endpoint, so this is the check that the value the
/// deployment gets is the one R2 wants, and that `auto` is a region the SDK will sign with. It is
/// also the one item that can say the bucket is not there, which is the difference between a
/// measurement and a page of 404s — so it reports that back to the caller.
async fn item_5_placement(
    client: &S3Impl,
    settings: &R2Settings,
    bucket: &str,
    table: &mut Table,
) -> Option<bool> {
    let head = client.bucket_exists(bucket.to_string()).await;
    let configured = match &head {
        Ok(true) => "head_bucket = 200".to_string(),
        Ok(false) => "head_bucket = 404 (bucket が無い)".to_string(),
        Err(error) => format!("head_bucket = {}", describe_aws_error(error)),
    };

    let without_region = settings.clone().with_region(None);
    let omitted = match build_client(&without_region).await {
        Ok(client) => match client.bucket_exists(bucket.to_string()).await {
            Ok(true) => "region を省いても 200".to_string(),
            Ok(false) => "region を省くと 404".to_string(),
            Err(error) => format!("region を省くと {}", describe_aws_error(&error)),
        },
        Err(message) => format!("region 無しの client を作れない: {message}"),
    };

    let value = format!(
        "endpoint={}, region={REGION}, path-style={}, timeout={REQUEST_TIMEOUT_MILLIS} ms: \
         {configured} / {omitted}",
        redact_endpoint(settings.endpoint_url().unwrap_or("<未設定>")),
        settings.force_path_style(),
    );

    let verdict = if matches!(&head, Ok(true)) {
        "endpoint / region / path-style で通る".to_string()
    } else {
        "通らない。endpoint か鍵を見る".to_string()
    };

    table.push(ITEM_5, "head_bucket (region あり / なし)", value, verdict);

    // `None` is "the answer said nothing about the bucket" (403, a timeout), which is not the same
    // as the bucket being absent: a bucket-scoped token may be refused a HEAD and still be able to
    // write the objects the items above write.
    head.ok()
}

/// Item 6: what happens when the same key is written twice inside a second?
///
/// Cloudflare's table gives a key one write per second. Content-addressed payloads make a repeated
/// PUT safe, so the store's answer to a 429 is to hand `SlowDown` to the caller rather than to
/// retry — this says whether that path is reachable at all.
async fn item_6_duplicate_put(client: &S3Impl, bucket: &str, key: &str, table: &mut Table) {
    let payload = Bytes::from(vec![9u8; 128]);

    let started = Instant::now();
    let first = client.put_object(bucket, key, payload.clone(), None).await;
    let first_elapsed = started.elapsed();
    let second = client.put_object(bucket, key, payload, None).await;
    let interval = started.elapsed();

    let first_text = match &first {
        Ok(_) => format!("成功 ({first_elapsed:?})"),
        Err(error) => describe_aws_error(error),
    };
    let second_text = match &second {
        Ok(_) => "成功".to_string(),
        Err(error) => describe_aws_error(error),
    };

    let value = format!("1 回目: {first_text} / 2 回目 ({interval:?} 後): {second_text}");

    let verdict = if second.is_ok() {
        "重複 PUT は通る (429 は出ない)".to_string()
    } else if is_throttled(&second) {
        "429 → store は SlowDown を返して呼び出し側に委ねる (第 3.5 節どおり)".to_string()
    } else {
        "429 以外の失敗。値の列を見る".to_string()
    };

    table.push(ITEM_6, "put_object ×2", value, verdict);
}

/// Delete every object this run created and say what is left.
///
/// A deletion is idempotent, so an object whose own item already deleted it is deleted again here
/// on purpose: the point is that the bucket has nothing of ours when the process exits.
async fn cleanup(client: &S3Impl, bucket: &str, keys: &[&str], table: &mut Table) {
    let mut deleted = Vec::new();
    for key in keys {
        match client.delete_object(bucket, key, None).await {
            Ok(_) => deleted.push(format!("{key}: 削除した")),
            Err(error) => deleted.push(format!("{key}: {}", describe_aws_error(&error))),
        }
    }

    let mut remaining = Vec::new();
    for key in keys {
        if client.head_object(bucket, key).await.is_ok() {
            remaining.push((*key).to_string());
        }
    }

    let verdict = if remaining.is_empty() {
        "object は残っていない".to_string()
    } else {
        format!("残っている object: {}", remaining.join(", "))
    };

    table.push(
        ITEM_CLEANUP,
        "delete_object ×3 → head_object ×3",
        deleted.join(" / "),
        verdict,
    );
}

/// The settings a deployment configures, from the same variable names the plugin reads.
fn settings_from_environment() -> Result<R2Settings, String> {
    let variable = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };

    let bucket = variable("LORE_R2_BUCKET")
        .ok_or("LORE_R2_BUCKET is not set (the bucket the store is configured with)")?;
    let endpoint = variable("LORE_R2_ENDPOINT_URL").ok_or(
        "LORE_R2_ENDPOINT_URL is not set (the store refuses to build without one, so the probe \
         does too)",
    )?;
    let access_key_id =
        variable("LORE_R2_ACCESS_KEY_ID").ok_or("LORE_R2_ACCESS_KEY_ID is not set")?;
    let secret_access_key =
        variable("LORE_R2_SECRET_ACCESS_KEY").ok_or("LORE_R2_SECRET_ACCESS_KEY is not set")?;

    Ok(
        R2Settings::new(bucket, R2Credentials::new(access_key_id, secret_access_key))
            .with_endpoint_url(Some(endpoint))
            .with_region(Some(REGION.to_string()))
            .with_timeout_millis(REQUEST_TIMEOUT_MILLIS),
    )
}

/// Build the store's own client, one link of `connect_r2`'s chain at a time.
///
/// The bucket is not checked here: `head_bucket` is one of the measured items, and it has to be
/// able to fail without taking the rest of the probe with it.
async fn build_client(settings: &R2Settings) -> Result<S3Impl, String> {
    settings
        .validate()
        .map_err(|message| format!("the R2 settings are invalid: {message}"))?;

    // The keys the deployment configured, and nothing else. There is no fallback to the SDK's own
    // chain: on a machine with an instance profile, a fallback would read and write somebody
    // else's bucket.
    let credentials = Credentials::new(
        settings.credentials().access_key_id(),
        settings.credentials().secret_access_key(),
        None,
        None,
        "lore-cloudflare-probe",
    );

    AwsClientBuilder::builder()
        .with_http_settings(settings.http_settings())
        .with_credentials_provider(SharedCredentialsProvider::new(credentials))
        .maybe_endpoint(settings.endpoint_url().map(str::to_string))
        .maybe_region(settings.region().map(str::to_string))
        .with_timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_millis(settings.timeout_millis()))
                .build(),
        )
        .build_config()
        .await
        .with_slow_operation_threshold(settings.slow_operation_threshold_millis())
        .s3_with_path_style(settings.force_path_style())
        .build()
        .await
        .map_err(|error| {
            format!(
                "failed to build the S3 client for bucket {}: {error}",
                settings.bucket()
            )
        })
}

/// The same service config with one setting changed, so a checksum failure is told apart from
/// everything else that could fail a PUT.
///
/// `to_builder` keeps the HTTP client, the endpoint and the credentials of the client built by the
/// store's own chain, which is what makes the two rows comparable.
fn client_requiring_checksums(client: &S3Impl) -> S3Impl {
    let config = client
        .sdk_client()
        .config()
        .to_builder()
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .build();

    S3Impl::new(
        aws_sdk_s3::Client::from_conf(config),
        Duration::from_millis(REQUEST_TIMEOUT_MILLIS),
    )
}

/// One key per measured object, under `probe/`, stamped and salted so two runs cannot collide.
fn probe_keys() -> (String, String, String) {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    let salt = rand::random::<u32>();

    let base = format!("probe/{millis}-{salt:08x}");

    (
        base.clone(),
        format!("{base}-checksum"),
        format!("{base}-duplicate"),
    )
}

/// Whether an error is an S3 "no such object".
fn is_not_found<E, O>(error: &Result<O, AwsError<SdkError<E>>>) -> bool
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    matches!(error, Err(error) if status_of(error) == Some(404))
}

/// Whether an error is R2's overload answer, which the store turns into `SlowDown`.
fn is_throttled<E, O>(error: &Result<O, AwsError<SdkError<E>>>) -> bool
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    matches!(error, Err(error) if matches!(status_of(error), Some(429 | 500..=599)))
}

/// The HTTP status an S3 failure carried, when the failure reached the service at all.
fn status_of<E>(error: &AwsError<SdkError<E>>) -> Option<u16>
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let AwsError::AwsSdkError(sdk_error) = error else {
        return None;
    };

    match &**sdk_error {
        SdkError::ServiceError(service) => Some(service.raw().status().as_u16()),
        _ => None,
    }
}

/// A failure in one line: the status and code R2 answered with, or what went wrong before there
/// was an answer. Never the request, which would carry the signature.
fn describe_aws_error<E>(error: &AwsError<SdkError<E>>) -> String
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let AwsError::AwsSdkError(sdk_error) = error else {
        return format!("{error:?}");
    };

    match &**sdk_error {
        SdkError::ServiceError(service) => format!(
            "HTTP {} ({})",
            service.raw().status().as_u16(),
            service.err().code().unwrap_or("<code 無し>")
        ),
        SdkError::TimeoutError(_) => "timeout".to_string(),
        SdkError::DispatchFailure(_) => "dispatch failure (接続できない)".to_string(),
        other => format!("{other:?}"),
    }
}

/// Redact the account out of an endpoint while leaving anything that is not one alone.
///
/// R2's endpoint is `<account>.r2.cloudflarestorage.com`, so the leftmost label of a name with
/// three or more labels is the account. A local endpoint such as `minio:9000` keeps its name,
/// which is not a secret and is the one thing a reader needs to tell the runs apart.
fn redact_endpoint(endpoint: &str) -> String {
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return "<endpoint の形が読めない>".to_string();
    };

    let (host, path) = match rest.split_once('/') {
        Some((host, path)) => (host, format!("/{path}")),
        None => (rest, String::new()),
    };

    // A signature sometimes travels in the query string, and it is never part of what was measured.
    let path = path.split('?').next().unwrap_or(&path).to_string();

    format!("{scheme}://{}{path}", redact_host(host))
}

fn redact_host(host: &str) -> String {
    let (name, port) = match host.rsplit_once(':') {
        Some((name, port)) if port.chars().all(|character| character.is_ascii_digit()) => {
            (name, Some(port.to_string()))
        }
        _ => (host, None),
    };

    let labels: Vec<&str> = name.split('.').collect();
    let name = if labels.len() >= 3 {
        format!("<account>.{}", labels[1..].join("."))
    } else {
        name.to_string()
    };

    match port {
        Some(port) => format!("{name}:{port}"),
        None => name,
    }
}

const ITEM_1: &str = "1: HeadObject が x-amz-meta を返すか";
const ITEM_2: &str = "2: Range GET の意味";
const ITEM_3: &str = "3: 既定の checksum が通るか";
const ITEM_4: &str = "4: versioning が無いこと";
const ITEM_5: &str = "5: endpoint / region / path-style";
const ITEM_6: &str = "6: 同一 key への PUT を 1 秒以内に 2 回";
const ITEM_CLEANUP: &str = "後片付け";

/// The rows, printed as the markdown table the PR body takes.
struct Table {
    rows: Vec<Row>,
}

struct Row {
    item: String,
    method: String,
    value: String,
    verdict: String,
}

impl Table {
    fn new() -> Self {
        Self { rows: Vec::new() }
    }

    fn push(
        &mut self,
        item: impl Into<String>,
        method: impl Into<String>,
        value: impl Into<String>,
        verdict: impl Into<String>,
    ) {
        self.rows.push(Row {
            item: item.into(),
            method: method.into(),
            value: value.into(),
            verdict: verdict.into(),
        });
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "| 項目 | 方法 | 値 | 判定 |")?;
        writeln!(f, "|---|---|---|---|")?;

        for row in &self.rows {
            writeln!(
                f,
                "| {} | {} | {} | {} |",
                escape(&row.item),
                escape(&row.method),
                escape(&row.value),
                escape(&row.verdict)
            )?;
        }

        Ok(())
    }
}

/// A cell cannot hold a newline or a bare `|`, either of which would break the table around it.
fn escape(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', "<br>")
}
