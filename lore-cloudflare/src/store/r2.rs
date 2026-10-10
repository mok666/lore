// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The R2 client, and the settings it is built from.
//!
//! R2 is S3 at another endpoint, so the client is [`lore_aws::s3::S3Impl`], the one the AWS store
//! already uses. Request signing, the HTTP pool, the retry the SDK applies on its own and the
//! telemetry are then the same on both stores, and a change to one cannot silently apply to only
//! the other. What is *not* shared is the credential chain: an R2 deployment has no IAM role and
//! no ambient profile, so the keys are passed in explicitly (spec §3.4) and a deployment that does
//! not configure them stops at startup rather than running against whatever credentials the
//! machine happens to hold.
//!
//! The store talks to [`R2Client`] rather than to the client directly, so that its tests can
//! script object answers without an object store — the same shape the Turso stores use for
//! statements, and the reason neither store needs a docker container to be unit tested.

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::SharedCredentialsProvider;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use bytes::Bytes;
use lore_aws::SdkError;
use lore_aws::aws_error::AwsError;
use lore_aws::clients::AwsClientBuilder;
use lore_aws::clients::HttpClientSettings;
use lore_aws::clients::TimeoutConfig;
use lore_aws::s3::S3Impl;
use lore_storage::SlowDown;
use lore_storage::StoreError;
use lore_storage::errors::NotFound;

/// The slow-operation threshold a deployment gets when it configures none. Every request that
/// takes longer than this is logged with its elapsed time, which is how a store that is merely
/// slow is told apart from one that is failing.
pub const DEFAULT_SLOW_OPERATION_THRESHOLD_MILLIS: u64 = 1_000;

/// The timeout a deployment gets when it configures none.
pub const DEFAULT_TIMEOUT_MILLIS: u64 = 5_000;

/// The R2 credentials, which are always explicit.
///
/// Passing them in is the point of the type: a deployment that has none cannot be built, and a
/// store that was handed some cannot go looking for others. The `Debug` implementation replaces
/// both with a placeholder, so a settings struct that is logged on the way past — as the plugin
/// logs it while the server starts — cannot put a key in a log.
#[derive(Clone)]
pub struct R2Credentials {
    access_key_id: String,
    secret_access_key: String,
}

impl R2Credentials {
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        }
    }

    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// The secret. Callers pass it to the driver; nothing logs it.
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
}

impl fmt::Debug for R2Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("R2Credentials")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

/// The settings an R2 client is built from.
///
/// The bucket, the endpoint and the region are the ones any S3 client needs; `force_path_style`
/// is on by default because both R2 and `MinIO` answer to a bucket in the path, while
/// virtual-hosted addressing needs a DNS name that only one of them has.
#[derive(Clone)]
pub struct R2Settings {
    bucket: String,
    credentials: R2Credentials,
    endpoint_url: Option<String>,
    region: Option<String>,
    force_path_style: bool,
    slow_operation_threshold_millis: u64,
    timeout_millis: u64,
    http: HttpClientSettings,
}

impl R2Settings {
    pub fn new(bucket: impl Into<String>, credentials: R2Credentials) -> Self {
        Self {
            bucket: bucket.into(),
            credentials,
            endpoint_url: None,
            region: None,
            force_path_style: true,
            slow_operation_threshold_millis: DEFAULT_SLOW_OPERATION_THRESHOLD_MILLIS,
            timeout_millis: DEFAULT_TIMEOUT_MILLIS,
            http: HttpClientSettings::default(),
        }
    }

    pub fn with_endpoint_url(mut self, endpoint_url: Option<String>) -> Self {
        self.endpoint_url = endpoint_url;
        self
    }

    pub fn with_region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn with_force_path_style(mut self, force_path_style: bool) -> Self {
        self.force_path_style = force_path_style;
        self
    }

    pub fn with_slow_operation_threshold_millis(mut self, millis: u64) -> Self {
        self.slow_operation_threshold_millis = millis;
        self
    }

    pub fn with_timeout_millis(mut self, millis: u64) -> Self {
        self.timeout_millis = millis;
        self
    }

    /// The HTTP pool and socket settings, which are the AWS plugin's own keys, so a deployment
    /// that already tuned them for S3 keeps them for R2.
    pub fn with_http_settings(mut self, http: HttpClientSettings) -> Self {
        self.http = http;
        self
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn credentials(&self) -> &R2Credentials {
        &self.credentials
    }

    pub fn endpoint_url(&self) -> Option<&str> {
        self.endpoint_url.as_deref()
    }

    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    pub fn force_path_style(&self) -> bool {
        self.force_path_style
    }

    pub fn slow_operation_threshold_millis(&self) -> u64 {
        self.slow_operation_threshold_millis
    }

    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }

    pub fn http_settings(&self) -> &HttpClientSettings {
        &self.http
    }

    /// Reject settings that cannot work, naming the key the operator has to fix. The caller turns
    /// this into a configuration error, so the server stops at startup rather than at the first
    /// write.
    ///
    /// Nothing here is reported with its value: the message says which key is wrong.
    pub fn validate(&self) -> Result<(), String> {
        if self.bucket.trim().is_empty() {
            return Err("bucket is empty".to_string());
        }

        if self.credentials.access_key_id.trim().is_empty() {
            return Err("access_key_id is empty".to_string());
        }

        if self.credentials.secret_access_key.trim().is_empty() {
            return Err("secret_access_key is empty".to_string());
        }

        if self
            .endpoint_url
            .as_deref()
            .is_some_and(|endpoint| endpoint.trim().is_empty())
        {
            return Err("endpoint_url is empty".to_string());
        }

        if self
            .region
            .as_deref()
            .is_some_and(|region| region.trim().is_empty())
        {
            return Err("region is empty".to_string());
        }

        if self.timeout_millis == 0 {
            return Err("timeout_millis is zero".to_string());
        }

        Ok(())
    }
}

impl fmt::Debug for R2Settings {
    /// Prints everything an operator needs to see and nothing an attacker does: the keys are
    /// replaced, and so is any query string, which is where a signed endpoint URL sometimes
    /// carries one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let endpoint_url = self
            .endpoint_url
            .as_deref()
            .map(|url| url.split('?').next().unwrap_or(url).to_string());

        f.debug_struct("R2Settings")
            .field("bucket", &self.bucket)
            .field("credentials", &self.credentials)
            .field("endpoint_url", &endpoint_url)
            .field("region", &self.region)
            .field("force_path_style", &self.force_path_style)
            .field(
                "slow_operation_threshold_millis",
                &self.slow_operation_threshold_millis,
            )
            .field("timeout_millis", &self.timeout_millis)
            .field("http", &self.http)
            .finish()
    }
}

/// An object as R2 returned it.
pub struct R2Object {
    /// The length the endpoint reported, where it reported one.
    pub content_length: Option<usize>,
    /// The `x-amz-meta-*` pairs the object was written with. The fragment lives here rather than
    /// in a separate record, so a reader always sees the metadata of the bytes it is holding.
    pub metadata: HashMap<String, String>,
    /// The payload, still on the wire.
    pub body: ByteStream,
}

/// How an R2 request failed, as far as the store is concerned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum R2Error {
    /// The object is not there.
    NotFound,
    /// No answer was obtained: a client-side timeout, a request that never reached the service, an
    /// HTTP 429, or an HTTP 5xx. All of them mean "ask again" to a caller, and the store reports
    /// them as [`StoreError::SlowDown`] with the trait's existing meaning.
    ///
    /// This is where the store deliberately does *not* retry on its own. A 429 is R2 saying the
    /// caller is sending too much, and a retry loop inside the store would both hide that signal
    /// and multiply the retry budget of every caller above it. The caller already has a backoff.
    SlowDown,
    /// Anything else: a rejected credential, a malformed request, a response that could not be
    /// read. Reported as an internal error rather than as an overload, so that a wrong key fails
    /// at once instead of being retried forever as if the service were busy.
    Failed(String),
}

impl R2Error {
    /// This failure as the store error it means, in the context of what was being done.
    pub fn into_store_error(self, context: &str) -> StoreError {
        match self {
            Self::NotFound => StoreError::from(NotFound),
            Self::SlowDown => StoreError::from(SlowDown),
            // The context reads as the action: "... while reading a payload from R2".
            Self::Failed(message) => StoreError::internal(format!("{message} while {context}")),
        }
    }

    /// Rewrite the message of a failure, leaving the meaning of the variant alone.
    fn map_failed(self, rewrite: impl FnOnce(String) -> String) -> Self {
        match self {
            Self::Failed(message) => Self::Failed(rewrite(message)),
            other => other,
        }
    }
}

/// The object operations the immutable store needs, and no more.
///
/// Keys are the store's own: a content address, hex-encoded, so that a payload is written to the
/// name of its own hash.
#[async_trait]
pub trait R2Client: Send + Sync {
    async fn put_object(
        &self,
        key: &str,
        payload: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<(), R2Error>;

    /// The object's metadata, without its body. An object that is not there is
    /// [`R2Error::NotFound`]; an object that is there but carries no fragment metadata is an empty
    /// map, which the store treats as damage rather than as absence.
    async fn head_object(&self, key: &str) -> Result<HashMap<String, String>, R2Error>;

    async fn get_object(&self, key: &str) -> Result<R2Object, R2Error>;

    /// Delete the object. Deleting a key that is not there is not an error: the key is a content
    /// address, so a delete that arrives twice is the same delete.
    async fn delete_object(&self, key: &str) -> Result<(), R2Error>;
}

/// Why a client could not be built.
#[derive(Clone, Debug)]
pub struct R2ConnectError(String);

impl fmt::Display for R2ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for R2ConnectError {}

/// Build the client the store talks to the bucket through.
///
/// The bucket is checked here, as the AWS plugin checks its own: a bucket that does not exist, or
/// credentials that are refused, stop the server at startup rather than at the first write. This
/// is the one request that is made outside an operation, and it is made once.
pub async fn connect(settings: &R2Settings) -> Result<Arc<dyn R2Client>, R2ConnectError> {
    settings
        .validate()
        .map_err(|message| R2ConnectError(format!("the R2 settings are invalid: {message}")))?;

    // The keys the deployment configured, and nothing else. There is no fallback to the SDK's own
    // chain: on a machine with an instance profile, a fallback would silently read and write
    // somebody else's bucket.
    let credentials = Credentials::new(
        settings.credentials().access_key_id(),
        settings.credentials().secret_access_key(),
        None,
        None,
        "lore-cloudflare",
    );

    let http_settings = settings.http_settings();

    let s3 = AwsClientBuilder::builder()
        .with_http_settings(http_settings)
        .with_credentials_provider(SharedCredentialsProvider::new(credentials))
        .maybe_endpoint(settings.endpoint_url().map(|url| url.to_string()))
        .maybe_region(settings.region().map(|region| region.to_string()))
        .with_timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_millis(settings.timeout_millis()))
                .build(),
        )
        .build_config()
        .await
        .with_slow_operation_threshold(settings.slow_operation_threshold_millis())
        .s3_with_path_style(settings.force_path_style())
        .ensure_bucket(settings.bucket())
        .build()
        .await
        .map_err(|error| {
            R2ConnectError(format!(
                "failed to reach the bucket {}: {error}",
                settings.bucket()
            ))
        })?;

    tracing::debug!(
        bucket = settings.bucket(),
        "Connected to R2 through the S3 API"
    );

    Ok(Arc::new(S3R2Client {
        s3,
        bucket: settings.bucket().to_string(),
    }))
}

/// An [`R2Client`] that is really an S3 client pointed at R2.
pub struct S3R2Client {
    s3: S3Impl,
    bucket: String,
}

impl fmt::Debug for S3R2Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3R2Client")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl R2Client for S3R2Client {
    async fn put_object(
        &self,
        key: &str,
        payload: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<(), R2Error> {
        let length = payload.len();

        self.s3
            .put_object(&self.bucket, key, payload, Some(metadata))
            .await
            .map(|_output| ())
            .map_err(|error| {
                classify(&error).map_failed(|message| {
                    format!("{message} while writing {length} bytes to {key}")
                })
            })
    }

    async fn head_object(&self, key: &str) -> Result<HashMap<String, String>, R2Error> {
        let output = self
            .s3
            .head_object(&self.bucket, key)
            .await
            .map_err(|error| classify(&error))?;

        Ok(output.metadata().cloned().unwrap_or_default())
    }

    async fn get_object(&self, key: &str) -> Result<R2Object, R2Error> {
        let output = self
            .s3
            .get_object(&self.bucket, key, None)
            .await
            .map_err(|error| classify(&error))?;

        Ok(R2Object {
            content_length: output
                .content_length()
                .and_then(|length| usize::try_from(length).ok()),
            metadata: output.metadata().cloned().unwrap_or_default(),
            body: output.body,
        })
    }

    async fn delete_object(&self, key: &str) -> Result<(), R2Error> {
        self.s3
            .delete_object(&self.bucket, key, None)
            .await
            .map(|_output| ())
            .map_err(|error| classify(&error))
    }
}

/// Which of the SDK's failures this is.
///
/// The shapes mean different things and are not interchangeable. Reporting a failed read as
/// not-found would tell a caller that content is absent when we merely failed to look, and a
/// not-found on a referenced hash is exactly what the store's own missing-payload path treats as
/// lost data — a throttle recorded as data loss would then clear a state row. Reporting a refused
/// credential as an overload would retry a request that can never succeed.
///
/// This is a deliberate departure from the AWS store, which reports every S3 failure as
/// `SlowDown`: a wrong key there is retried until the caller gives up.
fn classify<E>(error: &AwsError<SdkError<E>>) -> R2Error
where
    E: ProvideErrorMetadata + Debug,
{
    let AwsError::AwsSdkError(sdk_error) = error else {
        // The three remaining variants are DynamoDB batch bookkeeping and cannot be produced by
        // an S3 request, but they are not a "come back later" either.
        return R2Error::Failed(format!("{error:?}"));
    };

    match &**sdk_error {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => R2Error::SlowDown,
        SdkError::ServiceError(service) => {
            let status = service.raw().status().as_u16();
            let code = service.err().code().unwrap_or("<no code>");

            match status {
                404 => R2Error::NotFound,
                429 | 500..=599 => R2Error::SlowDown,
                _ => R2Error::Failed(format!("R2 answered HTTP {status} ({code})")),
            }
        }
        _ => R2Error::Failed(format!("{sdk_error:?}")),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn settings() -> R2Settings {
        R2Settings::new("bucket", R2Credentials::new("id", "secret"))
    }

    #[test]
    fn a_bucket_and_keys_are_required() {
        assert!(settings().validate().is_ok());

        let error = R2Settings::new("", R2Credentials::new("id", "secret"))
            .validate()
            .unwrap_err();
        assert!(error.contains("bucket"), "{error}");

        let error = R2Settings::new("bucket", R2Credentials::new("", "secret"))
            .validate()
            .unwrap_err();
        assert!(error.contains("access_key_id"), "{error}");

        let error = R2Settings::new("bucket", R2Credentials::new("id", "  "))
            .validate()
            .unwrap_err();
        assert!(error.contains("secret_access_key"), "{error}");
    }

    #[test]
    fn an_endpoint_or_a_region_is_rejected_when_it_is_empty() {
        let error = settings()
            .with_endpoint_url(Some(String::new()))
            .validate()
            .unwrap_err();
        assert!(error.contains("endpoint_url"), "{error}");

        let error = settings()
            .with_region(Some(" ".to_string()))
            .validate()
            .unwrap_err();
        assert!(error.contains("region"), "{error}");

        let error = settings().with_timeout_millis(0).validate().unwrap_err();
        assert!(error.contains("timeout_millis"), "{error}");
    }

    /// The point of the redaction: the plugin logs the settings while the server starts, and a
    /// key in a log is a key in a log.
    ///
    /// The assertion is on the values rather than on the field names, because the names are what
    /// makes the line readable and one of them has to say `secret_access_key`.
    #[test]
    fn printing_the_settings_never_prints_a_key() {
        let printed = format!(
            "{:?}",
            R2Settings::new(
                "bucket",
                R2Credentials::new("the-access-key-id", "the-secret-access-key")
            )
            .with_endpoint_url(Some(
                "https://account.r2.cloudflarestorage.com?token=also-a-secret".to_string()
            ))
        );

        assert!(!printed.contains("the-access-key-id"), "{printed}");
        assert!(!printed.contains("the-secret-access-key"), "{printed}");
        assert!(printed.contains("bucket"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");

        // The endpoint keeps its host, which is not a secret and which an operator has to be able
        // to see: an endpoint that is left out entirely points the client at S3, and the log line
        // is the only place that says which of the two the server is talking to. What goes is the
        // query string, where a signed url sometimes carries a token.
        assert!(printed.contains("account.r2"), "{printed}");
        assert!(!printed.contains("also-a-secret"), "{printed}");
    }

    #[test]
    fn a_missing_object_is_not_found_and_an_overload_is_a_slow_down() {
        assert!(matches!(
            R2Error::NotFound.into_store_error("reading a payload"),
            StoreError::NotFound(_)
        ));
        assert!(matches!(
            R2Error::SlowDown.into_store_error("reading a payload"),
            StoreError::SlowDown(_)
        ));

        let error = R2Error::Failed("R2 answered HTTP 403 (AccessDenied)".to_string())
            .into_store_error("reading a payload");
        let printed = format!("{error}");
        assert!(printed.contains("403"), "{printed}");
        assert!(printed.contains("reading a payload"), "{printed}");
        assert!(matches!(error, StoreError::Internal(_)), "{error:?}");
    }
}
