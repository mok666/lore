// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! What the integration tests need that the crate itself does not provide: a server to run
//! against, and an executor that can lose an answer on purpose.
//!
//! # Running them
//!
//! The tests reach a local libSQL server, which needs no token, and a local S3 endpoint, which is
//! `MinIO` with the credentials the upstream compose file gives it, so nothing secret is involved
//! and the same commands work on a laptop and in CI. The servers the plan names are
//! `ghcr.io/tursodatabase/libsql-server:v0.24.32` and `cgr.dev/chainguard/minio`, which the
//! upstream compose file (`lore-integration-tests/compose.yaml`) already starts:
//!
//! ```text
//! docker network create lore-test
//! docker run -d --name lore-minio --network lore-test --network-alias minio \
//!   -e MINIO_ROOT_USER=lorelocal -e MINIO_ROOT_PASSWORD=lorelocal \
//!   cgr.dev/chainguard/minio server --console-address :9001 /data
//! docker run -d --name lore-sqld --network lore-test --network-alias sqld \
//!   -v lore-sqld-data:/var/lib/sqld ghcr.io/tursodatabase/libsql-server:v0.24.32
//! docker run --rm --network lore-test -v "$PWD:/work" -v lore-cargo-registry:/usr/local/cargo/registry \
//!   -v lore-target:/work/target -w /work \
//!   -e LORE_TURSO_URL=http://sqld:8080 \
//!   -e LORE_R2_ENDPOINT_URL=http://minio:9000 -e LORE_R2_BUCKET=lore-immutable-store-test \
//!   -e LORE_R2_ACCESS_KEY_ID=lorelocal -e LORE_R2_SECRET_ACCESS_KEY=lorelocal \
//!   rust:1.97.1 cargo test -p lore-cloudflare --features integration_tests --tests
//! ```
//!
//! The R2 variables are the ones the plugin resolves, not test-only ones: the safe rehearsal and
//! the real measurement differ only in the values handed to them (spec §5.3), so a variable that
//! exists only in a test would be one more thing to get wrong. Without `LORE_TURSO_URL` or
//! `LORE_R2_ENDPOINT_URL` every test in these files says it did not run and returns, so that a
//! checkout without a server still has a green `cargo test`.
//!
//! The endpoints are the only thing this module makes for itself: a bucket that is not there yet is
//! created, which on `MinIO` is every bucket the first time. Nothing is dropped afterwards — the
//! content is addressed by a hash nobody else picks, so leftovers cost disk and nothing else.
//!
//! # Reading a failure
//!
//! The store translates a driver failure into a `StoreError` and writes the driver's own words to
//! `tracing`, which a test binary otherwise has no subscriber for. `install_tracing` gives it one,
//! so a red run against a real server can say what it was that did not answer. `RUST_LOG` chooses
//! the level and the targets (`RUST_LOG=lore_cloudflare=debug,warn`); the default `warn` is the
//! level the translations themselves log at.
//!
//! # When the server stops answering
//!
//! The pinned server can stop answering on the write path while reads keep working: measured on
//! `v0.24.32`, twice in about twenty runs of one test binary, with `BEGIN IMMEDIATE` still
//! unanswered minutes later, and it does not come back until the container is restarted. A run
//! that meets it is red rather than stuck — the store bounds every transaction by `timeout_millis`
//! (`TursoLimits::timeout`), so a request that got no answer at all becomes `SlowDown`, and the
//! caller's retry below then gives up after the write path's own budget — and it is worth telling
//! apart from the contention `SlowDown` the same budget is there to ride out: contention clears,
//! and it comes with answers rather than silence. Restart `lore-sqld` and rerun.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;

use async_trait::async_trait;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_base::types::LockData;
use lore_base::types::LockResource;
use lore_base::types::Partition;
use lore_cloudflare::CloudflareImmutableStore;
use lore_cloudflare::CloudflareImmutableStoreSettings;
use lore_cloudflare::CloudflareLockStore;
use lore_cloudflare::CloudflareMutableStore;
use lore_cloudflare::R2Client;
use lore_cloudflare::R2Credentials;
use lore_cloudflare::R2Settings;
use lore_cloudflare::TursoClient;
use lore_cloudflare::TursoError;
use lore_cloudflare::TursoErrorKind;
use lore_cloudflare::TursoExecutor;
use lore_cloudflare::TursoLimits;
use lore_cloudflare::TursoSettings;
use lore_cloudflare::TursoTransaction;
use lore_cloudflare::connect_r2;
use lore_cloudflare::turso::TursoRow;
use lore_cloudflare::turso::TursoValues;
use lore_revision::lock::LockError;
use lore_revision::lock::LockStore;
use tokio::sync::OnceCell;
use tracing_subscriber::EnvFilter;

// =============================================================================
// What the store only knows how to log
// =============================================================================

/// Send this crate's `tracing` events to the test's own output, once per test binary.
///
/// Every store method turns a driver failure into a `StoreError` and logs the reason the driver
/// gave, because the contract's vocabulary is all a caller may act on and it is not fine enough to
/// tell a timeout from a refused connection: `Disconnected` covers a request that never arrived, an
/// answer that was cut off, and one this crate gave up waiting for. The store's own tests can
/// script those, but an integration run meets them from a real server, and a run that reports
/// `Disconnected` without the reason is a run that has to be repeated to learn anything.
///
/// `RUST_LOG` selects what is printed, in `tracing-subscriber`'s own syntax; the default is `warn`,
/// which is the level those translations log at. The drivers' own events are below it and are
/// available by target, for example `RUST_LOG=lore_cloudflare=debug,libsql=trace,warn`.
///
/// The writer is the test harness's: events are captured with the test that produced them, so a
/// failing test shows them even without `--nocapture`, and `--nocapture` interleaves them in the
/// order they happened.
pub fn install_tracing() {
    static INSTALLED: OnceLock<()> = OnceLock::new();

    INSTALLED.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));

        // A subscriber another library already installed is left alone.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .try_init();
    });
}

// =============================================================================
// The server under test
// =============================================================================

/// The url of the server the tests run against, if the environment names one.
pub fn database_url() -> Option<String> {
    std::env::var("LORE_TURSO_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
}

/// Connect to the test server, or report that this run has none.
///
/// The schema is created once per test binary: every test that starts at once would otherwise
/// race its DDL, and the first statement of a race can be refused for a lock reason that has
/// nothing to do with what the test is about.
pub async fn client(label: &str) -> Option<TursoClient> {
    static SCHEMA: OnceCell<()> = const { OnceCell::const_new() };

    install_tracing();

    let Some(url) = database_url() else {
        eprintln!(
            "{label}: did not run — set LORE_TURSO_URL to a libSQL server (see tests/support/mod.rs)"
        );
        return None;
    };

    // A local server needs no token, and the environment only supplies one if a server does.
    let token = std::env::var("LORE_TURSO_AUTH_TOKEN").unwrap_or_default();
    let settings = TursoSettings::new(url, token);

    let client = TursoClient::connect(&settings)
        .await
        .unwrap_or_else(|error| panic!("{label}: could not reach {settings:?}: {error}"));

    SCHEMA
        .get_or_init(|| async {
            client
                .initialize_schema()
                .await
                .unwrap_or_else(|error| panic!("{label}: the server refused the schema: {error}"));
        })
        .await;

    Some(client)
}

/// The limits a store runs under when nothing says otherwise.
pub fn limits() -> TursoLimits {
    TursoLimits::default()
}

/// A mutable store over `executor`.
///
/// The store is handed back in an `Arc` because every method of the trait takes `self: Arc<Self>`:
/// an operation outlives the call that started it.
pub fn mutable_store(executor: Arc<dyn TursoExecutor>) -> Arc<CloudflareMutableStore> {
    Arc::new(CloudflareMutableStore::new(executor, limits()))
}

/// A lock store over `executor`.
pub fn lock_store(executor: Arc<dyn TursoExecutor>) -> CloudflareLockStore {
    CloudflareLockStore::new(executor, limits())
}

/// An immutable store over `r2` and `executor`.
pub fn immutable_store(
    r2: Arc<dyn R2Client>,
    executor: Arc<dyn TursoExecutor>,
    force_write: bool,
) -> Arc<CloudflareImmutableStore> {
    Arc::new(CloudflareImmutableStore::new(
        r2,
        executor,
        limits(),
        &CloudflareImmutableStoreSettings::new(force_write),
    ))
}

// =============================================================================
// The endpoint under test
// =============================================================================

/// The settings the immutable store is built from, if the environment names an endpoint.
///
/// The values are the plugin's own resolution: the same variable names, and the same defaults for
/// what a local endpoint leaves out. A missing endpoint, bucket or key reports that this run has no
/// endpoint rather than guessing at one — the SDK's own default is S3 itself, which is the one
/// place a test must never write.
pub fn r2_settings() -> Option<R2Settings> {
    let url = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };

    let endpoint = url("LORE_R2_ENDPOINT_URL")?;
    let bucket = url("LORE_R2_BUCKET")?;
    let access_key_id = url("LORE_R2_ACCESS_KEY_ID")?;
    let secret_access_key = url("LORE_R2_SECRET_ACCESS_KEY")?;

    // `MinIO` and R2 both answer to a bucket in the path, and neither cares what the signature
    // calls the region.
    Some(
        R2Settings::new(bucket, R2Credentials::new(access_key_id, secret_access_key))
            .with_endpoint_url(Some(endpoint))
            .with_region(Some("auto".to_string()))
            .with_force_path_style(true),
    )
}

/// A client over the bucket the environment names, or a report that this run has none.
pub async fn r2(label: &str) -> Option<Arc<dyn R2Client>> {
    install_tracing();

    let Some(settings) = r2_settings() else {
        eprintln!(
            "{label}: did not run — set LORE_R2_ENDPOINT_URL, LORE_R2_BUCKET, \
             LORE_R2_ACCESS_KEY_ID and LORE_R2_SECRET_ACCESS_KEY to an S3 endpoint (see \
             tests/support/mod.rs)"
        );
        return None;
    };

    // The store only checks that the bucket it was pointed at is there, because on R2 a bucket is
    // made in the Cloudflare dashboard and a server that made its own would be as likely to be
    // pointed at a typo as at a new bucket. A local `MinIO` starts with no buckets at all, so the
    // test makes the one it was pointed at, the way a deployment would.
    ensure_bucket(&settings)
        .await
        .unwrap_or_else(|error| panic!("{label}: could not prepare the bucket: {error}"));

    Some(
        connect_r2(&settings)
            .await
            .unwrap_or_else(|error| panic!("{label}: could not reach {settings:?}: {error}")),
    )
}

/// Make sure the endpoint has the bucket, the way an operator would before a deployment.
async fn ensure_bucket(settings: &R2Settings) -> Result<(), Box<dyn std::error::Error>> {
    use lore_aws::clients::AwsClientBuilder;

    let http_settings = lore_aws::clients::HttpClientSettings::default();
    let credentials = aws_sdk_s3::config::Credentials::new(
        settings.credentials().access_key_id(),
        settings.credentials().secret_access_key(),
        None,
        None,
        "lore-cloudflare-tests",
    );

    let client = AwsClientBuilder::builder()
        .with_http_settings(&http_settings)
        .with_credentials_provider(credentials)
        .maybe_endpoint(settings.endpoint_url().map(|url| url.to_string()))
        .maybe_region(settings.region().map(|region| region.to_string()))
        .build_config()
        .await
        .s3_with_path_style(settings.force_path_style())
        .build()
        .await?;

    if client.bucket_exists(settings.bucket().to_string()).await? {
        return Ok(());
    }

    tracing::info!(bucket = settings.bucket(), "Creating the test bucket");

    match client
        .sdk_client()
        .create_bucket()
        .bucket(settings.bucket())
        .send()
        .await
    {
        Ok(_) => Ok(()),
        // Two tests can reach this at once in a fresh endpoint.
        Err(error) if error.as_service_error().is_some() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

// =============================================================================
// Fixtures
// =============================================================================

/// A hash no other test has written.
pub fn random_hash() -> Hash {
    let mut hash = rand::random::<Hash>();
    if hash.is_zero() {
        hash.data_mut()[0] = 1;
    }
    hash
}

/// A repository no other test has written to.
pub fn random_partition() -> Partition {
    let mut partition = rand::random::<Partition>();
    if partition.is_zero() {
        partition.data_mut()[0] = 1;
    }
    partition
}

/// A branch no other test has locked.
pub fn random_context() -> Context {
    let mut context = rand::random::<Context>();
    if context.is_zero() {
        context.data_mut()[0] = 1;
    }
    context
}

/// A lock owner no other test has used.
pub fn random_owner() -> String {
    format!("owner-{:016x}", rand::random::<u64>())
}

/// A description no other resource in a test shares.
pub fn random_description() -> String {
    format!("src/file-{:016x}.rs", rand::random::<u64>())
}

// =============================================================================
// The caller's retry
// =============================================================================

/// The budget a request that met another writer gets before it gives up: the numbers the write
/// path uses for its own retries (`lore-revision`'s `RETRY_START_DURATION`, `RETRY_MAX_DURATION`
/// and `RETRY_MAX_ATTEMPTS`).
const RETRY_START_MILLIS: u64 = 100;
const RETRY_MAX_MILLIS: u64 = 10_000;
const RETRY_MAX_ATTEMPTS: usize = 10;

/// Reissue an operation while the server says another client holds the write lock: a lock request
/// hears `SlowDown` (spec §4.4), which says nothing was applied and the request can be reissued as
/// it stands (spec §4.6).
///
/// Written as a macro rather than a closure because each attempt needs a fresh future that borrows
/// the parameters, and each attempt is itself bounded by the store's `timeout_millis`, so an
/// operation that never settles still gives up rather than hanging the test.
macro_rules! reissue_while_busy {
    ($operation:expr) => {{
        let mut waiter =
            lore_storage::retry(RETRY_START_MILLIS, RETRY_MAX_MILLIS, RETRY_MAX_ATTEMPTS);

        loop {
            match $operation.await {
                Err(error) if error.is_slow_down() && waiter.wait().await => {
                    eprintln!(
                        "another client held the write lock; reissuing (attempt {})",
                        waiter.counter()
                    );
                }
                result => break result,
            }
        }
    }};
}

/// Take locks the way a caller of the store does.
///
/// `SlowDown` is the answer a lock request gets when it meets another client's unfinished
/// transaction: it says nothing was applied and the request can be reissued as it stands (spec
/// §4.4, §4.6). A caller retries it — the write path in the server re-queues the fragments it
/// could not check (`lore-server/src/grpc/handlers/branch_push.rs:854`), and the gRPC lock service
/// reports it to the client as `RESOURCE_EXHAUSTED` to be retried for the same reason — so a test
/// that failed on it would report whether the machine happened to be busy rather than what the
/// store does.
///
/// Reads are not wrapped: the store retries a lost answer inside its own limit (spec §4.6), and
/// the contended runs of this file failed only on the write paths.
pub async fn take_locks(
    store: &dyn LockStore,
    owner_id: &str,
    repository: Partition,
    resources: &[LockResource],
) -> Result<Vec<LockData>, LockError> {
    reissue_while_busy!(store.lock_resources(owner_id, repository, resources))
}

/// Release locks the way a caller of the store does. See [`take_locks`].
pub async fn release_locks(
    store: &dyn LockStore,
    owner_id: &str,
    validate_user: bool,
    repository: Partition,
    resources: &[LockResource],
) -> Result<Vec<LockResource>, LockError> {
    reissue_while_busy!(store.unlock_resources(owner_id, validate_user, repository, resources))
}

// =============================================================================
// Scripted executors
// =============================================================================

/// What was asked of a scripted executor, in the order it was asked.
#[derive(Default)]
pub struct StatementLog {
    statements: Mutex<Vec<String>>,
}

impl StatementLog {
    fn entries(&self) -> MutexGuard<'_, Vec<String>> {
        self.statements
            .lock()
            .expect("the statement log is not poisoned")
    }

    /// Record one statement.
    pub fn add(&self, statement: impl Into<String>) {
        self.entries().push(statement.into());
    }

    /// How many statements so far start with `prefix`.
    pub fn count(&self, prefix: &str) -> usize {
        self.entries()
            .iter()
            .filter(|statement| statement.starts_with(prefix))
            .count()
    }

    /// Every statement so far, clearing the log.
    pub fn take(&self) -> Vec<String> {
        std::mem::take(&mut self.entries())
    }
}

/// An executor that passes every statement through and loses the answer to every `COMMIT`.
///
/// The server applies the transaction: what is lost is the answer, which from the client's side is
/// the same as a statement that never arrived. This is the one failure a store cannot resolve by
/// looking, so it is the one the plan says not to guess at.
pub struct LostCommitExecutor {
    inner: Arc<dyn TursoExecutor>,
    log: Arc<StatementLog>,
}

impl LostCommitExecutor {
    pub fn new(inner: Arc<dyn TursoExecutor>, log: Arc<StatementLog>) -> Self {
        Self { inner, log }
    }
}

#[async_trait]
impl TursoExecutor for LostCommitExecutor {
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError> {
        self.log.add(format!("execute {sql}"));
        self.inner.execute(sql, parameters).await
    }

    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError> {
        self.log.add(format!("query {sql}"));
        self.inner.query(sql, parameters).await
    }

    async fn batch(&self, sql: &str) -> Result<(), TursoError> {
        self.log.add(format!("batch {sql}"));
        self.inner.batch(sql).await
    }

    async fn begin_immediate(&self) -> Result<Box<dyn TursoTransaction>, TursoError> {
        // One entry per request the store makes, however many attempts waiting takes: what is
        // under test is the store opening its transaction once (spec §4.6).
        self.log.add("begin immediate");

        // The store opens its transaction once and does not reissue it (spec §4.6), so a request
        // the server refused the write lock to would never reach the commit this executor is here
        // to lose. The connection waits for the lock instead, the way a caller of the store does —
        // over the same budget, but spelled out because it is a `TursoError` underneath the store
        // that says so rather than the `SlowDown` a caller sees.
        let mut waiter =
            lore_storage::retry(RETRY_START_MILLIS, RETRY_MAX_MILLIS, RETRY_MAX_ATTEMPTS);

        let transaction = loop {
            match self.inner.begin_immediate().await {
                Err(error) if error.kind() == TursoErrorKind::SlowDown && waiter.wait().await => {
                    eprintln!(
                        "the transaction was refused the write lock; opening it again (attempt {})",
                        waiter.counter()
                    );
                }
                result => break result?,
            }
        };

        Ok(Box::new(RecordingTransaction {
            inner: Some(transaction),
            log: self.log.clone(),
            lose_commit: true,
        }))
    }
}

/// An executor that passes every statement through and records it, so a test can count what one
/// request cost.
///
/// The statements a request makes are the round trips it spends, and a store that asks about
/// hundreds of resources has to spend a handful of them rather than one per resource (spec §4.6).
/// The transaction is wrapped for the same reason [`LostCommitExecutor`] wraps it: what a request
/// does between `BEGIN IMMEDIATE` and `COMMIT` is the whole of it, and none of it would show up
/// otherwise.
pub struct RecordingExecutor {
    inner: Arc<dyn TursoExecutor>,
    log: Arc<StatementLog>,
}

impl RecordingExecutor {
    pub fn new(inner: Arc<dyn TursoExecutor>, log: Arc<StatementLog>) -> Self {
        Self { inner, log }
    }
}

#[async_trait]
impl TursoExecutor for RecordingExecutor {
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError> {
        self.log.add(format!("execute {sql}"));
        self.inner.execute(sql, parameters).await
    }

    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError> {
        self.log.add(format!("query {sql}"));
        self.inner.query(sql, parameters).await
    }

    async fn batch(&self, sql: &str) -> Result<(), TursoError> {
        self.log.add(format!("batch {sql}"));
        self.inner.batch(sql).await
    }

    async fn begin_immediate(&self) -> Result<Box<dyn TursoTransaction>, TursoError> {
        self.log.add("begin immediate");

        // A request that meets another writer's transaction is reissued as it stands by the caller
        // (`support::take_locks`), so nothing is waited for here.
        let transaction = self.inner.begin_immediate().await?;

        Ok(Box::new(RecordingTransaction {
            inner: Some(transaction),
            log: self.log.clone(),
            lose_commit: false,
        }))
    }
}

/// A transaction that records what it is asked, and can lose the answer to its `COMMIT`.
struct RecordingTransaction {
    inner: Option<Box<dyn TursoTransaction>>,
    log: Arc<StatementLog>,
    lose_commit: bool,
}

impl RecordingTransaction {
    fn live(&self) -> &dyn TursoTransaction {
        self.inner
            .as_deref()
            .expect("the transaction is live until it is committed or rolled back")
    }

    fn take(&mut self) -> Result<Box<dyn TursoTransaction>, TursoError> {
        self.inner.take().ok_or_else(|| {
            TursoError::new(
                TursoErrorKind::Internal,
                "the transaction has already finished",
            )
        })
    }
}

#[async_trait]
impl TursoTransaction for RecordingTransaction {
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError> {
        self.log.add(format!("execute {sql}"));
        self.live().execute(sql, parameters).await
    }

    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError> {
        self.log.add(format!("query {sql}"));
        self.live().query(sql, parameters).await
    }

    async fn commit(self: Box<Self>) -> Result<(), TursoError> {
        self.log.add("commit");

        let mut slot = *self;

        let lose_commit = slot.lose_commit;

        match slot.take()?.commit().await {
            // The server applied it and the client never learned that it did.
            Ok(()) if lose_commit => Err(TursoError::disconnected(
                "test: the answer to the commit was lost after the server applied it",
            )),
            result => result,
        }
    }

    async fn rollback(self: Box<Self>) -> Result<(), TursoError> {
        self.log.add("rollback");

        let mut slot = *self;

        match slot.take() {
            Ok(inner) => inner.rollback().await,
            // Nothing was applied, so there is nothing to throw away.
            Err(_) => Ok(()),
        }
    }
}

/// An executor whose first `failures` operations never reach the server.
///
/// This is the other half of the failure the plan names: the operation did not arrive at all, so
/// reissuing it is safe as long as it is reissued a bounded number of times.
pub struct FlakyExecutor {
    inner: Arc<dyn TursoExecutor>,
    log: Arc<StatementLog>,
    failures: Mutex<usize>,
}

impl FlakyExecutor {
    pub fn new(inner: Arc<dyn TursoExecutor>, failures: usize, log: Arc<StatementLog>) -> Self {
        Self {
            inner,
            log,
            failures: Mutex::new(failures),
        }
    }

    /// Spend one failure on `what`, if any is left.
    fn spend(&self, what: impl Into<String>) -> Result<(), TursoError> {
        self.log.add(what);

        let mut failures = self
            .failures
            .lock()
            .expect("the failure budget is not poisoned");

        if *failures == 0 {
            return Ok(());
        }

        *failures -= 1;

        Err(TursoError::disconnected(
            "test: the operation never reached the server",
        ))
    }
}

#[async_trait]
impl TursoExecutor for FlakyExecutor {
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError> {
        self.spend(format!("execute {sql}"))?;
        self.inner.execute(sql, parameters).await
    }

    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError> {
        self.spend(format!("query {sql}"))?;
        self.inner.query(sql, parameters).await
    }

    async fn batch(&self, sql: &str) -> Result<(), TursoError> {
        self.spend(format!("batch {sql}"))?;
        self.inner.batch(sql).await
    }

    async fn begin_immediate(&self) -> Result<Box<dyn TursoTransaction>, TursoError> {
        self.spend("begin immediate")?;
        self.inner.begin_immediate().await
    }
}

/// An executor that loses the answer to the first statement matching a needle, after the server
/// has run it.
///
/// This is the failure a write that cannot be reissued from outside must recover from itself: the
/// statement arrived and did what it says, and the caller cannot tell that from a statement that
/// never arrived. It is lost once, so the reissue the store makes can be answered (spec §4.6).
pub struct LostStatementAnswerExecutor {
    inner: Arc<dyn TursoExecutor>,
    log: Arc<StatementLog>,
    /// The statement whose answer is lost, matched as a substring of the SQL.
    needle: String,
    lost: Mutex<bool>,
}

impl LostStatementAnswerExecutor {
    pub fn new(inner: Arc<dyn TursoExecutor>, log: Arc<StatementLog>, needle: &str) -> Self {
        Self {
            inner,
            log,
            needle: needle.to_string(),
            lost: Mutex::new(false),
        }
    }

    /// Whether this statement's answer is the one to lose, spending the loss.
    fn loses_answer(&self, sql: &str) -> bool {
        if !sql.contains(&self.needle) {
            return false;
        }

        let mut lost = self.lost.lock().expect("the loss is not poisoned");

        if *lost {
            return false;
        }

        *lost = true;
        true
    }
}

#[async_trait]
impl TursoExecutor for LostStatementAnswerExecutor {
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError> {
        self.log.add(format!("execute {sql}"));

        // The server runs it first: what is lost is the answer, not the statement.
        let changed = self.inner.execute(sql, parameters).await?;

        if self.loses_answer(sql) {
            return Err(TursoError::disconnected(
                "test: the answer to the statement was lost after the server ran it",
            ));
        }

        Ok(changed)
    }

    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError> {
        self.log.add(format!("query {sql}"));
        self.inner.query(sql, parameters).await
    }

    async fn batch(&self, sql: &str) -> Result<(), TursoError> {
        self.log.add(format!("batch {sql}"));
        self.inner.batch(sql).await
    }

    async fn begin_immediate(&self) -> Result<Box<dyn TursoTransaction>, TursoError> {
        self.log.add("begin immediate");
        self.inner.begin_immediate().await
    }
}
