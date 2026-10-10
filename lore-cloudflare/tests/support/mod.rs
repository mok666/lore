// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! What the integration tests need that the crate itself does not provide: a server to run
//! against, and an executor that can lose an answer on purpose.
//!
//! # Running them
//!
//! The tests reach a local libSQL server, which needs no token, so nothing secret is involved and
//! the same commands work on a laptop and in CI. The server the plan names is
//! `ghcr.io/tursodatabase/libsql-server:v0.24.32`:
//!
//! ```text
//! docker network create lore-test
//! docker run -d --name lore-sqld --network lore-test --network-alias sqld \
//!   -v lore-sqld-data:/var/lib/sqld ghcr.io/tursodatabase/libsql-server:v0.24.32
//! docker run --rm --network lore-test -v "$PWD:/work" -v lore-cargo-registry:/usr/local/cargo/registry \
//!   -v lore-target:/work/target -w /work -e LORE_TURSO_URL=http://sqld:8080 \
//!   rust:1.97.1 cargo test -p lore-cloudflare --tests
//! ```
//!
//! Without `LORE_TURSO_URL` every test in these files says it did not run and returns, so that a
//! checkout without a server still has a green `cargo test`.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use async_trait::async_trait;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_cloudflare::CloudflareLockStore;
use lore_cloudflare::CloudflareMutableStore;
use lore_cloudflare::TursoClient;
use lore_cloudflare::TursoError;
use lore_cloudflare::TursoErrorKind;
use lore_cloudflare::TursoExecutor;
use lore_cloudflare::TursoLimits;
use lore_cloudflare::TursoSettings;
use lore_cloudflare::TursoTransaction;
use lore_cloudflare::turso::TursoRow;
use lore_cloudflare::turso::TursoValues;
use tokio::sync::OnceCell;

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
        self.log.add("begin immediate");
        let inner = self.inner.begin_immediate().await?;

        Ok(Box::new(LostCommitTransaction {
            inner: Some(inner),
            log: self.log.clone(),
        }))
    }
}

/// A transaction whose `COMMIT` reaches the server and whose answer does not come back.
struct LostCommitTransaction {
    inner: Option<Box<dyn TursoTransaction>>,
    log: Arc<StatementLog>,
}

impl LostCommitTransaction {
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
impl TursoTransaction for LostCommitTransaction {
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

        match slot.take()?.commit().await {
            // The server applied it and the client never learned that it did.
            Ok(()) => Err(TursoError::disconnected(
                "test: the answer to the commit was lost after the server applied it",
            )),
            Err(error) => Err(error),
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
