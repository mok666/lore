// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The Turso side of the stores: the connection, the failures it can have, and the limits one
//! operation runs under.
//!
//! Turso is reached over HTTP with libSQL, and this crate keeps nothing open between operations:
//! the server closes an idle stream after ten seconds and the driver never reopens one by itself,
//! so every operation opens its own stream. A transaction is opened explicitly, must finish
//! within the server's five seconds, and is the only way two writes are ordered.
//!
//! The stores talk to [`TursoExecutor`] rather than to [`TursoClient`] so that a test can script
//! the answers, including the answers that never arrive.

pub mod error;
pub mod retry;
pub mod schema;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
pub use error::TursoError;
pub use error::TursoErrorKind;
pub use retry::TursoRetry;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// The longest one store operation may take, in milliseconds (spec §4.1).
pub const DEFAULT_TIMEOUT_MILLIS: u64 = 10_000;
/// How many statements may be in flight against Turso at once. The default matches the
/// configuration the plan gives the plugin.
pub const DEFAULT_MAX_CONNECTIONS: usize = 16;
/// The largest number of statements one transactional batch may hold.
pub const DEFAULT_MAX_TRANSACTION_STATEMENTS: usize = 64;

/// The parameters of one statement, in the order its numbered placeholders take them.
pub type TursoValues = Vec<libsql::Value>;

/// One row of a query, as its columns in order.
///
/// Rows are collected inside the driver's own types because a libSQL cursor is not `Send`, and the
/// stores' methods are.
pub type TursoRow = Vec<libsql::Value>;

/// One live transaction.
///
/// `commit` and `rollback` take the transaction by value because libSQL's own do: a transaction
/// that has been committed or rolled back is gone, and the type says so.
#[async_trait]
pub trait TursoTransaction: Send + Sync {
    /// Run a statement, returning how many rows it changed.
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError>;

    /// Run a query and collect every row it returns.
    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError>;

    /// Make what this transaction did visible to everyone else.
    async fn commit(self: Box<Self>) -> Result<(), TursoError>;

    /// Throw away what this transaction did.
    async fn rollback(self: Box<Self>) -> Result<(), TursoError>;
}

/// The Turso side of a store: statements, and transactions.
#[async_trait]
pub trait TursoExecutor: Send + Sync {
    /// Run a statement, returning how many rows it changed.
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError>;

    /// Run a query and collect every row it returns.
    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError>;

    /// Run `sql`, which may hold several statements, as one transaction.
    async fn batch(&self, sql: &str) -> Result<(), TursoError>;

    /// Open a transaction that holds the write lock from its first statement, so that a read and
    /// the write that depends on it cannot be interleaved by another writer.
    async fn begin_immediate(&self) -> Result<Box<dyn TursoTransaction>, TursoError>;
}

/// How long one store operation may take, and how often it may be reissued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TursoLimits {
    /// The deadline of the whole operation, retries included.
    pub timeout: Duration,
    /// The retry policy of the whole operation.
    pub retry: TursoRetry,
}

impl TursoLimits {
    pub const fn new(timeout: Duration, retry: TursoRetry) -> Self {
        Self { timeout, retry }
    }
}

impl Default for TursoLimits {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MILLIS),
            retry: TursoRetry::default(),
        }
    }
}

/// Everything the plugin's configuration says about reaching Turso and living within its limits.
///
/// The token lives here and nowhere else, and neither `Debug` nor this type's logs show it.
#[derive(Clone)]
pub struct TursoSettings {
    url: String,
    token: String,
    limits: TursoLimits,
    max_connections: usize,
    max_transaction_statements: usize,
}

impl TursoSettings {
    /// Settings for a database, with the plan's default limits.
    pub fn new(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: token.into(),
            limits: TursoLimits::default(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_transaction_statements: DEFAULT_MAX_TRANSACTION_STATEMENTS,
        }
    }

    pub fn with_limits(mut self, limits: TursoLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_max_transaction_statements(mut self, max_transaction_statements: usize) -> Self {
        self.max_transaction_statements = max_transaction_statements;
        self
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// The database's auth token. Callers pass it to the driver; nothing logs it.
    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn limits(&self) -> TursoLimits {
        self.limits
    }

    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    pub fn max_transaction_statements(&self) -> usize {
        self.max_transaction_statements
    }

    /// Reject settings that cannot work, naming the key the operator has to fix. The caller turns
    /// this into a configuration error, so the server stops at startup rather than at the first
    /// write.
    pub fn validate(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("url is empty".to_string());
        }

        // An empty token is not rejected here: a local libSQL server needs none, and a server
        // that does need one refuses the first statement with its own message. A deployment
        // against Turso cloud still stops at startup, because the plugin requires the key before
        // it gets this far.

        if self.limits.timeout.is_zero() {
            return Err("timeout_millis is 0, so no operation could finish".to_string());
        }

        if self.max_connections == 0 {
            return Err("max_connections is 0, so no operation could start".to_string());
        }

        // The schema is applied as one transactional batch, so the limit has to hold it.
        if self.max_transaction_statements < schema::DDL_STATEMENTS.len() {
            return Err(format!(
                "max_transaction_statements is {}, but creating the schema takes {} statements",
                self.max_transaction_statements,
                schema::DDL_STATEMENTS.len()
            ));
        }

        // Not an error: an operator may prefer a short deadline to a retry. It is worth saying out
        // loud, because in that case the retries are already out of time before they are used.
        if self.limits.retry.worst_case() >= self.limits.timeout {
            tracing::warn!(
                retry_worst_case_millis = self.limits.retry.worst_case().as_millis(),
                timeout_millis = self.limits.timeout.as_millis(),
                "retry_limit and retry_max_millis fill the whole timeout_millis, so the last \
                 retries will not be reached"
            );
        }

        Ok(())
    }
}

impl fmt::Debug for TursoSettings {
    /// Prints everything an operator needs to see and nothing an attacker does: the token is
    /// replaced, and so is any query string, which is where a Turso URL sometimes carries one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let url = self.url.split('?').next().unwrap_or(&self.url);

        f.debug_struct("TursoSettings")
            .field("url", &url)
            .field("token", &"<redacted>")
            .field("limits", &self.limits)
            .field("max_connections", &self.max_connections)
            .field(
                "max_transaction_statements",
                &self.max_transaction_statements,
            )
            .finish()
    }
}

/// A connection to a Turso database.
///
/// Connecting does not reach the network: the driver builds its HTTP connector and opens a stream
/// on the first statement. The first operation that needs the database is therefore where an
/// unreachable endpoint shows up, which is why the plugin creates the schema as it starts.
pub struct TursoClient {
    database: libsql::Database,
    limits: TursoLimits,
    /// Bounds how many statements are in flight at once. Every operation takes one for its whole
    /// life, including a transaction's, so a burst of writes queues instead of piling requests
    /// onto a database whose plan meters them.
    limiter: Arc<Semaphore>,
}

impl TursoClient {
    /// Prepare a client for `settings`. Returns an error only if the settings cannot describe a
    /// database at all; no request is made here.
    pub async fn connect(settings: &TursoSettings) -> Result<Self, TursoError> {
        settings
            .validate()
            .map_err(|message| TursoError::new(TursoErrorKind::Internal, message))?;

        let database =
            libsql::Builder::new_remote(settings.url().to_string(), settings.token().to_string())
                .build()
                .await
                .map_err(TursoError::from_libsql)?;

        tracing::debug!(settings = ?settings, "Connected to Turso");

        Ok(Self {
            database,
            limits: settings.limits(),
            limiter: Arc::new(Semaphore::new(settings.max_connections())),
        })
    }

    pub fn limits(&self) -> TursoLimits {
        self.limits
    }

    /// Create the schema if it is not there yet, and refuse to run against a version this build
    /// does not know (spec §4.1).
    ///
    /// Every statement is `IF NOT EXISTS`, so this is safe to call on every start and safe to
    /// reissue after a lost answer.
    pub async fn initialize_schema(&self) -> Result<(), TursoError> {
        retry::retry_within_deadline!(self.limits, async {
            self.batch(&schema::ddl()).await?;

            let rows = self
                .query(schema::SELECT_SCHEMA_VERSION, Vec::new())
                .await?;

            match rows.first().and_then(|row| row.first()) {
                None => {
                    // The ledger is empty, so this is the first start. `INSERT OR IGNORE` leaves a
                    // version another server wrote at the same moment alone.
                    self.execute(
                        schema::INSERT_SCHEMA_VERSION,
                        vec![libsql::Value::Integer(schema::SCHEMA_VERSION)],
                    )
                    .await?;
                    Ok(())
                }
                Some(libsql::Value::Integer(version)) if *version == schema::SCHEMA_VERSION => {
                    Ok(())
                }
                Some(other) => Err(TursoError::new(
                    TursoErrorKind::Internal,
                    format!(
                        "the database holds schema version {other:?}, but this build writes and \
                         understands version {}",
                        schema::SCHEMA_VERSION
                    ),
                )),
            }
        })
    }

    /// A new connection to the database. Synchronous, and no request is made until a statement
    /// runs on it.
    fn connection(&self) -> Result<libsql::Connection, TursoError> {
        self.database.connect().map_err(TursoError::from_libsql)
    }

    /// Wait for room to run one more statement.
    async fn permit(&self) -> Result<OwnedSemaphorePermit, TursoError> {
        self.limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|_closed| {
                TursoError::new(
                    TursoErrorKind::Internal,
                    "the connection limiter was closed",
                )
            })
    }
}

#[async_trait]
impl TursoExecutor for TursoClient {
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError> {
        let _permit = self.permit().await?;

        self.connection()?
            .execute(sql, parameters)
            .await
            .map_err(TursoError::from_libsql)
    }

    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError> {
        let _permit = self.permit().await?;

        let rows = self
            .connection()?
            .query(sql, parameters)
            .await
            .map_err(TursoError::from_libsql)?;

        collect_rows(rows).await
    }

    async fn batch(&self, sql: &str) -> Result<(), TursoError> {
        let _permit = self.permit().await?;

        self.connection()?
            .execute_transactional_batch(sql)
            .await
            .map_err(TursoError::from_libsql)?;

        Ok(())
    }

    async fn begin_immediate(&self) -> Result<Box<dyn TursoTransaction>, TursoError> {
        // Taken before the transaction opens and held until it is finished, so the limiter counts
        // transactions rather than statements.
        let permit = self.permit().await?;

        let transaction = self
            .connection()?
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(TursoError::from_libsql)?;

        Ok(Box::new(LibsqlTransaction {
            transaction: Some(transaction),
            _permit: permit,
        }))
    }
}

/// Collect a libSQL cursor, which is not `Send`, into rows that are.
async fn collect_rows(mut rows: libsql::Rows) -> Result<Vec<TursoRow>, TursoError> {
    let mut collected = Vec::new();

    while let Some(row) = rows.next().await.map_err(TursoError::from_libsql)? {
        let columns = usize::try_from(row.column_count()).unwrap_or(0);
        let mut values = Vec::with_capacity(columns);

        for index in 0..columns {
            values.push(
                row.get_value(index as i32)
                    .map_err(TursoError::from_libsql)?,
            );
        }

        collected.push(values);
    }

    Ok(collected)
}

/// A live libSQL transaction, holding a slot of the client's limiter for as long as it lives.
struct LibsqlTransaction {
    /// `None` only once the transaction has been committed or rolled back.
    transaction: Option<libsql::Transaction>,
    _permit: OwnedSemaphorePermit,
}

impl LibsqlTransaction {
    fn live(&self) -> Result<&libsql::Transaction, TursoError> {
        self.transaction.as_ref().ok_or_else(|| {
            TursoError::new(
                TursoErrorKind::Internal,
                "the transaction has already finished",
            )
        })
    }

    fn take(&mut self) -> Result<libsql::Transaction, TursoError> {
        self.transaction.take().ok_or_else(|| {
            TursoError::new(
                TursoErrorKind::Internal,
                "the transaction has already finished",
            )
        })
    }
}

#[async_trait]
impl TursoTransaction for LibsqlTransaction {
    async fn execute(&self, sql: &str, parameters: TursoValues) -> Result<u64, TursoError> {
        self.live()?
            .execute(sql, parameters)
            .await
            .map_err(TursoError::from_libsql)
    }

    async fn query(&self, sql: &str, parameters: TursoValues) -> Result<Vec<TursoRow>, TursoError> {
        let rows = self
            .live()?
            .query(sql, parameters)
            .await
            .map_err(TursoError::from_libsql)?;

        collect_rows(rows).await
    }

    async fn commit(self: Box<Self>) -> Result<(), TursoError> {
        // The transaction is taken out of the slot once, and the slot's permit is released with it.
        let mut slot = *self;

        slot.take()?.commit().await.map_err(TursoError::from_libsql)
    }

    async fn rollback(self: Box<Self>) -> Result<(), TursoError> {
        let mut slot = *self;

        slot.take()?
            .rollback()
            .await
            .map_err(TursoError::from_libsql)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn defaults_match_the_configuration_the_plan_gives() {
        let limits = TursoLimits::default();

        assert_eq!(limits.timeout, Duration::from_millis(10_000));
        assert_eq!(limits.retry.start_millis, 50);
        assert_eq!(limits.retry.max_millis, 1000);
        assert_eq!(limits.retry.limit, 3);
        assert_eq!(DEFAULT_MAX_CONNECTIONS, 16);
        assert_eq!(DEFAULT_MAX_TRANSACTION_STATEMENTS, 64);
    }

    #[test]
    fn the_token_never_reaches_debug() {
        let settings = TursoSettings::new("libsql://example.turso.io", "s3cret");

        let printed = format!("{settings:?}");

        assert!(printed.contains("libsql://example.turso.io"));
        assert!(printed.contains("<redacted>"));
        assert!(!printed.contains("s3cret"));
    }

    /// A Turso URL may carry the token in its query string, which must not reach a log either.
    #[test]
    fn a_query_string_in_the_url_never_reaches_debug() {
        let settings = TursoSettings::new("libsql://example.turso.io?authToken=s3cret", "s3cret");

        let printed = format!("{settings:?}");

        assert!(printed.contains("libsql://example.turso.io"));
        assert!(!printed.contains("s3cret"));
    }

    #[test]
    fn settings_have_to_fit_the_schema_and_the_limiter() {
        let settings = TursoSettings::new("libsql://example.turso.io", "token");
        assert!(settings.validate().is_ok());

        assert!(settings.clone().with_max_connections(0).validate().is_err());
        assert!(
            settings
                .clone()
                .with_max_transaction_statements(schema::DDL_STATEMENTS.len() - 1)
                .validate()
                .is_err()
        );
        assert!(
            settings
                .clone()
                .with_limits(TursoLimits::new(Duration::ZERO, TursoRetry::default()))
                .validate()
                .is_err()
        );
        assert!(TursoSettings::new("", "token").validate().is_err());

        // A token is not this type's business: a local server needs none, and the plugin is what
        // requires the key for a deployment that does.
        assert!(
            TursoSettings::new("libsql://example.turso.io", "")
                .validate()
                .is_ok()
        );
    }
}
