// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Cloudflare store plugin factories.
//!
//! This module provides plugin factories for Cloudflare-backed stores:
//! - [`CloudflareImmutableStorePluginFactory`] - Creates an R2/Turso-backed immutable store
//! - [`CloudflareMutableStorePluginFactory`] - Creates a Turso-backed mutable store
//! - [`CloudflareLockStorePluginFactory`] - Creates a Turso-backed lock store
//!
//! All three read the same configuration, one struct shared by every store the plugin provides, so
//! `[plugins.cloudflare]` can hold the settings of all of them and `#[serde(deny_unknown_fields)]`
//! can reject a key that no store reads (spec §2.2). A store that needs a value of its own writes
//! it in its own section, `[plugins.cloudflare.<store>]`, which the configuration layer merges over
//! the parent before the plugin sees it.
//!
//! The immutable store is the only one that reads the `s3_*` and `r2_*` keys: the other two never
//! touch R2, so a deployment that configures only those never needs R2 credentials (spec §2.2).

use std::sync::Arc;
use std::time::Duration;

use lore_aws::clients::HttpClientSettings;
use lore_base::error::PluginConfigError;
use lore_base::error::PluginInitError;
use lore_base::runtime::runtime;
use lore_cloudflare::CloudflareImmutableStore;
use lore_cloudflare::CloudflareImmutableStoreSettings;
use lore_cloudflare::CloudflareLockStore;
use lore_cloudflare::CloudflareMutableStore;
use lore_cloudflare::R2Credentials;
use lore_cloudflare::R2Settings;
use lore_cloudflare::TursoClient;
use lore_cloudflare::TursoExecutor;
use lore_cloudflare::TursoLimits;
use lore_cloudflare::TursoSettings;
use lore_cloudflare::connect_r2;
use lore_cloudflare::store::r2::DEFAULT_SLOW_OPERATION_THRESHOLD_MILLIS;
use lore_cloudflare::turso::DEFAULT_MAX_CONNECTIONS;
use lore_cloudflare::turso::DEFAULT_MAX_TRANSACTION_STATEMENTS;
use lore_cloudflare::turso::DEFAULT_TIMEOUT_MILLIS;
use lore_cloudflare::turso::TursoRetry;
use lore_revision::lock::LockStore;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use serde::Deserialize;
use tracing::info;

use crate::plugins::ImmutableStorePluginFactory;
use crate::plugins::LockStorePluginFactory;
use crate::plugins::MutableStorePluginFactory;
use crate::plugins::PluginError;
use crate::plugins::PluginRegistry;

#[lore_macro::test_pub]
const PLUGIN_NAME: &str = "cloudflare";

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for the Cloudflare plugin.
///
/// Read from `[plugins.cloudflare]` and shared by every store the plugin provides. A store may
/// override single keys in its own section, which the configuration layer merges over this one
/// before the plugin sees it.
///
/// The keys and the token are secrets' addresses, not secrets: a value given here ends up in a
/// file that is read by whoever can read the server's configuration, so the environment is where
/// they belong, and the error for a missing one names both the key and the variable.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudflarePluginConfig {
    /// The Turso database url (`libsql://…` or `http://…`).
    #[serde(default)]
    pub turso_url: Option<String>,

    /// The Turso auth token. Read from `LORE_TURSO_AUTH_TOKEN` unless it is given here.
    #[serde(default)]
    pub turso_auth_token: Option<String>,

    /// The R2 bucket the immutable store's payloads live in.
    ///
    /// Not a secret — the name is in every request that reaches the bucket and in the url of
    /// every object in it — but it still has an environment variable, so that a deployment can be
    /// pointed at a bucket by name without a copy of its configuration file per environment.
    /// Whatever it resolves to is logged at startup.
    #[serde(default)]
    pub s3_bucket: Option<String>,

    /// The R2 endpoint (`https://<account>.r2.cloudflarestorage.com`). Read from
    /// `LORE_R2_ENDPOINT_URL` unless it is given here.
    #[serde(default)]
    pub s3_endpoint_url: Option<String>,

    /// The R2 region. Required by the signature only; R2 answers to `auto`.
    #[serde(default)]
    pub s3_region: Option<String>,

    /// Address the bucket in the path rather than in the hostname, which is what R2 wants.
    #[serde(default = "default_force_path_style")]
    pub s3_force_path_style: bool,

    /// Every R2 request that takes longer than this is logged with its elapsed time.
    #[serde(default = "default_r2_slow_operation_threshold_millis")]
    pub r2_slow_operation_threshold_millis: u64,

    /// The R2 access key id. Read from `LORE_R2_ACCESS_KEY_ID` unless it is given here.
    #[serde(default)]
    pub r2_access_key_id: Option<String>,

    /// The R2 secret access key. Read from `LORE_R2_SECRET_ACCESS_KEY` unless it is given here.
    #[serde(default)]
    pub r2_secret_access_key: Option<String>,

    /// The HTTP pool the R2 requests share, under the names the AWS plugin gives them, so a
    /// deployment that already tuned them keeps its tuning.
    #[serde(default)]
    pub http: HttpClientSettings,

    /// Upload every put instead of asking first what is already stored. Read only by the immutable
    /// store.
    #[serde(default)]
    pub force_write: bool,

    /// The longest one store operation may take.
    ///
    /// One value for both services, because a store operation that spans them is bounded by the
    /// slower of the two rather than by either alone. A store whose two deadlines have to differ
    /// overrides this in its own section, where the override reaches only that store's clients.
    #[serde(default = "default_timeout_millis")]
    pub timeout_millis: u64,

    /// How many statements may be in flight against Turso at once.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// The largest number of statements one transactional batch may hold.
    #[serde(default = "default_max_transaction_statements")]
    pub max_transaction_statements: usize,

    /// Interval before the first retry of a lost answer.
    #[serde(default = "default_retry_start_millis")]
    pub retry_start_millis: u64,

    /// Largest interval between retries.
    #[serde(default = "default_retry_max_millis")]
    pub retry_max_millis: u64,

    /// How many waits a retried operation is allowed, so it runs at most `retry_limit + 1` times.
    #[serde(default = "default_retry_limit")]
    pub retry_limit: usize,
}

fn default_timeout_millis() -> u64 {
    DEFAULT_TIMEOUT_MILLIS
}

fn default_force_path_style() -> bool {
    // Both R2 and `MinIO` answer to a bucket in the path, so this is the addressing that works
    // everywhere this plugin is pointed at.
    true
}

fn default_r2_slow_operation_threshold_millis() -> u64 {
    DEFAULT_SLOW_OPERATION_THRESHOLD_MILLIS
}

fn default_max_connections() -> usize {
    DEFAULT_MAX_CONNECTIONS
}

fn default_max_transaction_statements() -> usize {
    DEFAULT_MAX_TRANSACTION_STATEMENTS
}

fn default_retry_start_millis() -> u64 {
    TursoRetry::default().start_millis
}

fn default_retry_max_millis() -> u64 {
    TursoRetry::default().max_millis
}

fn default_retry_limit() -> usize {
    TursoRetry::default().limit
}

impl CloudflarePluginConfig {
    /// The settings the stores are built from, with the environment filling in what the
    /// configuration leaves out.
    pub fn resolve(&self, plugin_name: &str) -> Result<TursoSettings, PluginError> {
        self.resolve_with(plugin_name, &|name| std::env::var(name).ok())
    }

    /// The same, reading the environment through `environment`.
    ///
    /// The plugin always passes the process environment; a test passes what it wants the
    /// environment to hold, so that whether `LORE_TURSO_URL` happens to be set cannot decide what
    /// a test asserts.
    pub fn resolve_with(
        &self,
        plugin_name: &str,
        environment: &dyn Fn(&str) -> Option<String>,
    ) -> Result<TursoSettings, PluginError> {
        let url = required(
            plugin_name,
            "turso_url",
            "LORE_TURSO_URL",
            self.turso_url.as_deref(),
            environment,
        )?;

        let token = required(
            plugin_name,
            "turso_auth_token",
            "LORE_TURSO_AUTH_TOKEN",
            self.turso_auth_token.as_deref(),
            environment,
        )?;

        let settings = TursoSettings::new(url, token)
            .with_limits(TursoLimits::new(
                Duration::from_millis(self.timeout_millis),
                TursoRetry::new(
                    self.retry_start_millis,
                    self.retry_max_millis,
                    self.retry_limit,
                ),
            ))
            .with_max_connections(self.max_connections)
            .with_max_transaction_statements(self.max_transaction_statements);

        // The message names the key that is wrong and never the value that is in it.
        settings.validate().map_err(|message| {
            PluginError::from(PluginConfigError {
                plugin_name: plugin_name.to_string(),
                message,
            })
        })?;

        Ok(settings)
    }

    /// The R2 settings the immutable store is built from, with the environment filling in what
    /// the configuration leaves out.
    pub fn resolve_r2(&self, plugin_name: &str) -> Result<R2Settings, PluginError> {
        self.resolve_r2_with(plugin_name, &|name| std::env::var(name).ok())
    }

    /// The same, reading the environment through `environment`.
    pub fn resolve_r2_with(
        &self,
        plugin_name: &str,
        environment: &dyn Fn(&str) -> Option<String>,
    ) -> Result<R2Settings, PluginError> {
        let bucket = required(
            plugin_name,
            "s3_bucket",
            "LORE_R2_BUCKET",
            self.s3_bucket.as_deref(),
            environment,
        )?;

        // Required, and not merely optional as it is for the AWS plugin: an S3 client with no
        // endpoint goes to S3 itself, so a deployment that meant R2 and forgot this line would
        // spend its R2 keys against another service.
        let endpoint = required(
            plugin_name,
            "s3_endpoint_url",
            "LORE_R2_ENDPOINT_URL",
            self.s3_endpoint_url.as_deref(),
            environment,
        )?;

        let access_key_id = required(
            plugin_name,
            "r2_access_key_id",
            "LORE_R2_ACCESS_KEY_ID",
            self.r2_access_key_id.as_deref(),
            environment,
        )?;

        let secret_access_key = required(
            plugin_name,
            "r2_secret_access_key",
            "LORE_R2_SECRET_ACCESS_KEY",
            self.r2_secret_access_key.as_deref(),
            environment,
        )?;

        let settings =
            R2Settings::new(bucket, R2Credentials::new(access_key_id, secret_access_key))
                .with_endpoint_url(Some(endpoint))
                .with_region(self.s3_region.clone())
                .with_force_path_style(self.s3_force_path_style)
                .with_slow_operation_threshold_millis(self.r2_slow_operation_threshold_millis)
                .with_timeout_millis(self.timeout_millis)
                .with_http_settings(self.http.clone());

        settings.validate().map_err(|message| {
            PluginError::from(PluginConfigError {
                plugin_name: plugin_name.to_string(),
                message,
            })
        })?;

        Ok(settings)
    }
}

/// A key that has to be there, from the configuration or from the environment.
///
/// The message names both places it could have been and never the value it holds: a value in the
/// log of a server that failed to start is a secret in a log.
fn required(
    plugin_name: &str,
    key: &str,
    variable: &str,
    configured: Option<&str>,
    environment: &dyn Fn(&str) -> Option<String>,
) -> Result<String, PluginError> {
    if let Some(value) = configured.filter(|value| !value.trim().is_empty()) {
        return Ok(value.to_string());
    }

    if let Some(value) = environment(variable).filter(|value| !value.trim().is_empty()) {
        return Ok(value);
    }

    Err(PluginError::from(PluginConfigError {
        plugin_name: plugin_name.to_string(),
        message: format!(
            "the configuration has no usable `{key}` in [plugins.{plugin_name}] and the \
             {variable} environment variable is not set either"
        ),
    }))
}

/// The configuration of one section, or the error that says which key is wrong.
fn deserialize(
    plugin_name: &str,
    config: &toml::Value,
) -> Result<CloudflarePluginConfig, PluginError> {
    config.clone().try_into().map_err(|e| {
        PluginError::from(PluginConfigError {
            plugin_name: plugin_name.to_string(),
            message: format!("Failed to deserialize Cloudflare plugin config: {e}"),
        })
    })
}

/// The error for a store that could not be built.
fn init_error(plugin_name: &str, what: &str, error: impl std::fmt::Display) -> PluginError {
    PluginError::from(PluginInitError {
        plugin_name: plugin_name.to_string(),
        message: format!("Failed to create {what}: {error}"),
    })
}

// =============================================================================
// Plugin Factory Implementations
// =============================================================================

/// Plugin factory for creating R2/Turso-backed immutable stores.
///
/// The payloads live in R2 and the tables that say which partitions hold which hashes live in the
/// same Turso database the mutable store and the lock store use, so the three stores share a
/// backup and a quota.
pub struct CloudflareImmutableStorePluginFactory;

impl ImmutableStorePluginFactory for CloudflareImmutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        // Deserialize and resolve both halves without reaching either service. A missing bucket,
        // endpoint or key, or a limit that cannot hold the schema, stops the server here.
        let config = deserialize(plugin_name, config)?;
        let _turso = config.resolve(plugin_name)?;
        let r2 = config.resolve_r2(plugin_name)?;

        // One line for the part of the resolution that is not a secret, so that a bucket that is
        // not the one meant is visible at startup rather than at the first write.
        info!(
            plugin_name,
            bucket = r2.bucket(),
            "Validated the Cloudflare immutable store config"
        );

        Ok(())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn ImmutableStore>, PluginError> {
        let plugin_name = self.name();

        let config = deserialize(plugin_name, config)?;
        let turso = config.resolve(plugin_name)?;
        let r2 = config.resolve_r2(plugin_name)?;

        // The debug print replaces the keys, and any query string in the endpoint, with a
        // placeholder.
        info!(
            plugin_name,
            settings = ?r2,
            "Creating the Cloudflare immutable store"
        );

        // Plugin construction is a synchronous trait method. It runs once at startup, one plugin
        // at a time, so at most one core is handed off at a time.
        #[allow(clippy::disallowed_methods)]
        let store = tokio::task::block_in_place(|| {
            runtime().block_on(Box::pin(async {
                let client = TursoClient::connect(&turso)
                    .await
                    .map_err(|error| init_error(plugin_name, "the Turso client", error))?;

                client
                    .initialize_schema()
                    .await
                    .map_err(|error| init_error(plugin_name, "the Turso schema", error))?;

                let executor: Arc<dyn TursoExecutor> = Arc::new(client);

                // Building the client asks the bucket whether it is there, so a bucket that cannot
                // be reached, or keys that are not allowed to read it, stop the server at startup
                // rather than at the first write. Boxed because the client it builds is large
                // enough that clippy counts the future against the caller's stack.
                let r2_client = Box::pin(connect_r2(&r2))
                    .await
                    .map_err(|error| init_error(plugin_name, "the R2 client", error))?;

                Ok::<_, PluginError>(CloudflareImmutableStore::new(
                    r2_client,
                    executor,
                    turso.limits(),
                    &CloudflareImmutableStoreSettings::new(config.force_write),
                ))
            }))
        })?;

        Ok(Arc::new(store))
    }
}

/// Plugin factory for creating Turso-backed mutable stores.
pub struct CloudflareMutableStorePluginFactory;

impl MutableStorePluginFactory for CloudflareMutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        // Deserialize and resolve the configuration without reaching Turso. A missing url, a
        // missing token or a limit that cannot hold the schema stops the server here.
        let _settings = deserialize(plugin_name, config)?.resolve(plugin_name)?;

        Ok(())
    }

    fn create(
        &self,
        config: &toml::Value,
        _immutable_store: Arc<dyn ImmutableStore>,
    ) -> Result<Arc<dyn MutableStore>, PluginError> {
        let plugin_name = self.name();

        let settings = deserialize(plugin_name, config)?.resolve(plugin_name)?;

        // The debug print replaces the token, and any query string in the url, with a placeholder.
        info!(
            plugin_name,
            settings = ?settings,
            "Creating the Cloudflare mutable store"
        );

        // Plugin construction is a synchronous trait method. It runs once at startup, one plugin
        // at a time, so at most one core is handed off at a time.
        #[allow(clippy::disallowed_methods)]
        let store = tokio::task::block_in_place(|| {
            runtime().block_on(Box::pin(async {
                let client = TursoClient::connect(&settings)
                    .await
                    .map_err(|error| init_error(plugin_name, "the Turso client", error))?;

                // Creating the schema is also the first request, so a database that cannot be
                // reached stops the server at startup rather than at the first write.
                client
                    .initialize_schema()
                    .await
                    .map_err(|error| init_error(plugin_name, "the Turso schema", error))?;

                let executor: Arc<dyn TursoExecutor> = Arc::new(client);

                Ok::<_, PluginError>(CloudflareMutableStore::new(executor, settings.limits()))
            }))
        })?;

        Ok(Arc::new(store))
    }
}

/// Plugin factory for creating Turso-backed lock stores.
pub struct CloudflareLockStorePluginFactory;

impl LockStorePluginFactory for CloudflareLockStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        let _settings = deserialize(plugin_name, config)?.resolve(plugin_name)?;

        Ok(())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn LockStore>, PluginError> {
        let plugin_name = self.name();

        let settings = deserialize(plugin_name, config)?.resolve(plugin_name)?;

        info!(
            plugin_name,
            settings = ?settings,
            "Creating the Cloudflare lock store"
        );

        // Plugin construction is a synchronous trait method. It runs once at startup, one plugin
        // at a time, so at most one core is handed off at a time.
        #[allow(clippy::disallowed_methods)]
        let store = tokio::task::block_in_place(|| {
            runtime().block_on(Box::pin(async {
                let client = TursoClient::connect(&settings)
                    .await
                    .map_err(|error| init_error(plugin_name, "the Turso client", error))?;

                client
                    .initialize_schema()
                    .await
                    .map_err(|error| init_error(plugin_name, "the Turso schema", error))?;

                let executor: Arc<dyn TursoExecutor> = Arc::new(client);

                Ok::<_, PluginError>(CloudflareLockStore::new(executor, settings.limits()))
            }))
        })?;

        Ok(Arc::new(store))
    }
}

// =============================================================================
// Registration
// =============================================================================

/// Registers the Cloudflare plugin factories with the given registry.
///
/// All three stores live in the same Turso database, so a deployment that configures all of them
/// holds one client for each: the two sections may differ, and each store keeps its own connection
/// limit. Only the immutable store reads the `s3_*` and `r2_*` keys.
pub fn register(registry: &mut PluginRegistry) {
    registry.register_immutable_store_plugin(Box::new(CloudflareImmutableStorePluginFactory));
    registry.register_mutable_store_plugin(Box::new(CloudflareMutableStorePluginFactory));
    registry.register_lock_store_plugin(Box::new(CloudflareLockStorePluginFactory));
}
