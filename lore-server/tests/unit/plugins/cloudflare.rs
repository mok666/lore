// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_cloudflare::R2Settings;
use lore_cloudflare::TursoRetry;
use lore_cloudflare::turso::DEFAULT_MAX_CONNECTIONS;
use lore_cloudflare::turso::DEFAULT_MAX_TRANSACTION_STATEMENTS;
use lore_cloudflare::turso::DEFAULT_TIMEOUT_MILLIS;
use lore_server::plugins::ImmutableStorePluginFactory;
use lore_server::plugins::LockStorePluginFactory;
use lore_server::plugins::MutableStorePluginFactory;
use lore_server::plugins::PluginRegistry;
use lore_server::plugins::cloudflare::*;
use tokio::runtime::Handle;

/// The environment a test pretends the process has. A test that read the real one would pass or
/// fail depending on what is set where it runs.
fn no_environment(_name: &str) -> Option<String> {
    None
}

/// An environment holding one variable.
fn only(variable: &'static str, value: &'static str) -> impl Fn(&str) -> Option<String> {
    move |name| (name == variable).then(|| value.to_string())
}

#[test]
fn test_mutable_store_factory_name() {
    let factory = CloudflareMutableStorePluginFactory;
    assert_eq!(factory.name(), PLUGIN_NAME);
}

#[test]
fn test_immutable_store_factory_name() {
    let factory = CloudflareImmutableStorePluginFactory;
    assert_eq!(factory.name(), PLUGIN_NAME);
}

#[test]
fn test_lock_store_factory_name() {
    let factory = CloudflareLockStorePluginFactory;
    assert_eq!(factory.name(), PLUGIN_NAME);
}

#[tokio::test]
async fn test_register_adds_every_store() {
    let mut registry = PluginRegistry::new();
    register(&mut registry);

    let immutable = registry.list_immutable_store_plugins();
    assert!(
        immutable.contains(&PLUGIN_NAME.to_string()),
        "Expected '{PLUGIN_NAME}' in immutable store plugins, found: {immutable:?}"
    );

    let mutable = registry.list_mutable_store_plugins();
    assert!(
        mutable.contains(&PLUGIN_NAME.to_string()),
        "Expected '{PLUGIN_NAME}' in mutable store plugins, found: {mutable:?}"
    );

    let locks = registry.list_lock_store_plugins();
    assert!(
        locks.contains(&PLUGIN_NAME.to_string()),
        "Expected '{PLUGIN_NAME}' in lock store plugins, found: {locks:?}"
    );

    // The plugin does not register a resource detector.
    assert_eq!(registry.resource_detectors(Handle::current()).len(), 0);
}

/// A key no store reads is refused rather than ignored, so a typo in a deployment's configuration
/// stops the server instead of quietly leaving a store unconfigured.
#[test]
fn test_an_unknown_key_is_refused() {
    // A key of another plugin's configuration, which no Cloudflare store reads.
    let config: toml::Value = toml::from_str(r#"dynamodb_table = "table""#).unwrap();

    for error in [
        CloudflareImmutableStorePluginFactory.validate_config(&config),
        CloudflareMutableStorePluginFactory.validate_config(&config),
        CloudflareLockStorePluginFactory.validate_config(&config),
    ] {
        let error = error.expect_err("an unknown key should fail");
        let config_error = error
            .as_plugin_config_error()
            .expect("should be PluginConfigError");

        assert_eq!(config_error.plugin_name, PLUGIN_NAME);
        assert!(
            config_error
                .message
                .contains("Failed to deserialize Cloudflare plugin config"),
            "got: {}",
            config_error.message
        );
        assert!(
            config_error.message.contains("dynamodb_table"),
            "the message names the key that is wrong, got: {}",
            config_error.message
        );
    }
}

/// A configuration with no url says which key is missing and where else it could have been.
#[test]
fn test_a_missing_url_names_the_key_and_the_variable() {
    let config: CloudflarePluginConfig = toml::from_str("").unwrap();

    let error = config
        .resolve_with(PLUGIN_NAME, &no_environment)
        .expect_err("a url is required");

    let config_error = error
        .as_plugin_config_error()
        .expect("should be PluginConfigError");

    assert_eq!(config_error.plugin_name, PLUGIN_NAME);
    assert!(
        config_error.message.contains("turso_url"),
        "got: {}",
        config_error.message
    );
    assert!(
        config_error.message.contains("LORE_TURSO_URL"),
        "got: {}",
        config_error.message
    );
}

/// The environment supplies what the configuration leaves out, and the configuration wins where
/// both say something.
#[test]
fn test_the_configuration_wins_over_the_environment() {
    let config: CloudflarePluginConfig = toml::from_str(
        r#"
        turso_url = "libsql://configured.turso.io"
        "#,
    )
    .unwrap();

    let settings = config
        .resolve_with(PLUGIN_NAME, &only("LORE_TURSO_AUTH_TOKEN", "token"))
        .expect("the url comes from the configuration and the token from the environment");

    assert_eq!(settings.url(), "libsql://configured.turso.io");
    assert_eq!(settings.token(), "token");

    let settings = config
        .resolve_with(PLUGIN_NAME, &|name| match name {
            "LORE_TURSO_URL" => Some("libsql://from-the-environment.turso.io".to_string()),
            "LORE_TURSO_AUTH_TOKEN" => Some("token".to_string()),
            _ => None,
        })
        .expect("the token still comes from the environment");

    assert_eq!(
        settings.url(),
        "libsql://configured.turso.io",
        "a value in the configuration is not overridden by the environment"
    );

    // An empty value is not a value: a variable that is set to nothing is as good as unset.
    let error = config
        .resolve_with(PLUGIN_NAME, &only("LORE_TURSO_AUTH_TOKEN", "  "))
        .expect_err("an empty token is not a token");

    assert!(
        error
            .as_plugin_config_error()
            .expect("should be PluginConfigError")
            .message
            .contains("turso_auth_token"),
        "got: {error:?}"
    );
}

/// A url and an auth token are both required, even though the settings themselves accept an empty
/// token: a local server needs none, and a deployment against Turso cloud has to stop at startup
/// rather than at its first write.
#[test]
fn test_an_auth_token_is_required_even_though_a_local_server_needs_none() {
    let config: CloudflarePluginConfig =
        toml::from_str(r#"turso_url = "http://localhost:8080""#).unwrap();

    let error = config
        .resolve_with(PLUGIN_NAME, &no_environment)
        .expect_err("the token key is required");

    let message = error
        .as_plugin_config_error()
        .expect("should be PluginConfigError")
        .message
        .clone();

    assert!(message.contains("turso_auth_token"), "got: {message}");
    assert!(message.contains("LORE_TURSO_AUTH_TOKEN"), "got: {message}");

    // A key that is there and empty is not a value either.
    let error = config
        .resolve_with(PLUGIN_NAME, &only("LORE_TURSO_AUTH_TOKEN", ""))
        .expect_err("an empty token is not a token");

    assert!(
        error
            .as_plugin_config_error()
            .expect("should be PluginConfigError")
            .message
            .contains("turso_auth_token"),
        "got: {error:?}"
    );
}

/// The keys nobody sets hold the plan's defaults.
#[test]
fn test_a_configuration_without_limits_holds_the_defaults() {
    let config: CloudflarePluginConfig = toml::from_str("").unwrap();

    assert_eq!(config.timeout_millis, DEFAULT_TIMEOUT_MILLIS);
    assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
    assert_eq!(
        config.max_transaction_statements,
        DEFAULT_MAX_TRANSACTION_STATEMENTS
    );
    assert_eq!(
        config.retry_start_millis,
        TursoRetry::default().start_millis
    );
    assert_eq!(config.retry_max_millis, TursoRetry::default().max_millis);
    assert_eq!(config.retry_limit, TursoRetry::default().limit);

    let settings = config
        .resolve_with(PLUGIN_NAME, &|name| match name {
            "LORE_TURSO_URL" => Some("http://localhost:8080".to_string()),
            "LORE_TURSO_AUTH_TOKEN" => Some("token".to_string()),
            _ => None,
        })
        .expect("every required key is supplied");

    assert_eq!(settings.limits().timeout.as_millis(), 10_000);
    assert_eq!(settings.limits().retry.limit, 3);
    assert_eq!(settings.max_connections(), DEFAULT_MAX_CONNECTIONS);
    assert_eq!(
        settings.max_transaction_statements(),
        DEFAULT_MAX_TRANSACTION_STATEMENTS
    );
}

/// The limits a configuration gives are the ones the stores are built with.
#[test]
fn test_the_limits_of_a_configuration_are_the_ones_the_stores_get() {
    let config: CloudflarePluginConfig = toml::from_str(
        r#"
        timeout_millis = 2500
        max_connections = 4
        max_transaction_statements = 16
        retry_start_millis = 10
        retry_max_millis = 100
        retry_limit = 5
        "#,
    )
    .unwrap();

    let settings = config
        .resolve_with(PLUGIN_NAME, &|name| match name {
            "LORE_TURSO_URL" => Some("http://localhost:8080".to_string()),
            "LORE_TURSO_AUTH_TOKEN" => Some("token".to_string()),
            _ => None,
        })
        .expect("every required key is supplied");

    assert_eq!(settings.limits().timeout.as_millis(), 2500);
    assert_eq!(settings.limits().retry.start_millis, 10);
    assert_eq!(settings.limits().retry.max_millis, 100);
    assert_eq!(settings.limits().retry.limit, 5);
    assert_eq!(settings.max_connections(), 4);
    assert_eq!(settings.max_transaction_statements(), 16);
}

/// A limit that cannot work stops the server at startup, and the message names the key rather than
/// the value.
#[test]
fn test_a_limit_that_cannot_hold_the_schema_is_refused() {
    let environment = |name: &str| match name {
        "LORE_TURSO_URL" => Some("http://localhost:8080".to_string()),
        "LORE_TURSO_AUTH_TOKEN" => Some("token".to_string()),
        _ => None,
    };

    let config: CloudflarePluginConfig = toml::from_str("max_connections = 0").unwrap();

    let error = config
        .resolve_with(PLUGIN_NAME, &environment)
        .expect_err("no operation could start with no connections");

    let config_error = error
        .as_plugin_config_error()
        .expect("should be PluginConfigError");

    assert_eq!(config_error.plugin_name, PLUGIN_NAME);
    assert!(
        config_error.message.contains("max_connections"),
        "got: {}",
        config_error.message
    );
}

/// The settings the plugin logs do not hold the token.
#[test]
fn test_the_settings_the_plugin_logs_do_not_hold_the_token() {
    let config: CloudflarePluginConfig = toml::from_str(
        r#"
        turso_url = "libsql://configured.turso.io?authToken=in-the-url"
        "#,
    )
    .unwrap();

    let settings = config
        .resolve_with(PLUGIN_NAME, &only("LORE_TURSO_AUTH_TOKEN", "s3cret"))
        .expect("both keys are supplied");

    let logged = format!("{settings:?}");

    assert!(
        !logged.contains("s3cret"),
        "the token is not in the log, got: {logged}"
    );
    assert!(
        !logged.contains("in-the-url"),
        "a token in the url's query string is not in the log either, got: {logged}"
    );
    assert!(
        logged.contains("configured.turso.io"),
        "the host is still logged, so an operator can see which database it is, got: {logged}"
    );
}

/// `validate_config` resolves both halves without reaching either service, so a complete
/// configuration is accepted with nothing listening where it points.
#[test]
fn test_the_immutable_config_is_validated_without_reaching_a_service() {
    let config: toml::Value = toml::from_str(
        r#"
        turso_url = "http://127.0.0.1:1"
        turso_auth_token = "token"
        s3_bucket = "bucket"
        s3_endpoint_url = "http://127.0.0.1:1"
        r2_access_key_id = "id"
        r2_secret_access_key = "secret"
        "#,
    )
    .unwrap();

    CloudflareImmutableStorePluginFactory
        .validate_config(&config)
        .expect("nothing is reached until the store is created");
}

/// The R2 keys are required as well, and each error names the key and where else it could have
/// been.
#[test]
fn test_the_r2_keys_are_required() {
    // The bucket has an environment variable of its own, because it is the one key that differs
    // between a rehearsal and the real thing.
    let error = resolve_r2("")
        .expect_err("a bucket is required")
        .as_plugin_config_error()
        .expect("should be PluginConfigError")
        .message
        .clone();

    assert!(error.contains("s3_bucket"), "got: {error}");
    assert!(error.contains("LORE_R2_BUCKET"), "got: {error}");

    let error = resolve_r2(r#"s3_bucket = "bucket""#)
        .expect_err("an endpoint is required")
        .as_plugin_config_error()
        .expect("should be PluginConfigError")
        .message
        .clone();

    assert!(error.contains("s3_endpoint_url"), "got: {error}");
    assert!(error.contains("LORE_R2_ENDPOINT_URL"), "got: {error}");

    let error = resolve_r2(
        r#"
        s3_bucket = "bucket"
        s3_endpoint_url = "https://account.r2.cloudflarestorage.com"
        "#,
    )
    .expect_err("an access key id is required")
    .as_plugin_config_error()
    .expect("should be PluginConfigError")
    .message
    .clone();

    assert!(error.contains("r2_access_key_id"), "got: {error}");
    assert!(error.contains("LORE_R2_ACCESS_KEY_ID"), "got: {error}");

    let error = resolve_r2(
        r#"
        s3_bucket = "bucket"
        s3_endpoint_url = "https://account.r2.cloudflarestorage.com"
        r2_access_key_id = "id"
        "#,
    )
    .expect_err("a secret access key is required")
    .as_plugin_config_error()
    .expect("should be PluginConfigError")
    .message
    .clone();

    assert!(error.contains("r2_secret_access_key"), "got: {error}");
    assert!(error.contains("LORE_R2_SECRET_ACCESS_KEY"), "got: {error}");
}

/// Resolves an R2 configuration as the plugin does, with nothing in the environment.
fn resolve_r2(configuration: &str) -> Result<R2Settings, lore_server::plugins::PluginError> {
    let config: CloudflarePluginConfig = toml::from_str(configuration).unwrap();
    config.resolve_r2_with(PLUGIN_NAME, &no_environment)
}

/// A whole `[plugins]` section, the way the server reads it out of the settings file.
#[derive(serde::Deserialize)]
struct Plugins {
    plugins: std::collections::HashMap<String, toml::Value>,
}

/// A per-store section is merged over the parent, so a deployment that needs a store pointed
/// somewhere else writes only the key that differs (spec §2.2).
///
/// The merge belongs to the server and not to the plugin — this is the server's function — but the
/// section it hands the factory is what the plugin's `deny_unknown_fields` sees, so the two have to
/// be checked together: a merge that left the store sections in the parent would be refused here.
#[test]
fn test_a_store_section_overrides_the_parent_and_keeps_the_rest() {
    let settings: Plugins = toml::from_str(
        r#"
        [plugins.cloudflare]
        s3_bucket = "the-parent-bucket"
        s3_endpoint_url = "https://account.r2.cloudflarestorage.com"
        r2_access_key_id = "the-access-key-id"
        r2_secret_access_key = "the-secret-access-key"
        turso_url = "http://parent.example:8080"
        turso_auth_token = "the-token"

        [plugins.cloudflare.immutable_store]
        s3_bucket = "the-immutable-bucket"
        "#,
    )
    .expect("a [plugins] section should deserialize");

    let resolved = lore_server::store::resolve_plugin_config_with_fallback(
        &settings.plugins,
        PLUGIN_NAME,
        "immutable_store",
    )
    .expect("the plugin is configured");

    CloudflareImmutableStorePluginFactory
        .validate_config(&resolved)
        .expect("the merged section is one the immutable store reads");

    let config: CloudflarePluginConfig = resolved
        .try_into()
        .expect("the merged section deserializes");
    let r2 = config
        .resolve_r2_with(PLUGIN_NAME, &no_environment)
        .expect("the merged section is complete");

    assert_eq!(
        r2.bucket(),
        "the-immutable-bucket",
        "the section's bucket wins over the parent's"
    );
    assert_eq!(
        r2.endpoint_url(),
        Some("https://account.r2.cloudflarestorage.com"),
        "the parent's keys survive the merge, so a section names only what differs"
    );
    assert_eq!(r2.credentials().access_key_id(), "the-access-key-id");

    // The other stores still resolve from the parent alone, without the section.
    let resolved = lore_server::store::resolve_plugin_config_with_fallback(
        &settings.plugins,
        PLUGIN_NAME,
        "mutable_store",
    )
    .expect("the plugin is configured");

    CloudflareMutableStorePluginFactory
        .validate_config(&resolved)
        .expect("the immutable store's section does not reach the mutable store");
}

/// What the configuration holds is what the client is built from, and what it leaves out comes
/// from the environment.
#[test]
fn test_the_r2_settings_come_from_the_configuration_and_the_environment() {
    let config: CloudflarePluginConfig = toml::from_str(
        r#"
        s3_bucket = "bucket"
        s3_region = "auto"
        s3_force_path_style = false
        r2_slow_operation_threshold_millis = 2500
        r2_access_key_id = "configured-id"
        "#,
    )
    .unwrap();

    let settings = config
        .resolve_r2_with(PLUGIN_NAME, &|name| match name {
            "LORE_R2_ENDPOINT_URL" => Some("https://account.r2.cloudflarestorage.com".to_string()),
            "LORE_R2_SECRET_ACCESS_KEY" => Some("from-the-environment".to_string()),
            _ => None,
        })
        .expect("the endpoint and the secret come from the environment");

    assert_eq!(settings.bucket(), "bucket");
    assert_eq!(
        settings.endpoint_url(),
        Some("https://account.r2.cloudflarestorage.com")
    );
    assert_eq!(settings.region(), Some("auto"));
    assert!(!settings.force_path_style());
    assert_eq!(settings.slow_operation_threshold_millis(), 2500);
    assert_eq!(settings.credentials().access_key_id(), "configured-id");
    assert_eq!(
        settings.credentials().secret_access_key(),
        "from-the-environment"
    );

    // One timeout for the whole plugin, so that the store's two clients are bounded together.
    assert_eq!(settings.timeout_millis(), DEFAULT_TIMEOUT_MILLIS);
}

/// The R2 keys default to the addressing `R2` and `MinIO` both answer, and the timeouts to the
/// ones the plan gives.
#[test]
fn test_an_r2_configuration_without_the_optional_keys_holds_the_defaults() {
    let settings = resolve_r2(
        r#"
        s3_bucket = "bucket"
        s3_endpoint_url = "https://account.r2.cloudflarestorage.com"
        r2_access_key_id = "id"
        r2_secret_access_key = "secret"
        "#,
    )
    .expect("every required key is supplied");

    assert!(
        settings.force_path_style(),
        "a bucket in the path is what both R2 and MinIO answer"
    );
    assert_eq!(settings.region(), None);
    assert_eq!(
        settings.slow_operation_threshold_millis(),
        lore_cloudflare::store::r2::DEFAULT_SLOW_OPERATION_THRESHOLD_MILLIS
    );
    assert_eq!(settings.timeout_millis(), DEFAULT_TIMEOUT_MILLIS);
}

/// The settings the plugin logs do not hold a key, and neither does a query string on the endpoint.
#[test]
fn test_the_r2_settings_the_plugin_logs_do_not_hold_a_key() {
    let settings = resolve_r2(
        r#"
        s3_bucket = "bucket"
        s3_endpoint_url = "https://account.r2.cloudflarestorage.com?signature=in-the-url"
        r2_access_key_id = "the-access-key-id"
        r2_secret_access_key = "the-secret-access-key"
        "#,
    )
    .expect("every required key is supplied");

    let logged = format!("{settings:?}");

    for secret in ["the-access-key-id", "the-secret-access-key", "in-the-url"] {
        assert!(
            !logged.contains(secret),
            "a secret is not in the log, got: {logged}"
        );
    }

    assert!(
        logged.contains("bucket"),
        "the bucket is still logged, so an operator can see which one it is, got: {logged}"
    );
}
