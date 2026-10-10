// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_cloudflare::TursoRetry;
use lore_cloudflare::turso::DEFAULT_MAX_CONNECTIONS;
use lore_cloudflare::turso::DEFAULT_MAX_TRANSACTION_STATEMENTS;
use lore_cloudflare::turso::DEFAULT_TIMEOUT_MILLIS;
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
fn test_lock_store_factory_name() {
    let factory = CloudflareLockStorePluginFactory;
    assert_eq!(factory.name(), PLUGIN_NAME);
}

#[tokio::test]
async fn test_register_adds_the_mutable_and_lock_stores() {
    let mut registry = PluginRegistry::new();
    register(&mut registry);

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

    // The immutable store is not part of this plugin yet, and the plugin does not register a
    // resource detector.
    let immutable = registry.list_immutable_store_plugins();
    assert!(
        !immutable.contains(&PLUGIN_NAME.to_string()),
        "The immutable store is not implemented yet, found: {immutable:?}"
    );
    assert_eq!(registry.resource_detectors(Handle::current()).len(), 0);
}

/// A key no store reads is refused rather than ignored, so a typo in a deployment's configuration
/// stops the server instead of quietly leaving a store unconfigured.
#[test]
fn test_an_unknown_key_is_refused() {
    // `s3_bucket` belongs to the immutable store, which this plugin does not provide yet.
    let config: toml::Value = toml::from_str(r#"s3_bucket = "bucket""#).unwrap();

    for error in [
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
            config_error.message.contains("s3_bucket"),
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
