// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The Cloudflare storage plugin for the Lore server.
//!
//! Cloudflare hosts the two stores that have to be shared between servers: a mutable key-value
//! store and a lock store, both in one Turso database (libSQL over HTTP). Its object storage, R2,
//! holds the immutable store, which this crate does not implement yet.
//!
//! The stores are written against [`TursoExecutor`] and [`TursoTransaction`](turso::TursoTransaction)
//! rather than against the driver, so a test can script every answer Turso can give, including the
//! answer that never arrives.
//!
//! ```
//! # async fn example() -> Result<(), lore_cloudflare::TursoError> {
//! use lore_cloudflare::TursoClient;
//! use lore_cloudflare::TursoSettings;
//!
//! let settings = TursoSettings::new("http://127.0.0.1:8080", "");
//! let client = TursoClient::connect(&settings).await?;
//! client.initialize_schema().await?;
//! # Ok(())
//! # }
//! ```

pub mod store;
pub mod turso;

pub use store::CloudflareLockStore;
pub use store::CloudflareMutableStore;
pub use turso::TursoClient;
pub use turso::TursoError;
pub use turso::TursoErrorKind;
pub use turso::TursoExecutor;
pub use turso::TursoLimits;
pub use turso::TursoRetry;
pub use turso::TursoSettings;
pub use turso::TursoTransaction;

/// The name the plugin registers itself under, which is what `mode = "cloudflare"` selects.
pub const CLOUDFLARE_PLUGIN_NAME: &str = "cloudflare";
