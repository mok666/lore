// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! libSQL failures, sorted into the cases the store contract distinguishes.
//!
//! The store traits can only say four things about a failure that reached them: the request did
//! not get through (`Disconnected`), it collided with another writer (`SlowDown`), the account is
//! blocked (`Maintenance`), or the store is broken (`Internal`). The stores in this crate retry
//! the first, hand the second to their caller, and log the last two. [`TursoErrorKind`] is that
//! choice, made once, before anything is logged or translated.
//!
//! Sorting a libSQL error is partly guesswork: `libsql::Error::Hrana` carries a
//! `Box<dyn Error>` whose concrete type (`libsql::hrana::HranaError`) is behind a private module,
//! so the failure's own text is the only thing left to read. [`TursoError::classify`] reads it.
//! The alternative -- calling everything `Internal` -- would turn a busy database into an error
//! nobody retries.

use std::fmt;

use lore_base::error::Disconnected;
use lore_base::error::Maintenance;
use lore_base::error::SlowDown;
use lore_revision::lock::LockError;
use lore_storage::StoreError;

/// `SQLite` primary result codes this crate tells apart. The extended codes share the low byte, so
/// `SQLITE_BUSY_RECOVERY` and friends arrive here as [`SQLITE_BUSY`].
const SQLITE_BUSY: i32 = 5;
const SQLITE_LOCKED: i32 = 6;
const SQLITE_IOERR: i32 = 10;

/// What a caller is expected to do about a failure, once it has been sorted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TursoErrorKind {
    /// The request did not reach the database, or the answer did not come back. Whether the
    /// database applied it is unknown. Retrying is safe for operations whose result does not
    /// depend on the current value, and meaningless for the rest.
    Disconnected,
    /// Another writer held the write lock, or this transaction lost a snapshot race. Nothing was
    /// applied, and the operation can be reissued as it stands.
    SlowDown,
    /// Turso refused the request because the account is over its quota. Retrying cannot fix it.
    Maintenance,
    /// A bug in this crate or in its configuration: a statement `SQLite` rejected, a column of the
    /// wrong type, a schema version this build does not understand.
    Internal,
}

/// A libSQL failure with the call the store should make about it.
///
/// Cloning shares nothing; the message is owned so the error can outlive the `libsql::Error` it
/// came from.
#[derive(Clone, Debug)]
pub struct TursoError {
    kind: TursoErrorKind,
    message: String,
}

impl TursoError {
    pub fn new(kind: TursoErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn disconnected(message: impl Into<String>) -> Self {
        Self::new(TursoErrorKind::Disconnected, message)
    }

    pub fn kind(&self) -> TursoErrorKind {
        self.kind
    }

    pub fn is_disconnected(&self) -> bool {
        self.kind == TursoErrorKind::Disconnected
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Sort a libSQL failure. The variants that name a `SQLite` result code are read directly;
    /// everything else goes through [`Self::classify`].
    pub fn from_libsql(error: libsql::Error) -> Self {
        match &error {
            libsql::Error::ConnectionFailed(_) => {
                Self::new(TursoErrorKind::Disconnected, error.to_string())
            }
            libsql::Error::SqliteFailure(code, _)
            | libsql::Error::RemoteSqliteFailure(code, _, _) => {
                Self::new(sqlite_primary_kind(*code), error.to_string())
            }
            other => {
                let message = other.to_string();
                Self::new(Self::classify(&message), message)
            }
        }
    }

    /// Which kind a libSQL failure's own text describes.
    ///
    /// The markers are the `Display`/`Debug` forms of the private `HranaError` and of the
    /// statement errors it carries:
    ///
    /// - An exchange that ended early says so: `http error`, `Failed to connect to database`,
    ///   `stream closed`, `unexpected response` and `json error` all name what went wrong in
    ///   front of the text the server sent back.
    /// - A status the driver refused to read as an answer is the request going out and the answer
    ///   not coming back with it, which is the case this crate must not guess about. Any status,
    ///   `429` or a 5xx, may be an answer that was lost rather than a request that was refused —
    ///   and so may an expired stream, which the server refuses as `STREAM_EXPIRED` without
    ///   running the statement. All of them are [`TursoErrorKind::Disconnected`] (spec §4.6).
    /// - A statement the server refused for a lock reason arrives as the `Debug` form of the
    ///   statement error it carries: a message, and a code of `SQLITE_BUSY` or `SQLITE_LOCKED`.
    /// - A quota refusal arrives as a body carrying `BLOCKED`.
    ///
    /// Anything unrecognised is [`TursoErrorKind::Internal`]: an error this crate does not
    /// understand is not one it should quietly retry.
    pub fn classify(message: &str) -> TursoErrorKind {
        if message.contains("http error")
            || message.contains("Failed to connect to database")
            || message.contains("stream closed")
            || message.contains("unexpected response")
            || message.contains("json error")
        {
            return TursoErrorKind::Disconnected;
        }

        // Before the statuses below: a quota refusal is not a lost answer however it arrives, and
        // it is the one kind nobody may retry. Measured against `v0.24.32`, the body is
        // `{"error":"BLOCKED: ..."}` behind a status of its own.
        if message.contains("BLOCKED") {
            return TursoErrorKind::Maintenance;
        }

        // Measured against `v0.24.32`: an answer the server refuses for a reason of its own comes
        // back as `status=503 Service Unavailable`, and one it refuses because the stream is gone
        // as `status=400 Bad Request` with `STREAM_EXPIRED` in the body. Neither is a statement
        // the server read and rejected — those arrive inside a `200`, as the codes below do — and
        // neither says whether the statement ran, so both are treated as an answer that was lost.
        if message.contains("STREAM_EXPIRED")
            || matches!(api_error_status(message), Some(429 | 500..=599))
        {
            return TursoErrorKind::Disconnected;
        }

        if message.contains("SQLITE_BUSY") || message.contains("SQLITE_LOCKED") {
            return TursoErrorKind::SlowDown;
        }

        TursoErrorKind::Internal
    }

    /// Translate into the mutable/immutable store vocabulary, logging what the caller cannot see.
    pub fn into_store_error(self, context: &str) -> StoreError {
        match self.kind {
            TursoErrorKind::Disconnected => {
                tracing::warn!(reason = %self.message, "{context}: disconnected from Turso");
                StoreError::from(Disconnected)
            }
            TursoErrorKind::SlowDown => {
                tracing::warn!(
                    reason = %self.message,
                    "{context}: Turso refused the write lock"
                );
                StoreError::from(SlowDown)
            }
            TursoErrorKind::Maintenance => {
                tracing::error!(
                    reason = %self.message,
                    "{context}: Turso refused the request; the account needs attention"
                );
                StoreError::from(Maintenance)
            }
            TursoErrorKind::Internal => {
                tracing::error!(
                    reason = %self.message,
                    "{context}: unexpected Turso failure"
                );
                StoreError::internal_with_context(self, context)
            }
        }
    }

    /// Translate into the lock vocabulary, which has no `Disconnected` and no `Maintenance`
    /// member: a lost answer becomes `SlowDown` (the only member that says "the state is not
    /// settled"), and a quota refusal becomes `Internal` (the only member that is not retried).
    pub fn into_lock_error(self, context: &str) -> LockError {
        match self.kind {
            TursoErrorKind::Disconnected | TursoErrorKind::SlowDown => {
                tracing::warn!(
                    reason = %self.message,
                    "{context}: Turso lock request did not settle"
                );
                LockError::from(SlowDown)
            }
            TursoErrorKind::Maintenance => {
                tracing::error!(
                    reason = %self.message,
                    "{context}: Turso refused the request; the account needs attention"
                );
                LockError::internal_with_context(self, context)
            }
            TursoErrorKind::Internal => {
                tracing::error!(
                    reason = %self.message,
                    "{context}: unexpected Turso failure"
                );
                LockError::internal_with_context(self, context)
            }
        }
    }
}

/// The status of the driver's api error, where the text is one.
///
/// A non-200 answer is written as `status=503 Service Unavailable, body=…`
/// (`libsql-0.9.30/src/hrana/hyper.rs:90`), so the answer's own status is the three digits after
/// the marker. They are read from the first marker, which is the driver's; a body that happens to
/// spell one out comes after it and cannot be mistaken for it.
fn api_error_status(message: &str) -> Option<u16> {
    message.split_once("status=")?.1.get(..3)?.parse().ok()
}

impl fmt::Display for TursoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for TursoError {}

impl From<libsql::Error> for TursoError {
    fn from(error: libsql::Error) -> Self {
        Self::from_libsql(error)
    }
}

/// The kind a `SQLite` primary result code means for a store operation.
/// Which kind a `SQLite` result code belongs to.
///
/// A server reports an extended code as readily as a primary one, and an extended code carries
/// the primary code in its low byte: `SQLITE_BUSY_SNAPSHOT` (261) is a kind of `SQLITE_BUSY` (5).
fn sqlite_primary_kind(code: i32) -> TursoErrorKind {
    match code & 0xff {
        SQLITE_BUSY | SQLITE_LOCKED => TursoErrorKind::SlowDown,
        SQLITE_IOERR => TursoErrorKind::Disconnected,
        _ => TursoErrorKind::Internal,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn transport_markers_are_disconnected() {
        for message in [
            "Hrana: `http error: `error sending request: connection refused``",
            "Failed to connect to database: `no such host`",
            "stream closed: `stream was closed`",
            "unexpected response: `expected a batch response`",
            "json error: `expected value at line 1 column 1`",
        ] {
            assert_eq!(
                TursoError::classify(message),
                TursoErrorKind::Disconnected,
                "{message}"
            );
        }
    }

    #[test]
    fn lock_markers_are_slow_down() {
        for message in [
            "stream error: `Error { message: \"database is locked\", code: \"SQLITE_BUSY\" }`",
            "stream error: `Error { message: \"database table is locked\", code: \"SQLITE_LOCKED\" }`",
            "stream error: `Error { message: \"\", code: \"SQLITE_BUSY_SNAPSHOT\" }`",
        ] {
            assert_eq!(
                TursoError::classify(message),
                TursoErrorKind::SlowDown,
                "{message}"
            );
        }
    }

    #[test]
    fn a_blocked_account_is_maintenance() {
        assert_eq!(
            TursoError::classify(
                "api error: `status=403 Forbidden, body={\"error\":\"BLOCKED: the account is over its quota\"}`"
            ),
            TursoErrorKind::Maintenance
        );
    }

    /// A refusal on account grounds outranks the status it arrives behind: it is the one kind
    /// that must not be read as a lost answer, whatever status carried it.
    #[test]
    fn a_blocked_account_is_maintenance_behind_any_status() {
        assert_eq!(
            TursoError::classify(
                "api error: `status=429 Too Many Requests, body={\"error\":\"BLOCKED: over quota\"}`"
            ),
            TursoErrorKind::Maintenance
        );
    }

    /// The statuses that mean the request went out and no answer to it came back (spec §4.6).
    #[test]
    fn a_status_that_lost_the_answer_is_disconnected() {
        for message in [
            "api error: `status=429 Too Many Requests, body={\"error\":\"too many requests\"}`",
            "api error: `status=500 Internal Server Error, body=`",
            "api error: `status=502 Bad Gateway, body=`",
            "api error: `status=503 Service Unavailable, body=`",
            "api error: `status=504 Gateway Timeout, body=`",
        ] {
            assert_eq!(
                TursoError::classify(message),
                TursoErrorKind::Disconnected,
                "{message}"
            );
        }
    }

    /// Measured against `v0.24.32`: a request on a stream the server has dropped is refused with
    /// this answer, and the statement in it was not run.
    #[test]
    fn an_expired_stream_is_disconnected() {
        assert_eq!(
            TursoError::classify(
                "api error: `status=400 Bad Request, body={\"message\":\"The stream has expired due to inactivity\",\"code\":\"STREAM_EXPIRED\"}`"
            ),
            TursoErrorKind::Disconnected
        );
    }

    /// A status that carried an answer is not a lost answer: the statement it names, or the
    /// credential it was sent with, is what went wrong.
    #[test]
    fn a_status_that_answered_is_not_a_lost_answer() {
        for message in [
            "api error: `status=400 Bad Request, body=Received an invalid baton`",
            "api error: `status=401 Unauthorized, body=`",
            "api error: `status=403 Forbidden, body={\"error\":\"forbidden\"}`",
            "api error: `status=404 Not Found, body={\"error\":\"not found\"}`",
        ] {
            assert_eq!(
                TursoError::classify(message),
                TursoErrorKind::Internal,
                "{message}"
            );
        }
    }

    #[test]
    fn anything_else_is_internal() {
        for message in [
            "API misuse: `connection is closed`",
            "syntax error around L1:7: `near \"SELCT\": syntax error`",
        ] {
            assert_eq!(
                TursoError::classify(message),
                TursoErrorKind::Internal,
                "{message}"
            );
        }
    }

    /// A marker has to be read from the whole message, not from the start: libSQL prefixes it.
    #[test]
    fn markers_are_read_anywhere_in_the_message() {
        assert_eq!(
            TursoError::classify("Hrana: `http error: `connection reset``"),
            TursoErrorKind::Disconnected
        );
    }

    #[test]
    fn sqlite_codes_map_by_family() {
        assert_eq!(sqlite_primary_kind(SQLITE_BUSY), TursoErrorKind::SlowDown);
        assert_eq!(sqlite_primary_kind(SQLITE_LOCKED), TursoErrorKind::SlowDown);
        // The extended codes share the primary code's low byte.
        assert_eq!(sqlite_primary_kind(261), TursoErrorKind::SlowDown);
        assert_eq!(
            sqlite_primary_kind(SQLITE_IOERR),
            TursoErrorKind::Disconnected
        );
        assert_eq!(sqlite_primary_kind(1), TursoErrorKind::Internal);
        assert_eq!(sqlite_primary_kind(19), TursoErrorKind::Internal);
    }

    #[test]
    fn a_connection_failure_is_disconnected_before_its_text_is_read() {
        let error = libsql::Error::ConnectionFailed("no such host".to_string());
        assert_eq!(
            TursoError::from_libsql(error).kind(),
            TursoErrorKind::Disconnected
        );
    }

    #[test]
    fn a_remote_sqlite_failure_is_read_from_its_code() {
        let error =
            libsql::Error::RemoteSqliteFailure(SQLITE_BUSY, 517, "database is locked".to_string());
        assert_eq!(
            TursoError::from_libsql(error).kind(),
            TursoErrorKind::SlowDown
        );
    }
}
