// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! How patient one Turso request is: its deadline, and how often a lost answer is retried.

use std::future::Future;
use std::time::Duration;

use lore_storage::Retry;

use super::TursoError;

/// The retry policy for one operation, in the shape the configuration gives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TursoRetry {
    /// Interval before the first retry, milliseconds.
    pub start_millis: u64,
    /// Largest interval between retries, milliseconds.
    pub max_millis: u64,
    /// How many waits are allowed, so an operation runs at most `limit + 1` times.
    pub limit: usize,
}

impl TursoRetry {
    pub const fn new(start_millis: u64, max_millis: u64, limit: usize) -> Self {
        Self {
            start_millis,
            max_millis,
            limit,
        }
    }

    /// A back-off waiter for one operation.
    pub fn waiter(&self) -> Retry {
        lore_storage::retry(self.start_millis, self.max_millis, self.limit)
    }

    /// The longest the retries of one operation can spend waiting: every interval at its maximum,
    /// and jitter never more than its clamp. Past this the deadline runs out before the last retry
    /// is used, so it is worth a warning at startup.
    pub fn worst_case(&self) -> Duration {
        // `Retry` doubles the interval, so the sum is bounded by `limit * max`.
        Duration::from_millis(self.max_millis.saturating_mul(self.limit as u64))
    }
}

impl Default for TursoRetry {
    fn default() -> Self {
        Self::new(50, 1000, 3)
    }
}

/// Run an operation that may be repeated, inside the deadline of one store operation: `$operation`
/// is evaluated and awaited again while it fails with `Disconnected`, up to the policy's wait
/// limit, and the whole of it must finish within `$limits.timeout`.
///
/// The operation is written at the call site rather than passed as a closure because each attempt
/// needs a fresh future that borrows the store.
macro_rules! retry_within_deadline {
    ($limits:expr, $operation:expr) => {{
        let limits = $limits;
        $crate::turso::retry::with_deadline(limits.timeout, async {
            let mut waiter = limits.retry.waiter();
            loop {
                match $operation.await {
                    Err(error) if error.is_disconnected() && waiter.wait().await => {
                        tracing::warn!(
                            message = error.message(),
                            attempt = waiter.counter(),
                            limit = limits.retry.limit,
                            "lost the answer from Turso; retrying"
                        );
                    }
                    result => break result,
                }
            }
        })
        .await
    }};
}

pub(crate) use retry_within_deadline;

/// Fail `operation` with a `Disconnected` error once `timeout` has passed. An operation that ran
/// out of time may or may not have reached the database, which is what `Disconnected` says.
pub async fn with_deadline<T>(
    timeout: Duration,
    operation: impl Future<Output = Result<T, TursoError>>,
) -> Result<T, TursoError> {
    within_deadline(timeout, std::convert::identity, operation).await
}

/// Fail `operation` once `timeout` has passed, naming the timeout in the errors of the store that
/// asked for it.
///
/// A transactional operation cannot use [`retry_within_deadline!`]: its answer may be one the
/// caller has to settle itself, so what it ends with is not always a [`TursoError`], and the lost
/// answer a timeout stands for has to be turned into that caller's own error by `expired`.
///
/// A transaction the deadline ends is one the server may still be running, and the server holds the
/// write lock of a transaction it has not finished. Measured against `v0.24.32`: a stream that went
/// idle is expired and rolled back, but a request abandoned while it was in flight left the write
/// path unanswerable for as long as it was watched — so this ends the caller's wait, not the
/// server's work, and a server that stops answering needs restarting (see the integration tests'
/// running notes in `tests/support/mod.rs`).
pub async fn within_deadline<T, E>(
    timeout: Duration,
    expired: impl FnOnce(TursoError) -> E,
    operation: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    match tokio::time::timeout(timeout, operation).await {
        Ok(result) => result,
        Err(_) => Err(expired(TursoError::disconnected(format!(
            "operation did not finish within {}ms",
            timeout.as_millis()
        )))),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::turso::TursoErrorKind;

    /// The limits a store method runs under, as the macro reads them.
    struct Limits {
        timeout: Duration,
        retry: TursoRetry,
    }

    fn limits(retry: TursoRetry) -> Limits {
        Limits {
            timeout: Duration::from_secs(30),
            retry,
        }
    }

    #[test]
    fn worst_case_covers_every_wait_at_the_maximum() {
        assert_eq!(
            TursoRetry::new(50, 1000, 3).worst_case(),
            Duration::from_millis(3000)
        );
    }

    /// The bound the retried operations rely on: one attempt plus `limit` retries, and no more.
    #[tokio::test]
    async fn an_operation_runs_once_per_attempt_plus_retries() {
        for limit in [0, 1, 3] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_ref = attempts.clone();

            let result: Result<(), TursoError> =
                retry_within_deadline!(limits(TursoRetry::new(0, 0, limit)), async {
                    attempts_ref.fetch_add(1, Ordering::SeqCst);
                    Err(TursoError::disconnected("lost"))
                });

            assert!(result.is_err());
            assert_eq!(attempts.load(Ordering::SeqCst), limit + 1);
        }
    }

    #[tokio::test]
    async fn a_failure_that_is_not_a_lost_answer_is_not_retried() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let result: Result<&str, TursoError> =
            retry_within_deadline!(limits(TursoRetry::new(0, 0, 3)), async {
                attempts_ref.fetch_add(1, Ordering::SeqCst);
                Err::<&str, _>(TursoError::new(TursoErrorKind::SlowDown, "busy"))
            });

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_success_is_returned_as_it_is() {
        let result: Result<u8, TursoError> =
            retry_within_deadline!(limits(TursoRetry::new(0, 0, 3)), async {
                Ok::<u8, TursoError>(7)
            });
        assert_eq!(result.unwrap(), 7);
    }

    /// The retries share one deadline, so a retried operation cannot outlive it.
    #[tokio::test]
    async fn the_retries_share_the_deadline_of_one_operation() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let result: Result<(), TursoError> = retry_within_deadline!(
            Limits {
                timeout: Duration::from_millis(20),
                retry: TursoRetry::new(10, 10, 100),
            },
            async {
                attempts_ref.fetch_add(1, Ordering::SeqCst);
                Err(TursoError::disconnected("lost"))
            }
        );

        let error = result.unwrap_err();
        assert_eq!(error.kind(), TursoErrorKind::Disconnected);
        assert!(error.message().contains("did not finish"));
        assert!(attempts.load(Ordering::SeqCst) < 101);
    }

    #[tokio::test]
    async fn a_deadline_turns_a_slow_operation_into_a_lost_answer() {
        let error = with_deadline(Duration::from_millis(1), async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<(), TursoError>(())
        })
        .await
        .unwrap_err();

        assert_eq!(error.kind(), TursoErrorKind::Disconnected);
    }

    #[tokio::test]
    async fn a_deadline_leaves_a_fast_operation_alone() {
        let result =
            with_deadline(Duration::from_secs(30), async { Ok::<u8, TursoError>(7) }).await;
        assert_eq!(result.unwrap(), 7);
    }

    /// An operation with errors of its own hears about the deadline in those terms.
    #[tokio::test]
    async fn a_deadline_names_a_timeout_the_way_the_caller_does() {
        let error = within_deadline(
            Duration::from_millis(1),
            |error| format!("gave up: {}", error.message()),
            async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<(), String>(())
            },
        )
        .await
        .unwrap_err();

        assert!(
            error.starts_with("gave up: operation did not finish"),
            "{error}"
        );
    }

    /// The deadline does not stand between a caller and an answer it did get.
    #[tokio::test]
    async fn a_deadline_leaves_a_fast_failure_of_the_caller_alone() {
        let error = within_deadline(
            Duration::from_secs(30),
            |error| format!("gave up: {}", error.message()),
            async { Err::<(), String>("refused".to_string()) },
        )
        .await
        .unwrap_err();

        assert_eq!(error, "refused");
    }
}
