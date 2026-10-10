// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The mutable store against a live libSQL server.
//!
//! `the_store_satisfies_the_battery...` hands the store to the same battery every other
//! implementation of the trait is held to, with no known violations declared: a check that passes
//! here passes for the same reason it passes for the local store.
//!
//! The rest covers what the battery cannot: the zero value that means "absent" (spec §4.3), and
//! the two failures a client cannot resolve by looking (spec §4.6).
#![cfg(feature = "integration_tests")]

mod support;

use std::sync::Arc;

use lore_base::types::Hash;
use lore_base::types::KeyType;
use lore_cloudflare::TursoExecutor;
use lore_storage::MutableStore;
use lore_storage::StoreError;
use lore_storage::mutable_conformance::Capabilities;
use lore_storage::mutable_conformance::verify_mutable_store;
use support::FlakyExecutor;
use support::LostCommitExecutor;
use support::StatementLog;

/// The battery, as the store a deployment gets.
#[tokio::test]
async fn the_store_satisfies_the_battery_its_callers_rely_on() {
    let Some(client) = support::client("the mutable store battery").await else {
        return;
    };

    let store: Arc<dyn MutableStore> = support::mutable_store(Arc::new(client));

    verify_mutable_store(
        store,
        Capabilities::new("CloudflareMutableStore/integration"),
    )
    .await;
}

/// A swap from zero to zero over a key that is not there is a success that changes nothing.
///
/// Creating a branch does exactly this: the caller has no revision yet, so it swaps zero for
/// zero. A store that answers "the value was already zero" as a failure would leave the branch
/// pointer where it was, and the first push after it would go nowhere (spec §4.3).
#[tokio::test]
async fn a_swap_of_zero_into_a_missing_key_succeeds_and_leaves_it_absent() {
    let Some(client) = support::client("the zero-to-zero swap").await else {
        return;
    };

    let store = support::mutable_store(Arc::new(client));
    let partition = support::random_partition();
    let key = support::random_hash();
    let value = support::random_hash();

    let previous = store
        .clone()
        .compare_and_swap(
            partition,
            key,
            Hash::default(),
            Hash::default(),
            KeyType::BranchLatestPointer,
        )
        .await
        .expect("swapping zero for zero over a key that is not there is a success");
    assert_eq!(previous, Hash::default());

    assert!(
        store
            .clone()
            .load(partition, key, KeyType::BranchLatestPointer)
            .await
            .is_err(),
        "a zero value is not a value: the key still looks absent"
    );

    // The first real push: the same key, now swapping zero for a revision.
    let previous = store
        .clone()
        .compare_and_swap(
            partition,
            key,
            Hash::default(),
            value,
            KeyType::BranchLatestPointer,
        )
        .await
        .expect("swapping zero for a value succeeds over a key that holds zero");
    assert_eq!(previous, Hash::default());

    assert_eq!(
        store
            .clone()
            .load(partition, key, KeyType::BranchLatestPointer)
            .await
            .expect("the pushed revision is readable"),
        value,
        "the swap has to have landed, not just been reported"
    );
}

/// A swap that expects a value it does not find reports what is there instead, and writes nothing.
#[tokio::test]
async fn a_swap_that_expects_something_else_reports_the_value_that_is_there() {
    let Some(client) = support::client("the refused swap").await else {
        return;
    };

    let store = support::mutable_store(Arc::new(client));
    let partition = support::random_partition();
    let key = support::random_hash();
    let held = support::random_hash();
    let wanted = support::random_hash();
    let expected = support::random_hash();

    store
        .clone()
        .store(partition, key, held, KeyType::BranchLatestPointer)
        .await
        .expect("the key can be stored");

    let previous = store
        .clone()
        .compare_and_swap(
            partition,
            key,
            expected,
            wanted,
            KeyType::BranchLatestPointer,
        )
        .await
        .expect("a refused swap is not an error");

    assert_ne!(
        previous, expected,
        "the caller learns the swap did not happen from a value that is not the one it expected"
    );
    assert_eq!(previous, held);
    assert_eq!(
        store
            .clone()
            .load(partition, key, KeyType::BranchLatestPointer)
            .await
            .expect("the key still holds what it held"),
        held
    );
}

/// A swap whose `COMMIT` answer was lost is not reissued, and its outcome is not guessed by
/// reading back (spec §4.6).
///
/// The server did apply it. A second attempt would either fail on a value that is already right or
/// overwrite what another client has written since, and a read would report that other client's
/// value as this caller's previous value. So the caller is told the outcome is unknown, and that
/// is the whole answer.
#[tokio::test]
async fn a_swap_whose_commit_answer_was_lost_is_not_reissued_or_read_back() {
    let Some(client) = support::client("a swap under a lost commit").await else {
        return;
    };

    let log = Arc::new(StatementLog::default());
    let executor: Arc<dyn TursoExecutor> =
        Arc::new(LostCommitExecutor::new(Arc::new(client), log.clone()));
    let store = support::mutable_store(executor);

    let partition = support::random_partition();
    let key = support::random_hash();
    let held = support::random_hash();
    let wanted = support::random_hash();

    store
        .clone()
        .store(partition, key, held, KeyType::BranchLatestPointer)
        .await
        .expect("the key can be stored");
    log.take();

    let error = store
        .clone()
        .compare_and_swap(partition, key, held, wanted, KeyType::BranchLatestPointer)
        .await
        .expect_err("the caller has to be told that the answer was lost");

    assert!(
        matches!(error, StoreError::Disconnected(_)),
        "a lost commit is a lost answer, not a slow database: {error:?}"
    );

    let statements = log.take();
    assert_eq!(
        statements.iter().filter(|s| s.starts_with("begin")).count(),
        1,
        "a swap is decided once: {statements:?}"
    );
    assert_eq!(
        statements.iter().filter(|s| s.starts_with("query")).count(),
        1,
        "the only read is the one the decision was made on: {statements:?}"
    );
    assert!(
        statements.last().is_some_and(|s| s == "commit"),
        "nothing may follow the commit that was lost: {statements:?}"
    );

    // The swap the server applied is the swap the caller is unsure about.
    assert_eq!(
        store
            .clone()
            .load(partition, key, KeyType::BranchLatestPointer)
            .await
            .expect("the value is readable"),
        wanted
    );
}

/// A read is reissued inside its limit and gives up beyond it (spec §4.6).
#[tokio::test]
async fn a_read_is_reissued_inside_its_limit_and_gives_up_beyond_it() {
    let Some(client) = support::client("a read against a flaky server").await else {
        return;
    };

    let real: Arc<dyn TursoExecutor> = Arc::new(client);
    let log = Arc::new(StatementLog::default());
    let limit = support::limits().retry.limit;

    let partition = support::random_partition();
    let key = support::random_hash();
    let value = support::random_hash();

    support::mutable_store(real.clone())
        .store(partition, key, value, KeyType::BranchId)
        .await
        .expect("the fixture can be stored");

    for failures in [1, limit, limit + 1] {
        let executor: Arc<dyn TursoExecutor> =
            Arc::new(FlakyExecutor::new(real.clone(), failures, log.clone()));
        let store = support::mutable_store(executor);
        let loaded = store.clone().load(partition, key, KeyType::BranchId).await;

        if failures <= limit {
            assert_eq!(
                loaded.unwrap_or_else(|error| panic!(
                    "{failures} failures is inside the limit: {error:?}"
                )),
                value,
                "{failures} failures is inside the limit of {limit}"
            );
        } else {
            assert!(
                matches!(loaded, Err(StoreError::Disconnected(_))),
                "{failures} failures is past the limit of {limit}: {loaded:?}"
            );
        }
    }
}

/// An idempotent write is reissued inside its limit and gives up beyond it (spec §4.6).
///
/// Writing the same value twice leaves the same table, so reissuing it after an answer was lost
/// cannot change the outcome: what it changes is only whether the caller is told it worked.
#[tokio::test]
async fn a_write_is_reissued_inside_its_limit_and_gives_up_beyond_it() {
    let Some(client) = support::client("a write against a flaky server").await else {
        return;
    };

    let real: Arc<dyn TursoExecutor> = Arc::new(client);
    let log = Arc::new(StatementLog::default());
    let limit = support::limits().retry.limit;

    let partition = support::random_partition();
    let key = support::random_hash();
    let value = support::random_hash();

    for failures in [1, limit, limit + 1] {
        let executor: Arc<dyn TursoExecutor> =
            Arc::new(FlakyExecutor::new(real.clone(), failures, log.clone()));
        let store = support::mutable_store(executor);
        let written = store
            .clone()
            .store(partition, key, value, KeyType::BranchId)
            .await;

        if failures <= limit {
            written.unwrap_or_else(|error| {
                panic!("{failures} failures is inside the limit of {limit}: {error:?}")
            });
        } else {
            assert!(
                matches!(written, Err(StoreError::Disconnected(_))),
                "{failures} failures is past the limit of {limit}: {written:?}"
            );
        }
    }
}

/// A swap that never reached the server is not reissued either (spec §4.6).
///
/// One failure is enough to tell the two policies apart: a read is reissued after one, a swap is
/// not.
#[tokio::test]
async fn a_swap_is_not_reissued_even_once() {
    let Some(client) = support::client("a swap against a flaky server").await else {
        return;
    };

    let log = Arc::new(StatementLog::default());
    let executor: Arc<dyn TursoExecutor> =
        Arc::new(FlakyExecutor::new(Arc::new(client), 1, log.clone()));
    let store = support::mutable_store(executor);

    let error = store
        .clone()
        .compare_and_swap(
            support::random_partition(),
            support::random_hash(),
            Hash::default(),
            support::random_hash(),
            KeyType::BranchLatestPointer,
        )
        .await
        .expect_err("the caller has to be told that the swap did not happen");

    assert!(
        matches!(error, StoreError::Disconnected(_)),
        "the outcome is unknown even when nothing was sent: {error:?}"
    );
    assert_eq!(
        log.count("begin immediate"),
        1,
        "a swap that failed is not tried again"
    );
}
