// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The lock store against a live libSQL server.
//!
//! There is no battery for locks upstream, so the cases the `DynamoDB` store is held to are restated
//! here for this one: acquiring what is already ours, being refused what is someone else's, and
//! the query variants. Two of them are the ones the plan names as easy to get wrong — a reacquired
//! lock is not reported as newly taken (spec §4.4), and an unlock that names a resource belonging
//! to someone else deletes nothing at all (spec §4.5).
//!
//! The calls that expect a settled answer go through [`support::take_locks`] and
//! [`support::release_locks`], which reissue a request that met another writer the way a caller
//! does (spec §4.6). The two tests that are about a lost answer not being reissued inside one call
//! use the store itself, because the caller's retry is what they are not about.
#![cfg(feature = "integration_tests")]

mod support;

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use lore_base::types::Context;
use lore_base::types::Hash;
use lore_base::types::LockData;
use lore_base::types::LockResource;
use lore_base::types::Partition;
use lore_cloudflare::TursoExecutor;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use support::FlakyExecutor;
use support::LostCommitExecutor;
use support::RecordingExecutor;
use support::StatementLog;

/// A resource no other test has locked.
fn resource(branch: Context, hash: Hash) -> LockResource {
    LockResource {
        branch,
        hash,
        description: support::random_description(),
    }
}

/// A resource no other test has locked.
fn random_resource() -> LockResource {
    resource(support::random_context(), support::random_hash())
}

/// The locks a query finds.
async fn held(store: &dyn LockStore, query: LockQuery) -> Vec<LockData> {
    let description = format!("{query:?}");

    store
        .query_locks(query)
        .await
        .unwrap_or_else(|error| panic!("{description} should be answerable: {error:?}"))
}

/// The locks one resource is held by.
async fn holders(store: &dyn LockStore, repository: Partition, resource: &LockResource) -> usize {
    held(
        store,
        LockQuery::HashRepositoryBranch(resource.hash, repository, resource.branch),
    )
    .await
    .len()
}

// =============================================================================
// Taking and releasing
// =============================================================================

/// Taking what the same owner already holds adds nothing and fails nothing.
///
/// A retried push locks the same resources again. The second call cannot distinguish the locks it
/// just took from the ones that were already there, so it reports neither as new, and the caller
/// learns that it is the owner of everything it asked for (spec §4.4).
#[tokio::test]
async fn a_lock_the_owner_already_holds_is_not_taken_twice() {
    let Some(client) = support::client("reacquiring a lock").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let owner = support::random_owner();
    let repository = support::random_partition();
    let first = random_resource();
    let second = random_resource();

    let taken = support::take_locks(&store, &owner, repository, &[first.clone(), second.clone()])
        .await
        .expect("the first lock is free");

    assert_eq!(taken.len(), 2);
    for lock in &taken {
        assert_eq!(lock.owner, owner);
        assert!(lock.resource == first || lock.resource == second);
        assert!(lock.locked_at > 0, "a lock says when it was taken");
    }

    let taken_again =
        support::take_locks(&store, &owner, repository, &[first.clone(), second.clone()])
            .await
            .expect("taking what is already ours succeeds");

    assert!(
        taken_again.is_empty(),
        "nothing was newly taken, so nothing is reported: {taken_again:?}"
    );
    assert_eq!(holders(&store, repository, &first).await, 1);
    assert_eq!(holders(&store, repository, &second).await, 1);
}

/// A resource someone else holds refuses the whole request, including the free ones in it.
#[tokio::test]
async fn a_resource_held_by_someone_else_refuses_the_whole_request() {
    let Some(client) = support::client("a contended lock").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let holder = support::random_owner();
    let other = support::random_owner();
    let repository = support::random_partition();
    let contended = random_resource();
    let free = random_resource();

    support::take_locks(
        &store,
        &holder,
        repository,
        std::slice::from_ref(&contended),
    )
    .await
    .expect("the resource is free for the first owner");

    let error = support::take_locks(
        &store,
        &other,
        repository,
        &[free.clone(), contended.clone()],
    )
    .await
    .expect_err("a resource held by someone else cannot be taken");

    assert!(
        matches!(error, LockError::LockNotOwned(_)),
        "the caller is told the resource is not theirs: {error:?}"
    );

    assert_eq!(
        holders(&store, repository, &contended).await,
        1,
        "the lock that was there is untouched"
    );
    assert_eq!(
        held(&store, LockQuery::Owner(other)).await.len(),
        0,
        "the free resource in the same request was not taken either: the request is one unit"
    );
}

/// A request that names the same resource twice takes it once.
#[tokio::test]
async fn duplicate_resources_collapse_to_one_lock() {
    let Some(client) = support::client("a lock request with a duplicate").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let owner = support::random_owner();
    let repository = support::random_partition();
    let resource = random_resource();

    let resources = vec![resource.clone(), random_resource(), resource.clone()];

    let taken = support::take_locks(&store, &owner, repository, &resources)
        .await
        .expect("the resources are free");

    assert_eq!(taken.len(), 2, "one lock per distinct resource");

    support::release_locks(&store, &owner, true, repository, &resources)
        .await
        .expect("unlocking what we hold twice succeeds");

    for resource in &resources {
        assert_eq!(
            holders(&store, repository, resource).await,
            0,
            "every resource in the request is released"
        );
    }
}

/// A request that names hundreds of resources costs a handful of statements.
///
/// Taking and releasing locks are one transaction each, and a statement per resource would make a
/// large push cost hundreds of round trips inside a transaction the server abandons after five
/// seconds (spec §4.6). A request names as many resources as one statement can hold, and as many
/// statements as that takes — which for 200 resources is two to take them and one to release them,
/// in either case well inside the interactive transaction limit.
#[tokio::test]
async fn a_large_lock_request_costs_a_handful_of_statements() {
    let Some(client) = support::client("a large lock request").await else {
        return;
    };

    let log = Arc::new(StatementLog::default());
    let executor: Arc<dyn TursoExecutor> =
        Arc::new(RecordingExecutor::new(Arc::new(client), log.clone()));
    let store = support::lock_store(executor);

    let owner = support::random_owner();
    let repository = support::random_partition();
    let resources: Vec<LockResource> = (0..200).map(|_| random_resource()).collect();

    let started = Instant::now();
    let taken = support::take_locks(&store, &owner, repository, &resources)
        .await
        .expect("the resources are free");
    let taking = started.elapsed();

    assert_eq!(taken.len(), 200, "every resource is locked once");

    // Counted from the last `BEGIN IMMEDIATE`: a request that met another writer and was reissued
    // is the caller's retry, not this request's cost.
    let statements = statements_of_the_last_attempt(&log.take());
    assert!(
        statements <= 6,
        "200 resources cost {statements} statements: {:?}",
        log.take()
    );

    let started = Instant::now();
    let released = support::release_locks(&store, &owner, true, repository, &resources)
        .await
        .expect("the resources are ours to release");
    let releasing = started.elapsed();

    assert_eq!(released.len(), 200, "every resource is released once");

    let statements = statements_of_the_last_attempt(&log.take());
    assert!(
        statements <= 6,
        "releasing 200 resources cost {statements} statements: {:?}",
        log.take()
    );

    for resource in &resources {
        assert_eq!(
            holders(&store, repository, resource).await,
            0,
            "every resource in the request is released"
        );
    }

    // The interactive transaction limit the plan names is five seconds. Reported rather than only
    // asserted, because it is the number the statement count is there to keep down.
    eprintln!("200 locks: taking {taking:?}, releasing {releasing:?}");
    for elapsed in [taking, releasing] {
        assert!(
            elapsed < Duration::from_secs(5),
            "one request has to finish inside the interactive transaction limit: {elapsed:?}"
        );
    }
}

/// How many statements the last attempt of a request made, from its own `BEGIN IMMEDIATE` on.
fn statements_of_the_last_attempt(statements: &[String]) -> usize {
    let started = statements
        .iter()
        .rposition(|statement| statement == "begin immediate")
        .unwrap_or(0);

    statements.len() - started
}

/// Unlocking a resource held by someone else deletes nothing, not even the caller's own locks in
/// the same request (spec §4.5).
///
/// The whole request is one transaction: either every resource it names is released or none is.
/// A store that deleted as it went would leave the caller's own lock gone and still report the
/// failure.
#[tokio::test]
async fn unlocking_a_resource_held_by_someone_else_deletes_nothing() {
    let Some(client) = support::client("a mixed unlock").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let owner = support::random_owner();
    let other = support::random_owner();
    let repository = support::random_partition();
    let mine = random_resource();
    let theirs = random_resource();

    support::take_locks(&store, &owner, repository, std::slice::from_ref(&mine))
        .await
        .expect("our own resource is free");
    support::take_locks(&store, &other, repository, std::slice::from_ref(&theirs))
        .await
        .expect("their resource is free");

    let error = support::release_locks(
        &store,
        &owner,
        true,
        repository,
        &[mine.clone(), theirs.clone()],
    )
    .await
    .expect_err("a resource held by someone else cannot be released");

    assert!(
        matches!(error, LockError::LockNotOwned(_)),
        "the caller is told the resource is not theirs: {error:?}"
    );

    assert_eq!(
        holders(&store, repository, &mine).await,
        1,
        "our own lock is still ours: the request was one unit"
    );
    assert_eq!(
        holders(&store, repository, &theirs).await,
        1,
        "their lock is untouched"
    );
}

/// Unlocking a resource nobody holds is not found, which is not the same as not owned.
#[tokio::test]
async fn unlocking_a_resource_nobody_holds_is_not_found() {
    let Some(client) = support::client("unlocking what nobody holds").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));

    let error = support::release_locks(
        &store,
        &support::random_owner(),
        true,
        support::random_partition(),
        &[random_resource()],
    )
    .await
    .expect_err("there is nothing to release");

    assert!(
        matches!(error, LockError::LockNotFound(_)),
        "a resource that was never locked is not a resource someone else holds: {error:?}"
    );
}

/// Unlocking without checking the owner releases whatever is there, and is happy when nothing is.
#[tokio::test]
async fn unlocking_without_checking_the_owner_releases_what_is_there() {
    let Some(client) = support::client("an unchecked unlock").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let owner = support::random_owner();
    let repository = support::random_partition();
    let resource = random_resource();

    support::take_locks(&store, &owner, repository, std::slice::from_ref(&resource))
        .await
        .expect("the resource is free");

    let released = support::release_locks(
        &store,
        "a-different-user",
        false,
        repository,
        std::slice::from_ref(&resource),
    )
    .await
    .expect("an unchecked unlock does not ask who is asking");

    assert_eq!(released, vec![resource.clone()]);
    assert_eq!(holders(&store, repository, &resource).await, 0);

    let released = support::release_locks(
        &store,
        "a-different-user",
        false,
        repository,
        std::slice::from_ref(&resource),
    )
    .await
    .expect("releasing what is already released is not an error");

    assert_eq!(released, vec![resource]);
}

// =============================================================================
// Querying
// =============================================================================

/// Every query variant selects the locks it names and no others.
#[tokio::test]
async fn the_query_variants_select_what_they_name() {
    let Some(client) = support::client("the lock queries").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let owner = support::random_owner();
    let stranger = support::random_owner();
    let repository = support::random_partition();
    let elsewhere = support::random_partition();
    let first = resource(support::random_context(), support::random_hash());
    let second = resource(support::random_context(), support::random_hash());
    let far = resource(support::random_context(), support::random_hash());

    support::take_locks(&store, &owner, repository, &[first.clone(), second.clone()])
        .await
        .expect("the resources are free");
    support::take_locks(&store, &stranger, elsewhere, std::slice::from_ref(&far))
        .await
        .expect("the far resource is free");

    let contains = |locks: &[LockData], resource: &LockResource| {
        locks
            .iter()
            .any(|lock| &lock.resource == resource && lock.owner == owner)
    };

    let locks = held(&store, LockQuery::Hash(first.hash)).await;
    assert_eq!(locks.len(), 1);
    assert!(contains(&locks, &first));

    let locks = held(&store, LockQuery::HashRepository(first.hash, repository)).await;
    assert_eq!(locks.len(), 1);
    assert!(contains(&locks, &first));
    assert!(
        held(&store, LockQuery::HashRepository(first.hash, elsewhere))
            .await
            .is_empty(),
        "a hash in another repository is a different lock"
    );

    let locks = held(
        &store,
        LockQuery::HashRepositoryBranch(first.hash, repository, first.branch),
    )
    .await;
    assert_eq!(locks.len(), 1);
    assert!(contains(&locks, &first));
    assert!(
        held(
            &store,
            LockQuery::HashRepositoryBranch(first.hash, repository, second.branch)
        )
        .await
        .is_empty(),
        "a hash on another branch is a different lock"
    );

    let locks = held(&store, LockQuery::Owner(owner.clone())).await;
    assert_eq!(locks.len(), 2);
    assert!(
        held(&store, LockQuery::Owner(stranger.clone())).await.len() == 1,
        "another owner's locks are reported to them"
    );

    let locks = held(
        &store,
        LockQuery::OwnerRepository(owner.clone(), repository),
    )
    .await;
    assert_eq!(locks.len(), 2);
    assert!(
        held(&store, LockQuery::OwnerRepository(owner.clone(), elsewhere))
            .await
            .is_empty()
    );

    let locks = held(
        &store,
        LockQuery::OwnerRepositoryBranch(owner.clone(), repository, first.branch),
    )
    .await;
    assert_eq!(locks.len(), 1);
    assert!(contains(&locks, &first));

    let locks = held(&store, LockQuery::Repository(repository)).await;
    assert_eq!(locks.len(), 2);
    assert_eq!(
        held(&store, LockQuery::Repository(elsewhere)).await.len(),
        1,
        "another repository's locks belong to it"
    );

    let locks = held(
        &store,
        LockQuery::RepositoryBranch(repository, first.branch),
    )
    .await;
    assert_eq!(locks.len(), 1);
    assert!(contains(&locks, &first));

    let locks = held(
        &store,
        LockQuery::RepositoryBranchDescription(repository, first.branch, first.description.clone()),
    )
    .await;
    assert_eq!(locks.len(), 1);
    assert!(contains(&locks, &first));
    assert!(
        held(
            &store,
            LockQuery::RepositoryBranchDescription(
                repository,
                first.branch,
                second.description.clone()
            )
        )
        .await
        .is_empty(),
        "the description is part of what is asked for"
    );
}

/// Checking a set of resources reports the ones that are locked and stays quiet about the rest.
#[tokio::test]
async fn checking_a_set_reports_the_locks_that_are_there() {
    let Some(client) = support::client("checking a set of resources").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let owner = support::random_owner();
    let repository = support::random_partition();
    let first = random_resource();
    let second = random_resource();
    let third = random_resource();

    support::take_locks(&store, &owner, repository, &[first.clone(), second.clone()])
        .await
        .expect("the resources are free");

    let checked = store
        .check_locks_status(repository, &[first.clone(), second.clone(), third.clone()])
        .await
        .expect("a check is answerable");

    assert_eq!(checked.len(), 2, "only the locked ones are reported");
    for lock in &checked {
        assert_eq!(lock.owner, owner);
        assert!(lock.resource == first || lock.resource == second);
    }
}

/// Checking the same resource twice reports it once.
#[tokio::test]
async fn checking_collapses_duplicates() {
    let Some(client) = support::client("checking a resource twice").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let owner = support::random_owner();
    let repository = support::random_partition();
    let resource = random_resource();

    support::take_locks(&store, &owner, repository, std::slice::from_ref(&resource))
        .await
        .expect("the resource is free");

    let checked = store
        .check_locks_status(
            repository,
            &[resource.clone(), resource.clone(), resource.clone()],
        )
        .await
        .expect("a check is answerable");

    assert_eq!(checked.len(), 1);
    assert_eq!(checked[0].resource, resource);
}

/// A check of more resources than fit in one statement still answers for all of them.
#[tokio::test]
async fn checking_answers_past_one_chunk() {
    let Some(client) = support::client("checking more than one chunk").await else {
        return;
    };

    let store = support::lock_store(Arc::new(client));
    let owner = support::random_owner();
    let repository = support::random_partition();

    // One past the chunk the store sends at a time, so that the answer comes from two statements.
    let resources: Vec<LockResource> = (0..51).map(|_| random_resource()).collect();

    for resource in &resources {
        support::take_locks(&store, &owner, repository, std::slice::from_ref(resource))
            .await
            .expect("each resource is free");
    }

    let checked = store
        .check_locks_status(repository, &resources)
        .await
        .expect("a check is answerable");

    assert_eq!(checked.len(), resources.len());
    for resource in &resources {
        assert!(
            checked.iter().any(|lock| lock.resource == *resource),
            "every resource is answered for"
        );
    }
}

// =============================================================================
// Failing
// =============================================================================

/// A lock whose `COMMIT` answer was lost is not reissued, and its outcome is not guessed at
/// (spec §4.6).
///
/// The locks are taken on the server. Reissuing would report every one of them as newly taken,
/// because a lock that was already ours and a lock we just took look the same, so the caller is
/// told the outcome is unknown.
#[tokio::test]
async fn a_lock_whose_commit_answer_was_lost_is_not_reissued() {
    let Some(client) = support::client("a lock under a lost commit").await else {
        return;
    };

    let log = Arc::new(StatementLog::default());
    let executor: Arc<dyn TursoExecutor> =
        Arc::new(LostCommitExecutor::new(Arc::new(client), log.clone()));
    let store = support::lock_store(executor);

    let owner = support::random_owner();
    let repository = support::random_partition();
    let resource = random_resource();

    let error = store
        .lock_resources(&owner, repository, std::slice::from_ref(&resource))
        .await
        .expect_err("the caller has to be told that the answer was lost");

    assert!(
        matches!(error, LockError::SlowDown(_)),
        "a lock has no way to say the outcome is unknown, so the caller is told to try later: {error:?}"
    );

    let statements = log.take();
    assert_eq!(
        statements.iter().filter(|s| s.starts_with("begin")).count(),
        1,
        "a lock is taken once: {statements:?}"
    );
    assert!(
        statements.iter().any(|s| s == "commit"),
        "the attempt has to reach the commit this test is about: a server that refused the write \
         lock to it leaves nothing here to be lost: {statements:?}"
    );
    assert!(
        statements.last().is_some_and(|s| s == "commit"),
        "nothing may follow the commit that was lost: {statements:?}"
    );

    assert_eq!(
        holders(&store, repository, &resource).await,
        1,
        "the server applied the lock the caller is unsure about"
    );
}

/// An unlock whose `COMMIT` answer was lost is not reissued either (spec §4.6).
#[tokio::test]
async fn an_unlock_whose_commit_answer_was_lost_is_not_reissued() {
    let Some(client) = support::client("an unlock under a lost commit").await else {
        return;
    };

    let owner = support::random_owner();
    let repository = support::random_partition();
    let resource = random_resource();

    let plain: Arc<dyn TursoExecutor> = Arc::new(client);
    let plain_store = support::lock_store(plain.clone());
    support::take_locks(
        &plain_store,
        &owner,
        repository,
        std::slice::from_ref(&resource),
    )
    .await
    .expect("the resource is free");

    let log = Arc::new(StatementLog::default());
    let executor: Arc<dyn TursoExecutor> =
        Arc::new(LostCommitExecutor::new(plain.clone(), log.clone()));
    let store = support::lock_store(executor);

    let error = store
        .unlock_resources(&owner, true, repository, std::slice::from_ref(&resource))
        .await
        .expect_err("the caller has to be told that the answer was lost");

    assert!(
        matches!(error, LockError::SlowDown(_)),
        "a lost answer is not a resource that was not there: {error:?}"
    );

    let statements = log.take();
    assert_eq!(
        statements.iter().filter(|s| s.starts_with("begin")).count(),
        1,
        "an unlock happens once: {statements:?}"
    );
    assert!(
        statements.iter().any(|s| s == "commit"),
        "the attempt has to reach the commit this test is about: a server that refused the write \
         lock to it leaves nothing here to be lost: {statements:?}"
    );
    assert!(
        statements.last().is_some_and(|s| s == "commit"),
        "nothing may follow the commit that was lost: {statements:?}"
    );

    assert_eq!(
        holders(&store, repository, &resource).await,
        0,
        "the server released the lock the caller is unsure about"
    );
}

/// A query is reissued inside its limit and gives up beyond it (spec §4.6).
#[tokio::test]
async fn a_query_is_reissued_inside_its_limit_and_gives_up_beyond_it() {
    let Some(client) = support::client("a lock query against a flaky server").await else {
        return;
    };

    let real: Arc<dyn TursoExecutor> = Arc::new(client);
    let log = Arc::new(StatementLog::default());
    let limit = support::limits().retry.limit;

    let owner = support::random_owner();
    let repository = support::random_partition();
    let resource = random_resource();

    let real_store = support::lock_store(real.clone());
    support::take_locks(
        &real_store,
        &owner,
        repository,
        std::slice::from_ref(&resource),
    )
    .await
    .expect("the resource is free");

    for failures in [1, limit, limit + 1] {
        let executor: Arc<dyn TursoExecutor> =
            Arc::new(FlakyExecutor::new(real.clone(), failures, log.clone()));
        let store = support::lock_store(executor);
        let found = store
            .query_locks(LockQuery::HashRepositoryBranch(
                resource.hash,
                repository,
                resource.branch,
            ))
            .await;

        if failures <= limit {
            assert_eq!(
                found
                    .unwrap_or_else(|error| panic!(
                        "{failures} failures is inside the limit of {limit}: {error:?}"
                    ))
                    .len(),
                1,
                "{failures} failures is inside the limit of {limit}"
            );
        } else {
            assert!(
                matches!(found, Err(LockError::SlowDown(_))),
                "{failures} failures is past the limit of {limit}: {found:?}"
            );
        }
    }
}
