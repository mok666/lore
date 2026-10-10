// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The immutable store against a live R2-compatible endpoint (`MinIO` in development) and a live
//! libSQL server.
//!
//! `the_store_satisfies_the_battery...` hands the store to the same battery every other
//! implementation of the trait is held to, with no known violations declared: a check that passes
//! here passes for the same reason it passes for the local store, and a store that is merely the
//! AWS one with different tables is told so by the same assertions.
//!
//! The rest covers what the battery cannot reach, and each test names the R2 behaviour it is really
//! about: an object that is gone rather than tombstoned (spec §3.3), an object whose description
//! cannot be read, an object larger than any fragment, and a missing payload the tables still
//! claim. The table side is the store's own business — the battery reads it through `query` — but
//! the bucket side is not visible from the trait at all, which is why these ask the endpoint.
#![cfg(feature = "integration_tests")]

mod support;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use libsql::Value;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::FRAGMENT_SIZE_THRESHOLD;
use lore_base::types::Fragment;
use lore_base::types::FragmentFlags;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_cloudflare::R2Error;
use lore_cloudflare::TursoExecutor;
use lore_storage::ImmutableStore;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_storage::conformance::Capabilities;
use lore_storage::conformance::verify_immutable_store;
use support::LostStatementAnswerExecutor;
use support::StatementLog;

/// The battery, as the store a deployment gets.
#[tokio::test]
async fn the_store_satisfies_the_battery_its_callers_rely_on() {
    let (Some(r2), Some(client)) = (
        support::r2("the immutable store battery").await,
        support::client("the immutable store battery").await,
    ) else {
        return;
    };

    let store = support::immutable_store(r2, Arc::new(client), false);

    verify_immutable_store(
        store,
        Capabilities::new("CloudflareImmutableStore/integration"),
    )
    .await;
}

/// Obliterating content takes the object out of the bucket, leaving no version behind.
///
/// The AWS store lists the object's versions and deletes them, because a bucket with versioning on
/// keeps the bytes of a delete as a tombstone; R2 has no versioning (spec §3.3), so one
/// `DeleteObject` is the whole job. This asks the endpoint directly, because the trait cannot tell
/// a store that removed the bytes from one that hid them.
#[tokio::test]
async fn an_obliteration_leaves_no_object_behind() {
    let (Some(r2), Some(client)) = (
        support::r2("an obliteration").await,
        support::client("an obliteration").await,
    ) else {
        return;
    };

    let store = support::immutable_store(r2.clone(), Arc::new(client), false);
    let (partition, address, payload, fragment) = a_fragment();
    let key = object_key(address.hash);

    put(&store, partition, address, fragment, &payload).await;

    assert!(
        r2.head_object(&key).await.is_ok(),
        "the object is in the bucket before it is obliterated"
    );

    store
        .clone()
        .obliterate(partition, address, Default::default())
        .await
        .expect("obliterating content that is there succeeds");

    let error = r2
        .head_object(&key)
        .await
        .expect_err("the object is gone, not merely unreferenced");

    assert!(
        matches!(error, R2Error::NotFound),
        "the endpoint says the key is not there, got: {error:?}"
    );

    // Reading it back is the same answer the bucket gave, not an internal error about a broken
    // object: the store cannot tell a delete it did from one it did not, and neither should a
    // caller.
    let matches = query(&store, partition, address).await;
    assert_eq!(matches, StoreMatch::MatchNone);
}

/// A payload the tables still claim, and the bucket no longer holds, is reported as gone and can be
/// stored again.
///
/// The two halves of a put are not one transaction, so an object can go missing underneath a row
/// that still points at it — a bucket emptied by an operator, an expired lifecycle rule, or an
/// obliteration that was half done. The caller has to be told the content is not there, and the row
/// has to stop vouching for it: left alone, it answers `Stored` and the next put of that content
/// takes the "already have it" path and writes nothing, so the bytes are lost for good.
#[tokio::test]
async fn a_payload_the_bucket_has_lost_can_be_stored_again() {
    let (Some(r2), Some(client)) = (
        support::r2("a lost payload").await,
        support::client("a lost payload").await,
    ) else {
        return;
    };

    let store = support::immutable_store(r2.clone(), Arc::new(client), false);
    let (partition, address, payload, fragment) = a_fragment();

    put(&store, partition, address, fragment, &payload).await;
    r2.delete_object(&object_key(address.hash))
        .await
        .expect("deleting the object underneath the row succeeds");

    let error = store
        .clone()
        .get(partition, address)
        .await
        .expect_err("the content is not there to serve");

    assert!(
        error.is_address_not_found(),
        "a missing payload is an address that is not found, got: {error:?}"
    );

    // The row no longer claims the content, so this is a first write rather than a re-write of
    // something the store believes it already has.
    put(&store, partition, address, fragment, &payload).await;

    let served = store
        .clone()
        .get(partition, address)
        .await
        .expect("the content stored again is served");

    assert_eq!(
        served.match_made,
        StoreMatch::MatchFull,
        "the content that was put back is found again"
    );
    let (_, served_payload) = served.into_payload().expect("a match carries its payload");
    assert_eq!(
        served_payload, payload,
        "the bytes served are the bytes that were put back"
    );

    assert!(
        r2.head_object(&object_key(address.hash)).await.is_ok(),
        "the object is in the bucket again"
    );
}

/// An object whose description cannot be read is an error, not an empty answer.
///
/// The fragment travels as object metadata, so an object without it is either not one of ours or
/// has lost the half that says what it is. Reporting a match with no fragment would hand a caller
/// bytes it has no sizes for; reporting no match would say the content is gone when it is sitting
/// right there.
#[tokio::test]
async fn an_object_without_a_description_is_not_read() {
    let (Some(r2), Some(client)) = (
        support::r2("an undescribed object").await,
        support::client("an undescribed object").await,
    ) else {
        return;
    };

    let store = support::immutable_store(r2.clone(), Arc::new(client), false);
    let (partition, address, payload, fragment) = a_fragment();

    put(&store, partition, address, fragment, &payload).await;

    // Overwrite the object with one that carries no fragment, leaving the tables pointing at it.
    r2.put_object(&object_key(address.hash), payload.clone(), HashMap::new())
        .await
        .expect("an object with no metadata can be written");

    let error = store
        .clone()
        .get(partition, address)
        .await
        .expect_err("an object with no fragment is not served");

    assert!(
        !error.is_address_not_found(),
        "the object is there; what is wrong with it is not that it is missing, got: {error:?}"
    );
    assert!(
        error.to_string().contains("metadata"),
        "the failure says which part could not be read, got: {error:?}"
    );
}

/// An object larger than any fragment is refused before its body is read.
///
/// A store that read it first would have to hold whatever it was given before it could decide,
/// which is the whole reason the check exists: the size is in the response `HeadObject`/`GetObject`
/// already returned, so nothing has to be buffered to say no.
#[tokio::test]
async fn an_object_larger_than_a_fragment_is_refused_without_reading_it() {
    let (Some(r2), Some(client)) = (
        support::r2("an oversized object").await,
        support::client("an oversized object").await,
    ) else {
        return;
    };

    let store = support::immutable_store(r2.clone(), Arc::new(client), false);
    let (partition, address, payload, fragment) = a_fragment();

    put(&store, partition, address, fragment, &payload).await;

    // One byte over the largest object a legitimate store could have written: the threshold plus
    // the fragment struct the oldest objects carried in front of their payload.
    let oversized = Bytes::from(vec![
        0u8;
        FRAGMENT_SIZE_THRESHOLD + size_of::<Fragment>() + 1
    ]);

    // The metadata describes the small payload that used to be there, so the size is the only thing
    // that is wrong with the object.
    let metadata = r2
        .head_object(&object_key(address.hash))
        .await
        .expect("the object is there");

    r2.put_object(&object_key(address.hash), oversized, metadata)
        .await
        .expect("an oversized object can be written");

    let error = store
        .clone()
        .get(partition, address)
        .await
        .expect_err("an object larger than any fragment is not served");

    assert!(
        error.to_string().contains("exceeds maximum"),
        "the failure names the limit it broke, got: {error:?}"
    );
}

// =============================================================================
// Obliteration
// =============================================================================

/// An address whose obliteration has started is absent from a batch as much as it is on its own.
///
/// The mark an obliteration takes comes before the payload is reclaimed, and the association that
/// is deleted first can still be seen from another connection in that window. A batch that decided
/// on the association alone would report the old content as a full match — bytes on their way out
/// of the bucket — while resolving the same address on its own reports it as gone, and this store
/// has no weaker level for a miss to fall back to (spec §3.3).
#[tokio::test]
async fn an_obliterating_address_is_absent_from_a_batch_too() {
    let (Some(r2), Some(client)) = (
        support::r2("an obliterating address").await,
        support::client("an obliterating address").await,
    ) else {
        return;
    };

    let client = Arc::new(client);
    let store = support::immutable_store(r2, client.clone(), false);

    let partition = support::random_partition();
    let (_, obliterating, payload, fragment) = a_fragment();
    let (_, stored, stored_payload, stored_fragment) = a_fragment();
    let (_, absent, _, _) = a_fragment();

    put(&store, partition, obliterating, fragment, &payload).await;
    put(&store, partition, stored, stored_fragment, &stored_payload).await;

    // The mark an obliteration takes, and nothing else: the association is still there, which is
    // the state a batch has to be able to read as "not content".
    client
        .execute(
            "UPDATE fragment_state SET state = ?1 WHERE hash = ?2",
            vec![
                Value::Integer(i64::from(FragmentFlags::PayloadObliterating.bits())),
                Value::Blob(obliterating.hash.data().to_vec()),
            ],
        )
        .await
        .expect("the state row can be moved to the mark an obliteration takes");

    let mut results = [
        StoreMatchResult::default(),
        StoreMatchResult::default(),
        StoreMatchResult::default(),
    ];

    store
        .clone()
        .query(partition, &[obliterating, stored, absent], &mut results)
        .await
        .expect("resolving a batch succeeds");

    assert_eq!(
        results.map(|result| result.match_made),
        [
            StoreMatch::MatchNone,
            StoreMatch::MatchFull,
            StoreMatch::MatchNone
        ],
        "the address being obliterated and the address nobody stored are both absent, and the \
         address that is stored is a full match"
    );

    assert_eq!(
        query(&store, partition, obliterating).await,
        StoreMatch::MatchNone,
        "one address at a time gives the same answer as the batch it was in"
    );
}

/// An obliteration whose mark was taken without an answer still finishes.
///
/// The mark is taken by a statement that says "move it if it is still where I read it", and the
/// answer to it can be lost after the server applied it. Nothing can read that back: a moved row
/// and another writer's move look the same afterwards, so a store that gave up would leave the
/// hash marked as being obliterated forever — every write of it answers `SlowDown`, and no put can
/// take it back (spec §4.6). The statement is reissued instead, which is what turns the moved row
/// into an answer.
#[tokio::test]
async fn an_obliteration_survives_a_lost_answer_to_its_mark() {
    let (Some(r2), Some(client)) = (
        support::r2("an obliteration whose mark answer was lost").await,
        support::client("an obliteration whose mark answer was lost").await,
    ) else {
        return;
    };

    let client = Arc::new(client);
    let log = Arc::new(StatementLog::default());
    let executor: Arc<dyn TursoExecutor> = Arc::new(LostStatementAnswerExecutor::new(
        client.clone(),
        log.clone(),
        "UPDATE fragment_state",
    ));
    let store = support::immutable_store(r2.clone(), executor, false);
    let (partition, address, payload, fragment) = a_fragment();
    let key = object_key(address.hash);

    put(&store, partition, address, fragment, &payload).await;

    store
        .clone()
        .obliterate(partition, address, Default::default())
        .await
        .expect("an obliteration whose mark was taken without an answer still finishes");

    let error = r2
        .head_object(&key)
        .await
        .expect_err("the object is gone, not merely unreferenced");
    assert!(matches!(error, R2Error::NotFound), "got: {error:?}");

    assert_eq!(
        query(&store, partition, address).await,
        StoreMatch::MatchNone,
        "the obliteration is complete: the content is not served from anywhere"
    );

    let state = client
        .query(
            "SELECT state FROM fragment_state WHERE hash = ?1",
            vec![Value::Blob(address.hash.data().to_vec())],
        )
        .await
        .expect("the state row can be read");

    assert_eq!(
        state.first().map(|row| row[0].clone()),
        Some(Value::Integer(i64::from(
            FragmentFlags::PayloadObliterated.bits()
        ))),
        "the hash is not left marked as being obliterated"
    );

    // The mark, lost as it was taken, reissued, and then the move that ends the obliteration: one
    // statement per step, which is what the store spends and nothing more.
    assert_eq!(
        log.count("execute UPDATE fragment_state"),
        3,
        "{:?}",
        log.take()
    );
}

// =============================================================================
// Fixtures
// =============================================================================

/// Content no other test has written, with the fragment describing it.
fn a_fragment() -> (Partition, Address, Bytes, Fragment) {
    let payload = Bytes::from(rand::random::<[u8; 32]>().to_vec());
    let address = Address {
        hash: lore_storage::hash_slice(payload.as_ref()),
        context: Context::from(rand::random::<[u8; 16]>()),
    };
    let fragment = Fragment {
        flags: 0,
        size_payload: payload.len() as u32,
        size_content: payload.len() as u64,
    };

    (
        Partition::from(rand::random::<[u8; 16]>()),
        address,
        payload,
        fragment,
    )
}

/// Where a hash lives in the bucket.
fn object_key(hash: Hash) -> String {
    lore_cloudflare::CloudflareImmutableStore::object_key(hash)
}

/// Store content with its payload, the way a caller does.
async fn put(
    store: &Arc<lore_cloudflare::CloudflareImmutableStore>,
    partition: Partition,
    address: Address,
    fragment: Fragment,
    payload: &Bytes,
) {
    store
        .clone()
        .put(partition, address, fragment, Some(payload.clone()), false)
        .await
        .expect("storing content succeeds");
}

/// Resolve one address the way the battery does.
async fn query(
    store: &Arc<lore_cloudflare::CloudflareImmutableStore>,
    partition: Partition,
    address: Address,
) -> StoreMatch {
    let mut results = [StoreMatchResult::default()];

    store
        .clone()
        .query(partition, &[address], &mut results)
        .await
        .expect("resolving one address succeeds");

    results[0].match_made
}
