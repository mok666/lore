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
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::FRAGMENT_SIZE_THRESHOLD;
use lore_base::types::Fragment;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_cloudflare::R2Error;
use lore_storage::ImmutableStore;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_storage::conformance::Capabilities;
use lore_storage::conformance::verify_immutable_store;

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
