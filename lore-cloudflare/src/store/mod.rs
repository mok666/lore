// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The stores Turso holds: the mutable key-value store, and the lock store.
//!
//! Both speak SQL through a [`TursoExecutor`](crate::turso::TursoExecutor), so both can be run
//! against a server in a test without a server, and neither knows how the connection is made.

pub mod lock_store;
pub mod mutable_store;

pub use lock_store::CloudflareLockStore;
pub use mutable_store::CloudflareMutableStore;

use libsql::Value;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_storage::KeyType;
use lore_storage::StoreError;

use crate::turso::TursoTransaction;

/// The bytes in a hash, which is how a stored hash is measured.
pub(crate) const HASH_LEN: usize = std::mem::size_of::<Hash>();
/// The bytes in a partition, which is the width of the zero partition that means "every
/// repository".
pub(crate) const PARTITION_LEN: usize = std::mem::size_of::<Partition>();

/// Put the key type in the first byte of the key, so that one table can hold every kind of key
/// without two kinds colliding. The AWS store established the convention, and both stores here
/// follow it.
pub(crate) fn typed_key(mut key: Hash, key_type: KeyType) -> Hash {
    key.data_mut()[0] = key_type as u8;
    key
}

/// The key type as the one byte `list` filters on.
///
/// A one-byte blob, not an integer: the filter compares it with `substr(key, 1, 1)`, which is a
/// blob, and an integer never equals a blob.
pub(crate) fn key_type_blob(key_type: KeyType) -> Value {
    Value::Blob(vec![key_type as u8])
}

pub(crate) fn hash_blob(hash: &Hash) -> Value {
    Value::Blob(hash.data().to_vec())
}

pub(crate) fn partition_blob(partition: Partition) -> Value {
    Value::Blob(partition.data().to_vec())
}

pub(crate) fn context_blob(context: Context) -> Value {
    Value::Blob(context.data().to_vec())
}

/// The repository and the branch as the one key a lock row is stored under.
pub(crate) fn repository_branch_blob(repository: Partition, branch: Context) -> Value {
    let mut bytes = Vec::with_capacity(PARTITION_LEN * 2);
    bytes.extend_from_slice(repository.data());
    bytes.extend_from_slice(branch.data());
    Value::Blob(bytes)
}

/// The current time in milliseconds since the epoch, which is how a lock records when it was
/// taken.
pub(crate) fn now_millis() -> u64 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();

    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}

/// An integer column as a count of milliseconds since the epoch.
pub(crate) fn integer_to_millis(column: &str, value: &Value) -> Result<u64, StoreError> {
    match value {
        Value::Integer(millis) => u64::try_from(*millis).map_err(|_| {
            StoreError::internal(format!("column {column} was {millis}, which is before the epoch"))
        }),
        other => Err(StoreError::internal(format!(
            "column {column} was {other:?}, not a count of milliseconds"
        ))),
    }
}

/// The column of a row, or an error saying the query returned a shape this build does not know.
pub(crate) fn cell<'a>(
    row: &'a [Value],
    index: usize,
    query: &str,
) -> Result<&'a Value, StoreError> {
    row.get(index).ok_or_else(|| {
        StoreError::internal(format!(
            "a row of `{query}` held fewer columns than the query selects"
        ))
    })
}

/// A 32-byte blob column as a hash.
///
/// A column of another shape means the row was not written by this crate, which is a store
/// inconsistency rather than an absent value.
pub(crate) fn blob_to_hash(column: &str, value: &Value) -> Result<Hash, StoreError> {
    match value {
        Value::Blob(bytes) if bytes.len() == HASH_LEN => {
            let mut hash = Hash::default();
            hash.data_mut().copy_from_slice(bytes);
            Ok(hash)
        }
        other => Err(StoreError::internal(format!(
            "column {column} was {other:?}, not a {HASH_LEN}-byte hash"
        ))),
    }
}

/// A 16-byte blob column as a partition.
pub(crate) fn blob_to_partition(column: &str, value: &Value) -> Result<Partition, StoreError> {
    match value {
        Value::Blob(bytes) if bytes.len() == PARTITION_LEN => {
            let mut partition = Partition::default();
            partition.data_mut().copy_from_slice(bytes);
            Ok(partition)
        }
        other => Err(StoreError::internal(format!(
            "column {column} was {other:?}, not a {PARTITION_LEN}-byte partition"
        ))),
    }
}

/// A 16-byte blob column as a branch.
pub(crate) fn blob_to_context(column: &str, value: &Value) -> Result<Context, StoreError> {
    match value {
        Value::Blob(bytes) if bytes.len() == PARTITION_LEN => {
            let mut branch = Context::default();
            branch.data_mut().copy_from_slice(bytes);
            Ok(branch)
        }
        other => Err(StoreError::internal(format!(
            "column {column} was {other:?}, not a {PARTITION_LEN}-byte branch"
        ))),
    }
}

/// A blob column that holds a description or an owner, as text.
pub(crate) fn blob_to_string(column: &str, value: &Value) -> Result<String, StoreError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(StoreError::internal(format!(
            "column {column} was {other:?}, not text"
        ))),
    }
}

/// Throw away a transaction without letting its failure hide the reason the transaction is being
/// thrown away.
///
/// Used wherever nothing was written, or where the outcome is already decided: the caller has
/// something to report that matters more than a rollback that did not answer.
pub(crate) async fn discard(transaction: Box<dyn TursoTransaction>, reason: &str) {
    if let Err(error) = transaction.rollback().await {
        tracing::warn!(
            %error,
            reason,
            "could not roll back a transaction; the outcome being reported stands"
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn a_key_type_is_stored_in_the_first_byte_of_its_key() {
        let key = Hash::from_u64(0x0102_0304_0506_0708);
        let typed = typed_key(key, KeyType::BranchLatestPointer);

        assert_eq!(typed.data()[0], KeyType::BranchLatestPointer as u8);
        assert_eq!(&typed.data()[1..], &key.data()[1..]);

        // The key itself is untouched: the caller's copy still has its original first byte.
        assert_eq!(key.data()[0], 0x08);
    }

    #[test]
    fn a_key_type_crosses_as_one_byte() {
        assert_eq!(
            key_type_blob(KeyType::Resolve),
            Value::Blob(vec![KeyType::Resolve as u8])
        );
    }

    #[test]
    fn a_hash_round_trips_through_its_column() {
        let hash = Hash::hash_buffer(b"value");
        assert_eq!(blob_to_hash("value", &hash_blob(&hash)).unwrap(), hash);
    }

    #[test]
    fn a_partition_round_trips_through_its_column() {
        let mut partition = Partition::default();
        partition.data_mut()[0] = 7;

        assert_eq!(
            blob_to_partition("repository_id", &partition_blob(partition)).unwrap(),
            partition
        );
    }

    #[test]
    fn a_column_of_the_wrong_shape_is_an_internal_error() {
        let error = blob_to_hash("value", &Value::Integer(7)).unwrap_err();
        assert!(format!("{error}").contains("not a 32-byte hash"));

        let error = blob_to_partition("repository_id", &Value::Null).unwrap_err();
        assert!(format!("{error}").contains("not a 16-byte partition"));

        let error = blob_to_context("branch", &Value::Integer(7)).unwrap_err();
        assert!(format!("{error}").contains("not a 16-byte branch"));

        let error = integer_to_millis("locked_at", &Value::Text("now".to_string())).unwrap_err();
        assert!(format!("{error}").contains("not a count of milliseconds"));

        let error = integer_to_millis("locked_at", &Value::Integer(-1)).unwrap_err();
        assert!(format!("{error}").contains("before the epoch"));
    }

    #[test]
    fn a_branch_round_trips_through_its_column() {
        let mut branch = Context::default();
        branch.data_mut()[0] = 3;

        assert_eq!(blob_to_context("branch", &context_blob(branch)).unwrap(), branch);
    }

    #[test]
    fn a_lock_key_is_the_repository_followed_by_the_branch() {
        let mut repository = Partition::default();
        repository.data_mut()[0] = 1;
        let mut branch = Context::default();
        branch.data_mut()[0] = 2;

        let mut expected = repository.data().to_vec();
        expected.extend_from_slice(branch.data());

        assert_eq!(repository_branch_blob(repository, branch), Value::Blob(expected));
    }
}
