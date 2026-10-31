// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The mutable store, held in one Turso table (spec §4.3).
//!
//! One row per key, and the first byte of the key names its [`KeyType`]. A zero value is not a row:
//! storing one removes the row, a compare and swap that writes one removes the row, and every read
//! treats such a row as no value at all. That keeps `load`'s contract — a zero value is the same as
//! no value — and `store`'s in one rule.

use std::sync::Arc;

use async_trait::async_trait;
use libsql::Value;
use lore_base::error::AddressNotFound;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_storage::KeyType;
use lore_storage::KeyValueStream;
use lore_storage::MutableStore;
use lore_storage::StoreError;
use tokio::sync::mpsc::UnboundedSender;

use crate::store::blob_to_hash;
use crate::store::cell;
use crate::store::discard;
use crate::store::hash_blob;
use crate::store::key_type_blob;
use crate::store::partition_blob;
use crate::store::typed_key;
use crate::turso::TursoError;
use crate::turso::TursoExecutor;
use crate::turso::TursoLimits;
use crate::turso::TursoRow;
use crate::turso::retry::retry_within_deadline;
use crate::turso::retry::within_deadline;

/// How many rows one page of `list` asks for.
///
/// `list` has no cursor in its signature, so the store pages behind it: a smaller page means more
/// round trips, a larger one more memory held at once.
pub const LIST_PAGE_SIZE: usize = 1000;

/// The value stored for a key, if it holds one. A row holding a zero value is not a row.
const LOAD_SQL: &str = "SELECT value FROM mutable \
     WHERE repository_id = ?1 AND key = ?2 AND value <> zeroblob(32)";

/// Write a value, replacing whatever was there.
const STORE_SQL: &str = "INSERT INTO mutable (repository_id, key, value) VALUES (?1, ?2, ?3) \
     ON CONFLICT (repository_id, key) DO UPDATE SET value = excluded.value";

/// Remove a key, whether or not it is there.
const DELETE_SQL: &str = "DELETE FROM mutable WHERE repository_id = ?1 AND key = ?2";

/// One page of a key type's keys, in the order the primary key holds them.
///
/// The cursor is the `(repository_id, key)` pair of the previous page's last row, and the
/// comparison is in that same order, so that with a null partition — every repository at once —
/// two repositories holding the same key cannot make a page boundary lose a row.
///
/// A null partition is 16 zero bytes, and a null cursor is a null parameter rather than a zero
/// pair, because a row's repository may legitimately be zero.
const LIST_SQL: &str = "SELECT repository_id, key, value FROM mutable \
     WHERE (?1 = zeroblob(16) OR repository_id = ?1) \
       AND value <> zeroblob(32) \
       AND substr(key, 1, 1) = ?2 \
       AND (?3 IS NULL OR (repository_id, key) > (?3, ?4)) \
     ORDER BY repository_id, key \
     LIMIT ?5";

/// A mutable key-value store in a Turso database.
pub struct CloudflareMutableStore {
    executor: Arc<dyn TursoExecutor>,
    limits: TursoLimits,
}

impl CloudflareMutableStore {
    pub fn new(executor: Arc<dyn TursoExecutor>, limits: TursoLimits) -> Self {
        Self { executor, limits }
    }

    /// The value stored for a key, or `None` when no value is stored.
    ///
    /// The contract's `AddressNotFound` is for the caller: `list` and a compare and swap both need
    /// the absence itself.
    async fn load_value(
        &self,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Option<Hash>, StoreError> {
        let key = typed_key(key, key_type);

        let rows = retry_within_deadline!(
            self.limits,
            self.executor
                .query(LOAD_SQL, vec![partition_blob(partition), hash_blob(&key)])
        )
        .map_err(|error| error.into_store_error("loading a mutable value"))?;

        match rows.first() {
            Some(row) => Ok(Some(blob_to_hash("value", cell(row, 0, LOAD_SQL)?)?)),
            None => Ok(None),
        }
    }

    /// Write a value, or remove the key when the value is zero.
    ///
    /// One statement, so no transaction is needed, and writing the same value again leaves the
    /// same table: a lost answer is safe to reissue.
    async fn store_value(
        &self,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError> {
        let key = typed_key(key, key_type);

        let (sql, parameters) = if value.is_zero() {
            (DELETE_SQL, vec![partition_blob(partition), hash_blob(&key)])
        } else {
            (
                STORE_SQL,
                vec![
                    partition_blob(partition),
                    hash_blob(&key),
                    hash_blob(&value),
                ],
            )
        };

        // The macro may run the operation again, and a value that was moved into the first
        // attempt is not there for the second, so the parameters are handed over afresh.
        retry_within_deadline!(self.limits, self.executor.execute(sql, parameters.clone()))
            .map_err(|error| error.into_store_error("storing a mutable value"))?;

        Ok(())
    }

    /// Swap a value when the current one matches, inside the store's deadline.
    ///
    /// Reading the current value and writing in separate transactions would let a write between
    /// them make the decision stale, so the transaction takes the write lock at its first
    /// statement and holds it across both. That lock is also why the whole of it is bounded: the
    /// driver waits on a transaction for as long as the server does, and a swap that waited there
    /// would be waiting on another client rather than on the database (spec §4.1).
    ///
    /// A lost `COMMIT` is not retried and the answer is not read back: another client may have
    /// written since, so the outcome is not knowable. The caller gets `Disconnected` and decides.
    async fn compare_and_swap_value(
        &self,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        within_deadline(
            self.limits.timeout,
            |error| error.into_store_error("swapping a value"),
            self.compare_and_swap_transaction(partition, key, expected, value, key_type),
        )
        .await
    }

    /// Swap a value when the current one matches: one transaction, bounded by its caller.
    async fn compare_and_swap_transaction(
        &self,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        let key = typed_key(key, key_type);
        let partition_parameter = partition_blob(partition);
        let key_parameter = hash_blob(&key);

        let transaction = self
            .executor
            .begin_immediate()
            .await
            .map_err(|error| error.into_store_error("opening a transaction to swap a value"))?;

        let rows = transaction
            .query(
                LOAD_SQL,
                vec![partition_parameter.clone(), key_parameter.clone()],
            )
            .await
            .map_err(|error| error.into_store_error("reading the current value to swap"));

        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => {
                discard(transaction, "could not read the current value").await;
                return Err(error);
            }
        };

        let current = match rows.first() {
            Some(row) => Some(blob_to_hash("value", cell(row, 0, LOAD_SQL)?)?),
            None => None,
        };

        // A row holding a zero value is not a row, so an absent key matches a zero expectation and
        // nothing else.
        let matches = match current {
            Some(current) => current == expected,
            None => expected.is_zero(),
        };

        if !matches {
            // Nothing was written, so the outcome is settled: what is there is what the caller
            // asked for, and the answer is the current value.
            discard(transaction, "the current value did not match").await;
            return Ok(current.unwrap_or_default());
        }

        let (sql, parameters) = if value.is_zero() {
            (
                DELETE_SQL,
                vec![partition_parameter.clone(), key_parameter.clone()],
            )
        } else {
            (
                STORE_SQL,
                vec![
                    partition_parameter.clone(),
                    key_parameter.clone(),
                    hash_blob(&value),
                ],
            )
        };

        if let Err(error) = transaction.execute(sql, parameters).await {
            discard(transaction, "could not write the new value").await;
            return Err(error.into_store_error("writing the new value of a swap"));
        }

        transaction
            .commit()
            .await
            .map_err(|error| error.into_store_error("committing a swap"))?;

        Ok(expected)
    }

    /// One page of keys and values, in primary key order.
    async fn list_page(
        &self,
        partition: Partition,
        key_type: KeyType,
        cursor: Option<(Partition, Hash)>,
    ) -> Result<Vec<TursoRow>, TursoError> {
        let (cursor_repository, cursor_key) = match cursor {
            Some((repository, key)) => (partition_blob(repository), hash_blob(&key)),
            None => (Value::Null, Value::Null),
        };

        self.executor
            .query(
                LIST_SQL,
                vec![
                    partition_blob(partition),
                    key_type_blob(key_type),
                    cursor_repository,
                    cursor_key,
                    Value::Integer(LIST_PAGE_SIZE as i64),
                ],
            )
            .await
    }

    /// Every page of a key type's keys, sent as they arrive.
    ///
    /// The caller may stop reading, in which case the send fails and there is nothing left to do.
    async fn list_pages(
        &self,
        partition: Partition,
        key_type: KeyType,
        sender: &UnboundedSender<(Hash, Hash)>,
    ) -> Result<(), StoreError> {
        let mut cursor = None;

        loop {
            let rows =
                retry_within_deadline!(self.limits, self.list_page(partition, key_type, cursor))
                    .map_err(|error| error.into_store_error("listing a mutable store's keys"))?;

            let more = rows.len() == LIST_PAGE_SIZE;

            for row in &rows {
                let repository =
                    crate::store::blob_to_partition("repository_id", cell(row, 0, LIST_SQL)?)?;
                let key = blob_to_hash("key", cell(row, 1, LIST_SQL)?)?;
                let value = blob_to_hash("value", cell(row, 2, LIST_SQL)?)?;

                if sender.send((key, value)).is_err() {
                    return Ok(());
                }

                cursor = Some((repository, key));
            }

            if !more {
                return Ok(());
            }
        }
    }
}

#[async_trait]
impl MutableStore for CloudflareMutableStore {
    #[tracing::instrument(name = "CloudflareMutableStore::load", skip_all)]
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        match self.load_value(partition, key, key_type).await? {
            Some(value) => Ok(value),
            None => Err(StoreError::from(AddressNotFound::from(
                Address::zero_context_hash(key),
            ))),
        }
    }

    #[tracing::instrument(name = "CloudflareMutableStore::store", skip_all)]
    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError> {
        self.store_value(partition, key, value, key_type).await
    }

    #[tracing::instrument(name = "CloudflareMutableStore::compare_and_swap", skip_all)]
    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        self.compare_and_swap_value(partition, key, expected, value, key_type)
            .await
    }

    #[tracing::instrument(name = "CloudflareMutableStore::list", skip_all)]
    async fn list(
        self: Arc<Self>,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        let (stream, sender) = KeyValueStream::new();

        // Every key carries its type in its first byte, and `Untyped` is the type of a key that has
        // none, so asking for it names no rows. The local store answers the same way, and the
        // conformance harness checks it.
        if key_type == KeyType::Untyped {
            return Ok(stream);
        }

        let store = self.clone();
        lore_base::lore_spawn!(async move {
            if let Err(error) = store.list_pages(partition, key_type, &sender).await {
                // The stream is the only channel left, and half a list is not a list: say so and
                // end it, rather than failing a caller that has no error to receive.
                tracing::error!(
                    %error,
                    "listing a mutable store stopped early; the caller sees a short list"
                );
            }
        });

        Ok(stream)
    }

    #[tracing::instrument(name = "CloudflareMutableStore::flush", skip_all)]
    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        // Every statement is committed by the server before its answer comes back, so there is
        // nothing waiting here. The AWS store is a no-op for the same reason.
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn a_swap_reads_through_the_same_filter_as_a_load() {
        // A row holding a zero value must look absent to the swap as well as to the load, or the
        // swap would report a current value that no reader can see.
        assert!(LOAD_SQL.contains("value <> zeroblob(32)"));
        assert!(LIST_SQL.contains("value <> zeroblob(32)"));
    }

    #[test]
    fn a_list_cursor_is_ordered_the_way_the_primary_key_is() {
        assert!(LIST_SQL.contains("(repository_id, key) > (?3, ?4)"));
        assert!(LIST_SQL.contains("ORDER BY repository_id, key"));
    }
}
