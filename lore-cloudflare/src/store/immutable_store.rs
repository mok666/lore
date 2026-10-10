// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The immutable store: payloads in R2, and their lifecycle in Turso (spec §3.1).
//!
//! This is the AWS store's immutable side with its tables swapped. The shape of the data is the
//! same as `lore-aws/src/store/immutable_store.rs`, because the shape is what the storage trait
//! asks for and not a property of `DynamoDB`:
//!
//! - The payload is an R2 object whose key is the hash in hex, and the fragment describing it
//!   travels as object metadata on that same object. Object metadata is part of the object, so a
//!   reader always sees the fragment written with the bytes it is reading; there is no second
//!   record that can describe different bytes.
//! - `fragments` says which partitions hold which hashes, one row per association. It is what makes
//!   a payload readable in a partition, and it is written last and deleted first, so a visible
//!   association already means retrievable content.
//! - `fragment_state` says where a hash is in its lifecycle — stored, being obliterated, or
//!   obliterated. It holds no representation, so it cannot disagree with the object, and it is what
//!   answers "is this durable" in one statement without an R2 request.
//!
//! Three differences from the AWS store are deliberate, and each is a property of the services
//! rather than a simplification:
//!
//! - **Obliteration is one `DeleteObject`.** R2 has no object versioning (spec §3.3), so there is no
//!   version list to enumerate and no tombstone to leave: the key is the content address, and a
//!   delete that arrives twice is the same delete.
//! - **There is no legacy metadata table.** Nothing ever wrote a fragment anywhere but the object,
//!   so an object carrying no fragment metadata is damaged rather than merely old, and there is no
//!   second place to look for a description of it.
//! - **No metrics.** The store records its operations as tracing spans and nothing else, so the
//!   crate does not depend on the telemetry stack. The conditions the AWS store counts (a
//!   referenced payload missing from the bucket, an association with no state row) are logged.
//!
//! Retrying is per statement rather than per operation: an operation here spans two backends, so
//! there is no single deadline over it. Each statement Turso is asked runs under the configured
//! limits, and the R2 request underneath runs under the HTTP client's own timeout and the SDK's
//! own retry.

use std::collections::HashMap;
use std::collections::HashSet;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use libsql::Value;
use lore_aws::store::object_metadata::ObjectMetadataError;
use lore_aws::store::object_metadata::from_object_metadata;
use lore_aws::store::object_metadata::to_object_metadata;
use lore_base::error::AddressNotFound;
use lore_base::error::SlowDown;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::FRAGMENT_SIZE_THRESHOLD;
use lore_base::types::Fragment;
use lore_base::types::FragmentFlags;
use lore_base::types::FragmentReference;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_base::types::TypedBytes;
use lore_storage::ImmutableStore as ImmutableStoreTrait;
use lore_storage::Oversized;
use lore_storage::StoreError;
use lore_storage::StoreGetData;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_storage::StoreObliterateStats;
use lore_storage::immutable_store::sanitise_fragment_behavior_flags;
use tokio::task::JoinSet;
use tracing::Instrument;
use tracing::debug;
use tracing::info;
use tracing::trace;
use tracing::warn;

use crate::store::blob_to_hash;
use crate::store::cell;
use crate::store::context_from_repository_context;
use crate::store::hash_blob;
use crate::store::partition_blob;
use crate::store::r2::R2Client;
use crate::store::r2::R2Error;
use crate::store::repository_context_blob;
use crate::turso::TursoExecutor;
use crate::turso::TursoLimits;
use crate::turso::TursoRow;
use crate::turso::TursoValues;
use crate::turso::retry::retry_within_deadline;

// =============================================================================
// The statements
// =============================================================================

/// Whether this partition holds this exact association, which is the whole of what this store
/// serves: it isolates partitions, so it reads no wider than the association asked about.
const EXISTS_SQL: &str =
    "SELECT 1 FROM fragments WHERE hash = ?1 AND repository_context = ?2 LIMIT 1";

/// The lifecycle state of one hash. No row means the hash is unknown, not that it is missing: an
/// obliterated hash has a row, so the difference between "never stored" and "destroyed" survives.
const LOAD_STATE_SQL: &str = "SELECT state FROM fragment_state WHERE hash = ?1";

/// Record a hash as stored, unless something is already holding it.
///
/// The insert is conditional, so it can never overwrite an obliteration's mark: a racing writer
/// loses the insert, reads the row, and sees the mark.
const PUBLISH_STATE_SQL: &str = "INSERT INTO fragment_state (hash, state) VALUES (?1, ?2) \
     ON CONFLICT (hash) DO NOTHING";

/// Move a state row, when it is still in the state the caller read.
///
/// Obliteration takes and releases its mark with this, so two obliterations racing for the same
/// hash cannot both believe they hold it.
const ADVANCE_STATE_SQL: &str =
    "UPDATE fragment_state SET state = ?3 WHERE hash = ?1 AND state = ?2";

/// Remove a state row, but only while it still describes a payload that is merely stored.
///
/// The condition rides on the statement rather than on a read before it: a check and a delete in
/// two statements would let an obliteration take its mark between them and have that mark deleted
/// underneath it, which would let a put republish content the obliteration is destroying (spec
/// §4.5).
const CLEAR_STATE_SQL: &str = "DELETE FROM fragment_state WHERE hash = ?1 AND state = ?2";

/// Record that a partition holds a hash. Written after the payload and its state, and deleted
/// before either, which is what makes a visible association mean retrievable content.
const ASSOCIATE_SQL: &str = "INSERT INTO fragments (hash, repository_context) VALUES (?1, ?2) \
     ON CONFLICT (hash, repository_context) DO NOTHING";

const DELETE_ASSOCIATION_SQL: &str =
    "DELETE FROM fragments WHERE hash = ?1 AND repository_context = ?2";

/// Whether any partition still names this hash, which is the only question this store asks about
/// more than one row at a time, and the only thing that decides whether a payload can be deleted.
const HAS_ASSOCIATIONS_SQL: &str = "SELECT 1 FROM fragments WHERE hash = ?1 LIMIT 1";

/// Whether one partition holds any association for a hash, whatever context it is under.
///
/// The sort key is the partition followed by the context, so the partition is a prefix and one
/// comparison bounds every association that partition holds — an unassociated hash and one
/// associated only elsewhere answer the same way, which is what isolation requires.
const HAS_PARTITION_ASSOCIATION_SQL: &str = "SELECT 1 FROM fragments WHERE hash = ?1 \
     AND substr(repository_context, 1, 16) = ?2 LIMIT 1";

/// How many addresses one resolution statement asks about.
///
/// A resolution is a set of exact `(hash, repository_context)` pairs with no cursor to follow, so a
/// batch is as many pairs as one statement can hold. A caller that asks about no more than this at
/// once gets one statement; a caller that asks about more gets several, and neither has to know
/// which.
pub const BATCH_ADDRESSES: usize = 64;

/// Lower bound on the obliteration drain, whatever the configured operation deadline is.
///
/// The drain is the window in which a writer that raced the association delete is given time to
/// finish, so it must be at least long enough for one statement to land.
const MIN_OBLITERATION_DRAIN_MILLIS: u64 = 100;

// =============================================================================
// Settings
// =============================================================================

/// What the immutable store itself is configured with, beyond its two connections.
///
/// The R2 bucket and its keys belong to [`R2Settings`](crate::store::R2Settings) and the Turso
/// limits to [`TursoLimits`], both of which the store is handed already built, so nothing about
/// reaching either service is repeated here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudflareImmutableStoreSettings {
    /// Upload and associate without asking what is already there.
    ///
    /// Off by default, and off is what makes a put idempotent: the store checks the association and
    /// the state first, so storing content twice costs no upload and cannot disturb an
    /// obliteration. On, every put uploads, which is only useful when the store's own answer about
    /// what it holds is known to be wrong.
    pub force_write: bool,
}

impl CloudflareImmutableStoreSettings {
    pub const fn new(force_write: bool) -> Self {
        Self { force_write }
    }

    pub const fn with_force_write(mut self, force_write: bool) -> Self {
        self.force_write = force_write;
        self
    }
}

impl Default for CloudflareImmutableStoreSettings {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Where a payload is in its lifecycle.
///
/// This is the whole of what Turso records about a payload. What the payload *is* — its
/// compression, its sizes — lives on the R2 object and is never duplicated here, so the two cannot
/// disagree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FragmentState {
    /// The payload is stored and readable.
    Stored,
    /// An obliteration holds this hash. Transient: it is either released back to [`Self::Stored`]
    /// or advanced to [`Self::Obliterated`].
    Obliterating,
    /// The payload has been obliterated and its object deleted. A tombstone, kept so the
    /// difference between "never stored" and "deliberately destroyed" survives.
    Obliterated,
}

impl FragmentState {
    /// The state the stored bits name.
    ///
    /// The bits are [`FragmentFlags`] rather than an enum of its own, because both stores record
    /// the same lifecycle and the migration between them reads it.
    fn from_bits(bits: u32) -> Self {
        if bits & FragmentFlags::PayloadObliterated == FragmentFlags::PayloadObliterated {
            Self::Obliterated
        } else if bits & FragmentFlags::PayloadObliterating == FragmentFlags::PayloadObliterating {
            Self::Obliterating
        } else {
            Self::Stored
        }
    }

    fn bits(self) -> u32 {
        match self {
            Self::Stored => 0,
            Self::Obliterating => FragmentFlags::PayloadObliterating.bits(),
            Self::Obliterated => FragmentFlags::PayloadObliterated.bits(),
        }
    }

    fn is_obliteration(self) -> bool {
        self != Self::Stored
    }
}

/// Mark a fragment as held durably, which every payload this store serves is: it is in an object
/// bucket, not on the machine that answered.
fn stored_durable(mut fragment: Fragment) -> Fragment {
    fragment.flags |= FragmentFlags::PayloadStoredDurable.bits();
    fragment
}

/// One object's bytes and the fragment the same object declares.
struct ObjectContents {
    /// How many bytes the body held, which is what the `Content-Length` header predicted where it
    /// was present.
    read: usize,
    bytes: BytesMut,
    /// Recovered from the object's own metadata, so it describes exactly these bytes.
    fragment: Result<Fragment, ObjectMetadataError>,
}

// =============================================================================
// The store
// =============================================================================

/// The immutable store: payloads in R2, and their lifecycle in Turso.
pub struct CloudflareImmutableStore {
    r2: Arc<dyn R2Client>,
    executor: Arc<dyn TursoExecutor>,
    limits: TursoLimits,
    force_write: bool,
    /// How long an obliteration waits before it checks whether it is the last reference, giving a
    /// writer that raced it time to land.
    obliteration_drain: Duration,
}

impl CloudflareImmutableStore {
    /// Build the store over a bucket and a database.
    ///
    /// The drain follows the Turso deadline, which is what bounds how long another operation —
    /// a put associating the hash this obliteration is destroying — can be in flight.
    pub fn new(
        r2: Arc<dyn R2Client>,
        executor: Arc<dyn TursoExecutor>,
        limits: TursoLimits,
        settings: &CloudflareImmutableStoreSettings,
    ) -> Self {
        Self {
            r2,
            executor,
            limits,
            force_write: settings.force_write,
            obliteration_drain: limits
                .timeout
                .max(Duration::from_millis(MIN_OBLITERATION_DRAIN_MILLIS)),
        }
    }

    /// The key a hash is stored under, in both the bucket and the tables.
    ///
    /// Content-addressed and unprefixed, so an object is found by the hash alone and two
    /// partitions holding the same content hold the same object. The key names no partition, which
    /// is why the association is the only thing that says who may read it.
    pub fn object_key(hash: Hash) -> String {
        let mut dst = [0u8; 64];
        lore_revision::util::to_hex_str(hash.data(), &mut dst).to_owned()
    }

    /// Run one statement, reissued within the operation's own deadline.
    async fn query(
        &self,
        sql: &str,
        parameters: TursoValues,
        what: &str,
    ) -> Result<Vec<TursoRow>, StoreError> {
        retry_within_deadline!(self.limits, self.executor.query(sql, parameters.clone()))
            .map_err(|error| error.into_store_error(what))
    }

    /// Run one statement that changes rows, reissued within the operation's own deadline.
    async fn execute(
        &self,
        sql: &str,
        parameters: TursoValues,
        what: &str,
    ) -> Result<u64, StoreError> {
        // The macro may run the statement again, and parameters that were moved into the first
        // attempt are not there for the second, so they are handed over afresh.
        retry_within_deadline!(self.limits, self.executor.execute(sql, parameters.clone()))
            .map_err(|error| error.into_store_error(what))
    }

    /// Whether this partition holds the association for this address.
    async fn exists(&self, partition: Partition, address: Address) -> Result<bool, StoreError> {
        let rows = self
            .query(
                EXISTS_SQL,
                vec![
                    hash_blob(&address.hash),
                    repository_context_blob(partition, address.context),
                ],
                "reading a fragment association",
            )
            .await?;

        Ok(!rows.is_empty())
    }

    /// The addresses among `addresses` that this partition holds an association for.
    ///
    /// A set rather than positions: a batch may name the same address more than once, and keying
    /// positions by address would lose every occurrence but the last.
    async fn associations_present(
        &self,
        partition: Partition,
        addresses: &[Address],
    ) -> Result<HashSet<Address>, StoreError> {
        let distinct: HashSet<Address> = addresses.iter().copied().collect();
        if distinct.is_empty() {
            return Ok(HashSet::new());
        }

        let distinct: Vec<Address> = distinct.into_iter().collect();
        let mut present = HashSet::with_capacity(distinct.len());

        for chunk in distinct.chunks(BATCH_ADDRESSES) {
            // One predicate per address, because the pair is what the primary key holds and a
            // row-value `IN` is not in the dialect this server speaks.
            let mut sql = String::from("SELECT hash, repository_context FROM fragments WHERE ");
            let mut parameters = Vec::with_capacity(chunk.len() * 2);

            for (index, address) in chunk.iter().enumerate() {
                if index > 0 {
                    sql.push_str(" OR ");
                }

                let hash_parameter = index * 2 + 1;
                let context_parameter = hash_parameter + 1;
                sql.push_str(&format!(
                    "(hash = ?{hash_parameter} AND repository_context = ?{context_parameter})"
                ));

                parameters.push(hash_blob(&address.hash));
                parameters.push(repository_context_blob(partition, address.context));
            }

            let rows = self
                .query(&sql, parameters, "reading fragment associations")
                .await?;

            for row in rows {
                present.insert(Address {
                    hash: blob_to_hash("hash", cell(&row, 0, &sql)?)?,
                    context: context_from_repository_context(
                        "repository_context",
                        cell(&row, 1, &sql)?,
                    )?,
                });
            }
        }

        Ok(present)
    }

    /// Resolve one address: a full match or nothing.
    ///
    /// This store reads no wider than the exact association, so there is no weaker level for a
    /// miss to fall back to. The state is read as well, because an address can be associated and
    /// mid-obliteration — a window that exists because the association is deleted first and the
    /// payload last.
    async fn do_query(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<StoreMatchResult, StoreError> {
        let (associated, state) = tokio::join!(
            self.exists(partition, address),
            self.load_state(address.hash)
        );

        if !associated? {
            return Ok(StoreMatchResult::default());
        }

        match state? {
            Some(FragmentState::Stored) => Ok(StoreMatchResult {
                match_made: StoreMatch::MatchFull,
                // This store isolates, so it only ever matches inside the partition asked about.
                partition,
                context: address.context,
                stored_local: false,
                stored_durable: true,
            }),
            Some(FragmentState::Obliterating | FragmentState::Obliterated) => {
                trace!("Query found an obliterated fragment at address {address}");
                Ok(StoreMatchResult::default())
            }
            None => {
                // A put publishes the state before it writes the association, so a correctly
                // written fragment always has one. A row that does not means either a put that
                // stopped between the two, or a deployment whose table was written by something
                // else: either way the read reports absence, and this is the only point at which
                // the reason is still known.
                warn!(
                    "Query found an association at {address} whose hash has no state recorded; \
                     reporting it as absent"
                );
                Ok(StoreMatchResult::default())
            }
        }
    }

    /// The batch resolution behind [`ImmutableStoreTrait::query`].
    ///
    /// Associations alone decide it. A put writes the association last, after the payload and the
    /// state are in place, and an obliteration deletes it first, so an association that is visible
    /// already means content that can be served.
    async fn do_query_batch(
        &self,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        let associations = self.associations_present(partition, addresses).await?;

        for (address, result) in addresses.iter().zip(results.iter_mut()) {
            *result = if associations.contains(address) {
                StoreMatchResult {
                    match_made: StoreMatch::MatchFull,
                    partition,
                    context: address.context,
                    stored_local: false,
                    stored_durable: true,
                }
            } else {
                StoreMatchResult::default()
            };
        }

        Ok(())
    }

    /// The lifecycle state of a hash, where a row exists.
    async fn load_state(&self, hash: Hash) -> Result<Option<FragmentState>, StoreError> {
        let rows = self
            .query(
                LOAD_STATE_SQL,
                vec![hash_blob(&hash)],
                "reading a fragment state",
            )
            .await?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };

        Ok(Some(state_from_cell(
            "state",
            cell(row, 0, LOAD_STATE_SQL)?,
        )?))
    }

    /// Record that a payload exists, without disturbing an obliteration that may hold the hash.
    ///
    /// The insert cannot overwrite a mark, so losing it is the ordinary outcome for content that is
    /// already stored — the row carries no representation, so there is nothing to reconcile, and
    /// the row that is there is already correct. The state it holds is returned, so the caller can
    /// tell "already published" from "an obliteration holds this".
    async fn publish_state(&self, hash: Hash) -> Result<FragmentState, StoreError> {
        let inserted = self
            .execute(
                PUBLISH_STATE_SQL,
                vec![
                    hash_blob(&hash),
                    Value::Integer(i64::from(FragmentState::Stored.bits())),
                ],
                "publishing a fragment state",
            )
            .await?;

        if inserted == 1 {
            return Ok(FragmentState::Stored);
        }

        // The insert was refused because a row was already there. One statement was enough for the
        // hot path — content nobody has stored — and only the contested path pays for the read.
        Ok(self
            .load_state(hash)
            .await?
            .unwrap_or(FragmentState::Stored))
    }

    /// Move the state row from one state to another, failing if it has moved underneath us.
    async fn advance_state(
        &self,
        hash: Hash,
        expected: FragmentState,
        updated: FragmentState,
    ) -> Result<(), StoreError> {
        let moved = self
            .execute(
                ADVANCE_STATE_SQL,
                vec![
                    hash_blob(&hash),
                    Value::Integer(i64::from(expected.bits())),
                    Value::Integer(i64::from(updated.bits())),
                ],
                "moving a fragment state",
            )
            .await?;

        if moved == 1 {
            return Ok(());
        }

        warn!("Fragment state for {hash} was not {expected:?} when moving to {updated:?}");
        Err(StoreError::internal(
            "Failed to update fragment state due to conflict",
        ))
    }

    /// Delete the state row for a hash, so the next put treats it as new content.
    ///
    /// Only called for a payload R2 has lost. An obliteration holding the mark is left alone by the
    /// caller, since removing a mark mid-obliteration would let a put republish underneath it, and
    /// the `state = Stored` condition here is what makes that true even when the caller races.
    async fn clear_state(&self, hash: Hash) -> Result<(), StoreError> {
        self.execute(
            CLEAR_STATE_SQL,
            vec![
                hash_blob(&hash),
                Value::Integer(i64::from(FragmentState::Stored.bits())),
            ],
            "clearing a fragment state",
        )
        .await?;

        Ok(())
    }

    /// Move a tombstoned hash back to stored, now that its payload has been uploaded again.
    ///
    /// Losing this race is not a failure: another writer reviving the same tombstone produced
    /// exactly the state this one wanted, and this one's bytes are already uploaded, so there is
    /// nothing left to disagree about. Only finding the hash back under an obliteration is a reason
    /// to stop, and that is a back-off rather than an error because the mark is transient.
    ///
    /// This tolerance belongs here rather than in [`Self::advance_state`], which is also how an
    /// obliteration takes its mark — treating "already in the target state" as success there would
    /// let two obliterations both believe they hold it.
    async fn revive_state(&self, hash: Hash) -> Result<(), StoreError> {
        if self
            .advance_state(hash, FragmentState::Obliterated, FragmentState::Stored)
            .await
            .is_ok()
        {
            return Ok(());
        }

        match self.load_state(hash).await? {
            Some(FragmentState::Stored) => {
                debug!(%hash, "Another writer revived this hash first");
                Ok(())
            }
            state => {
                info!(%hash, ?state, "Hash is no longer revivable, asking the caller to retry");
                Err(StoreError::from(SlowDown))
            }
        }
    }

    /// Record, and make repairable, a hash that is still referenced but whose payload is gone.
    ///
    /// Reaching here means the access check found an association while R2 reported no object, so
    /// the content was published once and has since been lost. The read is on its way back to the
    /// caller as an ordinary not-found, which is indistinguishable from content that was never
    /// stored, so this is the only point at which the difference is still known.
    ///
    /// Clearing the state row is what makes it repairable: with no row, the next put stops taking
    /// the "already stored" branch and uploads instead. That is safe only because the row holds no
    /// representation — there is nothing in it the next write does not re-derive.
    ///
    /// Both steps are best effort. Failing to clear the row leaves the hash exactly as it already
    /// was, and the loss has been reported regardless.
    async fn report_missing_payload(&self, address: Address) {
        warn!(
            %address,
            "Fragment is referenced by a partition but absent from R2; content for this hash has \
             been lost. Clearing its state so the content can be stored again."
        );

        match self.load_state(address.hash).await {
            Ok(Some(FragmentState::Stored)) => {
                if let Err(error) = self.clear_state(address.hash).await {
                    warn!(%address, ?error, "Failed to clear state for a lost payload");
                }
            }
            Ok(state) => {
                debug!(%address, ?state, "Leaving state alone for a lost payload");
            }
            Err(error) => {
                warn!(%address, ?error, "Failed to read state for a lost payload");
            }
        }
    }

    async fn associate_fragment(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<(), StoreError> {
        self.execute(
            ASSOCIATE_SQL,
            vec![
                hash_blob(&address.hash),
                repository_context_blob(partition, address.context),
            ],
            "writing a fragment association",
        )
        .await?;

        Ok(())
    }

    async fn delete_association(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<(), StoreError> {
        self.execute(
            DELETE_ASSOCIATION_SQL,
            vec![
                hash_blob(&address.hash),
                repository_context_blob(partition, address.context),
            ],
            "deleting a fragment association",
        )
        .await?;

        Ok(())
    }

    /// Whether `partition` holds any association for `hash`, whatever context it is under.
    async fn has_partition_association(
        &self,
        partition: Partition,
        hash: Hash,
    ) -> Result<bool, StoreError> {
        let rows = self
            .query(
                HAS_PARTITION_ASSOCIATION_SQL,
                vec![hash_blob(&hash), partition_blob(partition)],
                "reading a partition association",
            )
            .await?;

        Ok(!rows.is_empty())
    }

    /// Whether any partition still names this hash.
    async fn has_associations(&self, hash: Hash) -> Result<bool, StoreError> {
        let rows = self
            .query(
                HAS_ASSOCIATIONS_SQL,
                vec![hash_blob(&hash)],
                "reading fragment associations",
            )
            .await?;

        Ok(!rows.is_empty())
    }

    /// Upload a payload under its hash and publish its state, so a later put has nothing to do.
    pub(crate) async fn write_payload_and_state(
        &self,
        hash: Hash,
        fragment: Fragment,
        payload: Bytes,
    ) -> Result<(), StoreError> {
        if payload.len() != fragment.size_payload as usize {
            warn!(
                expected_size = fragment.size_payload,
                received_size = payload.len(),
                %hash,
                "Failed to write fragment to immutable store for hash: payload size invalid"
            );
            return Err(StoreError::internal(format!(
                "Failed to store in immutable store for put {hash}"
            )));
        }

        let key = Self::object_key(hash);
        self.r2
            .put_object(&key, payload, to_object_metadata(&fragment))
            .await
            .map_err(|error| {
                warn!(%hash, %key, ?error, "Failed to write payload for hash");
                error.into_store_error("writing a payload to R2")
            })?;

        match self.publish_state(hash).await? {
            FragmentState::Stored => {}
            FragmentState::Obliterating => {
                info!(
                    %hash,
                    "Payload was uploaded while an obliteration holds the hash; \
                     leaving it unassociated and asking the caller to retry"
                );
                return Err(StoreError::from(SlowDown));
            }
            FragmentState::Obliterated => {
                info!(%hash, "Payload revives a tombstoned hash");
                self.revive_state(hash).await?;
            }
        }

        Ok(())
    }

    /// Delete a payload from the bucket.
    ///
    /// One request, with no version to enumerate first: R2 keeps no object versions, and the key is
    /// the content address, so a delete that arrives twice is the same delete and a delete for
    /// content that is already gone has nothing left to do.
    async fn delete_payload(&self, hash: Hash) -> Result<(), StoreError> {
        let key = Self::object_key(hash);

        match self.r2.delete_object(&key).await {
            Ok(()) | Err(R2Error::NotFound) => Ok(()),
            Err(error) => {
                warn!(%hash, %key, ?error, "Failed to delete payload for hash");
                Err(error.into_store_error("deleting a payload from R2"))
            }
        }
    }

    /// The fragment an object declares, without transferring its body.
    ///
    /// The object is the only description of itself there is. Metadata that is present but
    /// unreadable means damage, and describing damaged bytes from a separate record is the mismatch
    /// this design exists to remove — and there is no separate record here to describe them from.
    fn fragment_from_object_metadata(
        hash: Hash,
        metadata: &HashMap<String, String>,
    ) -> Result<Fragment, StoreError> {
        match from_object_metadata(Some(metadata)) {
            Ok(fragment) => Ok(stored_durable(fragment)),
            Err(ObjectMetadataError::Absent) => {
                warn!(%hash, "Stored object carries no fragment metadata");
                Err(StoreError::internal(
                    "The R2 object carries no fragment metadata",
                ))
            }
            Err(e) => {
                warn!(%hash, "Stored object carries unusable fragment metadata: {e}");
                Err(StoreError::internal_with_context(
                    e,
                    "R2 object fragment metadata unusable",
                ))
            }
        }
    }

    /// Read a fragment without its payload. This is the one path that spends an R2 request purely
    /// on metadata, and it spends the cheapest one: `HeadObject` transfers no body.
    async fn head_fragment(&self, hash: Hash) -> Result<Fragment, StoreError> {
        let key = Self::object_key(hash);

        let metadata = self
            .r2
            .head_object(&key)
            .await
            .map_err(|error| match error {
                R2Error::NotFound => {
                    StoreError::from(AddressNotFound::from(Address::zero_context_hash(hash)))
                }
                other => {
                    debug!(%hash, ?other, "head_fragment failed to head the object");
                    other.into_store_error("heading an R2 object")
                }
            })?;

        Self::fragment_from_object_metadata(hash, &metadata)
    }

    /// Read an object's metadata and body in one request.
    async fn get_object_contents(&self, hash: Hash) -> Result<ObjectContents, StoreError> {
        let key = Self::object_key(hash);

        let mut object = self
            .r2
            .get_object(&key)
            .await
            .map_err(|error| match error {
                R2Error::NotFound => {
                    StoreError::from(AddressNotFound::from(Address::zero_context_hash(hash)))
                }
                other => {
                    debug!(%hash, ?other, "get_object_contents failed to get the object");
                    other.into_store_error("reading a payload from R2")
                }
            })?;

        // The length the endpoint reported, before a single byte of the body is read. Legacy S3
        // blobs had a `Fragment` struct prepended to the payload, so the largest legitimate object
        // is the threshold plus that prefix.
        const MAX_OBJECT_SIZE: usize = FRAGMENT_SIZE_THRESHOLD + size_of::<Fragment>();

        let content_length = object.content_length.filter(|length| *length > 0);
        if let Some(size) = content_length
            && size > MAX_OBJECT_SIZE
        {
            warn!(
                %hash,
                size,
                max = MAX_OBJECT_SIZE,
                "R2 object exceeds the maximum allowed size; treating it as malicious"
            );
            return Err(StoreError::from(Oversized {
                context: format!(
                    "R2 object size {size} for {hash} exceeds maximum {MAX_OBJECT_SIZE}"
                ),
            }));
        }

        let fragment = from_object_metadata(Some(&object.metadata));
        if let Ok(fragment) = &fragment {
            lore_storage::validate_fragment_size(fragment)?;
        }

        // Clamped to `MAX_OBJECT_SIZE`: legacy blobs carried a `Fragment` prefix that pushes their
        // size above the threshold, so capping at the threshold would under-allocate.
        let capacity = content_length.map_or(MAX_OBJECT_SIZE, |length| length.min(MAX_OBJECT_SIZE));

        let mut bytes = BytesMut::with_capacity(capacity);
        let mut read = 0_usize;
        while let Some(chunk) = object.body.next().await {
            let chunk = chunk.map_err(|e| {
                warn!(%hash, "Failed to read bytes from the R2 response: {e:?}");
                StoreError::internal_with_context(
                    e,
                    "Failed to read bytes from the R2 response stream",
                )
            })?;
            read += chunk.len();
            trace!("Read {read} bytes from the R2 stream");
            bytes.extend_from_slice(chunk.as_ref());
        }
        trace!("Total read {read} bytes from the R2 stream");

        Ok(ObjectContents {
            read,
            bytes,
            fragment,
        })
    }

    /// Check the object's own bytes against the fragment the same object declares.
    ///
    /// Both sides of this comparison come from one response, so it is a self-consistency check on a
    /// single object rather than a comparison between two stores.
    fn read_payload(
        contents: ObjectContents,
        hash: Hash,
        fragment: Fragment,
    ) -> Result<Bytes, StoreError> {
        let payload_size = fragment.size_payload as usize;
        let buffer_size = contents.bytes.len();

        if buffer_size == payload_size {
            Ok(contents.bytes.freeze())
        } else {
            warn!(
                "Wrong number of bytes read from payload, expected {payload_size} but got \
                 {buffer_size}, from a total of {} bytes read",
                contents.read
            );
            Err(StoreError::internal(format!(
                "Failed to load from immutable store, size mismatch (load {buffer_size}, expected \
                 {payload_size}) for get {hash}"
            )))
        }
    }

    /// Load a payload and the fragment describing it, in a single R2 request.
    ///
    /// There is no Turso read here at all. The fragment arrives as object metadata on the very
    /// response carrying the bytes, so it describes those bytes by construction.
    pub(crate) async fn load(&self, hash: Hash) -> Result<(Fragment, Bytes), StoreError> {
        let contents = self.get_object_contents(hash).await?;

        let fragment = match contents.fragment {
            Ok(fragment) => stored_durable(fragment),
            Err(ObjectMetadataError::Absent) => {
                warn!(%hash, "Stored object carries no fragment metadata");
                return Err(StoreError::internal(
                    "The R2 object carries no fragment metadata",
                ));
            }
            Err(e) => {
                warn!(%hash, "Stored object carries unusable fragment metadata: {e}");
                return Err(StoreError::internal_with_context(
                    e,
                    "R2 object fragment metadata unusable",
                ));
            }
        };

        lore_storage::validate_fragment_size(&fragment)?;

        let payload = Self::read_payload(contents, hash, fragment)?;
        Ok((fragment, payload))
    }

    /// Obliterate the fragments a fragmented payload points at, if it is one.
    ///
    /// Called once the mark is held and no association remains, so the parent payload is still
    /// present to be read and nothing can be adding references beneath it.
    async fn obliterate_sub_fragments(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        let (fragment, payload) = match self.load(address.hash).await {
            Ok(loaded) => loaded,
            Err(e) if e.is_address_not_found() => {
                info!("Payload for {address} is already gone, no sub-fragments to obliterate");
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        if fragment.flags & FragmentFlags::PayloadFragmented == 0 {
            return Ok(());
        }

        let payload = payload.to_aligned::<FragmentReference>();
        let sub_fragments = payload.as_type_slice::<FragmentReference>();
        info!(
            "Fragment {address} has {} sub-fragments",
            sub_fragments.len()
        );

        // The spawned tasks are not children of a caller's span, so they carry it themselves.
        let span = tracing::Span::current();
        let mut join_set = JoinSet::new();
        for reference in sub_fragments.iter() {
            let self_clone = self.clone();
            let stats = stats.clone();
            let sub_address = Address {
                hash: reference.hash,
                context: address.context,
            };

            info!("Spawning task to obliterate {sub_address}");
            lore_base::lore_spawn!(
                join_set,
                async move {
                    self_clone
                        .obliterate(partition, sub_address, stats)
                        .await
                        .map_err(|e| (sub_address, e))
                }
                .instrument(span.clone())
            );
        }

        let mut failures = false;
        while let Some(result) = join_set.join_next().await {
            match result {
                Err(e) => {
                    failures = true;
                    warn!("Failed to join task for fragment reference obliterate: {e:?}");
                }
                Ok(Err((sub_address, e))) => {
                    failures = true;
                    warn!("Obliteration failed for sub-fragment {sub_address}: {e:?}");
                }
                Ok(Ok(())) => {}
            }
        }

        if failures {
            warn!("Obliteration failed for at least one sub-fragment.");
            return Err(StoreError::internal(format!(
                "Failed to obliterate immutable {address}"
            )));
        }

        info!("Done obliterating sub-fragments");
        Ok(())
    }
}

/// A state row's `state` column as the lifecycle state it names.
fn state_from_cell(column: &str, value: &Value) -> Result<FragmentState, StoreError> {
    match value {
        Value::Integer(bits) => {
            u32::try_from(*bits)
                .map(FragmentState::from_bits)
                .map_err(|_out_of_range| {
                    StoreError::internal(format!(
                        "column {column} was {bits}, not a fragment state"
                    ))
                })
        }
        other => Err(StoreError::internal(format!(
            "column {column} was {other:?}, not a fragment state"
        ))),
    }
}

#[async_trait]
impl ImmutableStoreTrait for CloudflareImmutableStore {
    /// Durable storage for every tenant at once, so a payload is only ever served to the partition
    /// that holds an association for it.
    fn isolates_partitions(&self) -> bool {
        true
    }

    /// The exact association, which is also all this store reads: it reports no wider than it
    /// serves.
    ///
    /// Stated rather than defaulted. The default reads `MatchPartition` off
    /// [`ImmutableStoreTrait::isolates_partitions`], on the reasoning that a store reporting a
    /// partition match hands the caller a level to act on with a `copy` instead of a transfer. This
    /// store declines to establish that level: telling an unassociated hash apart from an absent one
    /// costs a statement per address, so it would be advertising a shortcut that never arrives.
    fn query_scope(&self) -> StoreMatch {
        StoreMatch::MatchFull
    }

    /// One statement per batch of associations. See [`Self::do_query_batch`].
    #[tracing::instrument(name = "CloudflareImmutableStore::query", skip_all)]
    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        debug_assert_eq!(addresses.len(), results.len());
        self.do_query_batch(partition, addresses, results).await
    }

    /// Unlike [`ImmutableStoreTrait::query`], this reads the object to report the representation
    /// actually stored, which costs a `HeadObject`. It transfers no body.
    #[tracing::instrument(name = "CloudflareImmutableStore::get_metadata", skip_all)]
    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        // The `HeadObject` goes out alongside the resolution rather than after it: the answer this
        // returns ends in that request, so waiting for the resolution first would put two round
        // trips in series on every hit.
        let mut query_fut = Box::pin(self.do_query(partition, address));
        let mut head_fut = Box::pin(self.head_fragment(address.hash));

        let mut head_result = None;
        let matched = loop {
            tokio::select! {
                result = &mut query_fut => break result?,
                result = &mut head_fut, if head_result.is_none() => {
                    head_result = Some(result);
                }
            }
        };

        if matched.match_made == StoreMatch::MatchNone {
            return Ok(StoreGetData::default());
        }

        let head_output = match head_result {
            Some(result) => result,
            None => head_fut.await,
        };

        match head_output {
            Ok(fragment) => Ok(StoreGetData::metadata(
                fragment,
                matched.match_made,
                partition,
            )),
            Err(e) if e.is_address_not_found() => {
                self.report_missing_payload(address).await;
                Ok(StoreGetData::default())
            }
            Err(e) => Err(e),
        }
    }

    #[tracing::instrument(name = "CloudflareImmutableStore::get", skip_all)]
    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        // Run both futures concurrently. The select! loop breaks as soon as exists resolves; a load
        // that finished first is stashed, and this keeps waiting for the association check.
        let exists_fut = self.exists(partition, address);
        let load_fut = self.load(address.hash);
        tokio::pin!(exists_fut, load_fut);

        let mut load_result = None;
        let exists_result = loop {
            tokio::select! {
                result = &mut exists_fut => break result,
                result = &mut load_fut, if load_result.is_none() => {
                    load_result = Some(result);
                }
            }
        };

        // If the association check failed, its error is returned here and the load is dropped
        // (cancelled) by the early return. A failed check takes priority over a failed read: it is
        // the answer the caller will act on.
        if !exists_result? {
            return Err(StoreError::from(AddressNotFound::from(address)));
        }

        let load_output = match load_result {
            Some(result) => result,
            None => load_fut.await,
        };

        // The association says the content is there, so an object that is not means the content was
        // published once and has since been lost — which is otherwise indistinguishable from
        // content that was never stored.
        if load_output
            .as_ref()
            .err()
            .is_some_and(StoreError::is_address_not_found)
        {
            self.report_missing_payload(address).await;
        }

        let (fragment, payload) = load_output?;
        lore_storage::validate_fragment_payload(&fragment, payload.len())?;

        Ok(StoreGetData {
            fragment,
            match_made: StoreMatch::MatchFull,
            partition,
            payload: Some(payload),
        })
    }

    #[tracing::instrument(name = "CloudflareImmutableStore::put", skip_all)]
    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        mut fragment: Fragment,
        payload: Option<Bytes>,
        _force: bool,
    ) -> Result<(), StoreError> {
        sanitise_fragment_behavior_flags(&mut fragment);

        if let Some(payload) = payload.as_ref() {
            lore_storage::validate_fragment_payload(&fragment, payload.len())?;
        } else {
            lore_storage::validate_fragment_size(&fragment)?;
        }

        let probe = if self.force_write {
            (None, false)
        } else {
            let (associated, state) = tokio::join!(
                self.exists(partition, address),
                self.load_state(address.hash)
            );
            (state?, associated?)
        };

        match probe {
            (Some(FragmentState::Obliterating), _) => {
                debug!(
                    "Received request to put fragment at {address} that is in the process of \
                     being obliterated"
                );
                Err(StoreError::from(SlowDown))
            }

            (Some(FragmentState::Stored), true) => Ok(()),

            (Some(FragmentState::Stored), false) if payload.is_some() => {
                self.associate_fragment(partition, address).await
            }

            (Some(FragmentState::Stored), false) => {
                Err(StoreError::internal("Payload buffer required"))
            }

            _ => match payload {
                Some(payload) => {
                    self.write_payload_and_state(address.hash, fragment, payload)
                        .await?;
                    self.associate_fragment(partition, address).await?;
                    Ok(())
                }
                None => Err(StoreError::internal("Payload buffer required")),
            },
        }
    }

    #[tracing::instrument(name = "CloudflareImmutableStore::obliterate", skip_all)]
    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        // Given the importance of the work done here, and how relatively infrequently it is
        // expected to be invoked, the log output in this method is intentionally verbose.
        let Some(state) = self.load_state(address.hash).await? else {
            info!("No fragment state for {address}, nothing to obliterate");
            return Ok(());
        };

        if state.is_obliteration() {
            info!("Fragment {address} is already being, or has already been, obliterated");
            return Ok(());
        }

        // The reference goes first, because that is the obligation: once this returns, this
        // partition does not name the content. Everything after it is reclamation.
        self.delete_association(partition, address).await?;
        stats
            .num_fragments
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.advance_state(
            address.hash,
            FragmentState::Stored,
            FragmentState::Obliterating,
        )
        .await?;
        info!("Acquired obliteration mark for {address}");

        // Again, now the mark is up: a writer that raced the first delete recreated the association
        // while the hash still looked live, and would otherwise outlive the obliteration. One that
        // associates after this point sees the mark and is turned away, or is re-storing content of
        // its own — which is a new write, not this one surviving.
        self.delete_association(partition, address).await?;

        tokio::time::sleep(self.obliteration_drain).await;

        info!("Association deleted, re-checking for other associations...");
        if self.has_associations(address.hash).await? {
            info!("Fragment still associated, releasing the obliteration mark");
            return self
                .advance_state(
                    address.hash,
                    FragmentState::Obliterating,
                    FragmentState::Stored,
                )
                .await
                .inspect_err(|e| {
                    warn!("Failed to release the obliteration mark: {e:?}");
                });
        }

        self.clone()
            .obliterate_sub_fragments(partition, address, stats.clone())
            .await?;

        self.delete_payload(address.hash).await?;
        stats
            .num_payloads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.advance_state(
            address.hash,
            FragmentState::Obliterating,
            FragmentState::Obliterated,
        )
        .await
        .inspect_err(|e| {
            warn!("Failed to finalize obliterate for {address}: {e:?}");
        })
    }

    #[tracing::instrument(name = "CloudflareImmutableStore::copy", skip_all)]
    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        // R2 itself tracks the destination object's existence as the source of durability; the
        // local-flag bookkeeping that `durable` controls is irrelevant here.
        _durable: bool,
    ) -> Result<(), StoreError> {
        // The destination tuple shares the source's hash but takes the caller's chosen context —
        // that is the only field the storage trait allows the caller to pivot on a copy.
        let destination_address = Address {
            hash: source_address.hash,
            context: destination_context,
        };

        let present = if source_address.context.is_zero() {
            self.has_partition_association(source_partition, source_address.hash)
                .await?
        } else {
            self.exists(source_partition, source_address).await?
        };

        if !present {
            return Err(StoreError::from(AddressNotFound::from(source_address)));
        }

        self.associate_fragment(destination_partition, destination_address)
            .await
    }

    async fn evict(
        self: Arc<Self>,
        _max_capacity: usize,
        _sync_data: bool,
        _sink: Option<lore_storage::gc_event::GcEventSinkRef>,
    ) -> Result<usize, StoreError> {
        // An object store is not something this process evicts from, ever.
        Ok(0)
    }

    async fn compact(
        self: Arc<Self>,
        _max_size: usize,
        _at: Option<usize>,
        _sync_data: bool,
        _sink: Option<lore_storage::gc_event::GcEventSinkRef>,
    ) -> Result<Option<usize>, StoreError> {
        // An object store is not something this process compacts, ever.
        Ok(None)
    }

    async fn compact_resume_at(self: Arc<Self>) -> Option<usize> {
        None
    }

    async fn verify(self: Arc<Self>, _heal: bool) -> Result<(), StoreError> {
        // Nothing to check: no two records describe the same bytes, so nothing can drift.
        Ok(())
    }

    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        // Every write has already been acknowledged by the service that holds it.
        Ok(())
    }

    fn max_query_batch(&self) -> Option<usize> {
        // A caller that honours this asks about a batch this store resolves in one statement.
        Some(BATCH_ADDRESSES)
    }
}
