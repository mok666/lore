// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The lock store, held in one Turso table (spec §4.4).
//!
//! One row per held lock, keyed by the hash and the `(repository, branch)` pair it was taken on.
//! Taking and releasing locks is all-or-nothing: a request that would leave a lock behind that
//! another client already holds writes nothing at all, because a caller that is told it failed
//! must not have to wonder which part of it happened.

use std::sync::Arc;

use async_trait::async_trait;
use libsql::Value;
use lore_base::error::LockNotFound;
use lore_base::error::LockNotOwned;
use lore_base::types::Hash;
use lore_base::types::LockData;
use lore_base::types::LockResource;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_storage::StoreError;

use crate::store::blob_to_context;
use crate::store::blob_to_hash;
use crate::store::blob_to_string;
use crate::store::cell;
use crate::store::discard;
use crate::store::hash_blob;
use crate::store::integer_to_millis;
use crate::store::repository_branch_blob;
use crate::turso::TursoExecutor;
use crate::turso::TursoLimits;
use crate::turso::TursoValues;
use crate::turso::retry::retry_within_deadline;

/// The columns every lock row is read as.
const SELECT_LOCKS: &str = "SELECT hash, repository, branch, owner_id, description, locked_at \
     FROM locks";

/// How many resources one `check_locks_status` statement asks about.
///
/// Three parameters per resource, so fifty keeps a statement well inside any server's parameter
/// limit, and a caller checking a large push still ends up with a handful of round trips rather
/// than one per resource.
const CHECK_LOCKS_CHUNK: usize = 50;

/// Take a lock unless the row is already there.
const LOCK_SQL: &str = "INSERT INTO locks \
     (hash, repository_branch, repository, branch, owner_id, description, locked_at) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
     ON CONFLICT (hash, repository_branch) DO NOTHING";

/// Whose lock a row is.
const OWNER_SQL: &str =
    "SELECT owner_id FROM locks WHERE hash = ?1 AND repository_branch = ?2";

/// Release a lock. Whether it is there, and whose it was, is decided before this runs.
const UNLOCK_SQL: &str = "DELETE FROM locks WHERE hash = ?1 AND repository_branch = ?2";

/// A lock store in a Turso database.
pub struct CloudflareLockStore {
    executor: Arc<dyn TursoExecutor>,
    limits: TursoLimits,
}

impl CloudflareLockStore {
    pub fn new(executor: Arc<dyn TursoExecutor>, limits: TursoLimits) -> Self {
        Self { executor, limits }
    }

    /// Take every resource named, or none of them.
    ///
    /// A candidate that was already held by the same owner is not an error and is not part of the
    /// answer: the caller hears about the locks this call took, and a retry of a request whose
    /// answer was lost takes nothing and says so.
    async fn lock_resources_impl(
        &self,
        owner_id: &str,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        // One timestamp for the whole request, so the locks it takes look like one moment.
        let locked_at = crate::store::now_millis();

        let transaction = self
            .executor
            .begin_immediate()
            .await
            .map_err(|error| error.into_lock_error("opening a transaction to lock resources"))?;

        let mut locked = Vec::with_capacity(resources.len());

        for resource in resources {
            let parameters = vec![
                hash_blob(&resource.hash),
                repository_branch_blob(repository, resource.branch),
                crate::store::partition_blob(repository),
                crate::store::context_blob(resource.branch),
                Value::Text(owner_id.to_string()),
                Value::Text(resource.description.clone()),
                Value::Integer(locked_at as i64),
            ];

            let taken = match transaction.execute(LOCK_SQL, parameters).await {
                Ok(taken) => taken,
                Err(error) => {
                    discard(transaction, "could not take a lock").await;
                    return Err(error.into_lock_error("taking a lock"));
                }
            };

            match taken {
                1 => locked.push(LockData {
                    resource: resource.clone(),
                    owner: owner_id.to_string(),
                    locked_at,
                }),
                0 => {
                    // The row is already there. Whose it is decides whether this is a lock the
                    // same owner already holds.
                    let rows = match transaction
                        .query(
                            OWNER_SQL,
                            vec![
                                hash_blob(&resource.hash),
                                repository_branch_blob(repository, resource.branch),
                            ],
                        )
                        .await
                    {
                        Ok(rows) => rows,
                        Err(error) => {
                            discard(transaction, "could not read whose lock a row is").await;
                            return Err(error.into_lock_error("reading a lock's owner"));
                        }
                    };

                    match rows.first() {
                        Some(row) => {
                            let owner = owner_of(row).map_err(malformed)?;

                            if owner == owner_id {
                                // Held by this owner already: nothing was taken here, so it is not
                                // part of what this call took.
                                continue;
                            }

                            discard(transaction, "a resource is locked by another owner").await;
                            return Err(LockNotOwned.into());
                        }
                        None => {
                            // The insert conflicted, so a row is there; not finding it means the
                            // row changed underneath a transaction that holds the write lock.
                            discard(transaction, "a conflicted row is not there").await;
                            return Err(LockError::internal(
                                "a lock insert conflicted, but the row it conflicted with is not \
                                 there",
                            ));
                        }
                    }
                }
                other => {
                    discard(transaction, "a lock insert reported an impossible row count").await;
                    return Err(LockError::internal(format!(
                        "taking a lock changed {other} rows, which is neither nothing nor one"
                    )));
                }
            }
        }

        transaction
            .commit()
            .await
            .map_err(|error| error.into_lock_error("committing a lock request"))?;

        Ok(locked)
    }

    /// Read the locks a query names, oldest row first.
    async fn query_locks_impl(&self, query: LockQuery) -> Result<Vec<LockData>, LockError> {
        let (sql, parameters) = query_locks_sql(query);

        // The parameters are cloned per attempt: the macro may run the operation again, and a
        // value that was moved into the first attempt is not there for the second.
        let rows = retry_within_deadline!(
            self.limits,
            self.executor.query(&sql, parameters.clone())
        )
        .map_err(|error| error.into_lock_error("querying locks"))?;

        rows.iter()
            .map(|row| lock_from_row(row, &sql))
            .collect::<Result<Vec<_>, _>>()
            .map_err(malformed)
    }

    /// Report the locks held on exactly these resources.
    ///
    /// Every resource is asked about as the key it is: its own hash, repository and branch. Asking
    /// about the hash and the repository alone would report a lock held on another branch as if it
    /// were held on this one.
    async fn check_locks_status_impl(
        &self,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        let mut locks = Vec::new();

        for chunk in unique_keys(repository, resources).chunks(CHECK_LOCKS_CHUNK) {
            let (sql, parameters) = check_locks_sql(chunk);

            let rows = retry_within_deadline!(
                self.limits,
                self.executor.query(&sql, parameters.clone())
            )
            .map_err(|error| error.into_lock_error("checking lock status"))?;

            for row in &rows {
                locks.push(lock_from_row(row, &sql).map_err(malformed)?);
            }
        }

        Ok(locks)
    }

    /// Release every resource named, or none of them.
    ///
    /// `validate_user` decides what a resource locked by someone else means: with it on, the
    /// whole request fails and no row is removed; with it off, the lock is released anyway, which
    /// is what an administrative unlock does.
    ///
    /// Whether a row is missing and whether it belongs to someone else are both decided before
    /// anything is removed, because an affected row count cannot tell the two apart. A row that
    /// belongs to someone else outranks a row that is not there, as it does in the DynamoDB store.
    async fn unlock_resources_impl(
        &self,
        owner_id: &str,
        validate_user: bool,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockResource>, LockError> {
        let keys = unique_resources(resources);

        let transaction = self
            .executor
            .begin_immediate()
            .await
            .map_err(|error| error.into_lock_error("opening a transaction to unlock resources"))?;

        let mut missing = false;

        for resource in &keys {
            if validate_user {
                let rows = match transaction
                    .query(
                        OWNER_SQL,
                        vec![
                            hash_blob(&resource.hash),
                            repository_branch_blob(repository, resource.branch),
                        ],
                    )
                    .await
                {
                    Ok(rows) => rows,
                    Err(error) => {
                        discard(transaction, "could not read whose lock a row is").await;
                        return Err(error.into_lock_error("reading a lock's owner"));
                    }
                };

                match rows.first() {
                    // Not there: nothing to release. Whether that fails the request depends on
                    // whether a row belonging to someone else turns up in the rest of the loop.
                    None => {
                        missing = true;
                        continue;
                    }
                    Some(row) => {
                        let owner = owner_of(row).map_err(malformed)?;

                        if owner != owner_id {
                            discard(transaction, "a resource is locked by another owner").await;
                            return Err(LockNotOwned.into());
                        }
                    }
                }
            }

            if let Err(error) = transaction
                .execute(
                    UNLOCK_SQL,
                    vec![
                        hash_blob(&resource.hash),
                        repository_branch_blob(repository, resource.branch),
                    ],
                )
                .await
            {
                discard(transaction, "could not release a lock").await;
                return Err(error.into_lock_error("releasing a lock"));
            }
        }

        if missing {
            discard(transaction, "some resources are not locked").await;
            return Err(LockNotFound.into());
        }

        transaction
            .commit()
            .await
            .map_err(|error| error.into_lock_error("committing an unlock request"))?;

        Ok(keys)
    }
}

#[async_trait]
impl LockStore for CloudflareLockStore {
    #[tracing::instrument(name = "CloudflareLockStore::lock_resources", skip_all)]
    async fn lock_resources(
        &self,
        owner_id: &str,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        self.lock_resources_impl(owner_id, repository, resources)
            .await
    }

    #[tracing::instrument(name = "CloudflareLockStore::query_locks", skip_all)]
    async fn query_locks(&self, query: LockQuery) -> Result<Vec<LockData>, LockError> {
        self.query_locks_impl(query).await
    }

    #[tracing::instrument(name = "CloudflareLockStore::check_locks_status", skip_all)]
    async fn check_locks_status(
        &self,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        self.check_locks_status_impl(repository, resources).await
    }

    #[tracing::instrument(name = "CloudflareLockStore::unlock_resources", skip_all)]
    async fn unlock_resources(
        &self,
        owner_id: &str,
        validate_user: bool,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockResource>, LockError> {
        self.unlock_resources_impl(owner_id, validate_user, repository, resources)
            .await
    }
}

/// The locks of a query, as one statement over the columns the table keeps.
///
/// A query that names a repository and a branch separately is written against those columns
/// rather than against the `repository_branch` bytes they would be joined into, so the indexes on
/// the columns can answer it.
fn query_locks_sql(query: LockQuery) -> (String, TursoValues) {
    let (condition, parameters) = match query {
        LockQuery::Hash(hash) => ("hash = ?1", vec![hash_blob(&hash)]),
        LockQuery::HashRepository(hash, repository) => (
            "hash = ?1 AND repository = ?2",
            vec![
                hash_blob(&hash),
                crate::store::partition_blob(repository),
            ],
        ),
        LockQuery::HashRepositoryBranch(hash, repository, branch) => (
            "hash = ?1 AND repository = ?2 AND branch = ?3",
            vec![
                hash_blob(&hash),
                crate::store::partition_blob(repository),
                crate::store::context_blob(branch),
            ],
        ),
        LockQuery::Owner(owner) => ("owner_id = ?1", vec![Value::Text(owner)]),
        LockQuery::OwnerRepository(owner, repository) => (
            "owner_id = ?1 AND repository = ?2",
            vec![Value::Text(owner), crate::store::partition_blob(repository)],
        ),
        LockQuery::OwnerRepositoryBranch(owner, repository, branch) => (
            "owner_id = ?1 AND repository = ?2 AND branch = ?3",
            vec![
                Value::Text(owner),
                crate::store::partition_blob(repository),
                crate::store::context_blob(branch),
            ],
        ),
        LockQuery::Repository(repository) => (
            "repository = ?1",
            vec![crate::store::partition_blob(repository)],
        ),
        LockQuery::RepositoryBranch(repository, branch) => (
            "repository = ?1 AND branch = ?2",
            vec![
                crate::store::partition_blob(repository),
                crate::store::context_blob(branch),
            ],
        ),
        LockQuery::RepositoryBranchDescription(repository, branch, description) => (
            "repository = ?1 AND branch = ?2 AND description = ?3",
            vec![
                crate::store::partition_blob(repository),
                crate::store::context_blob(branch),
                Value::Text(description),
            ],
        ),
    };

    (format!("{SELECT_LOCKS} WHERE {condition}"), parameters)
}

/// One lock per resource, each resource once, in a settled order.
fn unique_resources(resources: &[LockResource]) -> Vec<LockResource> {
    let mut keys = resources.to_vec();
    keys.sort();
    keys.dedup();
    keys
}

/// The key of every resource, each once, in a settled order.
fn unique_keys(
    repository: RepositoryId,
    resources: &[LockResource],
) -> Vec<(Hash, RepositoryId, BranchId)> {
    let mut keys: Vec<(Hash, RepositoryId, BranchId)> = resources
        .iter()
        .map(|resource| (resource.hash, repository, resource.branch))
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// The statement that asks about one group of resources, and its parameters.
fn check_locks_sql(keys: &[(Hash, RepositoryId, BranchId)]) -> (String, TursoValues) {
    let mut sql = String::from(SELECT_LOCKS);
    let mut parameters = TursoValues::with_capacity(keys.len() * 3);

    for (index, (hash, repository, branch)) in keys.iter().enumerate() {
        let first = index * 3 + 1;

        sql.push_str(if index == 0 { " WHERE " } else { " OR " });
        sql.push_str(&format!(
            "(hash = ?{first} AND repository = ?{} AND branch = ?{})",
            first + 1,
            first + 2
        ));

        parameters.push(hash_blob(hash));
        parameters.push(crate::store::partition_blob(*repository));
        parameters.push(crate::store::context_blob(*branch));
    }

    (sql, parameters)
}

/// Whose lock a row read for its owner alone is.
fn owner_of(row: &[Value]) -> Result<String, StoreError> {
    blob_to_string("owner_id", cell(row, 0, OWNER_SQL)?)
}

/// One row of a lock table, as the store reports it.
fn lock_from_row(row: &[Value], sql: &str) -> Result<LockData, StoreError> {
    let hash = blob_to_hash("hash", cell(row, 0, sql)?)?;
    let branch = blob_to_context("branch", cell(row, 2, sql)?)?;
    let owner = blob_to_string("owner_id", cell(row, 3, sql)?)?;
    let description = blob_to_string("description", cell(row, 4, sql)?)?;
    let locked_at = integer_to_millis("locked_at", cell(row, 5, sql)?)?;

    Ok(LockData {
        resource: LockResource {
            branch,
            hash,
            description,
        },
        owner,
        locked_at,
    })
}

/// A row that is not the shape the table holds: a store inconsistency, reported as an internal
/// lock error because the lock store's error type has no other way to say it.
fn malformed(error: StoreError) -> LockError {
    LockError::internal_with_context(error, "reading a lock row")
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::store::partition_blob;

    fn resource(hash: u64, branch: u64, description: &str) -> LockResource {
        let mut branch_id = BranchId::default();
        branch_id.data_mut()[0] = branch as u8;

        LockResource {
            branch: branch_id,
            hash: Hash::from_u64(hash),
            description: description.to_string(),
        }
    }

    #[test]
    fn a_query_names_the_columns_it_means() {
        let repository = RepositoryId::default();

        let (sql, parameters) = query_locks_sql(LockQuery::HashRepositoryBranch(
            Hash::from_u64(1),
            repository,
            BranchId::default(),
        ));

        assert!(sql.contains("WHERE hash = ?1 AND repository = ?2 AND branch = ?3"));
        assert_eq!(parameters.len(), 3);
    }

    #[test]
    fn every_query_has_one_condition_per_parameter() {
        let repository = RepositoryId::default();
        let hash = Hash::from_u64(1);
        let branch = BranchId::default();

        let queries = [
            LockQuery::Hash(hash),
            LockQuery::HashRepository(hash, repository),
            LockQuery::HashRepositoryBranch(hash, repository, branch),
            LockQuery::Owner("owner".to_string()),
            LockQuery::OwnerRepository("owner".to_string(), repository),
            LockQuery::OwnerRepositoryBranch("owner".to_string(), repository, branch),
            LockQuery::Repository(repository),
            LockQuery::RepositoryBranch(repository, branch),
            LockQuery::RepositoryBranchDescription(
                repository,
                branch,
                "description".to_string(),
            ),
        ];

        for query in queries {
            let (sql, parameters) = query_locks_sql(query);

            assert!(sql.starts_with(SELECT_LOCKS), "{sql}");
            assert!(sql.contains(" WHERE "), "{sql}");
            assert_eq!(
                parameters.len(),
                sql.matches('?').count(),
                "one parameter per placeholder: {sql}"
            );
            // A query never reaches for the concatenated key, which no index on the columns serves.
            assert!(!sql.contains("repository_branch"), "{sql}");
            assert!(!sql.contains("substr("), "{sql}");
        }
    }

    #[test]
    fn a_lock_status_check_asks_about_each_resource_exactly() {
        let repository = RepositoryId::default();
        let resources = [
            resource(1, 1, "one"),
            resource(2, 2, "two"),
        ];

        let keys = unique_keys(repository, &resources);
        let (sql, parameters) = check_locks_sql(&keys);

        assert_eq!(keys.len(), 2);
        assert!(sql.contains(
            "(hash = ?1 AND repository = ?2 AND branch = ?3) OR (hash = ?4 AND repository = ?5 AND \
             branch = ?6)"
        ));
        assert_eq!(parameters.len(), 6);
        assert_eq!(sql.matches('?').count(), 6);
    }

    #[test]
    fn a_lock_status_check_asks_about_each_resource_once() {
        let repository = RepositoryId::default();
        let resources = [
            resource(1, 1, "one"),
            resource(2, 2, "two"),
            // The same key as the first, and a different description, which is still one key.
            resource(1, 1, "one, again"),
        ];

        let keys = unique_keys(repository, &resources);

        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].0, Hash::from_u64(1));
        assert_eq!(keys[1].0, Hash::from_u64(2));
    }

    #[test]
    fn unlocking_takes_each_resource_once_and_settles_the_order() {
        let resources = [
            resource(2, 2, "two"),
            resource(1, 1, "one"),
            resource(2, 2, "two"),
        ];

        let keys = unique_resources(&resources);

        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].hash, Hash::from_u64(1));
        assert_eq!(keys[1].hash, Hash::from_u64(2));
    }

    #[test]
    fn a_lock_row_reads_back_as_the_lock_it_was() {
        let row = vec![
            hash_blob(&Hash::from_u64(9)),
            partition_blob(RepositoryId::default()),
            crate::store::context_blob(BranchId::default()),
            Value::Text("owner".to_string()),
            Value::Text("description".to_string()),
            Value::Integer(1_700_000_000_000),
        ];

        let lock = lock_from_row(&row, SELECT_LOCKS).unwrap();

        assert_eq!(lock.resource.hash, Hash::from_u64(9));
        assert_eq!(lock.owner, "owner");
        assert_eq!(lock.resource.description, "description");
        assert_eq!(lock.locked_at, 1_700_000_000_000);
    }

    #[test]
    fn a_lock_row_of_the_wrong_shape_is_an_internal_error() {
        let mut row = vec![
            hash_blob(&Hash::from_u64(9)),
            partition_blob(RepositoryId::default()),
            crate::store::context_blob(BranchId::default()),
            Value::Text("owner".to_string()),
            Value::Text("description".to_string()),
            Value::Integer(1),
        ];

        // The owner is not text.
        row[3] = Value::Integer(7);
        assert!(lock_from_row(&row, SELECT_LOCKS).is_err());

        // The branch column is the one the row is keyed by, so a row without one cannot be read
        // as a lock: it would report a lock on a branch that does not exist.
        row[3] = Value::Text("owner".to_string());
        row[2] = Value::Null;
        assert!(lock_from_row(&row, SELECT_LOCKS).is_err());

        // A row with no time cannot say when the lock was taken.
        row[2] = crate::store::context_blob(BranchId::default());
        row[5] = Value::Null;
        assert!(lock_from_row(&row, SELECT_LOCKS).is_err());
    }
}
