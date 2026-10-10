// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The lock store, held in one Turso table (spec §4.4).
//!
//! One row per held lock, keyed by the hash and the `(repository, branch)` pair it was taken on.
//! Taking and releasing locks is all-or-nothing: a request that would leave a lock behind that
//! another client already holds writes nothing at all, because a caller that is told it failed
//! must not have to wonder which part of it happened.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
use crate::turso::retry::within_deadline;

/// The columns every lock row is read as.
const SELECT_LOCKS: &str = "SELECT hash, repository, branch, owner_id, description, locked_at \
     FROM locks";

/// How many resources one `check_locks_status` statement asks about.
///
/// Three parameters per resource, so fifty keeps a statement well inside any server's parameter
/// limit, and a caller checking a large push still ends up with a handful of round trips rather
/// than one per resource.
const CHECK_LOCKS_CHUNK: usize = 50;

/// How many parameters one statement may be given.
///
/// The server this store talks to accepts far more than this — measured against `sqld v0.24.32`,
/// 32766, above which it answers `too many SQL variables` — and 999 is the oldest limit any
/// `SQLite` build has shipped, so a statement built to this budget runs anywhere the table can
/// live. Locks are taken and released a statement at a time, and a request naming thousands of
/// resources would otherwise cost a round trip per resource.
pub const PARAMETER_BUDGET: usize = 999;

/// How many locks one insert statement takes: seven parameters each.
const LOCK_INSERT_RESOURCES: usize = PARAMETER_BUDGET / 7;

/// How many locks one statement keyed by the row's own key can name: two parameters each.
///
/// The row is keyed by the hash and the `(repository, branch)` pair they are joined into, and that
/// pair is what an insert conflicts on, so the statements that read around an insert and the ones
/// that delete name exactly that pair.
const LOCK_ROW_KEY_RESOURCES: usize = PARAMETER_BUDGET / 2;

/// A lock store in a Turso database.
pub struct CloudflareLockStore {
    executor: Arc<dyn TursoExecutor>,
    limits: TursoLimits,
}

impl CloudflareLockStore {
    pub fn new(executor: Arc<dyn TursoExecutor>, limits: TursoLimits) -> Self {
        Self { executor, limits }
    }

    /// Take every resource named, or none of them, inside the store's deadline.
    ///
    /// A transaction is the one thing the driver waits on for as long as the server does, and the
    /// server holds the write lock until the transaction that took it is finished. A lock request
    /// that meets another client's unfinished transaction therefore has to give up rather than
    /// wait for it (spec §4.1), and it gives up with the one answer a lock request has for
    /// "nothing was applied, try again" (spec §4.6).
    async fn lock_resources_impl(
        &self,
        owner_id: &str,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        within_deadline(
            self.limits.timeout,
            |error| error.into_lock_error("taking locks"),
            self.lock_resources_transaction(owner_id, repository, resources),
        )
        .await
    }

    /// Take every resource named, or none of them: one transaction, bounded by its caller.
    ///
    /// A candidate that was already held by the same owner is not an error and is not part of the
    /// answer: the caller hears about the locks this call took, and a retry of a request whose
    /// answer was lost takes nothing and says so.
    ///
    /// As many resources as one statement can hold are taken in one statement, and only the keys
    /// that statement did not report are asked about — those, and no others, are the rows that
    /// were already there (spec §4.6).
    async fn lock_resources_transaction(
        &self,
        owner_id: &str,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        // One timestamp for the whole request, so the locks it takes look like one moment.
        let locked_at = crate::store::now_millis();

        // A resource named twice is one row, so it is locked once and reported once.
        let keys = unique_resources(resources);

        let transaction =
            self.executor.begin_immediate().await.map_err(|error| {
                error.into_lock_error("opening a transaction to lock resources")
            })?;

        // The keys the inserts took. `RETURNING` reports the rows an insert actually wrote, so a
        // key that is missing from this set is a row that was already there, and the keys in it
        // are the only rows that are this call's doing.
        let mut taken: BTreeSet<(Hash, BranchId)> = BTreeSet::new();

        for chunk in keys.chunks(LOCK_INSERT_RESOURCES) {
            let (sql, parameters) = lock_sql(repository, chunk, owner_id, locked_at);

            let rows = match transaction.query(&sql, parameters).await {
                Ok(rows) => rows,
                Err(error) => {
                    discard(transaction, "could not take a lock").await;
                    return Err(error.into_lock_error("taking a lock"));
                }
            };

            for row in &rows {
                taken.insert(row_key(row, &sql).map_err(malformed)?);
            }
        }

        // Whose the rows that were already there are. The insert has one condition — the primary
        // key — so it cannot have been refused for anything else, and every key it did not report
        // is a lock somebody holds: this owner, or another one.
        let conflicted: Vec<(Hash, BranchId)> = keys
            .iter()
            .map(|resource| (resource.hash, resource.branch))
            .filter(|key| !taken.contains(key))
            .collect();

        for chunk in conflicted.chunks(LOCK_ROW_KEY_RESOURCES) {
            let (sql, parameters) = owner_sql(repository, chunk);

            let rows = match transaction.query(&sql, parameters).await {
                Ok(rows) => rows,
                Err(error) => {
                    discard(transaction, "could not read whose lock a row is").await;
                    return Err(error.into_lock_error("reading a lock's owner"));
                }
            };

            let owners: BTreeMap<(Hash, BranchId), String> = rows
                .iter()
                .map(|row| Ok((row_key(row, &sql)?, owner_of(row, &sql)?)))
                .collect::<Result<_, StoreError>>()
                .map_err(malformed)?;

            for key in chunk {
                let Some(owner) = owners.get(key) else {
                    // The insert conflicted, so a row is there; not finding it means the row
                    // changed underneath a transaction that holds the write lock.
                    discard(transaction, "a conflicted row is not there").await;
                    return Err(LockError::internal(
                        "a lock insert conflicted, but the row it conflicted with is not there",
                    ));
                };

                if owner != owner_id {
                    discard(transaction, "a resource is locked by another owner").await;
                    return Err(LockNotOwned.into());
                }

                // Held by this owner already: nothing was taken here, so it is not part of what
                // this call took.
            }
        }

        transaction
            .commit()
            .await
            .map_err(|error| error.into_lock_error("committing a lock request"))?;

        // The request's own order, each resource once: a caller that asked about a set of
        // resources can line the answer up with what it asked about.
        let mut reported: BTreeSet<(Hash, BranchId)> = BTreeSet::new();
        let mut locked = Vec::with_capacity(taken.len());

        for resource in resources {
            let key = (resource.hash, resource.branch);

            if taken.contains(&key) && reported.insert(key) {
                locked.push(LockData {
                    resource: resource.clone(),
                    owner: owner_id.to_string(),
                    locked_at,
                });
            }
        }

        Ok(locked)
    }

    /// Read the locks a query names, oldest row first.
    async fn query_locks_impl(&self, query: LockQuery) -> Result<Vec<LockData>, LockError> {
        let (sql, parameters) = query_locks_sql(query);

        // The parameters are cloned per attempt: the macro may run the operation again, and a
        // value that was moved into the first attempt is not there for the second.
        let rows =
            retry_within_deadline!(self.limits, self.executor.query(&sql, parameters.clone()))
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

            let rows =
                retry_within_deadline!(self.limits, self.executor.query(&sql, parameters.clone()))
                    .map_err(|error| error.into_lock_error("checking lock status"))?;

            for row in &rows {
                locks.push(lock_from_row(row, &sql).map_err(malformed)?);
            }
        }

        Ok(locks)
    }

    /// Release every resource named, or none of them, inside the store's deadline (spec §4.1).
    ///
    /// `validate_user` decides what a resource locked by someone else means: with it on, the
    /// whole request fails and no row is removed; with it off, the lock is released anyway, which
    /// is what an administrative unlock does.
    ///
    /// Whether a row is missing and whether it belongs to someone else are both decided before
    /// anything is removed, because an affected row count cannot tell the two apart. A row that
    /// belongs to someone else outranks a row that is not there, as it does in the `DynamoDB`
    /// store.
    async fn unlock_resources_impl(
        &self,
        owner_id: &str,
        validate_user: bool,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockResource>, LockError> {
        within_deadline(
            self.limits.timeout,
            |error| error.into_lock_error("releasing locks"),
            self.unlock_resources_transaction(owner_id, validate_user, repository, resources),
        )
        .await
    }

    /// Release every resource named, or none of them: one transaction, bounded by its caller.
    ///
    /// As many resources as one statement can hold are released in one statement, in the same
    /// shape the lock request uses.
    async fn unlock_resources_transaction(
        &self,
        owner_id: &str,
        validate_user: bool,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockResource>, LockError> {
        // A resource named twice is one row: releasing it twice would report a row that is not
        // there, and asking whose it is twice is two reads of one answer.
        let keys = unique_resources(resources);

        let transaction =
            self.executor.begin_immediate().await.map_err(|error| {
                error.into_lock_error("opening a transaction to unlock resources")
            })?;

        if validate_user {
            // Whose every lock is, before anything is removed. A row that belongs to someone else
            // outranks a row that is not there, so the reads all happen and the foreign owner is
            // what is reported when there is one.
            let mut owner_of_key: BTreeMap<(Hash, BranchId), String> = BTreeMap::new();

            for chunk in keys
                .iter()
                .map(|resource| (resource.hash, resource.branch))
                .collect::<Vec<_>>()
                .chunks(LOCK_ROW_KEY_RESOURCES)
            {
                let (sql, parameters) = owner_sql(repository, chunk);

                let rows = match transaction.query(&sql, parameters).await {
                    Ok(rows) => rows,
                    Err(error) => {
                        discard(transaction, "could not read whose lock a row is").await;
                        return Err(error.into_lock_error("reading a lock's owner"));
                    }
                };

                for row in &rows {
                    owner_of_key.insert(
                        row_key(row, &sql).map_err(malformed)?,
                        owner_of(row, &sql).map_err(malformed)?,
                    );
                }
            }

            let mut missing = false;

            for resource in &keys {
                match owner_of_key.get(&(resource.hash, resource.branch)) {
                    // Not there: nothing to release. Whether that fails the request depends on
                    // whether a row belonging to someone else is behind one of the keys.
                    None => missing = true,
                    Some(owner) if owner != owner_id => {
                        discard(transaction, "a resource is locked by another owner").await;
                        return Err(LockNotOwned.into());
                    }
                    Some(_) => {}
                }
            }

            if missing {
                discard(transaction, "some resources are not locked").await;
                return Err(LockNotFound.into());
            }
        }

        for chunk in keys
            .iter()
            .map(|resource| (resource.hash, resource.branch))
            .collect::<Vec<_>>()
            .chunks(LOCK_ROW_KEY_RESOURCES)
        {
            let (sql, parameters) = unlock_sql(repository, chunk);

            if let Err(error) = transaction.execute(&sql, parameters).await {
                discard(transaction, "could not release a lock").await;
                return Err(error.into_lock_error("releasing a lock"));
            }
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
            vec![hash_blob(&hash), crate::store::partition_blob(repository)],
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

/// One lock per resource, each resource's row once, in a settled order.
///
/// The row is what a duplicate is, not the whole resource: two requests that name the same hash
/// and branch with different descriptions name one lock, and releasing or reading it twice would
/// report a row that is not there and a second answer to one question.
fn unique_resources(resources: &[LockResource]) -> Vec<LockResource> {
    let mut keys = resources.to_vec();
    keys.sort_by_key(|resource| (resource.hash, resource.branch));
    keys.dedup_by(|a, b| a.hash == b.hash && a.branch == b.branch);
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

/// The statement that takes one group of locks, and its parameters.
///
/// `RETURNING` is what makes a group cost one statement: the rows it hands back are the locks it
/// wrote, so a row that was already there is simply not among them. The insert has one condition —
/// the primary key — and nothing else can make it fail, so a key that is not reported is a lock
/// somebody already holds.
fn lock_sql(
    repository: RepositoryId,
    resources: &[LockResource],
    owner_id: &str,
    locked_at: u64,
) -> (String, TursoValues) {
    let mut sql = String::from(
        "INSERT INTO locks \
         (hash, repository_branch, repository, branch, owner_id, description, locked_at) VALUES ",
    );
    let mut parameters = TursoValues::with_capacity(resources.len() * 7);

    for (index, resource) in resources.iter().enumerate() {
        let first = index * 7 + 1;

        sql.push_str(if index == 0 { "(" } else { ", (" });
        sql.push_str(&format!(
            "?{first}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}",
            first + 1,
            first + 2,
            first + 3,
            first + 4,
            first + 5,
            first + 6
        ));
        sql.push(')');

        parameters.push(hash_blob(&resource.hash));
        parameters.push(repository_branch_blob(repository, resource.branch));
        parameters.push(crate::store::partition_blob(repository));
        parameters.push(crate::store::context_blob(resource.branch));
        parameters.push(Value::Text(owner_id.to_string()));
        parameters.push(Value::Text(resource.description.clone()));
        parameters.push(Value::Integer(locked_at as i64));
    }

    sql.push_str(
        " ON CONFLICT (hash, repository_branch) DO NOTHING RETURNING hash, repository, branch",
    );

    (sql, parameters)
}

/// The statement that asks whose a group of locks is, and its parameters.
///
/// It names the pair the insert conflicts on, one predicate per key.
fn owner_sql(repository: RepositoryId, keys: &[(Hash, BranchId)]) -> (String, TursoValues) {
    let mut sql = String::from("SELECT hash, repository, branch, owner_id FROM locks");
    let mut parameters = TursoValues::with_capacity(keys.len() * 2);

    for (index, (hash, branch)) in keys.iter().enumerate() {
        let first = index * 2 + 1;

        sql.push_str(if index == 0 { " WHERE " } else { " OR " });
        sql.push_str(&format!(
            "(hash = ?{first} AND repository_branch = ?{})",
            first + 1
        ));

        parameters.push(hash_blob(hash));
        parameters.push(repository_branch_blob(repository, *branch));
    }

    (sql, parameters)
}

/// The statement that releases a group of locks, and its parameters.
///
/// Whether each row is there, and whose it was, is decided before this runs.
fn unlock_sql(repository: RepositoryId, keys: &[(Hash, BranchId)]) -> (String, TursoValues) {
    let mut sql = String::from("DELETE FROM locks");
    let mut parameters = TursoValues::with_capacity(keys.len() * 2);

    for (index, (hash, branch)) in keys.iter().enumerate() {
        let first = index * 2 + 1;

        sql.push_str(if index == 0 { " WHERE " } else { " OR " });
        sql.push_str(&format!(
            "(hash = ?{first} AND repository_branch = ?{})",
            first + 1
        ));

        parameters.push(hash_blob(hash));
        parameters.push(repository_branch_blob(repository, *branch));
    }

    (sql, parameters)
}

/// The rows a `locks` table is read as for a `RETURNING` or keyed statement: `hash, repository,
/// branch, …`. The key is the hash and the branch, the two columns every one of those rows starts
/// with.
fn row_key(row: &[Value], sql: &str) -> Result<(Hash, BranchId), StoreError> {
    Ok((
        blob_to_hash("hash", cell(row, 0, sql)?)?,
        blob_to_context("branch", cell(row, 2, sql)?)?,
    ))
}

/// Whose lock a row read for its owner alone is.
fn owner_of(row: &[Value], sql: &str) -> Result<String, StoreError> {
    // The owner is the last column of every row these statements read, after the two key columns
    // and the repository between them.
    blob_to_string("owner_id", cell(row, 3, sql)?)
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
            LockQuery::RepositoryBranchDescription(repository, branch, "description".to_string()),
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
        let resources = [resource(1, 1, "one"), resource(2, 2, "two")];

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
    fn unlocking_takes_each_row_once_and_settles_the_order() {
        let resources = [
            resource(2, 2, "two"),
            resource(1, 1, "one"),
            // The same row as the first, described differently: one lock, not two.
            resource(2, 2, "two, again"),
        ];

        let keys = unique_resources(&resources);

        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].hash, Hash::from_u64(1));
        assert_eq!(keys[1].hash, Hash::from_u64(2));
        assert_eq!(
            keys[1].description, "two",
            "the first of the equal rows is the one kept"
        );
    }

    #[test]
    fn a_group_is_taken_by_one_statement() {
        let repository = RepositoryId::default();
        let resources = [resource(1, 1, "one"), resource(2, 2, "two")];

        let (sql, parameters) = lock_sql(repository, &resources, "owner", 7);

        assert!(sql.starts_with("INSERT INTO locks"), "{sql}");
        assert!(
            sql.contains("VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7), (?8, "),
            "{sql}"
        );
        assert!(sql.ends_with("ON CONFLICT (hash, repository_branch) DO NOTHING RETURNING hash, repository, branch"), "{sql}");
        assert_eq!(parameters.len(), 14);
        assert_eq!(sql.matches('?').count(), 14);
    }

    #[test]
    fn a_group_is_asked_about_and_released_by_one_statement_each() {
        let repository = RepositoryId::default();
        let keys = vec![
            (Hash::from_u64(1), BranchId::default()),
            (Hash::from_u64(2), BranchId::default()),
        ];

        let (owner_statement, owner_parameters) = owner_sql(repository, &keys);

        assert!(
            owner_statement.starts_with("SELECT hash, repository, branch, owner_id FROM locks"),
            "{owner_statement}"
        );
        assert!(
            owner_statement.contains(
                "WHERE (hash = ?1 AND repository_branch = ?2) OR (hash = ?3 AND \
                 repository_branch = ?4)"
            ),
            "{owner_statement}"
        );
        assert_eq!(owner_parameters.len(), 4);

        let (unlock_statement, unlock_parameters) = unlock_sql(repository, &keys);

        assert!(
            unlock_statement.contains(
                "DELETE FROM locks WHERE (hash = ?1 AND repository_branch = ?2) OR (hash = ?3 \
                 AND repository_branch = ?4)"
            ),
            "{unlock_statement}"
        );
        assert_eq!(unlock_parameters.len(), 4);
        assert_eq!(unlock_statement.matches('?').count(), 4);
    }

    /// A group of the largest shape is inside the parameter budget, and a request larger than a
    /// group is cut into groups that each are.
    #[test]
    fn a_group_stays_inside_the_parameter_budget() {
        let repository = RepositoryId::default();

        let resources: Vec<LockResource> = (0..LOCK_INSERT_RESOURCES + 1)
            .map(|index| resource(index as u64, 1, "resource"))
            .collect();

        let (_, parameters) = lock_sql(repository, &resources[..LOCK_INSERT_RESOURCES], "owner", 1);
        assert_eq!(parameters.len(), LOCK_INSERT_RESOURCES * 7);
        assert!(parameters.len() <= PARAMETER_BUDGET);

        let (_, parameters) = lock_sql(repository, &resources, "owner", 1);
        assert!(
            parameters.len() > PARAMETER_BUDGET,
            "a chunk is what keeps a request inside the budget"
        );
        for chunk in resources.chunks(LOCK_INSERT_RESOURCES) {
            let (sql, parameters) = lock_sql(repository, chunk, "owner", 1);
            assert!(parameters.len() <= PARAMETER_BUDGET, "{sql}");
        }

        let keys: Vec<(Hash, BranchId)> = (0..LOCK_ROW_KEY_RESOURCES + 1)
            .map(|index| (Hash::from_u64(index as u64), BranchId::default()))
            .collect();

        for chunk in keys.chunks(LOCK_ROW_KEY_RESOURCES) {
            let (sql, parameters) = owner_sql(repository, chunk);
            assert!(parameters.len() <= PARAMETER_BUDGET, "{sql}");

            let (sql, parameters) = unlock_sql(repository, chunk);
            assert!(parameters.len() <= PARAMETER_BUDGET, "{sql}");
        }
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
