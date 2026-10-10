// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The Turso schema, as one atomic batch (spec §4.2).

/// Which version of the schema this build writes and understands.
///
/// A database holding another version stops the server at startup rather than running against a
/// table layout it never agreed on. There is no migration: the database starts empty.
pub const SCHEMA_VERSION: i64 = 1;

/// The statement that asks which version is installed.
pub const SELECT_SCHEMA_VERSION: &str = "SELECT version FROM schema_version LIMIT 1";

/// The statement that records the version, once, when the ledger is empty.
pub const INSERT_SCHEMA_VERSION: &str =
    "INSERT OR IGNORE INTO schema_version (version) VALUES (?1)";

/// The statements that create the schema, in order.
///
/// One entry per statement so the count can be checked against the configured limit on the
/// statements in one transaction, and so the whole schema can be applied as a single batch.
///
/// `fragment_state` and `fragments` belong to the immutable store, which this crate does not
/// implement yet; they are created here so that the schema is one thing rather than pieces that
/// arrive with the code that reads them.
pub const DDL_STATEMENTS: &[&str] = &[
    // One row per hash the store knows, holding the fragment's state bits as they are written:
    // stored is 0, and obliteration adds its own bits to that value.
    "CREATE TABLE IF NOT EXISTS fragment_state (
        hash  BLOB NOT NULL PRIMARY KEY,
        state INTEGER NOT NULL
    ) WITHOUT ROWID",
    // Which repositories a fragment is stored for. `repository_context` is the repository's id
    // followed by the address context, 32 bytes together, so one fragment can be stored for
    // several repositories.
    "CREATE TABLE IF NOT EXISTS fragments (
        hash                BLOB NOT NULL,
        repository_context  BLOB NOT NULL,
        PRIMARY KEY (hash, repository_context)
    ) WITHOUT ROWID",
    // One row per key. `repository_id` is the partition, and a null partition is 16 zero bytes,
    // which `list` reads as "every repository". The first byte of `key` is its `KeyType`. A value
    // of 32 zero bytes is never written: it means "no value" (spec §4.3).
    "CREATE TABLE IF NOT EXISTS mutable (
        repository_id  BLOB NOT NULL,
        key            BLOB NOT NULL,
        value          BLOB NOT NULL,
        PRIMARY KEY (repository_id, key)
    ) WITHOUT ROWID",
    // One row per held lock. `repository_branch` is the repository followed by the branch, 32
    // bytes together; it is the sort key of the lock's own table, while the columns it is built
    // from are there so a query can name the repository and the branch it means.
    "CREATE TABLE IF NOT EXISTS locks (
        hash               BLOB NOT NULL,
        repository_branch  BLOB NOT NULL,
        repository         BLOB NOT NULL,
        branch             BLOB NOT NULL,
        owner_id           TEXT NOT NULL,
        description        TEXT NOT NULL,
        locked_at          INTEGER NOT NULL,
        PRIMARY KEY (hash, repository_branch)
    ) WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS locks_owner_repo_branch
        ON locks (owner_id, repository_branch)",
    "CREATE INDEX IF NOT EXISTS locks_repo_branch ON locks (repository, branch)",
    "CREATE INDEX IF NOT EXISTS locks_repo_branch_description
        ON locks (repository_branch, description)",
    // The ledger of the schema's version. A table rather than `PRAGMA user_version`, which Turso
    // Cloud has not been measured to accept.
    "CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY
    ) WITHOUT ROWID",
];

/// The whole schema as one batch, which `execute_transactional_batch` runs as a transaction.
pub fn ddl() -> String {
    DDL_STATEMENTS.join(";\n")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn the_schema_creates_every_table_once() {
        for table in [
            "fragment_state",
            "fragments",
            "mutable",
            "locks",
            "schema_version",
        ] {
            let creates = DDL_STATEMENTS
                .iter()
                .filter(|statement| statement.contains(&format!("CREATE TABLE IF NOT EXISTS {table}")))
                .count();
            assert_eq!(creates, 1, "{table}");
        }
    }

    /// The statements of a batch, split the way the parser splits them. None of these statements
    /// holds a semicolon in a string literal, so splitting on the semicolon is enough here; the
    /// batch reaching a server is covered by the integration test.
    fn parse(sql: &str) -> Vec<String> {
        sql.split(';')
            .map(|statement| statement.trim().to_string())
            .filter(|statement| !statement.is_empty())
            .collect()
    }

    /// Every statement but the last is terminated, so the batch is a list of complete statements
    /// rather than one statement that ends in the middle.
    #[test]
    fn the_batch_has_one_statement_per_entry() {
        let statements = parse(&ddl());
        assert_eq!(statements.len(), DDL_STATEMENTS.len());
        for (statement, expected) in statements.iter().zip(DDL_STATEMENTS.iter()) {
            assert_eq!(statement, expected.trim());
        }
    }

    #[test]
    fn no_statement_opens_a_transaction_of_its_own() {
        for statement in DDL_STATEMENTS {
            let upper = statement.to_uppercase();
            for forbidden in ["BEGIN", "COMMIT", "ROLLBACK"] {
                assert!(!upper.contains(forbidden), "{statement}");
            }
        }
    }
}
