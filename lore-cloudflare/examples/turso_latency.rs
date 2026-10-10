// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Measures what a Turso write costs, and what happens to it under a burst (spec §5.4).
//!
//! The store writes one fragment as two statements in two transactions — the state row and the
//! association row (`store/immutable_store.rs`) — so the cost that decides how long a migration
//! takes is the *commit*, not the statement. This probe puts three numbers next to each other:
//!
//! 1. the latency of one `BEGIN IMMEDIATE; INSERT; COMMIT`, over fifty serial transactions, as
//!    p50 / p95;
//! 2. what committing once for eight fragments saves over committing once per fragment;
//! 3. what four writers doing the same thing for thirty seconds do to each other — each operation
//!    given Turso's own five-second transaction limit, so a writer that goes over it is counted
//!    rather than waited for.
//!
//! ```text
//! LORE_TURSO_URL=libsql://<db>.turso.io LORE_TURSO_AUTH_TOKEN=... \
//!   cargo run -p lore-cloudflare --example turso_latency
//! ```
//!
//! Everything is written to a table named for this run and dropped before the process exits, so
//! the probe can be pointed at a database that holds real data. The token is never printed; the
//! leftmost label of a `turso.io` host becomes `<account>`.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use libsql::Value;
use lore_cloudflare::TursoClient;
use lore_cloudflare::TursoError;
use lore_cloudflare::TursoErrorKind;
use lore_cloudflare::TursoExecutor;
use lore_cloudflare::TursoSettings;
use tokio::time::timeout;

/// How many serial transactions item 1 times, after the warm-up ones.
const SERIAL_TRANSACTIONS: usize = 50;

/// Transactions run before the timed ones, to take the first connection's cost out of p50.
const WARMUP_TRANSACTIONS: usize = 5;

/// How many fragments item 2 writes each way.
const BATCH_FRAGMENTS: usize = 8;

/// How many writers item 3 runs at once.
const OVERLOAD_WORKERS: usize = 4;

/// How long item 3 runs for.
const OVERLOAD_DURATION: Duration = Duration::from_secs(30);

/// Turso's own ceiling on one transaction. An operation that has not answered by then is over it,
/// which is the thing item 3 counts.
const TRANSACTION_DEADLINE: Duration = Duration::from_secs(5);

/// The worker ids the three items use, so their rows cannot collide in the one table.
const ITEM_1_WORKER: i64 = 1;
const ITEM_2_SERIAL_WORKER: i64 = 2;
const ITEM_2_BATCHED_WORKER: i64 = 3;
const ITEM_3_FIRST_WORKER: i64 = 10;

#[tokio::main]
async fn main() {
    if let Err(message) = run().await {
        eprintln!("turso_latency: {message}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let settings = settings_from_environment()?;
    let client = Arc::new(
        TursoClient::connect(&settings)
            .await
            .map_err(|error| format!("connecting to Turso: {error}"))?,
    );

    let table = probe_table_name();

    println!("# Turso latency probe");
    println!();
    println!("- url: {}", redact_url(settings.url()));
    println!("- table: {table} (この run だけの使い捨て。最後に DROP する)");
    println!("- operation timeout: {:?}", settings.limits().timeout);
    println!("- transaction deadline (Turso の上限): {TRANSACTION_DEADLINE:?}");
    println!("- max connections: {}", settings.max_connections());
    println!();

    client
        .execute(&create_table_sql(&table), Vec::new())
        .await
        .map_err(|error| format!("creating {table}: {error}"))?;

    let measured = measure(&client, &table).await;

    match client
        .execute(&format!("DROP TABLE IF EXISTS {table}"), Vec::new())
        .await
    {
        Ok(_) => println!("- cleanup: DROP TABLE {table} 成功 (この run の行は残っていない)"),
        Err(error) => println!("- cleanup: DROP TABLE {table} 失敗: {error}"),
    }

    measured
}

async fn measure(client: &Arc<TursoClient>, table: &str) -> Result<(), String> {
    item_1_commit_latency(client, table).await?;
    item_2_batching(client, table).await?;
    item_3_overload(client, table).await;

    Ok(())
}

/// Item 1: one fragment's commit, fifty times over.
///
/// The store pays this twice per fragment, once for the state row and once for the association
/// row, so the number that matters for a migration is twice what is printed here.
async fn item_1_commit_latency(client: &TursoClient, table: &str) -> Result<(), String> {
    let insert = insert_sql(table);

    for sequence in 0..WARMUP_TRANSACTIONS {
        commit_one(client, &insert, ITEM_1_WORKER, sequence as i64).await?;
    }

    let mut samples = Vec::with_capacity(SERIAL_TRANSACTIONS);
    for sequence in 0..SERIAL_TRANSACTIONS {
        let started = Instant::now();
        commit_one(
            client,
            &insert,
            ITEM_1_WORKER,
            (WARMUP_TRANSACTIONS + sequence) as i64,
        )
        .await?;
        samples.push(started.elapsed());
    }

    let total: Duration = samples.iter().sum();
    let slowest = samples.iter().copied().max().unwrap_or_default();

    println!(
        "## 1: commit の遅延 (BEGIN IMMEDIATE; INSERT; COMMIT を {SERIAL_TRANSACTIONS} 回直列)"
    );
    println!();
    println!("| 項目 | 値 |");
    println!("|---|---|");
    println!("| 回数 | {SERIAL_TRANSACTIONS} (前に {WARMUP_TRANSACTIONS} 回の暖機) |");
    println!(
        "| min | {} ms |",
        millis(*samples.iter().min().unwrap_or(&Duration::ZERO))
    );
    println!("| p50 | {} ms |", millis(percentile(&samples, 0.50)));
    println!("| p95 | {} ms |", millis(percentile(&samples, 0.95)));
    println!("| max | {} ms |", millis(slowest));
    println!("| 合計 | {} ms |", millis(total));
    println!();
    println!(
        "判定: p50 {} ms / p95 {} ms。store は 1 fragment に 2 commit を払うので、fragment \
         あたりおよそ {} / {} ms (p50 / p95)。",
        millis(percentile(&samples, 0.50)),
        millis(percentile(&samples, 0.95)),
        millis(percentile(&samples, 0.50) * 2),
        millis(percentile(&samples, 0.95) * 2),
    );
    println!();

    Ok(())
}

/// Item 2: what one commit per eight fragments buys over one commit each.
///
/// Both sides write the same sixteen statements — two per fragment, the state row and the
/// association row the store writes — so the only difference is where the commits fall.
async fn item_2_batching(client: &TursoClient, table: &str) -> Result<(), String> {
    let insert = insert_sql(table);

    let started = Instant::now();
    for fragment in 0..BATCH_FRAGMENTS {
        let transaction = client
            .begin_immediate()
            .await
            .map_err(|error| format!("begin (1 fragment 1 commit): {error}"))?;

        for statement in 0..2 {
            transaction
                .execute(
                    &insert,
                    values(ITEM_2_SERIAL_WORKER, (fragment * 2 + statement) as i64),
                )
                .await
                .map_err(|error| format!("insert (1 fragment 1 commit): {error}"))?;
        }

        transaction
            .commit()
            .await
            .map_err(|error| format!("commit (1 fragment 1 commit): {error}"))?;
    }
    let serial = started.elapsed();

    let started = Instant::now();
    let transaction = client
        .begin_immediate()
        .await
        .map_err(|error| format!("begin (8 fragment 1 commit): {error}"))?;

    for fragment in 0..BATCH_FRAGMENTS {
        for statement in 0..2 {
            transaction
                .execute(
                    &insert,
                    values(ITEM_2_BATCHED_WORKER, (fragment * 2 + statement) as i64),
                )
                .await
                .map_err(|error| format!("insert (8 fragment 1 commit): {error}"))?;
        }
    }

    transaction
        .commit()
        .await
        .map_err(|error| format!("commit (8 fragment 1 commit): {error}"))?;
    let batched = started.elapsed();

    let ratio = if serial.is_zero() {
        0.0
    } else {
        batched.as_secs_f64() / serial.as_secs_f64()
    };

    println!("## 2: batch の効き ({BATCH_FRAGMENTS} fragment、各 fragment 2 文)");
    println!();
    println!("| 方法 | commit 数 | 文の数 | 合計 ms | fragment あたり ms |");
    println!("|---|---|---|---|---|");
    println!(
        "| 1 fragment ずつ commit | {BATCH_FRAGMENTS} | {} | {} | {} |",
        BATCH_FRAGMENTS * 2,
        millis(serial),
        millis(serial / BATCH_FRAGMENTS as u32),
    );
    println!(
        "| {BATCH_FRAGMENTS} fragment を 1 commit | 1 | {} | {} | {} |",
        BATCH_FRAGMENTS * 2,
        millis(batched),
        millis(batched / BATCH_FRAGMENTS as u32),
    );
    println!();
    println!(
        "判定: commit は {BATCH_FRAGMENTS} 分の 1 になり、合計は {ratio:.2} 倍。文の数は同じなので、差は commit \
         の往復回数。"
    );
    println!();

    Ok(())
}

/// Item 3: four writers, the lock's own transaction shape, for thirty seconds.
///
/// Each writer opens one immediate transaction per operation and counts what came back: a
/// completed write, a `SlowDown`, a disconnection, or nothing at all within Turso's own five
/// seconds. The last count is the one this item exists for — a writer that is over the deadline is
/// what the store's callers would see as a failure, and the question is whether a real Turso
/// produces it under a burst the way a local `sqld` was reported to.
async fn item_3_overload(client: &Arc<TursoClient>, table: &str) {
    let insert = insert_sql(table);
    let deadline = Instant::now() + OVERLOAD_DURATION;
    let started = Instant::now();

    // `tokio::join!` rather than spawning: the four writers are I/O-bound, so one task polls them
    // concurrently, and the root `clippy.toml` keeps `tokio::spawn` for work that carries a
    // `LORE_CONTEXT`, which a probe has none of.
    let (first, second, third, fourth) = tokio::join!(
        overload_worker(Arc::clone(client), insert.clone(), 10, deadline),
        overload_worker(Arc::clone(client), insert.clone(), 11, deadline),
        overload_worker(Arc::clone(client), insert.clone(), 12, deadline),
        overload_worker(Arc::clone(client), insert.clone(), 13, deadline),
    );
    let elapsed = started.elapsed();

    let reports = [first, second, third, fourth];

    println!(
        "## 3: 過負荷 ({OVERLOAD_WORKERS} 並列 × {OVERLOAD_DURATION:?}、1 操作 = BEGIN IMMEDIATE; \
         INSERT; COMMIT)"
    );
    println!();
    println!("| worker | 成功 | 5 秒超 | SlowDown | Disconnected | その他 | max ms |");
    println!("|---|---|---|---|---|---|---|");
    for (index, report) in reports.iter().enumerate() {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} |",
            ITEM_3_FIRST_WORKER + index as i64,
            report.operations,
            report.over_deadline,
            report.slow_down,
            report.disconnected,
            report.other,
            millis(report.max),
        );
    }

    let operations: u64 = reports.iter().map(|report| report.operations).sum();
    let over_deadline: u64 = reports.iter().map(|report| report.over_deadline).sum();
    let slow_down: u64 = reports.iter().map(|report| report.slow_down).sum();
    let disconnected: u64 = reports.iter().map(|report| report.disconnected).sum();
    let other: u64 = reports.iter().map(|report| report.other).sum();
    let slowest = reports
        .iter()
        .map(|report| report.max)
        .max()
        .unwrap_or_default();

    println!(
        "| 合計 | {operations} | {over_deadline} | {slow_down} | {disconnected} | {other} | \
         {} |",
        millis(slowest),
    );
    println!();
    println!("実測時間: {elapsed:?}");

    let verdict = if over_deadline == 0 && slow_down == 0 && disconnected == 0 {
        "全操作が 5 秒以内に終わった。並列 4 本では real Turso は詰まらない".to_string()
    } else if over_deadline == 0 {
        "5 秒超は無いが、拒否 (SlowDown / Disconnected) が出た → store は操作単位でやり直す \
         (第 4.6 節)。"
            .to_string()
    } else {
        "5 秒を超えた操作がある → 並列 4 本で Turso が応答を止める。store の timeout と \
         max_connections の見直しが要る。"
            .to_string()
    };

    println!("判定: {verdict}");
    println!();
}

/// One writer's thirty seconds, as the numbers item 3 prints.
#[derive(Debug, Default)]
struct WorkerReport {
    operations: u64,
    over_deadline: u64,
    slow_down: u64,
    disconnected: u64,
    other: u64,
    max: Duration,
}

/// Drive one writer until the shared deadline passes.
///
/// The loop does not stop at the first failure: how *often* Turso declines to answer is the
/// measurement, and a writer that stops after one would report a single five-second operation as
/// though the database were healthy for the rest of the run.
async fn overload_worker(
    client: Arc<TursoClient>,
    insert: String,
    worker: i64,
    deadline: Instant,
) -> WorkerReport {
    let mut report = WorkerReport::default();
    let mut sequence: i64 = 0;

    while Instant::now() < deadline {
        let started = Instant::now();

        let outcome = timeout(TRANSACTION_DEADLINE, async {
            let transaction = client.begin_immediate().await?;
            transaction
                .execute(&insert, values(worker, sequence))
                .await?;

            transaction.commit().await
        })
        .await;

        report.max = report.max.max(started.elapsed());

        match outcome {
            // The operation was still in flight when the deadline passed. The future is dropped,
            // which abandons the transaction rather than committing it.
            Err(_) => report.over_deadline += 1,
            Ok(Ok(())) => report.operations += 1,
            Ok(Err(error)) => match error.kind() {
                TursoErrorKind::SlowDown => report.slow_down += 1,
                TursoErrorKind::Disconnected => report.disconnected += 1,
                TursoErrorKind::Maintenance | TursoErrorKind::Internal => report.other += 1,
            },
        }

        sequence += 1;
    }

    report
}

/// One `BEGIN IMMEDIATE; INSERT; COMMIT`, timed from before the transaction opens.
async fn commit_one(
    client: &TursoClient,
    insert: &str,
    worker: i64,
    sequence: i64,
) -> Result<Duration, String> {
    let started = Instant::now();

    let transaction = client
        .begin_immediate()
        .await
        .map_err(|error| format!("begin: {error}"))?;
    transaction
        .execute(insert, values(worker, sequence))
        .await
        .map_err(|error| format!("insert: {error}"))?;
    transaction
        .commit()
        .await
        .map_err(|error| format!("commit: {error}"))?;

    Ok(started.elapsed())
}

/// The parameters of one row: whose it is, and a payload shaped like the store's.
fn values(worker: i64, sequence: i64) -> Vec<Value> {
    // A 32-byte blob, filled from the two keys, because the store's rows are keyed by a hash and
    // the probe's table should carry the same column widths.
    let seed = u8::try_from(sequence.rem_euclid(251)).unwrap_or(0);
    let hash = vec![seed; 32];

    vec![
        Value::Integer(worker),
        Value::Integer(sequence),
        Value::Blob(hash),
        Value::Integer(0),
    ]
}

/// The throwaway table.
///
/// `WITHOUT ROWID` and a two-column primary key, as the store's state table has, so the write this
/// measures is the write a fragment's state row is.
fn create_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table} (\
           worker INTEGER NOT NULL, \
           seq INTEGER NOT NULL, \
           payload BLOB NOT NULL, \
           state INTEGER NOT NULL, \
           PRIMARY KEY (worker, seq)\
         ) WITHOUT ROWID"
    )
}

/// The insert every item issues. `ON CONFLICT DO NOTHING` is the store's own shape: a repeated
/// write is a no-op rather than an error, so a retry can be run as it stands.
fn insert_sql(table: &str) -> String {
    format!(
        "INSERT INTO {table} (worker, seq, payload, state) VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT DO NOTHING"
    )
}

/// A name no other run can be using, so two probes against one database cannot collide.
///
/// Built from a timestamp and a random number, and only ever from hex digits, so it is an
/// identifier by construction rather than by escaping.
fn probe_table_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    let salt = rand::random::<u32>();

    format!("latency_probe_{millis:x}{salt:08x}")
}

/// The settings a deployment configures, from the same variable names the plugin reads.
///
/// The token is optional: a local `sqld` needs none, and a server that needs one refuses the first
/// statement with its own message.
fn settings_from_environment() -> Result<TursoSettings, String> {
    let url = std::env::var("LORE_TURSO_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or("LORE_TURSO_URL is not set")?;
    let token = std::env::var("LORE_TURSO_AUTH_TOKEN").unwrap_or_default();

    Ok(TursoSettings::new(url, token))
}

/// The nearest-rank percentile of a set of samples.
fn percentile(samples: &[Duration], percentile: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();

    let rank = ((sorted.len() - 1) as f64 * percentile).round() as usize;

    sorted[rank.min(sorted.len() - 1)]
}

/// A duration in milliseconds, which is the unit every table here is in.
fn millis(duration: Duration) -> String {
    format!("{:.1}", duration.as_secs_f64() * 1000.0)
}

/// The URL with a signature stripped and the account replaced, in the same spirit as the R2 probe.
///
/// A Turso URL may carry the token in its query string, so nothing after `?` is printed, and the
/// leftmost label of a name with three or more labels is the organisation.
fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return "<url の形が読めない>".to_string();
    };

    let host = rest.split(['/', '?']).next().unwrap_or(rest);

    format!("{scheme}://{}", redact_host(host))
}

fn redact_host(host: &str) -> String {
    let (name, port) = match host.rsplit_once(':') {
        Some((name, port)) if port.chars().all(|character| character.is_ascii_digit()) => {
            (name, Some(port.to_string()))
        }
        _ => (host, None),
    };

    let labels: Vec<&str> = name.split('.').collect();
    let name = if labels.len() >= 3 {
        format!("<account>.{}", labels[1..].join("."))
    } else {
        name.to_string()
    };

    match port {
        Some(port) => format!("{name}:{port}"),
        None => name,
    }
}

/// Kept so the module states that the errors it counts are Turso failures, not SDK ones.
#[allow(dead_code)]
fn describe(error: &TursoError) -> String {
    format!("{:?}: {}", error.kind(), error.message())
}
