//! Phase-11 stats subcommand. Spec §10.4: aggregate counts, per-channel
//! breakdown, last 10 dead-letter errors, failed-upload queue depth.
//!
//! `compose_report` is split from `run` so tests can drive the read-side
//! without touching `Cli`, stdout, or `config::load`.

use anyhow::{Context, Result};
use std::collections::BTreeMap;

use crate::store::Store;

const DEAD_LETTER_TAIL: usize = 10;

/// Build the human-readable report string from a `Store`. Pure read-only.
pub fn compose_report(store: &Store) -> Result<String> {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(2048);

    let by_status = store
        .count_files_by_status()
        .context("count_files_by_status")?;
    let total: i64 = by_status.iter().map(|(_, n)| *n).sum();

    writeln!(out, "tg-extract stats").unwrap();
    writeln!(out, "================").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Total files: {total}").unwrap();
    if !by_status.is_empty() {
        writeln!(out, "By status:").unwrap();
        let mut sorted = by_status;
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        for (status, n) in sorted {
            writeln!(out, "  {status:<12} {n}").unwrap();
        }
    }
    writeln!(out).unwrap();

    let by_chat = store
        .count_files_by_chat_status()
        .context("count_files_by_chat_status")?;
    if !by_chat.is_empty() {
        writeln!(out, "Per channel:").unwrap();
        let mut grouped: BTreeMap<i64, Vec<(String, i64)>> = BTreeMap::new();
        for (chat, status, n) in by_chat {
            grouped.entry(chat).or_default().push((status, n));
        }
        for (chat, mut rows) in grouped {
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            let summary = rows
                .iter()
                .map(|(s, n)| format!("{s}={n}"))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(out, "  chat {chat:<12} {summary}").unwrap();
        }
        writeln!(out).unwrap();
    }

    let queue = store.failed_upload_count().context("failed_upload_count")?;
    writeln!(out, "Failed-upload queue: {queue}").unwrap();
    writeln!(out).unwrap();

    let mut dead = store.dead_letters().context("dead_letters")?;
    // Newest first by insertion id (autoincrement; oldest=lowest id).
    dead.sort_by_key(|d| std::cmp::Reverse(d.id));
    let tail: Vec<_> = dead.into_iter().take(DEAD_LETTER_TAIL).collect();
    if tail.is_empty() {
        writeln!(out, "No recent errors.").unwrap();
    } else {
        writeln!(out, "Last {} errors (newest first):", tail.len()).unwrap();
        for d in tail {
            writeln!(
                out,
                "  [{}] chat {} msg {} ({}): {}",
                d.recorded_at, d.source_chat_id, d.source_msg_id, d.stage, d.error,
            )
            .unwrap();
        }
    }
    Ok(out)
}

/// Run the `stats` subcommand. Reads only the SQLite store; no Telegram I/O.
pub async fn run(_cfg: &crate::config::AppConfig, store: &Store) -> Result<()> {
    let report = compose_report(store)?;
    print!("{report}");
    Ok(())
}
