//! SQLite-backed state store. All methods are blocking; async callers wrap
//! Store calls in `tokio::task::spawn_blocking`.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::Connection;

const SCHEMA_SQL: &str = include_str!("schema.sql");

/// SQLite-backed state store. Owns a `Mutex<Connection>` so callers can
/// share the store across threads. All methods block; async callers wrap
/// each call in `tokio::task::spawn_blocking`.
pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    /// Open (or create) the SQLite database at `db_path`, applying schema
    /// migrations idempotently. Sets WAL + synchronous=NORMAL + foreign_keys.
    pub fn open(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("mkdir {}", parent.display()))?;
        }
        let conn = Connection::open(db_path)
            .with_context(|| format!("open sqlite at {}", db_path.display()))?;
        // PRAGMAs first (WAL persists in DB header; safe to issue every open).
        conn.pragma_update(None, "journal_mode", "WAL").context("set WAL")?;
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .context("query journal_mode after pragma_update")?;
        anyhow::ensure!(
            mode.eq_ignore_ascii_case("wal"),
            "failed to enable WAL journal mode (got '{mode}'); networked FS or locked DB?"
        );
        conn.pragma_update(None, "synchronous", "NORMAL").context("set synchronous")?;
        conn.pragma_update(None, "foreign_keys", true).context("set foreign_keys")?;
        // Migrations.
        conn.execute_batch(SCHEMA_SQL).context("apply schema.sql")?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// Test/observability accessor — production code uses the typed methods
    /// added in Tasks 7.2-7.5.
    pub fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().expect("store mutex poisoned")
    }
}

/// Metadata captured for a discovered source file before any download work.
#[derive(Debug, Clone)]
pub struct FileMeta {
    /// Hex-encoded SHA-256 of the source file; primary key in the `files` table.
    pub sha256:         String,
    /// Telegram chat the source message lives in.
    pub source_chat_id: i64,
    /// Telegram message id carrying the source document.
    pub source_msg_id:  i32,
    /// Original file name as posted on Telegram.
    pub original_name:  String,
    /// Source size in bytes (stored as `INTEGER` in SQLite).
    pub size_bytes:     u64,
    /// File format tag — one of `"txt"`, `"gz"`, `"zip"`.
    pub format:         String,
    /// Matcher rule key that selected this file.
    pub matcher_key:    String,
    /// Matcher mode tag — one of `"plain"`, `"url"`.
    pub matcher_mode:   String,
}

/// Outcome of a `try_enqueue` call (Spec §6.3).
#[derive(Debug, Clone)]
pub enum EnqueueResult {
    /// A new `queued` row was inserted for this `sha256`.
    New,
    /// The file is already fully processed (`status = 'done'`); skip.
    AlreadyDone,
    /// The file is already mid-pipeline; the inner string is the current status.
    InProgress(String),
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Store {
    /// Insert a new `queued` row for `m`, or report the existing row's status.
    pub fn try_enqueue(&self, m: &FileMeta) -> Result<EnqueueResult> {
        let conn = self.lock();
        // INSERT OR IGNORE atomically inserts iff sha256 is new; concurrent
        // callers see rows_affected==0 and fall through to a status SELECT.
        let rows_affected = conn
            .execute(
                "INSERT OR IGNORE INTO files (
                    sha256, source_chat_id, source_msg_id, original_name,
                    size_bytes, format, matcher_key, matcher_mode,
                    discovered_at, status
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'queued')",
                rusqlite::params![
                    m.sha256,
                    m.source_chat_id,
                    m.source_msg_id,
                    m.original_name,
                    i64::try_from(m.size_bytes).unwrap_or(i64::MAX),
                    m.format,
                    m.matcher_key,
                    m.matcher_mode,
                    now_secs(),
                ],
            )
            .with_context(|| format!("INSERT OR IGNORE files sha256={}", m.sha256))?;

        if rows_affected == 1 {
            return Ok(EnqueueResult::New);
        }

        // Row already existed: read its current status and report.
        let status: String = conn
            .query_row(
                "SELECT status FROM files WHERE sha256 = ?1",
                rusqlite::params![m.sha256],
                |r| r.get(0),
            )
            .with_context(|| {
                format!("SELECT status after no-op INSERT sha256={}", m.sha256)
            })?;

        if status == "done" {
            Ok(EnqueueResult::AlreadyDone)
        } else {
            Ok(EnqueueResult::InProgress(status))
        }
    }

    /// Transition `sha`'s row to `downloading`.
    pub fn mark_downloading(&self, sha: &str) -> Result<()> {
        self.set_status(sha, "downloading")
    }

    /// Transition `sha`'s row to `extracting` and stamp `download_done_at`.
    pub fn mark_downloaded(&self, sha: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE files SET status='extracting', download_done_at=?1 WHERE sha256=?2",
            rusqlite::params![now_secs(), sha],
        )
        .with_context(|| format!("UPDATE files mark_downloaded sha256={sha}"))?;
        Ok(())
    }

    /// Transition `sha`'s row to `uploading`, stamp `extract_done_at`, and
    /// record extraction stats plus the produced `output_path`.
    pub fn mark_extracted(
        &self,
        sha: &str,
        lines_scanned: u64,
        lines_matched: u64,
        out: &Path,
    ) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE files
                SET status='uploading',
                    extract_done_at=?1,
                    lines_scanned=?2,
                    lines_matched=?3,
                    output_path=?4
              WHERE sha256=?5",
            rusqlite::params![
                now_secs(),
                i64::try_from(lines_scanned).unwrap_or(i64::MAX),
                i64::try_from(lines_matched).unwrap_or(i64::MAX),
                out.to_string_lossy(),
                sha,
            ],
        )
        .with_context(|| format!("UPDATE files mark_extracted sha256={sha}"))?;
        Ok(())
    }

    /// Transition `sha`'s row to `done`, stamp `upload_done_at`, and record
    /// the destination Telegram message id.
    pub fn mark_uploaded(&self, sha: &str, output_msg_id: i64) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE files
                SET status='done', upload_done_at=?1, output_msg_id=?2
              WHERE sha256=?3",
            rusqlite::params![now_secs(), output_msg_id, sha],
        )
        .with_context(|| format!("UPDATE files mark_uploaded sha256={sha}"))?;
        Ok(())
    }

    /// Transition `sha`'s row to `failed` and store the error message.
    pub fn mark_failed(&self, sha: &str, err: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE files SET status='failed', error=?1 WHERE sha256=?2",
            rusqlite::params![err, sha],
        )
        .with_context(|| format!("UPDATE files mark_failed sha256={sha}"))?;
        Ok(())
    }

    fn set_status(&self, sha: &str, status: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE files SET status=?1 WHERE sha256=?2",
            rusqlite::params![status, sha],
        )
        .with_context(|| format!("UPDATE files set_status sha256={sha} status={status}"))?;
        Ok(())
    }
}
