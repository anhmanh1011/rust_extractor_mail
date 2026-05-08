//! SQLite-backed state store. All methods are blocking; async callers wrap
//! Store calls in `tokio::task::spawn_blocking`.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

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
