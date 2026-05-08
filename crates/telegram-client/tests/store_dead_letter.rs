use telegram_client::store::{DeadLetter, Store};

fn open() -> (tempfile::TempDir, Store) {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    (tmp, s)
}

#[test]
fn record_dead_letter_persists_row_with_all_fields() {
    let (_tmp, s) = open();
    s.record_dead_letter(
        /*source_chat_id*/ -100_111,
        /*source_msg_id*/  42,
        /*sha256*/         Some("aa00".into()),
        /*original_name*/  "bad.zip",
        /*size_bytes*/     1234,
        /*format*/         "zip",
        /*stage*/          "extract",
        /*error*/          "max_uncompressed_bytes exceeded at entry leak.txt",
    ).unwrap();

    let rows: Vec<DeadLetter> = s.dead_letters().unwrap();
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.source_chat_id, -100_111);
    assert_eq!(r.source_msg_id,  42);
    assert_eq!(r.sha256.as_deref(), Some("aa00"));
    assert_eq!(r.original_name,    "bad.zip");
    assert_eq!(r.size_bytes,       1234);
    assert_eq!(r.format,           "zip");
    assert_eq!(r.stage,            "extract");
    assert!(r.error.contains("max_uncompressed_bytes"));
    assert!(r.recorded_at > 0, "recorded_at must be a unix epoch second");
}

#[test]
fn record_dead_letter_allows_null_sha256_for_pre_hash_failures() {
    // A download that died before any bytes hashed has sha256 == None.
    // The cursor must still be able to advance past such jobs.
    let (_tmp, s) = open();
    s.record_dead_letter(
        -1, 7, None, "torn.txt", 0, "txt", "download", "transport closed mid-chunk",
    ).unwrap();
    let rows = s.dead_letters().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].sha256.is_none());
}

#[test]
fn record_dead_letter_appends_distinct_rows_for_same_source_msg() {
    // Two distinct failure attempts on the same (chat,msg) MUST appear as
    // separate rows so the post-mortem trail is preserved. (No UPSERT-by-source.)
    let (_tmp, s) = open();
    s.record_dead_letter(-1, 7, None, "x.txt", 0, "txt", "extract", "first").unwrap();
    s.record_dead_letter(-1, 7, None, "x.txt", 0, "txt", "extract", "second").unwrap();
    let rows = s.dead_letters().unwrap();
    assert_eq!(rows.len(), 2);
    let errors: Vec<&str> = rows.iter().map(|r| r.error.as_str()).collect();
    assert!(errors.contains(&"first"));
    assert!(errors.contains(&"second"));
}

#[test]
fn open_v2_schema_is_idempotent_across_reopens() {
    // Two consecutive opens of the v2 schema must (a) not duplicate the
    // `schema_version` row, (b) not destroy data inserted between opens,
    // and (c) leave `MAX(version) == 2`.
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("s.db");
    {
        let s = Store::open(&path).unwrap();
        s.record_dead_letter(-1, 7, None, "x.txt", 0, "txt", "extract", "boom").unwrap();
    }
    let s = Store::open(&path).unwrap();
    let v: i64 = s.lock()
        .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 2, "second open did not preserve v2 schema_version");
    let rows = s.dead_letters().unwrap();
    assert_eq!(rows.len(), 1, "row from first open must survive second open");
    // schema_version is INSERT OR IGNORE-ed twice (v1 + v2) per open. After
    // two opens we should still see exactly two distinct rows: (1) and (2).
    let count: i64 = s.lock()
        .query_row("SELECT COUNT(*) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2, "schema_version must hold exactly {{1, 2}} after two opens");
}

#[test]
fn open_migrates_a_seeded_v1_db_to_v2() {
    // Real v1 → v2 path: hand-seed a DB with the v1 schema only (no
    // dead_letter table, schema_version == 1), close it, then open
    // through `Store::open`. The new open must add `dead_letter` and
    // bump `schema_version` to 2 without dropping the v1 rows.
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("s.db");
    {
        let raw = rusqlite::Connection::open(&path).unwrap();
        // Minimal v1 surface: schema_version + a row in some pre-existing
        // table (we use `files` here — it's the broadest v1 table). The
        // seeded `files` schema must include the columns referenced by the
        // v1 indexes (`status`, `source_chat_id`, `source_msg_id`) so that
        // `CREATE INDEX IF NOT EXISTS` statements in `Store::open` can run
        // against this pre-existing table without erroring.
        raw.execute_batch(
            "CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
             INSERT OR IGNORE INTO schema_version VALUES (1);
             CREATE TABLE files (
                 sha256          TEXT PRIMARY KEY,
                 source_chat_id  INTEGER NOT NULL DEFAULT 0,
                 source_msg_id   INTEGER NOT NULL DEFAULT 0,
                 status          TEXT    NOT NULL DEFAULT 'queued'
             );
             INSERT INTO files (sha256) VALUES ('deadbeef');"
        ).unwrap();
        // Confirm dead_letter does NOT exist on the seeded v1 DB.
        let pre: i64 = raw.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='dead_letter'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(pre, 0, "v1 seed must not have dead_letter table");
    }
    // Now open through the production path — this re-runs SCHEMA_SQL,
    // which must add dead_letter + bump schema_version to 2.
    let s = Store::open(&path).unwrap();
    let v: i64 = s.lock()
        .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 2, "v1 -> v2 migration did not bump schema_version");
    // Pre-existing v1 row survives.
    let pre_row: String = s.lock()
        .query_row("SELECT sha256 FROM files WHERE sha256 = 'deadbeef'", [],
                   |r| r.get(0))
        .unwrap();
    assert_eq!(pre_row, "deadbeef");
    // Post-migration writes work.
    s.record_dead_letter(-1, 1, None, "y.txt", 0, "txt", "extract", "post-migration")
        .unwrap();
    assert_eq!(s.dead_letters().unwrap().len(), 1);
}
