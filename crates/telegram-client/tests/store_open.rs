use telegram_client::store::Store;

#[test]
fn open_creates_tables_and_sets_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let dbp = tmp.path().join("state.db");
    let store = Store::open(&dbp).unwrap();

    let conn = store.lock();
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_uppercase(), "WAL");

    let v: i64 = conn
        .query_row("SELECT version FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 1);

    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master \
             WHERE type='table' AND name IN \
             ('files','watch_state','backfill_state','failed_uploads','schema_version')",
            [], |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 5);
}

#[test]
fn open_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let dbp = tmp.path().join("state.db");
    let _ = Store::open(&dbp).unwrap();
    let _ = Store::open(&dbp).unwrap();          // no-op migrations
    let _ = Store::open(&dbp).unwrap();
}
