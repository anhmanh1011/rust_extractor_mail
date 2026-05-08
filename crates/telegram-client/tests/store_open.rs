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
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 5);
}

#[test]
fn open_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let dbp = tmp.path().join("state.db");
    let _ = Store::open(&dbp).unwrap();
    let _ = Store::open(&dbp).unwrap(); // no-op migrations
    let _ = Store::open(&dbp).unwrap();
}

#[test]
fn open_enables_foreign_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("s.db")).unwrap();
    let conn = store.lock();
    let fk: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0)).unwrap();
    assert_eq!(fk, 1, "foreign_keys must be ON for failed_uploads FK to be enforced");
}

#[test]
fn open_creates_required_indexes() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("s.db")).unwrap();
    let conn = store.lock();
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master \
         WHERE type='index' AND name IN ('idx_files_status','idx_files_source')",
        [],
        |r| r.get(0),
    ).unwrap();
    assert_eq!(n, 2, "both files indexes must exist after Store::open");
}

#[test]
fn enqueue_failed_for_unknown_sha_violates_fk() {
    // Spec §6.3 invariant: failed_uploads.sha256 -> files(sha256). Without
    // this FK enforced, retry-uploads could find rows referencing files that
    // never existed and the JOIN in cmd::retry_uploads would silently drop them.
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("s.db")).unwrap();
    let err = store
        .enqueue_failed_upload("ff_unknown", std::path::Path::new("/tmp/x.out"), "boom")
        .unwrap_err();
    let chain = format!("{err:#}");
    assert!(
        chain.contains("FOREIGN KEY constraint failed") || chain.to_lowercase().contains("foreign key"),
        "expected FK constraint error, got: {chain}",
    );
}
