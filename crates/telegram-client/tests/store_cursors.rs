use telegram_client::store::{BackfillState, Store};

#[test]
fn watch_cursor_returns_none_until_set() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    assert_eq!(s.watch_cursor(42).unwrap(), None);

    s.update_watch_cursor(42, "Test Channel", 100).unwrap();
    assert_eq!(s.watch_cursor(42).unwrap(), Some(100));

    s.update_watch_cursor(42, "Test Channel", 105).unwrap();
    assert_eq!(s.watch_cursor(42).unwrap(), Some(105));
}

#[test]
fn watch_cursor_is_per_chat() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    s.update_watch_cursor(1, "A", 10).unwrap();
    s.update_watch_cursor(2, "B", 20).unwrap();
    assert_eq!(s.watch_cursor(1).unwrap(), Some(10));
    assert_eq!(s.watch_cursor(2).unwrap(), Some(20));
    assert_eq!(s.watch_cursor(3).unwrap(), None);
}

#[test]
fn backfill_cursor_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    assert!(s.backfill_cursor(1).unwrap().is_none());

    s.advance_backfill(1, "Hist", 1_000).unwrap();
    let st: BackfillState = s.backfill_cursor(1).unwrap().unwrap();
    assert_eq!(st.next_msg_id, 1_000);
    assert_eq!(st.completed_at, None);

    s.advance_backfill(1, "Hist", 900).unwrap();
    let st = s.backfill_cursor(1).unwrap().unwrap();
    assert_eq!(st.next_msg_id, 900);

    s.complete_backfill(1).unwrap();
    let st = s.backfill_cursor(1).unwrap().unwrap();
    assert!(st.completed_at.is_some());
}
