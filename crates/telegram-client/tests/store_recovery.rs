use telegram_client::store::{FileMeta, Store, UploadJobRow};

fn meta(sha: &str, msg: i32) -> FileMeta {
    FileMeta {
        sha256:         sha.into(),
        source_chat_id: 1,
        source_msg_id:  msg,
        original_name:  format!("{sha}.txt"),
        size_bytes:     1,
        format:         "txt".into(),
        matcher_key:    "k".into(),
        matcher_mode:   "plain".into(),
    }
}

#[test]
fn reset_in_flight_returns_downloading_and_extracting_to_queued() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();

    let _ = s.try_enqueue(&meta("aa", 1)).unwrap();   // queued
    let _ = s.try_enqueue(&meta("bb", 2)).unwrap();   // queued
    let _ = s.try_enqueue(&meta("cc", 3)).unwrap();   // queued
    s.mark_downloading("aa").unwrap();                // downloading
    s.mark_downloading("bb").unwrap();
    s.mark_downloaded("bb").unwrap();                 // extracting
    s.mark_downloading("cc").unwrap();
    s.mark_downloaded("cc").unwrap();
    s.mark_extracted("cc", 1, 1, std::path::Path::new("/tmp/c.out")).unwrap();   // uploading

    let n = s.reset_in_flight().unwrap();
    assert_eq!(n, 2, "aa+bb should reset; cc remains uploading");
}

#[test]
fn list_pending_uploads_returns_uploading_rows_only() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    let _ = s.try_enqueue(&meta("aa", 1)).unwrap();
    s.mark_downloading("aa").unwrap();
    s.mark_downloaded("aa").unwrap();
    s.mark_extracted("aa", 5, 2, std::path::Path::new("/tmp/a.out")).unwrap();

    let _ = s.try_enqueue(&meta("bb", 2)).unwrap();   // queued only

    let pending: Vec<UploadJobRow> = s.list_pending_uploads().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sha256, "aa");
    assert_eq!(pending[0].output_path.to_string_lossy(), "/tmp/a.out");
}
