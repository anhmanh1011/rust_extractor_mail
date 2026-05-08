use telegram_client::store::{FailedUpload, FileMeta, Store};

fn meta(sha: &str) -> FileMeta {
    FileMeta {
        sha256: sha.into(),
        source_chat_id: 1, source_msg_id: 1,
        original_name: format!("{sha}.txt"),
        size_bytes: 1,
        format: "txt".into(),
        matcher_key: "k".into(), matcher_mode: "plain".into(),
    }
}

#[test]
fn enqueue_then_list_returns_failed_row() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    let _ = s.try_enqueue(&meta("aa")).unwrap();    // satisfies FK

    s.enqueue_failed_upload("aa", std::path::Path::new("/tmp/a.out"), "boom").unwrap();
    let pend: Vec<FailedUpload> = s.pending_failed_uploads().unwrap();
    assert_eq!(pend.len(), 1);
    assert_eq!(pend[0].sha256, "aa");
    assert_eq!(pend[0].error,  "boom");
    assert_eq!(pend[0].attempts, 1);
}

#[test]
fn re_enqueue_increments_attempts() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    let _ = s.try_enqueue(&meta("aa")).unwrap();
    s.enqueue_failed_upload("aa", std::path::Path::new("/tmp/a.out"), "e1").unwrap();
    s.enqueue_failed_upload("aa", std::path::Path::new("/tmp/a.out"), "e2").unwrap();

    let pend = s.pending_failed_uploads().unwrap();
    assert_eq!(pend.len(), 1);
    assert_eq!(pend[0].attempts, 2);
    assert_eq!(pend[0].error, "e2", "latest error wins");
}
