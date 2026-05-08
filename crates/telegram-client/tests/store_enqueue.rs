use telegram_client::store::{EnqueueResult, FileMeta, Store};

fn meta(sha: &str) -> FileMeta {
    FileMeta {
        sha256:         sha.into(),
        source_chat_id: 1,
        source_msg_id:  1,
        original_name:  "x.txt".into(),
        size_bytes:     10,
        format:         "txt".into(),
        matcher_key:    "k".into(),
        matcher_mode:   "plain".into(),
    }
}

#[test]
fn first_enqueue_returns_new() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    let r = s.try_enqueue(&meta("aa")).unwrap();
    assert!(matches!(r, EnqueueResult::New));
}

#[test]
fn second_enqueue_in_progress_returns_in_progress() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    let _ = s.try_enqueue(&meta("aa")).unwrap();              // queued
    let r = s.try_enqueue(&meta("aa")).unwrap();
    match r {
        EnqueueResult::InProgress(status) => assert_eq!(status, "queued"),
        other => panic!("got {other:?}"),
    }
}

#[test]
fn second_enqueue_after_done_returns_already_done() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(&tmp.path().join("s.db")).unwrap();
    let _ = s.try_enqueue(&meta("aa")).unwrap();
    s.mark_downloaded("aa").unwrap();
    s.mark_extracted("aa", 1, 2, std::path::Path::new("/tmp/o.out")).unwrap();
    s.mark_uploaded("aa", 999).unwrap();
    let r = s.try_enqueue(&meta("aa")).unwrap();
    assert!(matches!(r, EnqueueResult::AlreadyDone));
}
