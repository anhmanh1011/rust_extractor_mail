//! Phase-11 helpers backing `cmd::stats`. Read-only aggregations.

use telegram_client::store::{FileMeta, Store};

fn meta(sha: &str, chat: i64, msg: i32) -> FileMeta {
    FileMeta {
        sha256: sha.into(),
        source_chat_id: chat,
        source_msg_id: msg,
        original_name: format!("{sha}.txt"),
        size_bytes: 1024,
        format: "txt".into(),
        matcher_key: "gmail.com".into(),
        matcher_mode: "domain".into(),
    }
}

#[test]
fn count_files_by_status_groups_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(&dir.path().join("s.db")).unwrap();
    let _ = s.try_enqueue(&meta("aa", -100, 1)).unwrap();
    let _ = s.try_enqueue(&meta("bb", -100, 2)).unwrap();
    let _ = s.try_enqueue(&meta("cc", -100, 3)).unwrap();
    s.mark_failed("bb", "boom").unwrap();
    s.mark_downloaded("cc").unwrap(); // → 'extracting'
    s.mark_extracted("cc", 100, 5, std::path::Path::new("/x.out")).unwrap();
    s.mark_uploaded("cc", 999).unwrap(); // → 'done'

    let mut got = s.count_files_by_status().unwrap();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("done".to_string(), 1),
            ("failed".to_string(), 1),
            ("queued".to_string(), 1),
        ],
    );
}

#[test]
fn count_files_by_chat_status_breaks_down_per_channel() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(&dir.path().join("s.db")).unwrap();
    let _ = s.try_enqueue(&meta("aa", -100, 1)).unwrap();
    let _ = s.try_enqueue(&meta("bb", -200, 2)).unwrap();
    let _ = s.try_enqueue(&meta("cc", -200, 3)).unwrap();
    s.mark_downloaded("cc").unwrap();
    s.mark_extracted("cc", 1, 1, std::path::Path::new("/x.out")).unwrap();
    s.mark_uploaded("cc", 5).unwrap();

    let mut got = s.count_files_by_chat_status().unwrap();
    got.sort();
    let mut want: Vec<(i64, String, i64)> = vec![
        (-200, "done".to_string(), 1),
        (-200, "queued".to_string(), 1),
        (-100, "queued".to_string(), 1),
    ];
    want.sort();
    assert_eq!(got, want);
}

#[test]
fn failed_upload_count_zero_when_empty() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(&dir.path().join("s.db")).unwrap();
    assert_eq!(s.failed_upload_count().unwrap(), 0);
}
