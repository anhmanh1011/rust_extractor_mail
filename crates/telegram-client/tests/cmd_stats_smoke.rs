//! Phase-11 smoke. Asserts `cmd::stats::compose_report` produces a string
//! that contains the expected fragments. We test the composer (pure fn over
//! Store reads), not `cmd::stats::run` itself, so we sidestep `Cli` parsing
//! and stdout capture.

use telegram_client::cmd::stats;
use telegram_client::store::{FileMeta, Store};

fn meta(sha: &str, chat: i64, msg: i32) -> FileMeta {
    FileMeta {
        sha256: sha.into(),
        source_chat_id: chat,
        source_msg_id: msg,
        original_name: format!("{sha}.txt"),
        size_bytes: 4096,
        format: "txt".into(),
        matcher_key: "gmail.com".into(),
        matcher_mode: "domain".into(),
    }
}

#[test]
fn compose_report_contains_status_counts_and_per_channel_breakdown() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(&dir.path().join("s.db")).unwrap();
    let _ = s.try_enqueue(&meta("aa", -100, 1)).unwrap();
    let _ = s.try_enqueue(&meta("bb", -200, 2)).unwrap();
    s.mark_failed("aa", "first failure").unwrap();
    s.record_dead_letter(
        -100,
        1,
        None,
        "aa.txt",
        4096,
        "txt",
        "extract",
        "first failure",
    )
    .unwrap();

    let report = stats::compose_report(&s).expect("compose");
    assert!(report.contains("Total files: 2"), "missing total: {report}");
    assert!(report.contains("queued"), "missing queued count: {report}");
    assert!(report.contains("failed"), "missing failed count: {report}");
    assert!(report.contains("-100"), "missing chat -100: {report}");
    assert!(report.contains("-200"), "missing chat -200: {report}");
    assert!(
        report.contains("first failure"),
        "missing dead-letter excerpt: {report}",
    );
    assert!(
        report.contains("Failed-upload queue: 0"),
        "missing failed-upload queue line: {report}",
    );
}

#[test]
fn compose_report_truncates_dead_letters_to_last_10() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(&dir.path().join("s.db")).unwrap();
    for i in 1..=15_i32 {
        let sha = format!("sha{i:02}");
        let name = format!("{sha}.txt");
        let err = format!("err {i:02}");
        let _ = s.try_enqueue(&meta(&sha, -100, i)).unwrap();
        s.record_dead_letter(-100, i, None, &name, 4096, "txt", "extract", &err)
            .unwrap();
    }
    let report = stats::compose_report(&s).expect("compose");
    assert!(report.contains("err 15"), "newest must be present: {report}");
    assert!(
        report.contains("err 06"),
        "10th-newest must be present: {report}",
    );
    assert!(
        !report.contains("err 05"),
        "11th-newest must be truncated: {report}",
    );
}
