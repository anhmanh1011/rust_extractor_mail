use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use telegram_client::cmd::retry_uploads::run_with_store_and_client;
use telegram_client::pipeline::upload::UploadRunConfig;
use telegram_client::store::Store;
use telegram_client::telegram::mock::{MockClient, UploadOutcome as Mock};

fn upload_cfg_for_test(target_chat_id: i64) -> UploadRunConfig {
    UploadRunConfig {
        target_chat_id,
        upload_max_size_bytes: 2_000_000_000,
        upload_rate_seconds:   0,
        retry: telegram_client::pipeline::upload::RetryPolicy {
            max_attempts:    3,
            initial_backoff: Duration::from_millis(0),
            max_backoff:     Duration::from_millis(0),
            jitter_ratio:    0.0,
        },
    }
}

fn seed_failed_row(store: &Store, tmp: &std::path::Path, sha: &str) -> std::path::PathBuf {
    let _ = store.try_enqueue(&telegram_client::store::FileMeta {
        sha256: sha.into(), source_chat_id: 42, source_msg_id: 7,
        original_name: "dump.txt".into(), size_bytes: 1024,
        format: "txt".into(), matcher_key: "target.com".into(), matcher_mode: "plain".into(),
    }).unwrap();
    store.mark_downloading(sha).unwrap();
    store.mark_downloaded(sha).unwrap();
    let p = tmp.join(format!("{sha}.out"));
    std::fs::write(&p, b"x\n").unwrap();
    store.mark_extracted(sha, 137, 12, &p).unwrap();
    store.enqueue_failed_upload(sha, &p, "boom").unwrap();
    p
}

#[tokio::test]
async fn drains_pending_failed_uploads_on_success() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("s.db")).unwrap();
    let _ = seed_failed_row(&store, tmp.path(), "aa");

    let _ = Bytes::new();   // imports check
    let mock = Arc::new(MockClient::new().script_upload(vec![Mock::Ok(909)]));

    let target = -1001234567890_i64;
    run_with_store_and_client(&store, mock.as_ref(), &upload_cfg_for_test(target))
        .await.unwrap();

    assert!(store.pending_failed_uploads().unwrap().is_empty());
    let conn = store.lock();
    let st: String = conn.query_row(
        "SELECT status FROM files WHERE sha256='aa'", [], |r| r.get(0),
    ).unwrap();
    assert_eq!(st, "done");
}

#[tokio::test]
async fn keeps_row_when_retry_fails_permanently() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("s.db")).unwrap();
    let _ = seed_failed_row(&store, tmp.path(), "aa");

    let mock = Arc::new(
        MockClient::new().script_upload(vec![Mock::Permanent("STILL_BROKEN".into())]),
    );

    let target = -1001234567890_i64;
    let _ = run_with_store_and_client(&store, mock.as_ref(), &upload_cfg_for_test(target)).await;

    let pend = store.pending_failed_uploads().unwrap();
    assert_eq!(pend.len(), 1);
    assert_eq!(pend[0].attempts, 2, "attempts incremented");
}

/// Issue 6: caption provenance must survive a retry. The original `cmd::fetch`
/// run that produced this `failed_uploads` row crashed without persisting the
/// rendered caption — but the source-of-truth fields (original_name,
/// source_chat_id, source_msg_id, matcher_key, matcher_mode, size_bytes,
/// lines_scanned, lines_matched) all live in `files`. Retry reconstructs
/// `CaptionData` via a JOIN so the uploaded message's caption matches what
/// the first attempt would have produced.
#[tokio::test]
async fn retry_renders_caption_from_files_join() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("s.db")).unwrap();
    let _ = seed_failed_row(&store, tmp.path(), "aa");

    let mock = Arc::new(MockClient::new().script_upload(vec![Mock::Ok(910)]));

    let target = -1001234567890_i64;
    run_with_store_and_client(&store, mock.as_ref(), &upload_cfg_for_test(target))
        .await.unwrap();

    let snapshot = mock.uploaded.lock().unwrap().clone();
    assert_eq!(snapshot.len(), 1);
    let caption = snapshot[0].2.as_deref().unwrap_or("");
    // Caption must NOT be empty (Issue 6 fix).
    assert!(!caption.is_empty(), "retry caption was empty — provenance lost");
    // Caption renderer (Phase 6 contract) prefixes with the original filename
    // and includes the source chat / msg ids, the matcher key, and the
    // lines-matched/lines-scanned counters from the seeded files row.
    assert!(caption.contains("dump.txt"),
        "caption missing original_name: {caption:?}");
    assert!(caption.contains("42")  && caption.contains("7"),
        "caption missing source chat/msg: {caption:?}");
    assert!(caption.contains("target.com"),
        "caption missing matcher_key: {caption:?}");
    assert!(caption.contains("12") && caption.contains("137"),
        "caption missing line counts: {caption:?}");
}
