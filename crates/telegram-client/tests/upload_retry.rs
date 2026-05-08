use std::sync::Arc;
use std::time::Duration;

use telegram_client::pipeline::upload::{upload_with_retry, RetryPolicy};
use telegram_client::telegram::mock::{MockClient, UploadOutcome};

fn fast_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts:    5,
        initial_backoff: Duration::from_millis(1),
        max_backoff:     Duration::from_millis(8),
        jitter_ratio:    0.0,
    }
}

#[tokio::test]
async fn flood_wait_then_success() {
    let mock = Arc::new(MockClient::new());
    mock.script_upload(vec![
        UploadOutcome::FloodWait { seconds: 1 },
        UploadOutcome::FloodWait { seconds: 2 },
        UploadOutcome::Ok(42),
    ]);
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("out.txt");
    std::fs::write(&p, b"hello").unwrap();

    let id = upload_with_retry(mock.as_ref(), 999, &p, Some("c"), &fast_policy())
        .await
        .unwrap();
    assert_eq!(id, 42);
    assert_eq!(mock.uploaded.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn permanent_error_short_circuits() {
    let mock = Arc::new(MockClient::new());
    mock.script_upload(vec![
        UploadOutcome::Permanent("CHAT_INVALID".into()),
    ]);
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("out.txt");
    std::fs::write(&p, b"x").unwrap();

    let err = upload_with_retry(mock.as_ref(), 999, &p, None, &fast_policy())
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("CHAT_INVALID"), "got: {msg}");
    assert!(msg.contains("permanent"),    "expected permanent classification: {msg}");
}

#[tokio::test]
async fn budget_exhausted_after_max_attempts_floods() {
    let mock = Arc::new(MockClient::new());
    mock.script_upload(vec![
        UploadOutcome::FloodWait { seconds: 1 },
        UploadOutcome::FloodWait { seconds: 1 },
        UploadOutcome::FloodWait { seconds: 1 },
        UploadOutcome::FloodWait { seconds: 1 },
        UploadOutcome::FloodWait { seconds: 1 },
    ]);
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("out.txt");
    std::fs::write(&p, b"x").unwrap();

    let err = upload_with_retry(mock.as_ref(), 999, &p, None, &fast_policy())
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("budget exhausted") || msg.contains("max_attempts"),
        "got: {msg}");
}
