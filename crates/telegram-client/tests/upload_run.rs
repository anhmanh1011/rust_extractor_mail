use std::sync::Arc;
use std::time::Duration;

use telegram_client::pipeline::upload::{
    run, RetryPolicy, UploadJob, UploadOutcome, UploadRunConfig,
};
use telegram_client::telegram::mock::{MockClient, UploadOutcome as Mock};
use telegram_client::upload::caption::CaptionData;

fn cap(name: &str) -> CaptionData {
    CaptionData {
        original_name:  name.into(),
        source_chat_id: 1,
        source_msg_id:  1,
        matcher_key:    "k".into(),
        matcher_mode:   "plain".into(),
        size_bytes:     0,
        lines_scanned:  0,
        lines_matched:  0,
    }
}

fn fast_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts:    3,
        initial_backoff: Duration::from_millis(1),
        max_backoff:     Duration::from_millis(2),
        jitter_ratio:    0.0,
    }
}

#[tokio::test]
async fn happy_path_two_jobs_two_outputs() {
    let mock = Arc::new(MockClient::new());
    mock.script_upload(vec![Mock::Ok(101), Mock::Ok(102)]);

    let tmp = tempfile::tempdir().unwrap();
    let p1 = tmp.path().join("a.out");
    let p2 = tmp.path().join("b.out");
    std::fs::write(&p1, b"a@a.com:p\n").unwrap();
    std::fs::write(&p2, b"b@b.com:p\n").unwrap();

    let (in_tx, in_rx)   = tokio::sync::mpsc::channel::<UploadJob>(2);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<UploadOutcome>(4);

    let cfg = UploadRunConfig {
        target_chat_id:        999,
        upload_max_size_bytes: u64::MAX,        // never split
        upload_rate_seconds:   0,               // no inter-job sleep
        retry:                 fast_policy(),
    };
    let on_failed = |_job: UploadJob, _err: anyhow::Error| { /* no-op */ };
    let mock_clone = mock.clone();
    let handle = tokio::spawn(async move {
        run(mock_clone.as_ref(), in_rx, out_tx, &cfg, on_failed).await
    });

    in_tx.send(UploadJob {
        sha256:        "aaaa".into(),
        output_path:   p1.clone(),
        caption:       cap("a.out"),
    }).await.unwrap();
    in_tx.send(UploadJob {
        sha256:        "bbbb".into(),
        output_path:   p2.clone(),
        caption:       cap("b.out"),
    }).await.unwrap();
    drop(in_tx);
    handle.await.unwrap().unwrap();

    let mut got = Vec::new();
    while let Some(o) = out_rx.recv().await { got.push(o); }
    assert_eq!(got.len(), 2);
    match &got[0] {
        UploadOutcome::Done { sha256, output_msg_ids } => {
            assert_eq!(sha256, "aaaa");
            assert_eq!(output_msg_ids, &vec![101]);
        }
        other => panic!("unexpected: {other:?}"),
    }
    match &got[1] {
        UploadOutcome::Done { sha256, output_msg_ids } => {
            assert_eq!(sha256, "bbbb");
            assert_eq!(output_msg_ids, &vec![102]);
        }
        other => panic!("unexpected: {other:?}"),
    }
    // Both captions rendered without a Part line (single-part jobs).
    let uploads = mock.uploaded.lock().unwrap();
    for (_chat, _path, caption, _msg) in uploads.iter() {
        let c = caption.as_deref().unwrap_or("");
        assert!(!c.contains("Part "), "single-part caption must NOT contain Part: {c}");
    }
}

#[tokio::test]
async fn permanent_failure_calls_on_failed_and_continues() {
    let mock = Arc::new(MockClient::new());
    mock.script_upload(vec![
        Mock::Permanent("CHAT_INVALID".into()),
        Mock::Ok(202),
    ]);

    let tmp = tempfile::tempdir().unwrap();
    let p1 = tmp.path().join("a.out");
    let p2 = tmp.path().join("b.out");
    std::fs::write(&p1, b"x\n").unwrap();
    std::fs::write(&p2, b"y\n").unwrap();

    let (in_tx, in_rx)   = tokio::sync::mpsc::channel::<UploadJob>(2);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<UploadOutcome>(4);

    let cfg = UploadRunConfig {
        target_chat_id:        999,
        upload_max_size_bytes: u64::MAX,
        upload_rate_seconds:   0,
        retry:                 fast_policy(),
    };
    let failed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let failed_for_cb = failed.clone();
    let on_failed = move |job: UploadJob, _err: anyhow::Error| {
        failed_for_cb.lock().unwrap().push(job.sha256);
    };

    let mock_clone = mock.clone();
    let handle = tokio::spawn(async move {
        run(mock_clone.as_ref(), in_rx, out_tx, &cfg, on_failed).await
    });

    in_tx.send(UploadJob { sha256: "aaaa".into(), output_path: p1, caption: cap("x.out") }).await.unwrap();
    in_tx.send(UploadJob { sha256: "bbbb".into(), output_path: p2, caption: cap("y.out") }).await.unwrap();
    drop(in_tx);
    handle.await.unwrap().unwrap();

    let mut got = Vec::new();
    while let Some(o) = out_rx.recv().await { got.push(o); }
    assert_eq!(got.len(), 1, "only the successful job emits Done");
    let stored_failures = failed.lock().unwrap().clone();
    assert_eq!(stored_failures, vec!["aaaa".to_string()]);
}

async fn run_one_split_job(
    cap_bytes: u64,
    file_bytes: &'static [u8],
    caption_data: telegram_client::upload::caption::CaptionData,
    upload_script: Vec<Mock>,
) -> Vec<(i64, std::path::PathBuf, Option<String>, i64)> {
    let mock = Arc::new(MockClient::new());
    mock.script_upload(upload_script);

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("big.out");
    std::fs::write(&path, file_bytes).unwrap();

    let (in_tx, in_rx)   = tokio::sync::mpsc::channel::<UploadJob>(1);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<UploadOutcome>(1);

    let cfg = UploadRunConfig {
        target_chat_id:        7,
        upload_max_size_bytes: cap_bytes,
        upload_rate_seconds:   0,
        retry:                 fast_policy(),
    };
    let mc = mock.clone();
    let handle = tokio::spawn(async move {
        run(
            mc.as_ref(), in_rx, out_tx, &cfg,
            |_j: UploadJob, _e: anyhow::Error| {},
        ).await
    });

    in_tx.send(UploadJob {
        sha256:      "h".into(),
        output_path: path.clone(),
        caption:     caption_data,
    }).await.unwrap();
    drop(in_tx);
    handle.await.unwrap().unwrap();
    let _ = out_rx.recv().await.expect("done");

    // Keep the tempdir alive until we have copied the uploads vec out,
    // then return the owned snapshot. The MockClient's path values still
    // point inside the tempdir but the test only inspects captions+ids.
    let snapshot = mock.uploaded.lock().unwrap().clone();
    drop(tmp);
    snapshot
}

#[tokio::test]
async fn multi_part_caption_stays_within_telegram_cap() {
    // Pathologically long original_name forces caption truncation;
    // each part's caption must STILL stay <= 1024 chars (proving the
    // truncation runs AFTER the Part i/N line is added).
    let mut data = cap("big.out");
    data.original_name = "L".repeat(2_000);
    let uploads = run_one_split_job(
        16,
        b"aaaaaaaaaaaaaa\nbbbbbbbbbbbbbb\n",
        data,
        vec![Mock::Ok(11), Mock::Ok(12)],
    ).await;
    assert_eq!(uploads.len(), 2, "expected 2-part upload, got {uploads:?}");
    for (i, (_chat, _path, caption, _msg)) in uploads.iter().enumerate() {
        let c = caption.as_deref().unwrap_or("");
        assert!(c.chars().count() <= 1024, "part {} len = {}", i + 1, c.chars().count());
    }
}

#[tokio::test]
async fn multi_part_caption_includes_part_label() {
    // Realistic short original_name → "Part i/N" is visible in each
    // rendered caption (i.e. render() is called per part, not once).
    let uploads = run_one_split_job(
        16,
        b"aaaaaaaaaaaaaa\nbbbbbbbbbbbbbb\n",
        cap("big.out"),
        vec![Mock::Ok(21), Mock::Ok(22)],
    ).await;
    assert_eq!(uploads.len(), 2);
    assert!(uploads[0].2.as_deref().unwrap().contains("Part 1/2"));
    assert!(uploads[1].2.as_deref().unwrap().contains("Part 2/2"));
}
