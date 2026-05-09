//! End-to-end happy path: 3 mock txt jobs through all three stages.
//! Outcomes must arrive in FIFO order with sha256 + >=1 output_msg_id each.

use std::sync::Arc;
use std::sync::Mutex;

use telegram_client::pipeline::interfile::{
    self, CursorAdvance, Job, JobOutcome, OutcomeKind, PipelineConfig,
};
use telegram_client::telegram::mock::{MockClient, UploadOutcome};
use telegram_client::telegram::MessageInfo;

fn cfg_with_dir(dir: std::path::PathBuf) -> PipelineConfig {
    PipelineConfig {
        matcher_key:                 "gmail.com".into(),
        matcher_mode:                "plain".into(),
        output_dir:                  dir,
        max_line_bytes:              64 * 1024,
        max_uncompressed_bytes:      10 * 1024 * 1024 * 1024,
        intra_file_channel_capacity: 4,
        inter_file_channel_capacity: 1,
        upload_channel_capacity:     2,
        outcomes_channel_capacity:   2,
        upload_max_size_bytes:       2 * 1024 * 1024 * 1024,
        upload_rate_seconds:         0,
        target_chat_id:              42,
        progress:                    None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_jobs_flow_in_order() {
    let dir = tempfile::tempdir().unwrap();

    // Build the mock with three documents. `with_document` consumes `self`,
    // so we use the inner Mutex directly (cf. cmd_watch.rs:210-216).
    let mock = MockClient::new();
    let mut docs: Vec<MessageInfo> = Vec::new();
    for &msg in &[11, 12, 13] {
        let info = MessageInfo {
            chat_id:       -100_777,
            msg_id:        msg,
            original_name: format!("d{msg}.txt"),
            size_bytes:    32,
            mime:          Some("text/plain".into()),
            date:          0,
        };
        mock.messages.lock().unwrap().insert(
            (info.chat_id, info.msg_id),
            (info.clone(), b"gmail.com:u@x.com:p\n".to_vec()),
        );
        docs.push(info);
    }
    // Script ALL upload outcomes in one call (FIFO). `script_upload` REPLACES
    // the queue, so multiple calls would only retain the last entry.
    let mock = mock.script_upload(vec![
        UploadOutcome::Ok(1011),
        UploadOutcome::Ok(1012),
        UploadOutcome::Ok(1013),
    ]);

    // Channel capacity is 3 so we can pre-send all jobs synchronously and
    // drop the sender before invoking `interfile::run`. With cap=2 and 3
    // sends, the third send would deadlock waiting for a consumer that
    // hasn't been started yet.
    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(3);
    for (k, &msg) in [11, 12, 13].iter().enumerate() {
        jobs_tx
            .send(Job {
                source_chat_id: -100_777,
                source_msg_id:  msg,
                info:           docs[k].clone(),
            })
            .await
            .unwrap();
    }
    drop(jobs_tx);

    let outcomes: Arc<Mutex<Vec<JobOutcome>>> = Arc::new(Mutex::new(Vec::new()));
    let coll = outcomes.clone();
    let on_outcome: CursorAdvance = Arc::new(move |o| coll.lock().unwrap().push(o));

    let cfg = cfg_with_dir(dir.path().to_path_buf());
    interfile::run(&mock, None, &cfg, jobs_rx, on_outcome)
        .await
        .expect("happy path");

    let got = outcomes.lock().unwrap();
    assert_eq!(got.len(), 3);
    let ids: Vec<i32> = got.iter().map(|o| o.job.source_msg_id).collect();
    assert_eq!(ids, vec![11, 12, 13], "outcomes must fire in input order");
    for o in got.iter() {
        match &o.kind {
            OutcomeKind::Uploaded { sha256, output_msg_ids } => {
                assert_eq!(sha256.len(), 64);
                assert!(!output_msg_ids.is_empty());
            }
            other => panic!("expected Uploaded, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_processes_zip_through_disk_path() {
    use std::io::Write;
    use zip::write::FileOptions;

    let mut zip_bytes: Vec<u8> = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut zip_bytes);
        let mut zw = zip::ZipWriter::new(cursor);
        let opts: FileOptions = FileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zw.start_file("a.txt", opts).unwrap();
        zw.write_all(b"gmail.com:alice@x.com:pwd1\n").unwrap();
        zw.start_file("b.gz", opts).unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(b"gmail.com:bob@x.com:pwd2\n").unwrap();
        let gz_bytes = gz.finish().unwrap();
        zw.write_all(&gz_bytes).unwrap();
        zw.finish().unwrap();
    }

    let info = MessageInfo {
        chat_id: -100, msg_id: 555,
        original_name: "creds.zip".into(),
        size_bytes: u64::try_from(zip_bytes.len()).unwrap(),
        mime: Some("application/zip".into()),
        date: 1_700_000_000,
    };
    let mock = MockClient::new();
    mock.messages.lock().unwrap().insert(
        (info.chat_id, info.msg_id),
        (info.clone(), zip_bytes),
    );
    let mock = mock.script_upload(vec![UploadOutcome::Ok(50_555)]);

    let tmp = tempfile::tempdir().unwrap();
    let cfg = cfg_with_dir(tmp.path().to_path_buf());

    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(2);
    jobs_tx.send(Job {
        source_chat_id: info.chat_id,
        source_msg_id:  info.msg_id,
        info:           info.clone(),
    }).await.unwrap();
    drop(jobs_tx);

    let outcomes: Arc<Mutex<Vec<JobOutcome>>> = Arc::new(Mutex::new(Vec::new()));
    let coll = outcomes.clone();
    let on_outcome: CursorAdvance = Arc::new(move |o| coll.lock().unwrap().push(o));

    interfile::run(&mock, None, &cfg, jobs_rx, on_outcome).await.unwrap();

    let got = outcomes.lock().unwrap();
    assert_eq!(got.len(), 1);
    assert!(matches!(got[0].kind, OutcomeKind::Uploaded { .. }), "got {:?}", got[0].kind);
    assert_eq!(mock.uploaded.lock().unwrap().len(), 1);

    // build_output_path layout: <output_dir>/<source_chat_id>/<source_msg_id>_<strip_known_ext(stem)>.out
    let out_path = tmp.path().join("-100").join("555_creds.out");
    let body = std::fs::read_to_string(&out_path).unwrap();
    assert!(body.contains("alice@x.com:pwd1"));
    assert!(body.contains("bob@x.com:pwd2"));
}
