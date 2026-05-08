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
    mock.script_upload(vec![
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
