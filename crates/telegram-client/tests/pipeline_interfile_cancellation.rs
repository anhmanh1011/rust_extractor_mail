//! Cancellation regression: dropping `jobs_tx` mid-stream must shut every
//! pipeline stage down cleanly within a generous deadline. Pins down the
//! cancellation contract from spec §4.3 as a runtime property.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use telegram_client::pipeline::interfile::{
    self, CursorAdvance, Job, JobOutcome, PipelineConfig,
};
use telegram_client::telegram::mock::{MockClient, UploadOutcome};
use telegram_client::telegram::MessageInfo;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_jobs_tx_cleanly_shuts_pipeline_down() {
    let dir  = tempfile::tempdir().unwrap();
    let info = MessageInfo {
        chat_id:       -1005,
        msg_id:        1,
        original_name: "x.txt".into(),
        size_bytes:    16,
        mime:          Some("text/plain".into()),
        date:          0,
    };

    // `with_document` consumes `self`, so register the document via the
    // inner Mutex (cf. pipeline_interfile_e2e.rs:47-50, cmd_watch.rs:210-216).
    let mock = MockClient::new();
    mock.messages.lock().unwrap().insert(
        (info.chat_id, info.msg_id),
        (info.clone(), b"gmail.com:a@b.c:d\n".to_vec()),
    );
    mock.script_upload(vec![UploadOutcome::Ok(1)]);

    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(2);
    jobs_tx
        .send(Job {
            source_chat_id: -1005,
            source_msg_id:  1,
            info,
        })
        .await
        .unwrap();
    drop(jobs_tx); // close immediately after enqueue

    let outcomes: Arc<Mutex<Vec<JobOutcome>>> = Arc::new(Mutex::new(Vec::new()));
    let coll = outcomes.clone();
    let on_outcome: CursorAdvance = Arc::new(move |o| coll.lock().unwrap().push(o));

    let cfg = PipelineConfig {
        matcher_key:                 "gmail.com".into(),
        matcher_mode:                "plain".into(),
        output_dir:                  dir.path().to_path_buf(),
        max_line_bytes:              64 * 1024,
        max_uncompressed_bytes:      10 * 1024 * 1024 * 1024,
        intra_file_channel_capacity: 4,
        inter_file_channel_capacity: 1,
        upload_channel_capacity:     2,
        outcomes_channel_capacity:   2,
        upload_max_size_bytes:       2 * 1024 * 1024 * 1024,
        upload_rate_seconds:         0,
        target_chat_id:              42,
    };

    let r = tokio::time::timeout(
        Duration::from_secs(5),
        interfile::run(&mock, None, &cfg, jobs_rx, on_outcome),
    )
    .await;
    let inner = r.expect("orchestrator must shut down within 5s of jobs_tx drop");
    inner.expect("clean shutdown returns Ok(())");

    assert_eq!(outcomes.lock().unwrap().len(), 1);
}
