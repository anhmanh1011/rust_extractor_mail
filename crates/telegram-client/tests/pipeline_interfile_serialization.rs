//! End-to-end serialization regression: 3 jobs flow through the pipeline
//! with a slow `on_outcome` callback. Asserts FIFO outcome ordering and
//! that total elapsed >= 2 × stall (proves the orchestrator drains work
//! sequentially to a slow consumer; does NOT prove cap=1 is enforced —
//! see Chunk 6c for the true cap=1 instrumented test).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use telegram_client::pipeline::interfile::{
    self, CursorAdvance, Job, JobOutcome, OutcomeKind, PipelineConfig,
};
use telegram_client::telegram::mock::{MockClient, UploadOutcome};
use telegram_client::telegram::MessageInfo;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_serializes_under_slow_on_outcome() {
    let dir = tempfile::tempdir().unwrap();

    // Build the mock with three documents. `with_document` consumes `self`,
    // and `script_upload` REPLACES the queue, so we insert into the inner
    // Mutex directly and script all upload outcomes in one combined call
    // (cf. pipeline_interfile_e2e.rs).
    let mock = MockClient::new();
    let mut docs: Vec<MessageInfo> = Vec::new();
    for &msg in &[101, 102, 103] {
        let info = MessageInfo {
            chat_id:       -100_777,
            msg_id:        msg,
            original_name: format!("p{msg}.txt"),
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
    // Single combined script_upload (FIFO). Use i64::from(msg) for the
    // i32 -> i64 widening per project convention (no `as` casts).
    let mock = mock.script_upload(vec![
        UploadOutcome::Ok(1000 + i64::from(101_i32)),
        UploadOutcome::Ok(1000 + i64::from(102_i32)),
        UploadOutcome::Ok(1000 + i64::from(103_i32)),
    ]);

    // Channel capacity is 3 so we can pre-send all jobs synchronously and
    // drop the sender before invoking `interfile::run`. With cap=2 and 3
    // sends, the third send would deadlock waiting for a consumer that
    // hasn't been started yet. Production capacities in `PipelineConfig`
    // are untouched — only this local test fixture's `jobs_tx`/`jobs_rx`.
    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(3);
    for (k, &msg) in [101, 102, 103].iter().enumerate() {
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

    let order:  Arc<Mutex<Vec<i32>>>     = Arc::new(Mutex::new(Vec::new()));
    let stamps: Arc<Mutex<Vec<Instant>>> = Arc::new(Mutex::new(Vec::new()));
    let order_cb  = order.clone();
    let stamps_cb = stamps.clone();
    // The callback runs synchronously on Stage 3's task. Use a
    // std::thread::sleep here — it pins the worker thread, which under
    // the multi_thread flavor lets the other workers continue. With a
    // current_thread runtime this would deadlock; the #[tokio::test]
    // attribute above pins multi_thread + 4 worker_threads as the
    // contract.
    let on_outcome: CursorAdvance = Arc::new(move |o: JobOutcome| {
        order_cb.lock().unwrap().push(o.job.source_msg_id);
        stamps_cb.lock().unwrap().push(Instant::now());
        if matches!(o.kind, OutcomeKind::Uploaded { .. }) {
            std::thread::sleep(Duration::from_millis(150));
        }
    });

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
    let t0 = Instant::now();
    interfile::run(&mock, None, &cfg, jobs_rx, on_outcome).await.unwrap();
    let elapsed = t0.elapsed();

    // FIFO outcome ordering — this IS a tight invariant of Stage 3.
    assert_eq!(
        *order.lock().unwrap(),
        vec![101, 102, 103],
        "Stage 3 must invoke on_outcome in input order"
    );

    // Total elapsed >= 2 × stall. Three outcomes × 150 ms = 450 ms ideal,
    // but Stage 3 hasn't started the third stall when the orchestrator
    // joins on the second-to-last call, so the floor is 2 × 150 = 300 ms
    // with a 20 ms slack for scheduling.
    assert!(
        elapsed >= Duration::from_millis(280),
        "expected serialized stalls; got elapsed = {:?}",
        elapsed
    );
}
