//! Phase-10 regression. Asserts the orchestrator honors
//! `inter_file_channel_capacity = 1` end-to-end: with five jobs in the
//! queue and a download-side mock that sleeps 50 ms per file, the maximum
//! number of concurrent downloads observed across the run is exactly 1.
//!
//! Spec §4.2: the inter-file channel is the throttle that prevents
//! Telegram from rate-limiting a flood of concurrent reads. A regression
//! that lifts the cap (e.g., spawning per-job tasks instead of looping
//! sequentially in `download_stage`) is silent at the unit-test level.

use std::sync::Arc;
use std::time::Duration;

use telegram_client::pipeline::interfile::{self, CursorAdvance, Job, JobOutcome, PipelineConfig};
use telegram_client::store::Store;
use telegram_client::telegram::mock::{MockClient, UploadOutcome};
use telegram_client::telegram::MessageInfo;

mod common;
use common::cfg_with_dir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inter_file_channel_capacity_one_caps_inflight_downloads_at_one() {
    let store_dir = tempfile::tempdir().unwrap();
    let out_dir   = tempfile::tempdir().unwrap();
    let store     = Store::open(&store_dir.path().join("s.db")).unwrap();

    // Five healthy txt jobs; mock download holds the slot 50 ms each so the
    // race-window is wide enough that any concurrency bug in `download_stage`
    // would be caught reliably.
    let mut mock = MockClient::new().download_delay(Duration::from_millis(50));
    let mut docs: Vec<MessageInfo> = Vec::with_capacity(5);
    for i in 1..=5_i32 {
        let info = MessageInfo {
            chat_id:       -100,
            msg_id:        i,
            original_name: format!("d{i}.txt"),
            size_bytes:    32,
            mime:          Some("text/plain".into()),
            date:          0,
        };
        mock = mock.with_document(info.clone(), b"target.com:user@x.com:p\n".to_vec());
        docs.push(info);
    }
    mock = mock.script_upload(
        std::iter::repeat_with(|| UploadOutcome::Ok(50_000))
            .take(5)
            .collect(),
    );
    let mock_arc = Arc::new(mock);

    let cfg: PipelineConfig = {
        let mut c = cfg_with_dir(out_dir.path().to_path_buf());
        c.matcher_key = "target.com".into();
        c.inter_file_channel_capacity = 1;
        c
    };

    // Channel cap=5 lets the test pre-load all jobs and drop the sender so
    // the orchestrator runs to natural completion. The PROPERTY under test
    // is `cfg.inter_file_channel_capacity = 1` (the Stage1→Stage2 hop), not
    // the jobs-feeder hop.
    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(5);
    for (idx, info) in docs.iter().enumerate() {
        jobs_tx
            .send(Job {
                source_chat_id: -100,
                source_msg_id:  i32::try_from(idx + 1).unwrap(),
                info:           info.clone(),
            })
            .await
            .unwrap();
    }
    drop(jobs_tx);

    let advance: CursorAdvance = Arc::new(|_o: JobOutcome| { /* noop */ });
    interfile::run(mock_arc.as_ref(), Some(&store), &cfg, jobs_rx, advance)
        .await
        .expect("orchestrator must complete cleanly with healthy jobs");

    let observed = mock_arc.inflight_observed();
    assert_eq!(
        observed, 1,
        "inter_file_channel_capacity=1 broke: max in-flight downloads = {observed}",
    );
}
