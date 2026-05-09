//! Skeleton test for `pipeline::interfile`: an empty job stream returns Ok(())
//! and produces zero outcomes. This pins down the empty-input contract before
//! any stage logic exists.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use telegram_client::pipeline::interfile::{
    self, CursorAdvance, JobOutcome, PipelineConfig,
};
use telegram_client::telegram::mock::MockClient;

fn empty_cfg() -> PipelineConfig {
    PipelineConfig {
        matcher_key:                 "gmail.com".into(),
        matcher_mode:                "plain".into(),
        output_dir:                  std::env::temp_dir().join("interfile-skel"),
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
async fn empty_job_stream_returns_ok_with_zero_outcomes() {
    let mock = MockClient::new();
    let cfg = empty_cfg();

    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel(1);
    drop(jobs_tx); // immediately close the input

    let counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let counter_cb = counter.clone();
    let on_outcome: CursorAdvance =
        Arc::new(move |_o: JobOutcome| {
            counter_cb.fetch_add(1, Ordering::SeqCst);
        });

    interfile::run(&mock, None, &cfg, jobs_rx, on_outcome)
        .await
        .expect("empty stream is the canonical happy path");
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}
