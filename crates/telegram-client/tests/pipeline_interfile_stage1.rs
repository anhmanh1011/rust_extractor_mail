//! Drive Stage 1 in isolation: a single mock txt job through download_stage,
//! observe the resulting Stage1Out::Stream variant on the cap=1 channel.
//! Stage 2 + Stage 3 are not yet wired; this test exits as soon as the
//! download_stage handle joins.

use telegram_client::pipeline::interfile::{
    download_stage, Job, PipelineConfig, Stage1Out,
};
use telegram_client::telegram::mock::MockClient;
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
async fn stage1_emits_stream_variant_for_txt() {
    let dir  = tempfile::tempdir().unwrap();
    let mock = MockClient::new()
        .with_document(
            MessageInfo {
                chat_id:       -100_111,
                msg_id:        7,
                original_name: "dump.txt".into(),
                size_bytes:    32,
                mime:          Some("text/plain".into()),
                date:          0,
            },
            b"gmail.com:alice@x.com:hunter2\n".to_vec(),
        );

    let (jobs_tx,    jobs_rx)    = tokio::sync::mpsc::channel::<Job>(2);
    let (s1_out_tx,  mut s1_out_rx) = tokio::sync::mpsc::channel::<Stage1Out>(1);

    // Hoist the cloned `MessageInfo` out of the `Mutex` guard so we don't
    // hold a `std::sync::MutexGuard` across the `.await` (clippy's
    // `await_holding_lock` lint).
    let info = mock.messages.lock().unwrap()[&(-100_111, 7)].0.clone();
    jobs_tx.send(Job {
        source_chat_id: -100_111,
        source_msg_id:  7,
        info,
    }).await.unwrap();
    drop(jobs_tx);

    let cfg     = cfg_with_dir(dir.path().to_path_buf());
    let mock_ref = std::sync::Arc::new(mock);
    let mock_run = mock_ref.clone();
    let cfg_run  = cfg.clone();
    tokio::spawn(async move {
        download_stage(&*mock_run, &cfg_run, jobs_rx, s1_out_tx).await
    });

    let out = s1_out_rx.recv().await.expect("Stage 1 must emit one Stage1Out");
    match out {
        Stage1Out::Stream { ref first_chunk, .. } => {
            assert_eq!(first_chunk.as_ref(), b"gmail.com:alice@x.com:hunter2\n");
        }
        Stage1Out::Disk { .. } => panic!("expected Stream variant for txt"),
        Stage1Out::Failed { .. } => panic!("Stage 1 should not fail on a healthy mock"),
    }
    assert!(s1_out_rx.recv().await.is_none(), "channel must close after one job");
}
