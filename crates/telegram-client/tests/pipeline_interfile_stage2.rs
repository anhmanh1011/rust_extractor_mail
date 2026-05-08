//! Drive Stage 2 in isolation: feed a synthetic Stage1Out::Stream, observe
//! a Stage2Out::Ready with correct sha256 + match count.

use bytes::Bytes;
use telegram_client::pipeline::interfile::{
    extract_stage, Job, PipelineConfig, Stage1Out, Stage2Out,
};
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
async fn stage2_emits_ready_with_sha_and_match_count() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_with_dir(dir.path().to_path_buf());

    let payload = b"gmail.com:alice@x.com:hunter2\n\
                    other.com:bob@y.com:nope\n\
                    gmail.com:carol@z.com:hello\n";
    let job = Job {
        source_chat_id: -100_555,
        source_msg_id:  9,
        info: MessageInfo {
            chat_id:       -100_555,
            msg_id:        9,
            original_name: "leak.txt".into(),
            size_bytes:    payload.len() as u64,
            mime:          Some("text/plain".into()),
            date:          0,
        },
    };

    let (chunks_tx, chunks_rx) = tokio::sync::mpsc::channel::<anyhow::Result<Bytes>>(4);
    let p = payload.to_vec();
    tokio::spawn(async move {
        let _ = chunks_tx.send(Ok(Bytes::from(p))).await;
        // first_chunk is empty in this test; the entire payload arrives
        // via chunks_rx as a single Bytes message.
    });

    let (s1_tx, s1_rx)   = tokio::sync::mpsc::channel::<Stage1Out>(1);
    let (s2_tx, mut s2_rx) = tokio::sync::mpsc::channel::<Stage2Out>(2);

    s1_tx.send(Stage1Out::Stream {
        job, format: telegram_client::pipeline::format::Format::Txt,
        is_gzip: false, first_chunk: Bytes::new(), chunks_rx,
    }).await.unwrap();
    drop(s1_tx);

    let cfg_run = cfg.clone();
    tokio::spawn(async move {
        extract_stage(None, &cfg_run, s1_rx, s2_tx).await
    });

    match s2_rx.recv().await.expect("Stage 2 emits one Stage2Out") {
        Stage2Out::Ready { sha256, lines_matched, .. } => {
            assert_eq!(sha256.len(), 64, "hex sha256");
            assert_eq!(lines_matched, 2, "two gmail.com lines should match");
        }
        other => panic!("expected Ready, got {other:?}"),
    }
}
