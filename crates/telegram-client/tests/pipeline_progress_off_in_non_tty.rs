//! Phase-11 regression. Drives the orchestrator with `cfg.progress = None`
//! (the daemon / CI default) and asserts the orchestrator behaves identically
//! to the pre-bars baseline: outcomes still fire in FIFO order, no panic, no
//! extra side effects. Tests under non-TTY only — TTY rendering is exercised
//! manually per spec §9.3.

mod common;

use std::sync::{Arc, Mutex};

use telegram_client::pipeline::interfile::{
    self, CursorAdvance, Job, JobOutcome, OutcomeKind,
};
use telegram_client::telegram::mock::{MockClient, UploadOutcome};
use telegram_client::telegram::MessageInfo;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orchestrator_runs_to_completion_with_progress_none() {
    let dir = tempfile::tempdir().unwrap();

    // Three .txt jobs; the no-bar path must produce identical results to
    // an instrumented run. We assert ordering + count, NOT timing.
    let mock = MockClient::new();
    let mut docs: Vec<MessageInfo> = Vec::new();
    for &msg in &[101_i32, 102, 103] {
        let info = MessageInfo {
            chat_id:       -100_900,
            msg_id:        msg,
            original_name: format!("dump-{msg}.txt"),
            size_bytes:    64,
            mime:          Some("text/plain".into()),
            date:          0,
        };
        let body = format!("gmail.com:hit{msg}@x.com:p\nnoise\n");
        mock.messages.lock().unwrap().insert(
            (info.chat_id, info.msg_id),
            (info.clone(), body.into_bytes()),
        );
        docs.push(info);
    }
    let mock = mock.script_upload(vec![
        UploadOutcome::Ok(7_001),
        UploadOutcome::Ok(7_002),
        UploadOutcome::Ok(7_003),
    ]);

    let cfg = common::cfg_with_dir(dir.path().to_path_buf());
    assert!(cfg.progress.is_none(), "non-TTY default must be None");

    // Channel capacity 3 so we can pre-send all jobs and drop the sender
    // before invoking `interfile::run` (cf. pipeline_interfile_e2e.rs:66).
    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(3);
    for d in docs {
        jobs_tx
            .send(Job {
                source_chat_id: d.chat_id,
                source_msg_id:  d.msg_id,
                info:           d,
            })
            .await
            .unwrap();
    }
    drop(jobs_tx);

    let outcomes: Arc<Mutex<Vec<JobOutcome>>> = Arc::new(Mutex::new(Vec::new()));
    let coll = outcomes.clone();
    let on_outcome: CursorAdvance = Arc::new(move |o| coll.lock().unwrap().push(o));

    interfile::run(&mock, None, &cfg, jobs_rx, on_outcome)
        .await
        .expect("clean shutdown");

    let got = outcomes.lock().unwrap();
    let ids: Vec<i32> = got.iter().map(|o| o.job.source_msg_id).collect();
    assert_eq!(ids, vec![101, 102, 103], "FIFO ordering preserved");
    for o in got.iter() {
        assert!(
            matches!(o.kind, OutcomeKind::Uploaded { .. }),
            "expected Uploaded with no-bar path: {:?}",
            o.kind
        );
    }
}
