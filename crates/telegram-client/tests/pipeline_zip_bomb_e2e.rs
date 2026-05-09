//! Phase-10 regression. Pushes a 2-entry zip with cumulative-uncompressed
//! 8 KiB through the orchestrator with `max_uncompressed_bytes = 6 KiB`.
//! The bomb job MUST fail; a follow-up healthy txt job MUST succeed; and
//! the bomb's would-be output path MUST NOT exist on disk.

use std::sync::Arc;
use std::sync::Mutex;

use telegram_client::pipeline::interfile::{
    self, CursorAdvance, Job, JobOutcome,
};
use telegram_client::store::Store;
use telegram_client::telegram::mock::MockClient;
use telegram_client::telegram::MessageInfo;

mod common;
use common::cfg_with_dir;

/// 2-entry zip with two 4 KiB payloads each containing a target.com hit.
fn build_bomb_zip() -> Vec<u8> {
    use std::io::Write;
    let body_a = {
        let mut v = Vec::new();
        v.extend_from_slice(b"target.com:hit-a@x.com:pwd\n");
        v.extend(vec![b'A'; 4096 - 27]);
        v
    };
    let body_b = body_a.clone();
    let cur = std::io::Cursor::new(Vec::new());
    let mut zw = zip::ZipWriter::new(cur);
    let opts: zip::write::FileOptions =
        zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
    zw.start_file("e1.txt", opts).unwrap();
    zw.write_all(&body_a).unwrap();
    zw.start_file("e2.txt", opts).unwrap();
    zw.write_all(&body_b).unwrap();
    zw.finish().unwrap().into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bombed_zip_fails_dead_letters_no_partial_out_next_job_succeeds() {
    use telegram_client::pipeline::interfile::OutcomeKind as OK;
    use telegram_client::telegram::mock::UploadOutcome as MockUploadOutcome;

    let store_dir = tempfile::tempdir().unwrap();
    let out_dir   = tempfile::tempdir().unwrap();
    let store     = Arc::new(Store::open(&store_dir.path().join("s.db")).unwrap());
    let zipb      = build_bomb_zip();
    let zipb_len  = u64::try_from(zipb.len()).unwrap();

    let mock = MockClient::new()
        .with_document(
            MessageInfo {
                chat_id: -100, msg_id: 7,
                original_name: "bomb.zip".into(),
                size_bytes:    zipb_len,
                mime:          Some("application/zip".into()),
                date: 0,
            },
            zipb,
        )
        .with_document(
            MessageInfo {
                chat_id: -100, msg_id: 8,
                original_name: "clean.txt".into(),
                size_bytes:    25,
                mime:          Some("text/plain".into()),
                date: 0,
            },
            b"target.com:hit-c@x.com:p\n".to_vec(),
        )
        .script_upload(vec![MockUploadOutcome::Ok(50_001)]);
    let mock_arc = Arc::new(mock);

    let cfg = {
        let mut c = cfg_with_dir(out_dir.path().to_path_buf());
        c.matcher_key = "target.com".into();
        c.max_uncompressed_bytes = 6 * 1024; // strictly < 8 KiB cumulative
        c
    };

    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(2);
    for msg_id in [7_i32, 8_i32] {
        let info = mock_arc.messages.lock().unwrap()[&(-100i64, msg_id)].0.clone();
        jobs_tx.send(Job { source_chat_id: -100, source_msg_id: msg_id, info })
            .await.unwrap();
    }
    drop(jobs_tx);

    // Emulate the production `cmd::watch` CursorAdvance: record_dead_letter
    // on Failed, push the outcome to a vec for FIFO/ordering assertions.
    // Without this, `interfile::run` itself does NOT touch the
    // dead_letter table — that write is callback-driven by design.
    let outcomes: Arc<Mutex<Vec<JobOutcome>>> = Arc::new(Mutex::new(Vec::new()));
    let cb_outs  = outcomes.clone();
    let cb_store = store.clone();
    let advance: CursorAdvance = Arc::new(move |o: JobOutcome| {
        if let OK::Failed { error } = &o.kind {
            // Emulating production cmd::watch CursorAdvance arm. The signature
            // is the full 8-arg form from Chunk 6c; production uses
            // `classify_format` / `classify_stage` helpers, but in this test
            // fixture hardcoded "zip" / "extract" suffices because the only
            // Failed outcome we exercise here is the Stage-2 zip-bomb cap.
            // We `.expect` (not `let _`) so a regression in
            // `Store::record_dead_letter` itself surfaces as a clear panic
            // rather than as an opaque "expected 1 dead_letter row, got 0".
            cb_store
                .record_dead_letter(
                    o.job.source_chat_id,
                    o.job.source_msg_id,
                    None,
                    &o.job.info.original_name,
                    o.job.info.size_bytes,
                    "zip",
                    "extract",
                    error,
                )
                .expect("test fixture must record dead_letter");
        }
        cb_outs.lock().unwrap().push(o);
    });
    interfile::run(mock_arc.as_ref(), Some(store.as_ref()), &cfg, jobs_rx, advance)
        .await.expect("orchestrator must drain even when one job fails");

    // (1) Outcome ordering is FIFO; first is the bomb (Failed), second is healthy (Uploaded).
    let outs = outcomes.lock().unwrap().clone();
    assert_eq!(outs.len(), 2, "expected 2 outcomes, got {outs:#?}");
    match &outs[0].kind {
        OK::Failed { error } => {
            assert!(
                error.contains("max_uncompressed_bytes") || error.contains("zip bomb"),
                "expected bomb cap error, got: {error}",
            );
        }
        other => panic!("expected Failed for bomb job, got {other:?}"),
    }
    match &outs[1].kind {
        OK::Uploaded { .. } => {}
        other => panic!("expected Uploaded for clean job, got {other:?}"),
    }

    // (2) dead_letter row exists for the bombed (chat, msg). The CursorAdvance
    //     above writes it explicitly, mirroring production cmd::watch.
    let dl = store.dead_letters().unwrap();
    assert_eq!(dl.len(), 1, "expected 1 dead_letter row, got {dl:#?}");
    assert_eq!(dl[0].source_chat_id, -100);
    assert_eq!(dl[0].source_msg_id,    7);

    // (3) No partial .out for the bombed job.
    let bomb_out = out_dir.path().join("-100").join("7_bomb.out");
    assert!(
        !bomb_out.exists(),
        "bombed job left a partial .out at {}; cleanup regressed",
        bomb_out.display(),
    );

    // (4) Healthy job's .out exists with the expected hit.
    let clean_out = out_dir.path().join("-100").join("8_clean.out");
    assert_eq!(
        std::fs::read(&clean_out).unwrap(),
        b"hit-c@x.com:p\n",
    );
}
