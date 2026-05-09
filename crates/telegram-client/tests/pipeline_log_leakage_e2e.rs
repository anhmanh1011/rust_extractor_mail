//! Phase-10 regression. Orchestrator-level capture: scrubber + format
//! layer wrapping a complete `interfile::run` covering both a successful
//! upload AND a Stage-2 failure (so error-path tracing calls are
//! exercised). Asserts no credential / session bytes appear in capture.
//!
//! TODO(v1.1): mirror this assertion against the JSON-formatter path
//! (tracing-appender) once Chunk 6f wires file-rotated JSON output, so
//! both the human formatter AND the file output are locked down
//! independently.

use std::io;
use std::sync::{Arc, Mutex};

use telegram_client::observability::SecretScrubLayer;
use telegram_client::pipeline::interfile::{
    self, CursorAdvance, Job, JobOutcome,
};
use telegram_client::store::Store;
use telegram_client::telegram::mock::{MockClient, UploadOutcome as MockUploadOutcome};
use telegram_client::telegram::MessageInfo;
use tracing_subscriber::fmt::{self, MakeWriter};
// `.with(...)` on Registry comes from SubscriberExt — this import is
// LOAD-BEARING; without it the subscriber-builder lines below fail to compile.
use tracing_subscriber::layer::SubscriberExt as _;

mod common;
use common::cfg_with_dir;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn bytes(&self) -> Vec<u8> { self.0.lock().unwrap().clone() }
}

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl<'a> MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Capture { self.clone() }
}

fn build_bomb_zip(target_marker: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut payload = Vec::new();
    payload.extend_from_slice(b"target.com:");
    payload.extend_from_slice(target_marker);
    payload.extend_from_slice(b":pwd-LEAK-MARKER\n");
    payload.extend(vec![b'A'; 4096]);
    let cur = std::io::Cursor::new(Vec::new());
    let mut zw = zip::ZipWriter::new(cur);
    let opts: zip::write::FileOptions =
        zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
    zw.start_file("e1.txt", opts).unwrap();
    zw.write_all(&payload).unwrap();
    zw.start_file("e2.txt", opts).unwrap();
    zw.write_all(&payload).unwrap();
    zw.finish().unwrap().into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_orchestrator_run_does_not_leak_credentials_or_session() {
    const CRED_EMAIL:    &[u8] = b"alice-DO-NOT-LOG@example.com";
    const CRED_PASSWORD: &[u8] = b"pwd-LEAK-MARKER";
    const SESSION_BYTES: &[u8] = b"deadbeefcafef00d-session-bytes";

    let store_dir = tempfile::tempdir().unwrap();
    let out_dir   = tempfile::tempdir().unwrap();
    let store     = Store::open(&store_dir.path().join("s.db")).unwrap();

    // Healthy txt with a credential the scanner WILL match — these bytes go
    // through LineSink only; tracing must NOT see them.
    let mut txt = Vec::new();
    txt.extend_from_slice(b"target.com:");
    txt.extend_from_slice(CRED_EMAIL);
    txt.extend_from_slice(b":");
    txt.extend_from_slice(CRED_PASSWORD);
    txt.extend_from_slice(b"\n");
    let txt_len = u64::try_from(txt.len()).unwrap();

    let bomb = build_bomb_zip(CRED_EMAIL);
    let bomb_len = u64::try_from(bomb.len()).unwrap();

    let mock = Arc::new(
        MockClient::new()
            .with_document(
                MessageInfo {
                    chat_id: -100, msg_id: 11,
                    original_name: "creds.txt".into(),
                    size_bytes:    txt_len,
                    mime:          Some("text/plain".into()),
                    date: 0,
                },
                txt,
            )
            .with_document(
                MessageInfo {
                    chat_id: -100, msg_id: 12,
                    original_name: "bomb.zip".into(),
                    size_bytes:    bomb_len,
                    mime:          Some("application/zip".into()),
                    date: 0,
                },
                bomb,
            )
            .script_upload(vec![MockUploadOutcome::Ok(50_003)]),
    );

    let cfg = {
        let mut c = cfg_with_dir(out_dir.path().to_path_buf());
        c.matcher_key = "target.com".into();
        c.max_uncompressed_bytes = 6 * 1024; // bomb job will fail
        c
    };

    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(2);
    for msg_id in [11_i32, 12_i32] {
        let info = mock.messages.lock().unwrap()[&(-100i64, msg_id)].0.clone();
        jobs_tx.send(Job { source_chat_id: -100, source_msg_id: msg_id, info })
            .await.unwrap();
    }
    drop(jobs_tx);

    let cap = Capture::default();
    let layer = fmt::layer()
        .with_writer(cap.clone())
        .with_ansi(false)
        .fmt_fields(SecretScrubLayer::new());
    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_subscriber::EnvFilter::new("trace"))
        .with(layer);

    // Inject a synthetic top-level span that carries the would-be
    // session-ish field name to exercise the scrubber's regex.
    let advance: CursorAdvance = Arc::new(|_o: JobOutcome| { /* noop */ });
    let run_fut = async {
        let span = tracing::info_span!(
            "orchestrator_run",
            session = std::str::from_utf8(SESSION_BYTES).unwrap(),
            api_hash = "ff00ff00ff00ff00",
        );
        let _enter = span.enter();
        tracing::info!("starting full pipeline run");
        interfile::run(mock.as_ref(), Some(&store), &cfg, jobs_rx, advance)
            .await.expect("orchestrator must drain even with a bomb job");
        tracing::info!("pipeline run complete");
    };
    tracing::subscriber::with_default(subscriber, || {
        // tokio runtime is in scope already; we're inside #[tokio::test].
        // Use Handle::current().block_on(run_fut) — but block_on within an
        // active runtime requires block_in_place.
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(run_fut);
        });
    });

    let bytes = cap.bytes();
    let s = String::from_utf8_lossy(&bytes);

    // (a) The credential literal must NOT appear anywhere.
    assert!(
        !bytes.windows(CRED_EMAIL.len()).any(|w| w == CRED_EMAIL),
        "credential email leaked into tracing output:\n{s}",
    );
    assert!(
        !bytes.windows(CRED_PASSWORD.len()).any(|w| w == CRED_PASSWORD),
        "credential password leaked into tracing output:\n{s}",
    );

    // (b) The synthetic session bytes were carried via a `session=` field;
    //     SecretScrubLayer MUST replace them with a redaction marker.
    assert!(
        !bytes.windows(SESSION_BYTES.len()).any(|w| w == SESSION_BYTES),
        "session bytes leaked through SecretScrubLayer:\n{s}",
    );

    // (c) Sanity: capture is non-empty (i.e., the test actually wired
    //     subscriber + run, not a no-op that vacuously passes).
    assert!(!bytes.is_empty(), "no log output captured — subscriber wiring is wrong");
}
