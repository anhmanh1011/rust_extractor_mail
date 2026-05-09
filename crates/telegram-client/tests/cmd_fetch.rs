//! End-to-end tests for `cmd::fetch::run_with_client` (Task 4.7 + Task 5.2).
//!
//! Covers:
//! - Plain-text stream: only matching lines written to expected path.
//! - Gzip stream: decoded then extracted.
//! - Zip disk-spill: all text entries are extracted into a single merged output.
//! - Link resolution: `https://t.me/<username>/<id>` resolves via mock dialogs.

use std::sync::Arc;

use bytes::Bytes;
use telegram_client::cmd::fetch::{run_with_client, FetchArgs};
use telegram_client::config::{
    AppConfig, BackfillSection, ExtractMode, ExtractSection, LogSection, OutputSection,
    PipelineSection, TelegramSection, WatchSection,
};
use telegram_client::telegram::mock::MockClient;
use telegram_client::telegram::{Dialog, DialogKind, MessageInfo};

/// Build a minimal valid [`AppConfig`] rooted at `out_dir`.
///
/// Default `output.chat`/`chat_id` are both `None`; tests that exercise the
/// upload step explicitly set one (or both) on the returned `AppConfig`.
/// Leaving them `None` here keeps existing extract-only tests from tripping
/// the public-chat gate or attempting an upload they don't script.
fn cfg(out_dir: &std::path::Path) -> AppConfig {
    AppConfig {
        telegram: TelegramSection {
            session_path: out_dir.join(".session").to_string_lossy().into_owned(),
            download_concurrent_chunks: 4,
            output: OutputSection {
                chat: None,
                chat_id: None,
            },
        },
        pipeline: PipelineSection {
            work_dir: out_dir.to_string_lossy().into_owned(),
            output_dir: out_dir.to_string_lossy().into_owned(),
            chunk_bytes: 1 << 20,
            intra_file_channel_capacity: 4,
            inter_file_channel_capacity: 1,
            upload_channel_capacity: 2,
            max_line_bytes: 64 * 1024,
            upload_rate_seconds: 0,
            upload_max_size_bytes: 2 * 1024 * 1024 * 1024,
            max_uncompressed_bytes: 10 * 1024 * 1024 * 1024,
        },
        extract: ExtractSection {
            mode: ExtractMode::Plain,
            key: "target.com".into(),
        },
        watch: WatchSection::default(),
        backfill: BackfillSection::default(),
        log: LogSection {
            level: "info".into(),
            format: "human".into(),
            file: None,
            rotation: "never".into(),
        },
    }
}

/// Build a real zip archive byte-buffer with the given (name, body) entries.
fn build_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    use std::io::Write;
    use zip::write::FileOptions;
    let mut buf: Vec<u8> = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut buf);
        let mut zip = zip::ZipWriter::new(cursor);
        let opts = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        for (name, body) in entries {
            zip.start_file(*name, opts).unwrap();
            zip.write_all(body).unwrap();
        }
        zip.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn fetch_stream_txt_writes_only_matches_to_expected_path() {
    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new().with_document(
        MessageInfo {
            chat_id: 42,
            msg_id: 7,
            original_name: "dump.txt".into(),
            size_bytes: 1_000,
            mime: None,
            date: 0,
        },
        Vec::new(), // bytes unused -- script takes precedence
    ));
    mock.script_download(
        42,
        7,
        vec![Ok(Bytes::from_static(
            b"target.com:alice@x.com:p1
other.com:bob@y.com:p2
target.com:carol@z.com:p3
",
        ))],
    );

    let cfg = cfg(tmp.path());
    let args = FetchArgs {
        link: None,
        chat: Some(42),
        msg_id: Some(7),
        no_upload: false,
        confirm_public: false,
    };
    run_with_client(&cfg, &args, mock.as_ref()).await.unwrap();

    let out_path = tmp.path().join("42").join("7_dump.out");
    let content = std::fs::read(&out_path).unwrap();
    assert_eq!(
        content,
        b"alice@x.com:p1
carol@z.com:p3
"
    );
}

#[tokio::test]
async fn fetch_stream_gz_decodes_and_extracts() {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(
        b"target.com:a@a.com:p1
noise.com:b@b.com:p2
target.com:c@c.com:p3
",
    )
    .unwrap();
    let gz = enc.finish().unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new().with_document(
        MessageInfo {
            chat_id: 1,
            msg_id: 1,
            original_name: "dump.gz".into(),
            size_bytes: gz.len() as u64,
            mime: None,
            date: 0,
        },
        Vec::new(), // bytes unused -- script takes precedence
    ));
    mock.script_download(1, 1, vec![Ok(Bytes::from(gz))]);

    let cfg = cfg(tmp.path());
    let args = FetchArgs {
        link: None,
        chat: Some(1),
        msg_id: Some(1),
        no_upload: false,
        confirm_public: false,
    };
    run_with_client(&cfg, &args, mock.as_ref()).await.unwrap();

    let out_path = tmp.path().join("1").join("1_dump.out");
    let content = std::fs::read(&out_path).unwrap();
    assert_eq!(
        content,
        b"a@a.com:p1
c@c.com:p3
"
    );
}

#[tokio::test]
async fn fetch_zip_extracts_all_text_entries() {
    // Build a real zip with two .txt entries; the disk-spill path
    // (`pipeline::disk::disk_extract`) must spill, open, decompress each
    // entry, and merge matching lines into a single output file.
    let zip_bytes = build_zip(&[
        ("a.txt", b"target.com:alice@x.com:p1\nnoise\n"),
        ("b.txt", b"noise\ntarget.com:bob@y.com:p2\n"),
    ]);

    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new().with_document(
        MessageInfo {
            chat_id: 7,
            msg_id: 7,
            original_name: "dump.zip".into(),
            size_bytes: zip_bytes.len() as u64,
            mime: None,
            date: 0,
        },
        Vec::new(), // bytes unused -- script takes precedence
    ));
    mock.script_download(7, 7, vec![Ok(Bytes::from(zip_bytes))]);

    let cfg = cfg(tmp.path());
    let args = FetchArgs {
        link: None,
        chat: Some(7),
        msg_id: Some(7),
        no_upload: false,
        confirm_public: false,
    };
    run_with_client(&cfg, &args, mock.as_ref()).await.unwrap();

    let out_path = tmp.path().join("7").join("7_dump.out");
    let content = std::fs::read(&out_path).unwrap();
    assert_eq!(content, b"alice@x.com:p1\nbob@y.com:p2\n");
}

#[tokio::test]
async fn fetch_link_resolves_to_chat_and_msg_id() {
    let tmp = tempfile::tempdir().unwrap();
    // Register the username -> chat_id mapping via with_dialog,
    // and the message via with_document, then script the download.
    let mock = Arc::new(
        MockClient::new()
            .with_dialog(Dialog {
                chat_id: 5050,
                kind: DialogKind::Channel,
                title: "FooChan".into(),
                username: Some("foochan".into()),
            })
            .with_document(
                MessageInfo {
                    chat_id: 5050,
                    msg_id: 12,
                    original_name: "small.txt".into(),
                    size_bytes: 26,
                    mime: None,
                    date: 0,
                },
                Vec::new(), // bytes unused -- script takes precedence
            ),
    );
    mock.script_download(
        5050,
        12,
        vec![Ok(Bytes::from_static(
            b"target.com:user@x.com:pwd
",
        ))],
    );

    let cfg = cfg(tmp.path());
    let args = FetchArgs {
        link: Some("https://t.me/foochan/12".into()),
        chat: None,
        msg_id: None,
        no_upload: false,
        confirm_public: false,
    };
    run_with_client(&cfg, &args, mock.as_ref()).await.unwrap();

    let out_path = tmp.path().join("5050").join("12_small.out");
    assert_eq!(
        std::fs::read(&out_path).unwrap(),
        b"user@x.com:pwd
"
    );
}

// ── Task 6.6: upload-into-cmd::fetch tests ───────────────────────────────────

#[tokio::test]
async fn fetch_uploads_to_configured_chat_id() {
    let tmp = tempfile::tempdir().unwrap();
    let mock = MockClient::new().with_document(
        MessageInfo {
            chat_id: 42,
            msg_id: 7,
            original_name: "dump.txt".into(),
            size_bytes: 1_000,
            mime: None,
            date: 0,
        },
        Vec::new(), // bytes unused -- script takes precedence
    );
    mock.script_download(
        42,
        7,
        vec![Ok(Bytes::from_static(b"target.com:alice@x.com:p1\n"))],
    );
    let mock = Arc::new(mock.script_upload(vec![
        telegram_client::telegram::mock::UploadOutcome::Ok(909),
    ]));

    let mut cfg = cfg(tmp.path());
    cfg.telegram.output.chat = None;
    cfg.telegram.output.chat_id = Some(-1001234567890);

    let args = FetchArgs {
        link: None,
        chat: Some(42),
        msg_id: Some(7),
        no_upload: false,
        confirm_public: false,
    };
    run_with_client(&cfg, &args, mock.as_ref()).await.unwrap();

    let uploads = mock.uploaded.lock().unwrap();
    assert_eq!(uploads.len(), 1, "expected one upload, got {uploads:?}");
    let (target_chat, ref path, ref caption, msg_id) = uploads[0];
    assert_eq!(target_chat, -1001234567890);
    assert!(path.ends_with("7_dump.out"), "{path:?}");
    let cap = caption.as_deref().unwrap_or("");
    assert!(cap.contains("dump.txt"), "caption = {cap}");
    assert_eq!(msg_id, 909);
}

#[tokio::test]
async fn fetch_skips_upload_when_no_upload_flag_set() {
    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new().with_document(
        MessageInfo {
            chat_id: 1,
            msg_id: 1,
            original_name: "x.txt".into(),
            size_bytes: 10,
            mime: None,
            date: 0,
        },
        Vec::new(),
    ));
    mock.script_download(
        1,
        1,
        vec![Ok(Bytes::from_static(b"target.com:a@a.com:p\n"))],
    );

    let mut cfg = cfg(tmp.path());
    cfg.telegram.output.chat_id = Some(-1001234567890);

    let args = FetchArgs {
        link: None,
        chat: Some(1),
        msg_id: Some(1),
        no_upload: true,
        confirm_public: false,
    };
    run_with_client(&cfg, &args, mock.as_ref()).await.unwrap();
    assert!(
        mock.uploaded.lock().unwrap().is_empty(),
        "no upload expected"
    );
}

#[tokio::test]
async fn fetch_aborts_on_public_chat_without_confirm() {
    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new().with_document(
        MessageInfo {
            chat_id: 1,
            msg_id: 1,
            original_name: "x.txt".into(),
            size_bytes: 10,
            mime: None,
            date: 0,
        },
        Vec::new(),
    ));
    mock.script_download(
        1,
        1,
        vec![Ok(Bytes::from_static(b"target.com:a@a.com:p\n"))],
    );

    let mut cfg = cfg(tmp.path());
    cfg.telegram.output.chat = Some("@public_chan".into());
    cfg.telegram.output.chat_id = None;

    let args = FetchArgs {
        link: None,
        chat: Some(1),
        msg_id: Some(1),
        no_upload: false,
        confirm_public: false,
    };
    let err = run_with_client(&cfg, &args, mock.as_ref())
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("--confirm-public"), "got: {msg}");
}

#[tokio::test]
async fn fetch_aborts_on_bare_username_without_confirm() {
    // A `chat` value with no leading '@' that ALSO doesn't parse as a
    // numeric chat id is treated as public per spec §11.2 -- covers
    // "my_channel" or "Some Title" typos.
    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new().with_document(
        MessageInfo {
            chat_id: 1,
            msg_id: 1,
            original_name: "x.txt".into(),
            size_bytes: 10,
            mime: None,
            date: 0,
        },
        Vec::new(),
    ));
    mock.script_download(
        1,
        1,
        vec![Ok(Bytes::from_static(b"target.com:a@a.com:p\n"))],
    );

    let mut cfg = cfg(tmp.path());
    cfg.telegram.output.chat = Some("my_channel".into());
    cfg.telegram.output.chat_id = None;

    let args = FetchArgs {
        link: None,
        chat: Some(1),
        msg_id: Some(1),
        no_upload: false,
        confirm_public: false,
    };
    let err = run_with_client(&cfg, &args, mock.as_ref())
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("--confirm-public"));
}

#[tokio::test]
async fn fetch_does_not_gate_numeric_chat_string() {
    // `chat = "-1001234567890"` is a private channel id stored as a
    // string. It MUST NOT trigger the public-chat gate. The `chat_id`
    // numeric field is the canonical form, but accepting numeric
    // strings here matches how some configs are templated.
    let tmp = tempfile::tempdir().unwrap();
    let mock = MockClient::new().with_document(
        MessageInfo {
            chat_id: 1,
            msg_id: 1,
            original_name: "x.txt".into(),
            size_bytes: 10,
            mime: None,
            date: 0,
        },
        Vec::new(),
    );
    mock.script_download(
        1,
        1,
        vec![Ok(Bytes::from_static(b"target.com:a@a.com:p\n"))],
    );
    let mock = Arc::new(
        mock.script_upload(vec![telegram_client::telegram::mock::UploadOutcome::Ok(7)]),
    );

    let mut cfg = cfg(tmp.path());
    cfg.telegram.output.chat = Some("-1001234567890".into());
    cfg.telegram.output.chat_id = None;

    let args = FetchArgs {
        link: None,
        chat: Some(1),
        msg_id: Some(1),
        no_upload: false,
        confirm_public: false,
    };
    run_with_client(&cfg, &args, mock.as_ref())
        .await
        .expect("numeric-string chat must not trigger public gate");
    let uploads = mock.uploaded.lock().unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].0, -1001234567890);
}

// ── Task 7.6: Store wiring + sha256 dedup tests ──────────────────────────────

#[tokio::test]
async fn fetch_persists_files_row_and_dedupes_on_second_run() {
    use telegram_client::store::{EnqueueResult, Store};

    let tmp = tempfile::tempdir().unwrap();
    let mock = MockClient::new();
    mock.set_message(
        42,
        7,
        MessageInfo {
            chat_id: 42,
            msg_id: 7,
            original_name: "dump.txt".into(),
            size_bytes: 10,
            mime: None,
            date: 0,
        },
    );
    mock.script_download(
        42,
        7,
        vec![Ok(Bytes::from_static(b"target.com:a@a.com:p\n"))],
    );
    let mock = Arc::new(
        mock.script_upload(vec![telegram_client::telegram::mock::UploadOutcome::Ok(701)]),
    );

    let mut cfg = cfg(tmp.path());
    cfg.telegram.output.chat_id = Some(-1001234567890);

    let store = Store::open(&tmp.path().join("state.db")).unwrap();

    let args = telegram_client::cmd::fetch::FetchArgs {
        link: None,
        chat: Some(42),
        msg_id: Some(7),
        no_upload: false,
        confirm_public: false,
    };
    telegram_client::cmd::fetch::run_with_store_and_client(
        &cfg,
        &args,
        mock.as_ref(),
        Some(&store),
    )
    .await
    .unwrap();

    // Second run: same source, fresh download; second mock script must be primed,
    // but try_enqueue should short-circuit before re-uploading.
    mock.script_download(
        42,
        7,
        vec![Ok(Bytes::from_static(b"target.com:a@a.com:p\n"))],
    );
    let result = telegram_client::cmd::fetch::run_with_store_and_client(
        &cfg,
        &args,
        mock.as_ref(),
        Some(&store),
    )
    .await;

    assert!(result.is_ok(), "second run: {:?}", result);
    let uploads = mock.uploaded.lock().unwrap();
    assert_eq!(uploads.len(), 1, "second run must NOT re-upload");

    // Validate one row, status=done.
    let conn = store.lock();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM files", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
    let st: String = conn
        .query_row("SELECT status FROM files", [], |r| r.get(0))
        .unwrap();
    assert_eq!(st, "done");
    let _ = EnqueueResult::AlreadyDone; // sentinel to keep the import alive
}

#[tokio::test]
async fn fetch_no_upload_does_not_pollute_dedup_state() {
    use telegram_client::store::Store;

    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new());
    mock.set_message(
        42,
        7,
        MessageInfo {
            chat_id: 42,
            msg_id: 7,
            original_name: "dump.txt".into(),
            size_bytes: 10,
            mime: None,
            date: 0,
        },
    );
    mock.script_download(
        42,
        7,
        vec![Ok(Bytes::from_static(b"target.com:a@a.com:p\n"))],
    );

    let mut cfg = cfg(tmp.path());
    cfg.telegram.output.chat_id = Some(-1001234567890);

    let store = Store::open(&tmp.path().join("state.db")).unwrap();

    let args = telegram_client::cmd::fetch::FetchArgs {
        link: None,
        chat: Some(42),
        msg_id: Some(7),
        no_upload: true,
        confirm_public: false,
    };
    telegram_client::cmd::fetch::run_with_store_and_client(
        &cfg,
        &args,
        mock.as_ref(),
        Some(&store),
    )
    .await
    .unwrap();

    // The row exists but is NOT 'done' -- no upload happened, so a future
    // run without --no-upload must be allowed to proceed.
    let conn = store.lock();
    let st: String = conn
        .query_row("SELECT status FROM files", [], |r| r.get(0))
        .unwrap();
    assert_ne!(st, "done", "--no-upload must not mark status=done");
    let omid: Option<i64> = conn
        .query_row("SELECT output_msg_id FROM files", [], |r| r.get(0))
        .unwrap();
    assert!(
        omid.is_none(),
        "--no-upload must leave output_msg_id NULL (got {omid:?})"
    );

    // No upload was attempted.
    let uploads = mock.uploaded.lock().unwrap();
    assert_eq!(uploads.len(), 0);
}
