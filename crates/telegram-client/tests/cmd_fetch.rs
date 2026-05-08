//! End-to-end tests for `cmd::fetch::run_with_client` (Task 4.7).
//!
//! Covers:
//! - Plain-text stream: only matching lines written to expected path.
//! - Gzip stream: decoded then extracted.
//! - Zip stream: returns "zip not yet implemented (Phase 5)" error.
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
fn cfg(out_dir: &std::path::Path) -> AppConfig {
    AppConfig {
        telegram: TelegramSection {
            session_path: out_dir.join(".session").to_string_lossy().into_owned(),
            download_concurrent_chunks: 4,
            output: OutputSection {
                chat: Some("me".into()),
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

#[tokio::test]
async fn fetch_stream_txt_writes_only_matches_to_expected_path() {
    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new().with_document(
        MessageInfo {
            chat_id: 42,
            msg_id: 7,
            file_name: "dump.txt".into(),
            size: 1_000,
            mime: None,
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
            file_name: "dump.gz".into(),
            size: gz.len() as u64,
            mime: None,
        },
        Vec::new(), // bytes unused -- script takes precedence
    ));
    mock.script_download(1, 1, vec![Ok(Bytes::from(gz))]);

    let cfg = cfg(tmp.path());
    let args = FetchArgs {
        link: None,
        chat: Some(1),
        msg_id: Some(1),
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
async fn fetch_zip_returns_phase5_error_in_phase4() {
    // ZIP local file header magic: PK
    let zip_local_header = [0x50u8, 0x4B, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00];
    let tmp = tempfile::tempdir().unwrap();
    let mock = Arc::new(MockClient::new().with_document(
        MessageInfo {
            chat_id: 1,
            msg_id: 1,
            file_name: "dump.zip".into(),
            size: 8,
            mime: None,
        },
        Vec::new(),
    ));
    mock.script_download(1, 1, vec![Ok(Bytes::copy_from_slice(&zip_local_header))]);

    let cfg = cfg(tmp.path());
    let args = FetchArgs {
        link: None,
        chat: Some(1),
        msg_id: Some(1),
    };
    let err = run_with_client(&cfg, &args, mock.as_ref())
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("zip not yet implemented") || msg.contains("Phase 5"),
        "expected zip-phase5 error, got: {msg}",
    );
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
                    file_name: "small.txt".into(),
                    size: 26,
                    mime: None,
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
    };
    run_with_client(&cfg, &args, mock.as_ref()).await.unwrap();

    let out_path = tmp.path().join("5050").join("12_small.out");
    assert_eq!(
        std::fs::read(&out_path).unwrap(),
        b"user@x.com:pwd
"
    );
}
