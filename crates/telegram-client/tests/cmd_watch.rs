//! End-to-end tests for `cmd::watch::run_with_store_and_client` (Task 8.2).
//!
//! Covers:
//! - Each scripted update is processed once and the per-chat cursor is
//!   advanced to the highest seen msg_id.
//! - Two messages with byte-identical payloads dedup on sha256: only the
//!   first uploads, the second short-circuits on `AlreadyDone` and the
//!   cursor still advances past the deduped message.
//! - `--duration-seconds` honors the wall-clock budget when no updates ever
//!   arrive (loop must NOT hang).

use std::sync::Arc;

use telegram_client::cmd::watch::{run_with_store_and_client, WatchArgs};
use telegram_client::config::{
    AppConfig, BackfillSection, ExtractMode, ExtractSection, LogSection, OutputSection,
    PipelineSection, TelegramSection, WatchChannel, WatchSection,
};
use telegram_client::store::Store;
use telegram_client::telegram::mock::MockClient;
use telegram_client::telegram::MessageInfo;

/// Build a `(MessageInfo, payload bytes)` pair for the given identifiers.
fn doc(chat_id: i64, msg_id: i32, name: &str, bytes: &[u8]) -> (MessageInfo, Vec<u8>) {
    (
        MessageInfo {
            chat_id,
            msg_id,
            original_name: name.into(),
            size_bytes: bytes.len() as u64,
            mime: Some("text/plain".into()),
            date: 1_700_000_000 + i64::from(msg_id),
        },
        bytes.to_vec(),
    )
}

/// Build a minimal valid [`AppConfig`] with `[[watch.channel]]` listing
/// chat_id 42, output to numeric `target`, extract key `target.com`.
fn cfg_for(out_dir: &std::path::Path, target: i64) -> AppConfig {
    AppConfig {
        telegram: TelegramSection {
            session_path: out_dir.join(".session").to_string_lossy().into_owned(),
            download_concurrent_chunks: 4,
            output: OutputSection {
                chat: None,
                chat_id: Some(target),
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
        watch: WatchSection {
            channels: vec![WatchChannel {
                chat: None,
                chat_id: Some(42),
                extract: None,
            }],
        },
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
async fn watch_processes_each_update_once_and_advances_cursor() {
    let tmp = tempfile::tempdir().unwrap();
    let store_path = tmp.path().join("store.db");
    let out_dir = tmp.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let store = Store::open(&store_path).unwrap();

    // Two distinct payloads → distinct sha256, so neither dedups against
    // the other. Both messages must round-trip a full upload and the
    // per-chat cursor must end up at the highest msg_id observed.
    let body_a = b"target.com:alice@x.com:pwd1\notherdomain.com:noise\n".as_slice();
    let body_b = b"target.com:bob@y.com:pwd2\notherdomain.com:noise\n".as_slice();
    let (info_a, bytes_a) = doc(42, 100, "a.txt", body_a);
    let (info_b, bytes_b) = doc(42, 101, "b.txt", body_b);
    let mock = Arc::new(
        MockClient::new()
            .with_document(info_a.clone(), bytes_a)
            .with_document(info_b.clone(), bytes_b),
    );
    mock.script_updates(vec![info_a.clone(), info_b.clone()]);

    let cfg = cfg_for(&out_dir, /*target*/ 7);
    let args = WatchArgs {
        duration_seconds: Some(2),
        confirm_public: false,
    };
    run_with_store_and_client(&cfg, &args, mock.as_ref(), &store)
        .await
        .unwrap();

    // Both messages produced one upload each.
    assert_eq!(mock.uploaded.lock().unwrap().len(), 2);
    // Per-chat cursor advanced to the highest seen msg_id.
    assert_eq!(store.watch_cursor(42).unwrap(), Some(101));
}

#[tokio::test]
async fn watch_dedups_same_sha256_across_two_messages() {
    // Two distinct (chat,msg_id) pairs carrying byte-identical documents:
    // the first round-trips fully; the second short-circuits on AlreadyDone
    // and produces NO upload.
    let tmp = tempfile::tempdir().unwrap();
    let store_path = tmp.path().join("store.db");
    let out_dir = tmp.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let store = Store::open(&store_path).unwrap();

    let body = b"target.com:alice@x.com:pwd1\n".as_slice();
    let (info_a, bytes_a) = doc(42, 200, "x.txt", body);
    let (info_b, bytes_b) = doc(42, 201, "y.txt", body); // identical bytes
    let mock = Arc::new(
        MockClient::new()
            .with_document(info_a.clone(), bytes_a)
            .with_document(info_b.clone(), bytes_b),
    );
    mock.script_updates(vec![info_a, info_b]);

    let cfg = cfg_for(&out_dir, 7);
    let args = WatchArgs {
        duration_seconds: Some(2),
        confirm_public: false,
    };
    run_with_store_and_client(&cfg, &args, mock.as_ref(), &store)
        .await
        .unwrap();

    assert_eq!(
        mock.uploaded.lock().unwrap().len(),
        1,
        "second msg should dedup"
    );
    assert_eq!(
        store.watch_cursor(42).unwrap(),
        Some(201),
        "cursor still advances past the deduped message",
    );
}

#[tokio::test]
async fn watch_terminates_on_duration_seconds() {
    // No scripted updates — the receiver delivers nothing. With
    // duration_seconds=1, the loop must return Ok(()) within ~1 s rather
    // than hang.
    let tmp = tempfile::tempdir().unwrap();
    let store_path = tmp.path().join("store.db");
    let out_dir = tmp.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let store = Store::open(&store_path).unwrap();
    let mock = Arc::new(MockClient::new());

    let cfg = cfg_for(&out_dir, 7);
    let args = WatchArgs {
        duration_seconds: Some(1),
        confirm_public: false,
    };
    let started = std::time::Instant::now();
    run_with_store_and_client(&cfg, &args, mock.as_ref(), &store)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "duration_seconds did not honor the budget; elapsed {elapsed:?}",
    );
}

#[tokio::test]
async fn watch_gap_fills_messages_above_cursor_then_subscribes() {
    let tmp = tempfile::tempdir().unwrap();
    let store_path = tmp.path().join("store.db");
    let out_dir    = tmp.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let store = Store::open(&store_path).unwrap();

    // Pre-seed cursor at msg_id=100. Three new messages exist in history:
    // 101 (new, gap-fill), 102 (new, gap-fill), 103 (new, gap-fill).
    // Then a live update arrives: 104.
    store.update_watch_cursor(42, "Test", 100).unwrap();

    // Distinct payloads per id so sha256 dedup does NOT mask whether each
    // gap-fill message actually round-tripped through the fetch dispatch.
    let mut docs: Vec<(MessageInfo, Vec<u8>)> = Vec::new();
    for id in [101, 102, 103, 104] {
        let body = format!("target.com:a{id}@x.com:p{id}\n").into_bytes();
        docs.push(doc(42, id, &format!("m{id}.txt"), &body));
    }
    let mock = Arc::new(MockClient::new());
    for (info, bytes) in &docs {
        // Re-create with_document via a mutating helper since the builder
        // returned `self` by value; here we use the inner Mutex directly.
        mock.messages.lock().unwrap()
            .insert((info.chat_id, info.msg_id), (info.clone(), bytes.clone()));
    }
    // History contains the gap (101..=103) newest-first; the live update is 104.
    mock.script_history(42, vec![docs[2].0.clone(), docs[1].0.clone(), docs[0].0.clone()]);
    mock.script_updates(vec![docs[3].0.clone()]);

    let cfg  = cfg_for(&out_dir, 7);
    let args = WatchArgs { duration_seconds: Some(2), confirm_public: false };
    run_with_store_and_client(&cfg, &args, mock.as_ref(), &store).await.unwrap();

    // All four messages were processed.
    assert_eq!(mock.uploaded.lock().unwrap().len(), 4);
    assert_eq!(store.watch_cursor(42).unwrap(), Some(104));
}
