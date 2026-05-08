//! End-to-end tests for `cmd::backfill::run_with_store_and_client` (Task 9.1).
//!
//! Covers the three core invariants of the Phase-9 backfill walker:
//! - `--limit` truncates the run after N messages and leaves
//!   `backfill_state.completed_at = NULL` so a follow-up `--resume` can
//!   continue from `next_msg_id`.
//! - `--since` (RFC-3339 UTC) terminates the run when a page yields a
//!   message older than the cutoff and stamps `completed_at`.
//! - Natural exhaustion (paging until `iter_history` returns an empty page)
//!   stamps `completed_at` and records the oldest processed `msg_id`.

use std::sync::Arc;

use telegram_client::cmd::backfill::{run_with_store_and_client, BackfillArgs};
use telegram_client::config::{
    AppConfig, BackfillSection, ExtractMode, ExtractSection, LogSection, OutputSection,
    PipelineSection, TelegramSection, WatchSection,
};
use telegram_client::store::Store;
use telegram_client::telegram::mock::MockClient;
use telegram_client::telegram::MessageInfo;

/// Build a `(MessageInfo, payload bytes)` pair with an explicit `date`
/// (Unix seconds) so the `--since` cutoff test can drive the boundary
/// deterministically.
fn doc(chat_id: i64, msg_id: i32, name: &str, bytes: &[u8], date: i64) -> (MessageInfo, Vec<u8>) {
    (
        MessageInfo {
            chat_id,
            msg_id,
            original_name: name.into(),
            size_bytes: bytes.len() as u64,
            mime: Some("text/plain".into()),
            date,
        },
        bytes.to_vec(),
    )
}

/// Build a minimal valid [`AppConfig`] with output to numeric `target`,
/// extract key `target.com`, and the supplied backfill knobs.
fn cfg_for(
    out_dir: &std::path::Path,
    target: i64,
    page_size: u32,
    since: Option<&str>,
) -> AppConfig {
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
        watch: WatchSection { channels: vec![] },
        backfill: BackfillSection {
            page_size,
            since: since.map(str::to_string),
        },
        log: LogSection {
            level: "info".into(),
            format: "human".into(),
            file: None,
            rotation: "never".into(),
        },
    }
}

#[tokio::test]
async fn backfill_walks_history_until_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("store.db")).unwrap();
    let out_dir = tmp.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();

    // Distinct payloads per id so sha256 dedup never masks a missed dispatch.
    let mut docs: Vec<(MessageInfo, Vec<u8>)> = Vec::new();
    for id in (1..=10).rev() {
        let body = format!("target.com:a{id}@x.com:p{id}\n").into_bytes();
        docs.push(doc(
            42,
            id,
            &format!("m{id}.txt"),
            &body,
            1_700_000_000 + i64::from(id),
        ));
    }
    let mock = Arc::new(MockClient::new());
    for (info, bytes) in &docs {
        mock.messages
            .lock()
            .unwrap()
            .insert((info.chat_id, info.msg_id), (info.clone(), bytes.clone()));
    }
    mock.script_history(42, docs.iter().map(|(i, _)| i.clone()).collect());

    let cfg = cfg_for(&out_dir, /*target*/ 7, /*page_size*/ 3, /*since*/ None);
    let args = BackfillArgs {
        chat: "42".into(),
        since: None,
        limit: Some(4),
        resume: false,
    };
    run_with_store_and_client(&cfg, &args, mock.as_ref(), &store)
        .await
        .unwrap();

    // limit=4 → newest four (msg_ids 10, 9, 8, 7) processed.
    assert_eq!(
        mock.uploaded.lock().unwrap().len(),
        4,
        "limit-bounded run must process exactly --limit messages",
    );
    let bf = store
        .backfill_cursor(42)
        .unwrap()
        .expect("backfill_state row must exist after at least one advance");
    assert_eq!(
        bf.next_msg_id, 7,
        "next_msg_id is the OLDEST processed (resume point)",
    );
    assert!(
        bf.completed_at.is_none(),
        "limit-bounded run did not exhaust history: completed_at must be NULL",
    );
}

#[tokio::test]
async fn backfill_stops_at_since_cutoff_and_marks_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("store.db")).unwrap();
    let out_dir = tmp.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();

    // Five messages dated D-0, D-1, D-2, D-3, D-4 (newest-first).
    // --since = D-2 (RFC-3339) → process messages dated STRICTLY NEWER than
    // D-2 (i.e. D-0 and D-1); a message at exactly the cutoff terminates
    // the run without being processed (cutoff is exclusive).
    let base = 1_700_000_000_i64;
    let day = 86_400_i64;
    let dates = [base, base - day, base - 2 * day, base - 3 * day, base - 4 * day];
    let mut docs: Vec<(MessageInfo, Vec<u8>)> = Vec::new();
    for (i, id) in (1..=5).rev().enumerate() {
        let body = format!("target.com:a{id}@x.com:p{id}\n").into_bytes();
        docs.push(doc(42, id, &format!("m{id}.txt"), &body, dates[i]));
    }
    let mock = Arc::new(MockClient::new());
    for (info, bytes) in &docs {
        mock.messages
            .lock()
            .unwrap()
            .insert((info.chat_id, info.msg_id), (info.clone(), bytes.clone()));
    }
    mock.script_history(42, docs.iter().map(|(i, _)| i.clone()).collect());

    // since = base - 2*day → RFC-3339 (UTC).
    let since_rfc = chrono::DateTime::<chrono::Utc>::from_timestamp(base - 2 * day, 0)
        .unwrap()
        .to_rfc3339();
    let cfg = cfg_for(&out_dir, 7, /*page_size*/ 10, Some(&since_rfc));
    let args = BackfillArgs {
        chat: "42".into(),
        since: None,
        limit: None,
        resume: false,
    };
    run_with_store_and_client(&cfg, &args, mock.as_ref(), &store)
        .await
        .unwrap();

    // Only the two newest (D-0, D-1) are dispatched; D-2 hits the cutoff.
    assert_eq!(
        mock.uploaded.lock().unwrap().len(),
        2,
        "since cutoff must terminate the run before processing D-2",
    );
    let bf = store
        .backfill_cursor(42)
        .unwrap()
        .expect("backfill_state row must exist after at least one advance");
    assert!(
        bf.completed_at.is_some(),
        "since-cutoff run is complete: completed_at must be stamped",
    );
}

#[tokio::test]
async fn backfill_marks_complete_when_history_exhausts() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("store.db")).unwrap();
    let out_dir = tmp.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();

    // Three messages, page_size=2 → forces a second page that returns empty
    // and triggers the natural-exhaustion path.
    let mut docs: Vec<(MessageInfo, Vec<u8>)> = Vec::new();
    for id in [3, 2, 1] {
        let body = format!("target.com:a{id}@x.com:p{id}\n").into_bytes();
        docs.push(doc(
            42,
            id,
            &format!("m{id}.txt"),
            &body,
            1_700_000_000 + i64::from(id),
        ));
    }
    let mock = Arc::new(MockClient::new());
    for (info, bytes) in &docs {
        mock.messages
            .lock()
            .unwrap()
            .insert((info.chat_id, info.msg_id), (info.clone(), bytes.clone()));
    }
    mock.script_history(42, docs.iter().map(|(i, _)| i.clone()).collect());

    let cfg = cfg_for(&out_dir, 7, /*page_size*/ 2, /*since*/ None);
    let args = BackfillArgs {
        chat: "42".into(),
        since: None,
        limit: None,
        resume: false,
    };
    run_with_store_and_client(&cfg, &args, mock.as_ref(), &store)
        .await
        .unwrap();

    assert_eq!(
        mock.uploaded.lock().unwrap().len(),
        3,
        "natural-exhaustion run must process every message in history",
    );
    let bf = store
        .backfill_cursor(42)
        .unwrap()
        .expect("backfill_state row must exist after at least one advance");
    assert!(
        bf.completed_at.is_some(),
        "natural-exhaustion run is complete: completed_at must be stamped",
    );
    assert_eq!(
        bf.next_msg_id, 1,
        "next_msg_id is the OLDEST processed message (msg_id=1)",
    );
}
