//! Integration tests for `MockClient::download_stream` (Task 4.5).
//!
//! Covers:
//! - Scripted chunks emitted in order via `script_download`.
//! - Mid-stream scripted error forwarded, channel closed afterwards.
//! - Unscripted `(chat_id, msg_id)` returns an `Err` whose message contains
//!   "no script" or "not scripted".

use bytes::Bytes;
use telegram_client::telegram::mock::MockClient;
use telegram_client::telegram::TelegramClient;

#[tokio::test]
async fn mock_download_stream_emits_scripted_chunks_in_order() {
    let mock = MockClient::new();
    mock.script_download(
        42,
        7,
        vec![
            Ok(Bytes::from_static(b"chunk1")),
            Ok(Bytes::from_static(b"chunk2")),
            Ok(Bytes::from_static(b"chunk3")),
        ],
    );

    let mut rx = mock.download_stream(42, 7).await.unwrap();
    let mut out: Vec<u8> = Vec::new();
    while let Some(item) = rx.recv().await {
        out.extend_from_slice(&item.unwrap());
    }
    assert_eq!(out, b"chunk1chunk2chunk3");
}

#[tokio::test]
async fn mock_download_stream_propagates_scripted_error() {
    let mock = MockClient::new();
    mock.script_download(
        1,
        1,
        vec![
            Ok(Bytes::from_static(b"ok-prefix\n")),
            Err(anyhow::anyhow!("simulated network error")),
        ],
    );

    let mut rx = mock.download_stream(1, 1).await.unwrap();
    let first = rx.recv().await.unwrap().unwrap();
    assert_eq!(&first[..], b"ok-prefix\n");
    let second = rx.recv().await.unwrap();
    let err = second.unwrap_err();
    assert!(format!("{err}").contains("simulated network error"));
    assert!(rx.recv().await.is_none(), "stream must close after scripted error");
}

#[tokio::test]
async fn mock_download_stream_unscripted_chat_returns_err() {
    let mock = MockClient::new();
    let err = mock.download_stream(999, 999).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("no script") || msg.contains("not scripted"),
        "unexpected error: {msg}");
}
