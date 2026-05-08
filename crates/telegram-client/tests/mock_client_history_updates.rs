//! Task 8.1 tests: `MessageInfo::date`, `iter_history`, `subscribe_updates`,
//! and the `MockClient` test helpers `script_history` / `script_updates`.

use telegram_client::telegram::{MessageInfo, MockClient, TelegramClient};

fn info(chat_id: i64, msg_id: i32, name: &str, size: u64, date: i64) -> MessageInfo {
    MessageInfo {
        chat_id,
        msg_id,
        original_name: name.into(),
        size_bytes: size,
        mime: Some("application/zip".into()),
        date,
    }
}

#[tokio::test]
async fn iter_history_returns_pages_in_descending_msg_id() {
    let m = MockClient::new();
    m.script_history(
        42,
        vec![
            info(42, 100, "a.zip", 10, 1_700_000_000),
            info(42, 99, "b.zip", 20, 1_699_999_000),
            info(42, 98, "c.zip", 30, 1_699_998_000),
        ],
    );

    let page1 = m.iter_history(42, None, 2).await.unwrap();
    assert_eq!(
        page1.iter().map(|i| i.msg_id).collect::<Vec<_>>(),
        vec![100, 99]
    );

    // max_id is exclusive: returns msg_ids strictly less than max_id.
    let page2 = m.iter_history(42, Some(99), 2).await.unwrap();
    assert_eq!(
        page2.iter().map(|i| i.msg_id).collect::<Vec<_>>(),
        vec![98]
    );
}

#[tokio::test]
async fn iter_history_respects_limit_and_returns_empty_after_exhaustion() {
    let m = MockClient::new();
    m.script_history(42, vec![info(42, 5, "x.txt", 1, 1_700_000_000)]);
    let p1 = m.iter_history(42, None, 100).await.unwrap();
    assert_eq!(p1.len(), 1);
    let p2 = m.iter_history(42, Some(5), 100).await.unwrap();
    assert!(p2.is_empty(), "no msg_ids strictly < 5");
}

#[tokio::test]
async fn subscribe_updates_filters_to_configured_chats_and_documents() {
    let m = MockClient::new();
    m.script_updates(vec![
        info(42, 200, "doc.zip", 100, 1_700_000_100),
        info(99, 300, "noise.zip", 100, 1_700_000_101), // not in subscription
        info(42, 201, "doc2.gz", 200, 1_700_000_102),
    ]);

    let mut rx = m.subscribe_updates(&[42]).await.unwrap();
    let m1 = rx.recv().await.unwrap();
    assert_eq!((m1.chat_id, m1.msg_id), (42, 200));
    let m2 = rx.recv().await.unwrap();
    assert_eq!((m2.chat_id, m2.msg_id), (42, 201));
    // After scripted updates drain, channel closes.
    assert!(rx.recv().await.is_none());
}

#[tokio::test]
async fn message_info_carries_date() {
    let m = MockClient::new().with_document(
        info(42, 10, "x.zip", 100, 1_700_000_500),
        vec![0u8; 100],
    );
    let got = m.message_info(42, 10).await.unwrap();
    assert_eq!(got.date, 1_700_000_500);
}
