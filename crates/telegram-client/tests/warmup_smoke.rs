//! Spec §9.2 — pin the `connect_and_warm` contract via `MockClient`:
//! warm-up survives both empty and populated dialog lists, and the
//! subsequent `iter_dialogs()` returns what the builder seeded.
//! Real-grammers warm-up is exercised manually through `tg-extract auth`.

use telegram_client::telegram::mock::MockClient;
use telegram_client::telegram::{Dialog, DialogKind, TelegramClient};

#[tokio::test]
async fn warm_up_with_zero_dialogs() {
    let c = MockClient::new();
    c.connect_and_warm().await.unwrap();
    assert!(c.iter_dialogs().await.unwrap().is_empty());
}

#[tokio::test]
async fn warm_up_with_many_dialogs() {
    let c = MockClient::new()
        .with_dialog(Dialog {
            chat_id: 1,
            kind: DialogKind::User,
            title: "Alice".into(),
            username: Some("alice".into()),
        })
        .with_dialog(Dialog {
            chat_id: -1_001_234_567_890,
            kind: DialogKind::Channel,
            title: "Dump A".into(),
            username: None,
        })
        .with_dialog(Dialog {
            chat_id: 42,
            kind: DialogKind::Group,
            title: "Friends".into(),
            username: None,
        });
    c.connect_and_warm().await.unwrap();
    let ds = c.iter_dialogs().await.unwrap();
    assert_eq!(ds.len(), 3);
    assert!(ds.iter().any(|d| d.kind == DialogKind::Channel));
}
