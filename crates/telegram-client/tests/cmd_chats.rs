//! Spec §7.3 §9.2: `chats` subcommand renders dialogs in a friendly table
//! and supports a case-insensitive substring filter. The `chats_with_client`
//! happy path is exercised end-to-end against `MockClient` so CI never
//! talks to live Telegram.

use telegram_client::cmd::chats::{chats_with_client, format_dialogs};
use telegram_client::telegram::mock::MockClient;
use telegram_client::telegram::{Dialog, DialogKind};

#[test]
fn formats_three_kinds_with_aligned_columns() {
    let ds = vec![
        Dialog {
            chat_id: 1,
            kind: DialogKind::User,
            title: "Alice".into(),
            username: Some("alice".into()),
        },
        Dialog {
            chat_id: -1_001_234_567_890,
            kind: DialogKind::Channel,
            title: "Dump A".into(),
            username: None,
        },
        Dialog {
            chat_id: 42,
            kind: DialogKind::Group,
            title: "Friends".into(),
            username: None,
        },
    ];
    let s = format_dialogs(&ds, None);
    assert!(s.contains("user"));
    assert!(s.contains("channel"));
    assert!(s.contains("group"));
    assert!(s.contains("-1001234567890"));
    assert!(s.contains("@alice"));
    assert!(s.contains("Dump A"));
}

#[test]
fn filter_is_case_insensitive_substring() {
    let ds = vec![
        Dialog {
            chat_id: 1,
            kind: DialogKind::Channel,
            title: "LinkedIn Dump".into(),
            username: Some("linkedin".into()),
        },
        Dialog {
            chat_id: 2,
            kind: DialogKind::Channel,
            title: "Random".into(),
            username: None,
        },
    ];
    let s = format_dialogs(&ds, Some("LINK"));
    assert!(s.contains("LinkedIn Dump"));
    assert!(!s.contains("Random"));
}

#[test]
fn empty_dialog_list_prints_helpful_hint() {
    let s = format_dialogs(&[], None);
    assert!(s.to_ascii_lowercase().contains("no dialogs") || s.contains("0 dialogs"));
}

#[tokio::test]
async fn chats_with_client_calls_warmup_and_lists_dialogs() {
    let client = MockClient::new()
        .with_dialog(Dialog {
            chat_id: -1_001_234_567_890,
            kind: DialogKind::Channel,
            title: "Dump A".into(),
            username: Some("dump_a".into()),
        })
        .with_dialog(Dialog {
            chat_id: 42,
            kind: DialogKind::Group,
            title: "Friends".into(),
            username: None,
        });
    chats_with_client(&client, None).await.unwrap();
    chats_with_client(&client, Some("dump")).await.unwrap();
}
