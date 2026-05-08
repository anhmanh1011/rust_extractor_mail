//! Spec §7.3: `join` subcommand accepts a t.me invite link and threads
//! it through to the underlying client. Mock-backed so CI never talks
//! to live Telegram.

use telegram_client::cmd::join;
use telegram_client::telegram::mock::MockClient;

#[tokio::test]
async fn join_records_invite_link_in_mock() {
    let client = MockClient::new();
    join::join_with_client(&client, "https://t.me/+abcDEF").await.unwrap();
    let joined = client.joined.lock().unwrap().clone();
    assert_eq!(joined, vec!["https://t.me/+abcDEF".to_string()]);
}

#[tokio::test]
async fn join_validates_link_shape() {
    let client = MockClient::new();
    let r = join::join_with_client(&client, "not-an-invite").await;
    assert!(r.is_err());
}
