use telegram_client::telegram::link_parser::{parse_message_link, MessageRef};

#[test]
fn public_username_link() {
    let r = parse_message_link("https://t.me/durov/42").unwrap();
    assert_eq!(r, MessageRef::Username { username: "durov".into(), msg_id: 42 });
}

#[test]
fn private_chat_link_uses_neg100_prefix() {
    let r = parse_message_link("https://t.me/c/1234567890/42").unwrap();
    assert_eq!(r, MessageRef::ChatId { chat_id: -1001234567890, msg_id: 42 });
}

#[test]
fn tg_scheme_supported() {
    let r = parse_message_link("tg://resolve?domain=durov&post=42").unwrap();
    assert_eq!(r, MessageRef::Username { username: "durov".into(), msg_id: 42 });
}

#[test]
fn rejects_non_telegram_url() {
    assert!(parse_message_link("https://example.com/foo/42").is_err());
}

#[test]
fn rejects_missing_msg_id() {
    assert!(parse_message_link("https://t.me/durov").is_err());
}

#[test]
fn rejects_non_numeric_msg_id() {
    assert!(parse_message_link("https://t.me/durov/abc").is_err());
}
