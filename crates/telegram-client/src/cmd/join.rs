//! `join` subcommand: accept an invite link to a private channel.
//!
//! Spec §7.3: validates link shape locally before issuing the network
//! call so a typo returns a clear error instead of an opaque grammers
//! `RpcError`. The generic helper `join_with_client` is exercised by
//! `MockClient` in CI per spec §9.2.

use crate::config::{AppConfig, Secrets};
use crate::telegram::client::GrammersClient;
use crate::telegram::TelegramClient;
use anyhow::{anyhow, Result};

/// Production entry point: connect via grammers, warm up, then delegate
/// to `join_with_client`. Spec §7.3.
pub async fn run(cfg: &AppConfig, secrets: &Secrets, invite_link: &str) -> Result<()> {
    let client = GrammersClient::connect(
        secrets.api_id,
        &secrets.api_hash,
        std::path::Path::new(&cfg.telegram.session_path),
    )
    .await?;
    client.connect_and_warm().await?;
    join_with_client(&client, invite_link).await?;
    println!("Joined: {invite_link}");
    Ok(())
}

/// Generic helper used by both production `run` (with `GrammersClient`)
/// and unit tests (`MockClient`). Validates the link shape locally
/// before delegating to the client. Spec §9.2.
pub async fn join_with_client<C: TelegramClient>(client: &C, link: &str) -> Result<()> {
    if !is_valid_invite_link(link) {
        return Err(anyhow!("not a valid t.me invite link: {link}"));
    }
    client.join_invite_link(link).await
}

/// Local shape check: accepts `https://t.me/+TOKEN`, `http://t.me/+TOKEN`,
/// `https://t.me/joinchat/TOKEN`, and `http://t.me/joinchat/TOKEN`. The
/// token must be at least 4 chars of `[A-Za-z0-9_-]`. Telegram itself
/// is the source of truth — this is just a fast-fail for typos.
fn is_valid_invite_link(link: &str) -> bool {
    const MIN_TOKEN_LEN: usize = 4;
    let token = if let Some(t) = link.strip_prefix("https://t.me/+") {
        t
    } else if let Some(t) = link.strip_prefix("http://t.me/+") {
        t
    } else if let Some(t) = link.strip_prefix("https://t.me/joinchat/") {
        t
    } else if let Some(t) = link.strip_prefix("http://t.me/joinchat/") {
        t
    } else {
        return false;
    };
    token.len() >= MIN_TOKEN_LEN
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}
