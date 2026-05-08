//! `chats` subcommand: list dialogs (helps user find chat_id for config).

use crate::config::{AppConfig, Secrets};
use crate::telegram::client::GrammersClient;
use crate::telegram::{Dialog, DialogKind, TelegramClient};
use anyhow::Result;

/// Production entry point: connect via grammers, then delegate to
/// `chats_with_client`. Spec §7.3.
pub async fn run(cfg: &AppConfig, secrets: &Secrets, filter: Option<&str>) -> Result<()> {
    let client = GrammersClient::connect(
        secrets.api_id,
        &secrets.api_hash,
        std::path::Path::new(&cfg.telegram.session_path),
    )
    .await?;
    chats_with_client(&client, filter).await
}

/// Generic helper used by both the production `run` (with `GrammersClient`)
/// and the unit test (`MockClient`) — keeps the rendering pure and
/// testable. Spec §9.2 requires CI tests run without live Telegram.
///
/// `connect_and_warm` internally calls `save_session()` to persist the
/// refreshed access_hash cache — that is the only persistence point for
/// this subcommand. The client is dropped at function exit.
pub async fn chats_with_client<C: TelegramClient>(
    client: &C,
    filter: Option<&str>,
) -> Result<()> {
    client.connect_and_warm().await?;
    let dialogs = client.iter_dialogs().await?;
    print!("{}", format_dialogs(&dialogs, filter));
    Ok(())
}

/// Pure formatter — testable without a client. Returns an empty-state hint
/// when `filter` is `None` (run `auth` first) or a `0 dialogs match …` line
/// when the filter excluded everything.
pub fn format_dialogs(dialogs: &[Dialog], filter: Option<&str>) -> String {
    let needle = filter.map(|s| s.to_ascii_lowercase());
    let filtered: Vec<&Dialog> = dialogs
        .iter()
        .filter(|d| match &needle {
            None => true,
            Some(n) => {
                d.title.to_ascii_lowercase().contains(n)
                    || d.username
                        .as_deref()
                        .map(|u| u.to_ascii_lowercase().contains(n))
                        .unwrap_or(false)
            }
        })
        .collect();

    if filtered.is_empty() {
        return match filter {
            Some(f) => format!("(0 dialogs match {f:?})\n"),
            None => "No dialogs found. Run `tg-extract auth` first.\n".to_string(),
        };
    }

    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<8} {:<18} {:<40} username",
        "kind", "chat_id", "title"
    );
    let _ = writeln!(out, "{}", "-".repeat(80));
    for d in &filtered {
        let kind_str = match d.kind {
            DialogKind::User => "user",
            DialogKind::Group => "group",
            DialogKind::Channel => "channel",
        };
        let user = d
            .username
            .as_deref()
            .map(|u| format!("@{u}"))
            .unwrap_or_default();
        let _ = writeln!(
            out,
            "{:<8} {:<18} {:<40} {}",
            kind_str, d.chat_id, d.title, user
        );
    }
    let _ = writeln!(out, "\n{} dialogs", filtered.len());
    out
}
