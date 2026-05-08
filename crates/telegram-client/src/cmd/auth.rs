//! `auth` subcommand: interactive phone → code → 2FA → save session.
//!
//! Concurrency notes:
//! - All stdin prompts run inside `tokio::task::spawn_blocking` so they do
//!   NOT pin a tokio worker thread for the duration of the user's typing.
//! - The 2FA password is read via `rpassword::prompt_password` so it does
//!   not echo to the terminal nor land in shell history.
//! - Each grammers network call is wrapped in `tokio::time::timeout` so a
//!   wrong-code-typed-three-times scenario fails loud instead of hanging
//!   the runtime indefinitely.

use crate::config::{AppConfig, Secrets};
use crate::telegram::client::GrammersClient;
use anyhow::{anyhow, Context, Result};
use std::path::PathBuf;
use std::time::Duration;

/// Per-call network timeout (each grammers RPC has its own ceiling).
const AUTH_RPC_TIMEOUT: Duration = Duration::from_secs(120);

/// Arguments for `tg-extract auth`.
#[derive(clap::Args, Debug)]
pub struct AuthArgs {
    /// Override session output path (default: `telegram.session_path` from config).
    #[arg(long)]
    pub session: Option<PathBuf>,
}

/// Run the `auth` subcommand: connect, prompt for credentials interactively,
/// complete grammers sign-in (with optional 2FA), and persist the session
/// file. If the session is already authorized, returns immediately without
/// re-prompting.
pub async fn run(cfg: &AppConfig, secrets: &Secrets, args: &AuthArgs) -> Result<()> {
    let session_path = args
        .session
        .clone()
        .unwrap_or_else(|| PathBuf::from(&cfg.telegram.session_path));

    if let Some(parent) = session_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("mkdir {}", parent.display()))?;
        }
    }

    let client = tokio::time::timeout(
        AUTH_RPC_TIMEOUT,
        GrammersClient::connect(secrets.api_id, &secrets.api_hash, &session_path),
    )
    .await
    .map_err(|_| anyhow!("connect to Telegram timed out after {:?}", AUTH_RPC_TIMEOUT))??;

    let already = tokio::time::timeout(AUTH_RPC_TIMEOUT, client.client.is_authorized())
        .await
        .map_err(|_| anyhow!("is_authorized timed out"))?
        .context("is_authorized")?;
    if already {
        println!("Already authorized — session at {}", session_path.display());
        return Ok(());
    }

    let phone = prompt("Phone number (international, e.g. +1234567890): ").await?;
    println!("Sending code to {phone}…");
    let code = prompt("Code received: ").await?;
    let password = prompt_password_optional("2FA password (blank if not enabled): ").await?;
    let pwd_opt = password.as_deref().filter(|s| !s.trim().is_empty());

    tokio::time::timeout(
        AUTH_RPC_TIMEOUT,
        client.sign_in_with_code(&phone, &code, pwd_opt),
    )
    .await
    .map_err(|_| anyhow!("sign_in timed out — wrong code or network issue"))??;
    client.save_session()?;

    let me = tokio::time::timeout(AUTH_RPC_TIMEOUT, client.client.get_me())
        .await
        .map_err(|_| anyhow!("get_me timed out"))?
        .context("get_me")?;
    let user_name = me.full_name();
    let user_id = me.id();
    println!(
        "Logged in as {user_name} (id={user_id}). Session saved to {}",
        session_path.display()
    );

    tracing::info!(
        session_path = %session_path.display(),
        user_id,
        "auth complete"
    );
    Ok(())
}

/// Read a line from stdin without blocking the tokio reactor. Wraps the
/// blocking `std::io::stdin().lock().read_line()` in `spawn_blocking`.
async fn prompt(label: &str) -> Result<String> {
    let label = label.to_string();
    tokio::task::spawn_blocking(move || -> Result<String> {
        use std::io::{BufRead, Write};
        print!("{label}");
        std::io::stdout().flush().ok();
        let mut line = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut line)
            .context("stdin")?;
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    })
    .await
    .context("spawn_blocking prompt")?
}

/// Read a password without echoing to the terminal. `rpassword` is pure
/// Rust on all supported platforms (Unix tcsetattr / Windows ReadConsoleW).
/// Empty input is returned as `None`.
async fn prompt_password_optional(label: &str) -> Result<Option<String>> {
    let label = label.to_string();
    tokio::task::spawn_blocking(move || -> Result<Option<String>> {
        let pwd = rpassword::prompt_password(&label).context("rpassword")?;
        Ok(if pwd.trim().is_empty() {
            None
        } else {
            Some(pwd)
        })
    })
    .await
    .context("spawn_blocking password prompt")?
}
