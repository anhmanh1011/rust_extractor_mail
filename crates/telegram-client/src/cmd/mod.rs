//! CLI surface — clap parser + dispatch. Filled in Task 2.2.

use clap::Parser;
use std::path::PathBuf;

pub mod auth;
pub mod join;
pub mod chats;
pub mod fetch;
pub mod watch;
pub mod backfill;
pub mod retry_uploads;
pub mod stats;

/// Top-level CLI. Subcommand bodies are filled in Phase 3-9.
#[derive(Parser, Debug)]
#[command(name = "tg-extract", version, about)]
pub struct Cli {
    /// Path to config TOML. Overridable via $RUST_TG_CONFIG.
    #[arg(short, long, env = "RUST_TG_CONFIG", default_value = "config.toml")]
    pub config: PathBuf,

    /// Override extract.key (e.g. "gmail.com")
    #[arg(short = 'k', long)]
    pub key: Option<String>,

    /// Override extract.mode (validated by clap against the enum variants).
    #[arg(long, value_enum)]
    pub mode: Option<crate::config::ExtractMode>,

    /// The subcommand to run.
    #[command(subcommand)]
    pub cmd: Cmd,
}

/// Top-level subcommand variants. Bodies live in their respective modules.
#[derive(clap::Subcommand, Debug)]
pub enum Cmd {
    /// Interactive login: phone → code → save session
    Auth(auth::AuthArgs),
    /// Accept a t.me invite link to a private channel
    Join {
        /// The t.me/joinchat/... invite link.
        invite_link: String,
    },
    /// List dialogs (find chat_id for config)
    Chats {
        /// Filter by case-insensitive substring of title or username
        #[arg(long)]
        filter: Option<String>,
    },
    /// Fetch a single message by t.me link or chat+msg_id
    Fetch(fetch::FetchArgs),
    /// Watch one or more channels for new messages
    Watch(watch::WatchArgs),
    /// Backfill historical messages from a channel
    Backfill(backfill::BackfillArgs),
    /// Re-attempt previously failed uploads
    RetryUploads,
    /// Print aggregate stats from the SQLite store
    Stats,
}
