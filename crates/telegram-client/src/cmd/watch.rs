//! `watch` subcommand. Filled in Phase 8.

/// Arguments for `tg-extract watch`.
#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    /// Chat reference (@username, chat_id, or "title-substring")
    #[arg(long)]
    pub chat: Option<String>,
}

/// Run the `watch` subcommand. Filled in Phase 8.
pub async fn run(_cfg: &crate::config::AppConfig, _secrets: &crate::config::Secrets, _args: &WatchArgs) -> anyhow::Result<()> {
    unimplemented!("Phase 8")
}
