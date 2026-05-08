//! `backfill` subcommand. Filled in Phase 9.

/// Arguments for `tg-extract backfill`.
#[derive(clap::Args, Debug)]
pub struct BackfillArgs {
    /// Chat reference (@username, chat_id, or "title-substring")
    #[arg(long)]
    pub chat: Option<String>,
}

/// Run the `backfill` subcommand. Filled in Phase 9.
pub async fn run(_cfg: &crate::config::AppConfig, _secrets: &crate::config::Secrets, _args: &BackfillArgs) -> anyhow::Result<()> {
    unimplemented!("Phase 9")
}
