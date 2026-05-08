//! `auth` subcommand. Filled in Task 3.3.

/// Arguments for `tg-extract auth`.
#[derive(clap::Args, Debug)]
pub struct AuthArgs {
    /// Override session output path
    #[arg(long)]
    pub session: Option<std::path::PathBuf>,
}

/// Run the `auth` subcommand. Filled in Task 3.3.
pub async fn run(_cfg: &crate::config::AppConfig, _secrets: &crate::config::Secrets, _args: &AuthArgs) -> anyhow::Result<()> {
    unimplemented!("Task 3.3")
}
