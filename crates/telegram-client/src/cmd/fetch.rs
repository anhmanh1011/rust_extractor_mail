//! `fetch` subcommand. Filled in Task 4.x.

/// Arguments for `tg-extract fetch`.
#[derive(clap::Args, Debug)]
pub struct FetchArgs {
    /// t.me message link (e.g. https://t.me/c/1234567890/42)
    #[arg(long, conflicts_with_all = ["chat", "msg_id"])]
    pub link: Option<String>,

    /// Chat reference (@username, chat_id, or "title-substring")
    #[arg(long, requires = "msg_id")]
    pub chat: Option<String>,

    /// Message ID
    #[arg(long, requires = "chat")]
    pub msg_id: Option<i32>,
}

/// Run the `fetch` subcommand. Filled in Task 4.x.
pub async fn run(_cfg: &crate::config::AppConfig, _secrets: &crate::config::Secrets, _args: &FetchArgs) -> anyhow::Result<()> {
    unimplemented!("Task 4.x")
}
