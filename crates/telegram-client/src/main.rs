use anyhow::Result;
use clap::Parser;
use telegram_client::cmd::{Cli, Cmd};
use telegram_client::config;
use telegram_client::observability;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::load(&cli.config)?;
    let secrets = config::load_secrets()?;
    let _guard = observability::init(
        &cfg.log.level,
        &cfg.log.format,
        cfg.log.file.as_deref().map(std::path::Path::new),
        &cfg.log.rotation,
    );
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        cmd = ?std::mem::discriminant(&cli.cmd),
        "tg-extract starting"
    );

    match cli.cmd {
        Cmd::Auth(args)            => telegram_client::cmd::auth::run(&cfg, &secrets, &args).await,
        Cmd::Join { invite_link }  => telegram_client::cmd::join::run(&cfg, &secrets, &invite_link).await,
        Cmd::Chats { filter }      => telegram_client::cmd::chats::run(&cfg, &secrets, filter.as_deref()).await,
        Cmd::Fetch(args)           => telegram_client::cmd::fetch::run(&cfg, &secrets, &args).await,
        Cmd::Watch(args)           => telegram_client::cmd::watch::run(&cfg, &secrets, &args).await,
        Cmd::Backfill(args)        => telegram_client::cmd::backfill::run(&cfg, &secrets, &args).await,
        Cmd::RetryUploads          => telegram_client::cmd::retry_uploads::run(&cfg, &secrets).await,
        Cmd::Stats                 => telegram_client::cmd::stats::run(&cfg).await,
    }
}
