use anyhow::Result;
use clap::Parser;
use telegram_client::cmd::Cli;
use telegram_client::config;
use telegram_client::observability;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::load(&cli.config)?;
    let _guard = observability::init(
        &cfg.log.level,
        &cfg.log.format,
        cfg.log.file.as_deref().map(std::path::Path::new),
        &cfg.log.rotation,
    );
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "tg-extract starting");

    // Dispatch lands in Task 2.6.
    let _ = cli;
    let _ = cfg;
    Ok(())
}
