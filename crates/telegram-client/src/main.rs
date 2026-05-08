use anyhow::{Context, Result};
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

    // Phase 7: open the SQLite state Store once at startup and recover any
    // rows stuck mid-pipeline by a previous interrupted run. `reset_in_flight`
    // returns transient states ('downloading'/'extracting') back to 'queued'
    // so the next subcommand can re-process them from scratch.
    let store = telegram_client::store::Store::open(
        &std::path::Path::new(&cfg.pipeline.work_dir).join("state.db"),
    )
    .context("open Store")?;
    let reset = store.reset_in_flight().context("reset_in_flight")?;
    if reset > 0 {
        tracing::info!(reset, "recovered: returned in-flight rows to 'queued'");
    }

    match cli.cmd {
        Cmd::Auth(args)            => telegram_client::cmd::auth::run(&cfg, &secrets, &args).await,
        Cmd::Join { invite_link }  => telegram_client::cmd::join::run(&cfg, &secrets, &invite_link).await,
        Cmd::Chats { filter }      => telegram_client::cmd::chats::run(&cfg, &secrets, filter.as_deref()).await,
        Cmd::Fetch(args)           => {
            let client = telegram_client::telegram::client::GrammersClient::connect(
                secrets.api_id,
                &secrets.api_hash,
                std::path::Path::new(&cfg.telegram.session_path),
            )
            .await
            .context("GrammersClient::connect")?;
            telegram_client::cmd::fetch::run_with_store_and_client(&cfg, &args, &client, Some(&store)).await
        }
        Cmd::Watch(args)           => telegram_client::cmd::watch::run(&cfg, &secrets, &args).await,
        Cmd::Backfill(args)        => telegram_client::cmd::backfill::run(&cfg, &secrets, &args).await,
        Cmd::RetryUploads          => telegram_client::cmd::retry_uploads::run(&cfg, &secrets).await,
        Cmd::Stats                 => telegram_client::cmd::stats::run(&cfg).await,
    }
}
