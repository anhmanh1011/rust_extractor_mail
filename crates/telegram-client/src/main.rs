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
            client.set_concurrent_chunks(cfg.telegram.download_concurrent_chunks);
            telegram_client::cmd::fetch::run_with_store_and_client(&cfg, &args, &client, Some(&store)).await
        }
        Cmd::Watch(args)           => {
            let client = telegram_client::telegram::client::GrammersClient::connect(
                secrets.api_id,
                &secrets.api_hash,
                std::path::Path::new(&cfg.telegram.session_path),
            )
            .await
            .context("GrammersClient::connect")?;
            client.set_concurrent_chunks(cfg.telegram.download_concurrent_chunks);
            telegram_client::cmd::watch::run_with_store_and_client(&cfg, &args, &client, &store).await
        }
        Cmd::Backfill(args)        => telegram_client::cmd::backfill::run(&cfg, &secrets, &args).await,
        Cmd::RetryUploads          => {
            let target = match (cfg.telegram.output.chat_id, cfg.telegram.output.chat.as_deref()) {
                (Some(id), _) => id,
                (None, Some(_)) => anyhow::bail!(
                    "retry-uploads requires telegram.output.chat_id (numeric); \
                     username-only output is rejected here for safety",
                ),
                _ => anyhow::bail!("retry-uploads: telegram.output.chat_id is not configured"),
            };
            let upload_cfg = telegram_client::pipeline::upload::UploadRunConfig {
                target_chat_id:        target,
                upload_max_size_bytes: cfg.pipeline.upload_max_size_bytes,
                upload_rate_seconds:   cfg.pipeline.upload_rate_seconds,
                retry: telegram_client::pipeline::upload::RetryPolicy::default(),
            };
            let client = telegram_client::telegram::client::GrammersClient::connect(
                secrets.api_id,
                &secrets.api_hash,
                std::path::Path::new(&cfg.telegram.session_path),
            )
            .await
            .context("GrammersClient::connect")?;
            telegram_client::cmd::retry_uploads::run_with_store_and_client(&store, &client, &upload_cfg).await
        }
        Cmd::Stats                 => telegram_client::cmd::stats::run(&cfg, &store).await,
    }
}
