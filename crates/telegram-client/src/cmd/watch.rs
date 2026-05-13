//! `watch` subcommand. Phase 10: drive `pipeline::interfile::run` (one
//! orchestrator invocation) instead of dispatching per-message through
//! `cmd::fetch::run_with_store_and_client`. This:
//! - Lifts the §4.2 deferral.
//! - Records `dead_letter` rows on Failed outcomes.
//! - Advances the cursor on EVERY outcome (including Failed) so a poison
//!   pill doesn't make the daemon loop forever.
//! - Preserves §6.4 gap-fill semantics: between cursor's last write and
//!   process restart, missed history is walked oldest-first into the
//!   pipeline before the live update stream takes over.
//!
//! Per-channel `[extract]` overrides (spec §7.1) are accepted by the loader
//! but ignored at runtime in v1 — the active matcher is always
//! `cfg.extract.{mode,key}`.

use anyhow::{anyhow, bail, Context, Result};

use crate::config::{AppConfig, Secrets};
use crate::store::Store;
use crate::telegram::{ChatRef, TelegramClient};

/// Arguments for `tg-extract watch`.
#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    /// Maximum wall-clock seconds to run before exiting cleanly.
    /// Useful for smoke tests, CI, and time-bounded scrapes. None = run
    /// until Ctrl-C or stream closure.
    #[arg(long)]
    pub duration_seconds: Option<u64>,

    /// Permit uploading to a public destination chat. Forwarded into the
    /// once-at-startup output-chat resolver, which gates on this per
    /// spec §11.2.
    #[arg(long, default_value_t = false)]
    pub confirm_public: bool,
}

/// Stand-alone entry point used by callers that don't already hold a
/// [`Store`]. The production binary path in `main.rs` does **not** go
/// through this function — `main.rs` opens the `Store` once at startup,
/// runs `reset_in_flight`, and dispatches directly into
/// [`run_with_store_and_client`]. This wrapper is kept for parity with
/// sibling subcommands' `run`/`run_with_store_and_client` pair and for
/// future callers (e.g. embedding tests). It opens its own `Store` and
/// performs `reset_in_flight` so it is safe to call standalone.
pub async fn run(cfg: &AppConfig, secrets: &Secrets, args: &WatchArgs) -> Result<()> {
    let client = crate::telegram::client::GrammersClient::connect(
        secrets.api_id,
        &secrets.api_hash,
        std::path::Path::new(&cfg.telegram.session_path),
    )
    .await
    .context("GrammersClient::connect")?;
    let store_path = std::path::Path::new(&cfg.pipeline.work_dir).join("state.db");
    let store = Store::open(&store_path)
        .with_context(|| format!("open store {}", store_path.display()))?;
    store
        .reset_in_flight()
        .context("reset_in_flight on standalone watch::run")?;
    run_with_store_and_client(cfg, args, &client, &store).await
}

/// Build a [`crate::pipeline::interfile::PipelineConfig`] from the loaded
/// [`AppConfig`] plus a resolved `target_chat_id`. Default outcomes-channel
/// capacity is 2 (spec §4.2).
///
/// `progress` is `None` to keep this helper environment-agnostic; the
/// subcommand entry points (`run_with_store_and_client` for watch and
/// backfill) overwrite the field with a TTY-conditional `MultiProgress`
/// before passing the cfg into the orchestrator.
pub(crate) fn pipeline_config_from_app(
    cfg: &AppConfig,
    target_chat_id: i64,
) -> crate::pipeline::interfile::PipelineConfig {
    const OUTCOMES_CHANNEL_CAPACITY_DEFAULT: usize = 2;
    crate::pipeline::interfile::PipelineConfig {
        matcher_key:                 cfg.extract.key.clone(),
        matcher_mode: match cfg.extract.mode {
            crate::config::ExtractMode::Plain => "plain".into(),
            crate::config::ExtractMode::Url   => "url".into(),
        },
        output_dir:                  std::path::PathBuf::from(&cfg.pipeline.output_dir),
        max_line_bytes:              cfg.pipeline.max_line_bytes,
        max_uncompressed_bytes:      cfg.pipeline.max_uncompressed_bytes,
        intra_file_channel_capacity: cfg.pipeline.intra_file_channel_capacity,
        inter_file_channel_capacity: cfg.pipeline.inter_file_channel_capacity,
        upload_channel_capacity:     cfg.pipeline.upload_channel_capacity,
        outcomes_channel_capacity:   OUTCOMES_CHANNEL_CAPACITY_DEFAULT,
        upload_max_size_bytes:       cfg.pipeline.upload_max_size_bytes,
        upload_rate_seconds:         cfg.pipeline.upload_rate_seconds,
        target_chat_id,
        progress:                    None,
    }
}

/// Construct a `MultiProgress` only when stderr is a TTY. CI / cron /
/// daemon environments get `None` so progress bars do not pollute logs.
/// Spec §10.3.
pub(crate) fn make_progress_if_tty() -> Option<std::sync::Arc<indicatif::MultiProgress>> {
    use std::io::IsTerminal as _;
    if std::io::stderr().is_terminal() {
        Some(std::sync::Arc::new(indicatif::MultiProgress::new()))
    } else {
        None
    }
}

/// Best-effort format classification by filename suffix. Returned tag
/// matches the `format` column of `dead_letter` rows.
pub(crate) fn classify_format(info: &crate::telegram::MessageInfo) -> &'static str {
    let lower = info.original_name.to_ascii_lowercase();
    if lower.ends_with(".txt") {
        "txt"
    } else if lower.ends_with(".gz") {
        "gz"
    } else if lower.ends_with(".zip") {
        "zip"
    } else {
        "unknown"
    }
}

/// Best-effort stage classification by error chain. Returned tag matches
/// the `stage` column of `dead_letter` rows.
pub(crate) fn classify_stage(err: &anyhow::Error) -> &'static str {
    let s = format!("{err:#}").to_ascii_lowercase();
    if s.contains("download") || s.contains("transport") {
        "download"
    } else if s.contains("upload") || s.contains("flood") {
        "upload"
    } else {
        "extract"
    }
}

/// Render an anyhow error chain as a single line for `dead_letter.error`.
pub(crate) fn one_line(err: &anyhow::Error) -> String {
    format!("{err:#}").replace('\n', " | ")
}

/// Generic watch implementation. Caller supplies the [`TelegramClient`] and
/// the [`Store`]; used both by [`run`] and by `tests/cmd_watch.rs`.
///
/// Flow:
/// 1. Warm the client.
/// 2. Resolve every `[[watch.channel]]` entry to a numeric chat id.
/// 3. Resolve the output chat once at startup (spec §11.2 public-chat gate).
/// 4. Spawn the inter-file orchestrator; jobs come from `(jobs_tx, jobs_rx)`.
/// 5. Gap-fill: per chat, walk history newest→oldest until cursor, then
///    push oldest-first onto `jobs_tx`.
/// 6. Drive the live update stream via `subscribe_with_reconnect`; each
///    message info becomes a `Job` pushed onto `jobs_tx`.
/// 7. After the feeder returns, drop `jobs_tx` so the orchestrator drains.
/// 8. Await the orchestrator handle.
pub async fn run_with_store_and_client<C: TelegramClient>(
    cfg:    &AppConfig,
    args:   &WatchArgs,
    client: &C,
    store:  &Store,
) -> Result<()> {
    use crate::pipeline::interfile::{self, CursorAdvance, Job, JobOutcome, OutcomeKind};

    let t_warm = std::time::Instant::now();
    client
        .connect_and_warm()
        .await
        .context("connect_and_warm")?;
    tracing::info!(
        elapsed_ms = t_warm.elapsed().as_millis() as u64,
        "watch: client connected and warmed",
    );

    // 1. Resolve the configured chat list to numeric chat ids.
    if cfg.watch.channels.is_empty() {
        bail!("watch: no [[watch.channel]] entries configured");
    }
    let mut chat_ids: Vec<i64> = Vec::with_capacity(cfg.watch.channels.len());
    for ch in &cfg.watch.channels {
        let id = match (ch.chat_id, ch.chat.as_deref()) {
            (Some(id), _) => id,
            (None, Some(name)) => {
                let r = if let Some(stripped) = name.strip_prefix('@') {
                    ChatRef::Username(stripped.to_string())
                } else if let Ok(n) = name.parse::<i64>() {
                    ChatRef::ChatId(n)
                } else {
                    ChatRef::Username(name.to_string())
                };
                client
                    .resolve_chat(&r)
                    .await
                    .with_context(|| format!("watch: resolve {name:?}"))?
            }
            (None, None) => bail!("watch.channel: must set chat or chat_id"),
        };
        chat_ids.push(id);
    }

    // 2. Resolve output chat ONCE at startup; bail before any download if
    //    the public-chat gate trips (spec §11.2).
    let target_chat_id = crate::cmd::fetch::resolve_output_chat_for_watch(
        cfg,
        args.confirm_public,
        client,
    )
    .await
    .context("watch: resolve output chat")?
    .ok_or_else(|| anyhow!("watch: telegram.output.{{chat,chat_id}} unset"))?;

    // 3. Build PipelineConfig and the (jobs_tx, jobs_rx) channel.
    //    TTY check at this layer: if stderr is a terminal we surface a
    //    `MultiProgress` for download/upload bars; in CI/cron/daemon mode
    //    bars are suppressed at the source so logs stay clean (spec §10.3).
    let mut pcfg = pipeline_config_from_app(cfg, target_chat_id);
    pcfg.progress = make_progress_if_tty();
    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(2);

    tracing::info!(
        watch_channels = chat_ids.len(),
        ?chat_ids,
        target_chat_id,
        mode = %pcfg.matcher_mode,
        domain = %pcfg.matcher_key,
        download_concurrent_chunks = cfg.telegram.download_concurrent_chunks,
        inter_file_cap = pcfg.inter_file_channel_capacity,
        upload_cap = pcfg.upload_channel_capacity,
        upload_rate_seconds = pcfg.upload_rate_seconds,
        "watch: pipeline configured, starting orchestrator + feeder",
    );

    // 4. CursorAdvance callback: persist watch_cursor on every outcome,
    //    record dead_letter on Failed. Advances the cursor past Failed
    //    so the daemon doesn't loop on a poison message.
    let store_arc: std::sync::Arc<Store> = std::sync::Arc::new(store.clone_handle());
    let cb_store = store_arc.clone();
    let advance: CursorAdvance = std::sync::Arc::new(move |o: JobOutcome| {
        let chat_id = o.job.source_chat_id;
        let msg_id  = o.job.source_msg_id;
        match &o.kind {
            OutcomeKind::Uploaded { sha256, output_msg_ids } => {
                // files.output_msg_id holds the head part — same convention as
                // cmd::fetch (see fetch.rs:532-545 for the multi-part rationale).
                let head = output_msg_ids.first().copied().unwrap_or_else(|| {
                    tracing::error!(
                        %sha256,
                        "Uploaded outcome had empty output_msg_ids; recording 0",
                    );
                    0
                });
                if let Err(e) = cb_store.mark_uploaded(sha256, head) {
                    tracing::error!(?e, %sha256, "watch: mark_uploaded failed");
                }
                let title = format!("chat:{chat_id}");
                if let Err(e) =
                    cb_store.update_watch_cursor(chat_id, &title, i64::from(msg_id))
                {
                    tracing::error!(
                        ?e,
                        chat_id,
                        msg_id,
                        "watch: failed to advance cursor",
                    );
                }
            }
            OutcomeKind::Deduped { .. } => {
                // Dedup means the prior run already wrote files.status='done';
                // no mark_uploaded needed.
                let title = format!("chat:{chat_id}");
                if let Err(e) =
                    cb_store.update_watch_cursor(chat_id, &title, i64::from(msg_id))
                {
                    tracing::error!(
                        ?e,
                        chat_id,
                        msg_id,
                        "watch: failed to advance cursor",
                    );
                }
            }
            OutcomeKind::Failed { error } => {
                // Dead-letter the failed message, then advance the cursor
                // past it so the daemon doesn't loop on this poison pill.
                let err = anyhow::anyhow!(error.clone());
                if let Err(e) = cb_store.record_dead_letter(
                    chat_id,
                    msg_id,
                    None,
                    &o.job.info.original_name,
                    o.job.info.size_bytes,
                    classify_format(&o.job.info),
                    classify_stage(&err),
                    &one_line(&err),
                ) {
                    tracing::error!(
                        ?e,
                        chat_id,
                        msg_id,
                        "watch: failed to record dead_letter",
                    );
                }
                let title = format!("chat:{chat_id}");
                if let Err(e) =
                    cb_store.update_watch_cursor(chat_id, &title, i64::from(msg_id))
                {
                    tracing::error!(
                        ?e,
                        chat_id,
                        msg_id,
                        "watch: failed to advance cursor past dead-letter",
                    );
                }
            }
        }
    });

    // 5. Run the orchestrator concurrently with the feeder. The
    //    orchestrator borrows `&C` and `Option<&Store>`, so we use
    //    `tokio::join!` instead of `tokio::spawn` to avoid 'static bounds.
    let pipeline_fut = async {
        interfile::run(client, Some(store_arc.as_ref()), &pcfg, jobs_rx, advance).await
    };

    let feed_fut = async {
        // 5a. Gap-fill per chat — newest→oldest until cursor, then push
        //     oldest-first into jobs_tx (spec §6.4). Without this, messages
        //     posted between the last cursor write and process restart would
        //     be missed by `subscribe_updates` alone.
        for &chat_id in &chat_ids {
            let cursor = store
                .watch_cursor(chat_id)
                .with_context(|| format!("watch_cursor chat={chat_id}"))?
                .unwrap_or(0);
            let t_gap = std::time::Instant::now();
            tracing::info!(chat_id, cursor, "watch: gap-fill starting");
            let page_size = cfg.backfill.page_size.max(1);
            let mut next_max: Option<i32> = None;
            let mut stack: Vec<crate::telegram::MessageInfo> = Vec::new();
            let mut pages_fetched: u32 = 0;
            loop {
                let page = client
                    .iter_history(chat_id, next_max, page_size)
                    .await
                    .with_context(|| {
                        format!("gap-fill iter_history chat={chat_id} max={next_max:?}")
                    })?;
                pages_fetched += 1;
                if page.is_empty() {
                    break;
                }
                let mut crossed = false;
                for info in &page {
                    if i64::from(info.msg_id) <= cursor {
                        crossed = true;
                        break;
                    }
                    stack.push(info.clone());
                }
                if crossed {
                    break;
                }
                next_max = page.last().map(|m| m.msg_id);
            }
            let enqueued = stack.len();
            tracing::info!(
                chat_id,
                cursor,
                pages_fetched,
                enqueued,
                elapsed_ms = t_gap.elapsed().as_millis() as u64,
                "watch: gap-fill discovered, draining into pipeline",
            );
            stack.reverse();
            for info in stack {
                let job = Job {
                    source_chat_id: info.chat_id,
                    source_msg_id:  info.msg_id,
                    info,
                };
                if jobs_tx.send(job).await.is_err() {
                    // Orchestrator died; cooperate by exiting cleanly.
                    return Ok::<(), anyhow::Error>(());
                }
            }
        }
        tracing::info!("watch: gap-fill complete for all channels, entering live mode");

        // 5b. Live updates with reconnect. Each scripted/live message becomes
        //     a Job pushed onto jobs_tx. On send error (orchestrator hung
        //     up) return Err to terminate the reconnect loop.
        let deadline = args
            .duration_seconds
            .map(|s| tokio::time::Instant::now() + std::time::Duration::from_secs(s));
        let jobs_tx_for_live = jobs_tx.clone();
        subscribe_with_reconnect(client, &chat_ids, deadline, |info| {
            let tx = jobs_tx_for_live.clone();
            async move {
                tracing::info!(
                    chat_id = info.chat_id,
                    msg_id = info.msg_id,
                    name = %info.original_name,
                    size_bytes = info.size_bytes,
                    "watch: live message received, enqueueing job",
                );
                let job = Job {
                    source_chat_id: info.chat_id,
                    source_msg_id:  info.msg_id,
                    info,
                };
                tx.send(job)
                    .await
                    .map_err(|_| anyhow!("watch: pipeline orchestrator closed jobs_rx"))
            }
        })
        .await?;

        // 5c. Drop the feeder's sender so the orchestrator drains.
        drop(jobs_tx);
        Ok(())
    };

    let (feed_res, run_res) = tokio::join!(feed_fut, pipeline_fut);
    feed_res?;
    run_res
}

/// Drive `client.subscribe_updates(chat_ids)` with reconnect-on-closure.
/// `on_message` is called per `MessageInfo` and may return `Err` to
/// terminate (e.g., orchestrator hung up). `deadline` is the wall-clock
/// budget from `--duration-seconds`; `None` means run forever.
///
/// Backoff schedule: 1 s, 2 s, 4 s, 8 s, 16 s, 30 s, 30 s, … capped.
/// Reset to 1 s after every successful subscribe. Ctrl-C aborts via
/// `tokio::signal::ctrl_c` (biased select).
pub(crate) async fn subscribe_with_reconnect<C, F, Fut>(
    client: &C,
    chat_ids: &[i64],
    deadline: Option<tokio::time::Instant>,
    mut on_message: F,
) -> Result<()>
where
    C: TelegramClient,
    F: FnMut(crate::telegram::MessageInfo) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut backoff_ms: u64 = 1_000;
    loop {
        // Honor deadline + Ctrl-C before subscribing.
        if let Some(d) = deadline {
            if tokio::time::Instant::now() >= d {
                tracing::info!("watch: --duration-seconds elapsed, exiting");
                return Ok(());
            }
        }

        let mut rx = match client.subscribe_updates(chat_ids).await {
            Ok(rx) => rx,
            Err(e) => {
                tracing::warn!(
                    ?e,
                    backoff_ms,
                    "watch: subscribe_updates failed, backing off"
                );
                if !sleep_with_deadline(backoff_ms, deadline).await {
                    return Ok(());
                }
                backoff_ms = (backoff_ms * 2).min(30_000);
                continue;
            }
        };
        // Successful subscribe → reset backoff.
        backoff_ms = 1_000;

        loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("watch: Ctrl-C received, shutting down");
                    return Ok(());
                }
                _ = async {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None    => std::future::pending::<()>().await,
                    }
                } => {
                    tracing::info!("watch: --duration-seconds elapsed, exiting");
                    return Ok(());
                }
                opt = rx.recv() => match opt {
                    Some(info) => {
                        on_message(info).await?;
                    }
                    None => {
                        tracing::warn!("watch: update stream closed by peer, will reconnect");
                        break; // inner loop → re-subscribe
                    }
                }
            }
        }
    }
}

/// Sleep for `ms` milliseconds, honoring `deadline` and Ctrl-C. Returns
/// `false` if the deadline elapsed or Ctrl-C fired (caller exits).
async fn sleep_with_deadline(ms: u64, deadline: Option<tokio::time::Instant>) -> bool {
    let until = tokio::time::Instant::now() + std::time::Duration::from_millis(ms);
    let final_until = match deadline {
        Some(d) if d < until => d,
        _ => until,
    };
    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c()                   => false,
        _ = tokio::time::sleep_until(final_until)     => {
            // If the chopped sleep was the deadline, signal exit.
            !matches!(deadline, Some(d) if final_until == d)
        }
    }
}
