//! `watch` subcommand. Phase 8: subscribe to grammers updates for the
//! configured `[[watch.channel]]` chats, dedup via [`Store`], dispatch each
//! discovered document message through the existing single-message
//! [`crate::cmd::fetch::run_with_store_and_client`] pipeline, and persist a
//! per-chat `last_msg_id` cursor for restart safety.
//!
//! Sequencing: this v1 implementation is single-file in flight at a time
//! per the chunk-level Scope note. The full §4.2 inter-file pipeline lands
//! in Phase 10.
//!
//! Per-channel `[extract]` overrides (spec §7.1) are accepted by the loader
//! but ignored at runtime in v1 — the active matcher is always
//! `cfg.extract.{mode,key}`. Phase 10 may wire it through.

use anyhow::{bail, Context, Result};

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

    /// Permit uploading to a public destination chat (forwarded into the
    /// per-message `FetchArgs`, which gates on this in `resolve_output_chat`
    /// per spec §11.2).
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

/// Generic watch implementation. Caller supplies the [`TelegramClient`] and
/// the [`Store`]; used both by [`run`] and by `tests/cmd_watch.rs`.
///
/// Flow:
/// 1. Warm the client (no-op for `MockClient`; warms dialogs for the real client).
/// 2. Resolve every `[[watch.channel]]` entry to a numeric chat id (via
///    `client.resolve_chat` for the username variant).
/// 3. Open the live update stream for those chat ids.
/// 4. Loop: select on the next update, Ctrl-C, or the optional
///    `--duration-seconds` deadline. On stream close, exit Ok(()).
/// 5. Per message: synthesize a `FetchArgs { chat, msg_id, no_upload=false,
///    confirm_public }` and call
///    [`crate::cmd::fetch::run_with_store_and_client`]. On Ok (including
///    dedup short-circuit `AlreadyDone`), advance
///    `update_watch_cursor(chat_id, &title, i64::from(msg_id))`. On Err,
///    log at `tracing::error!` and continue — never bail.
pub async fn run_with_store_and_client<C: TelegramClient>(
    cfg: &AppConfig,
    args: &WatchArgs,
    client: &C,
    store: &Store,
) -> Result<()> {
    client
        .connect_and_warm()
        .await
        .context("connect_and_warm")?;

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

    // 2. Open the live update stream.
    let mut updates = client
        .subscribe_updates(&chat_ids)
        .await
        .context("subscribe_updates")?;

    // 3. Loop with optional time bound + Ctrl-C escape hatch.
    let deadline = args
        .duration_seconds
        .map(|s| tokio::time::Instant::now() + std::time::Duration::from_secs(s));
    loop {
        let info = tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("watch: Ctrl-C received, shutting down");
                return Ok(());
            }
            () = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None    => std::future::pending::<()>().await, // never
                }
            } => {
                tracing::info!("watch: --duration-seconds elapsed, exiting");
                return Ok(());
            }
            opt = updates.recv() => match opt {
                Some(info) => info,
                None => {
                    tracing::warn!("watch: update stream closed by peer; exiting");
                    return Ok(());
                }
            },
        };

        // 4. Dispatch one message through cmd::fetch::run_with_store_and_client.
        let synth = crate::cmd::fetch::FetchArgs {
            link: None,
            chat: Some(info.chat_id),
            msg_id: Some(info.msg_id),
            no_upload: false,
            confirm_public: args.confirm_public,
        };
        let chat_id = info.chat_id;
        let msg_id = info.msg_id;
        match crate::cmd::fetch::run_with_store_and_client(cfg, &synth, client, Some(store)).await {
            Ok(()) => {
                // 5. Cursor advances on every observed message — including
                //    dedup hits (AlreadyDone returns Ok). Title is best-effort:
                //    we don't have it on `MessageInfo`, so reuse the
                //    configured chat id; `update_watch_cursor` requires a
                //    title. Fall back to the numeric id stringified — Phase 10
                //    can pull a real title via `iter_dialogs`.
                let title = format!("chat:{chat_id}");
                if let Err(e) = store.update_watch_cursor(chat_id, &title, i64::from(msg_id)) {
                    tracing::error!(?e, chat_id, msg_id, "watch: failed to advance cursor");
                }
            }
            Err(e) => {
                // Per-message failures are logged and skipped; the daemon
                // does NOT exit on a single bad file. The row's status in
                // the store reflects partial progress, and a restart's
                // `reset_in_flight` (Task 7.3) will retry.
                tracing::error!(
                    ?e,
                    chat_id,
                    msg_id,
                    "watch: per-message processing failed, continuing",
                );
            }
        }
    }
}
