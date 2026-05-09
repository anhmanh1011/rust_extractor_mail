//! `backfill` subcommand. Phase 10 (Task 10.10): walk a single chat's
//! history backwards from the most recent message (or `--resume`'s
//! `backfill_cursor`) towards the channel beginning or a `--since` UTC
//! cutoff, feeding each [`crate::telegram::MessageInfo`] as a [`Job`]
//! into a single [`crate::pipeline::interfile::run`] orchestrator
//! invocation (lifts the Phase-9 "sequential per-message" deferral).
//!
//! Cursor advancement and dead-letter recording happen inside the
//! [`CursorAdvance`] callback — the cursor advances on EVERY outcome
//! (including [`OutcomeKind::Failed`]) so a poison-pill message does not
//! make `--resume` loop forever. `complete_backfill` is stamped only on
//! natural exhaustion (`iter_history` returns an empty page); a
//! `--since`-bounded or `--limit`-bounded run leaves the cursor open so a
//! later `--resume` can continue past the prior cutoff.
//!
//! [`Job`]: crate::pipeline::interfile::Job
//! [`CursorAdvance`]: crate::pipeline::interfile::CursorAdvance
//! [`OutcomeKind::Failed`]: crate::pipeline::interfile::OutcomeKind::Failed

use anyhow::{anyhow, Context, Result};

use crate::config::{AppConfig, Secrets};
use crate::store::Store;
use crate::telegram::{ChatRef, TelegramClient};

/// Arguments for `tg-extract backfill`.
#[derive(clap::Args, Debug)]
pub struct BackfillArgs {
    /// Chat reference: `"@username"`, a numeric chat id like
    /// `"-1001234567890"`, or a bare username (without `@`). Numeric strings
    /// parse as `i64` chat ids; everything else is resolved via
    /// `TelegramClient::resolve_chat`. Positional per spec §8 line 562
    /// (`<chat>` notation): `tg-extract backfill @dump --since … --limit …`.
    pub chat: String,

    /// RFC-3339 UTC cutoff. The cutoff is **exclusive**: a message dated at
    /// or before the cutoff terminates the run without being processed.
    /// Example: `--since 2024-01-01T00:00:00Z` processes messages dated
    /// strictly newer than midnight on 2024-01-01 UTC; a message dated
    /// exactly `2024-01-01T00:00:00Z` is the cutoff trigger and is itself
    /// excluded. If both `--since` and `[backfill].since` (TOML) are set,
    /// the CLI flag wins.
    #[arg(long)]
    pub since: Option<String>,

    /// Maximum number of messages to process across pages. `None` ⇒ unlimited.
    /// A truncated run leaves `backfill_state.completed_at = NULL` so a
    /// follow-up `--resume` can continue from `next_msg_id`.
    #[arg(long)]
    pub limit: Option<u32>,

    /// Resume from `backfill_cursor.next_msg_id` instead of starting at the
    /// most-recent message. Returns an error if no prior run exists for the
    /// resolved chat. If the prior row is already complete this is a no-op.
    #[arg(long, default_value_t = false)]
    pub resume: bool,

    /// Required to send extracted output into a public destination chat
    /// (mirrors `--confirm-public` on `watch`). Default: false. Spec §11.2:
    /// long-running backfills into a public chat are exactly the operator
    /// footgun the gate is designed to catch — a silent default would be
    /// indistinguishable from explicit deny.
    #[arg(long, default_value_t = false)]
    pub confirm_public: bool,
}

/// Stand-alone entry point used by callers that don't already hold a
/// [`Store`]. The production binary path in `main.rs` opens the `Store`
/// once at startup, runs `reset_in_flight`, and dispatches directly into
/// [`run_with_store_and_client`]. This wrapper mirrors the `cmd::watch`
/// pattern (own-Store + reset) so it is safe to call from tests or
/// embedding hosts that do not share `main.rs`'s Store handle.
pub async fn run(cfg: &AppConfig, secrets: &Secrets, args: &BackfillArgs) -> Result<()> {
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
        .context("reset_in_flight on standalone backfill::run")?;
    run_with_store_and_client(cfg, args, &client, &store).await
}

/// Generic backfill implementation. Caller supplies the [`TelegramClient`]
/// and the [`Store`]; used both by [`run`] and by `tests/cmd_backfill.rs`.
///
/// Flow:
/// 1. Warm the client (no-op for `MockClient`; warms dialogs for the real client).
/// 2. Resolve `args.chat` to a numeric chat id (numeric → parsed; otherwise
///    via `client.resolve_chat`). The leading `@` on usernames is stripped.
/// 3. Compute the starting `next_max: Option<i32>` from `--resume`. With
///    `--resume` this is `backfill_state.next_msg_id` (errors out if the
///    row is missing; logs and exits `Ok(())` if it is already complete).
///    Without `--resume` it is `None` (start at the most recent message).
/// 4. Compute `since_unix` from `args.since.or(cfg.backfill.since)` via
///    `chrono::DateTime::parse_from_rfc3339`.
/// 5. Resolve the destination chat ONCE (spec §11.2: bail before any
///    download if the public-chat gate trips and `--confirm-public` is
///    not set).
/// 6. Build the inter-file pipeline: a [`PipelineConfig`] from `cfg`, a
///    bounded `(jobs_tx, jobs_rx)` channel, and a [`CursorAdvance`]
///    callback that — for every outcome including `Failed` — advances
///    `backfill_state.next_msg_id` and (for `Failed`) records a
///    `dead_letter` row before advancing.
/// 7. Concurrently feed history pages newest-first onto `jobs_tx` and
///    await [`crate::pipeline::interfile::run`]. Track WHY the feed loop
///    terminated (`--since`, `--limit`, orchestrator-died, or natural
///    exhaustion) so `complete_backfill` is stamped iff exhaustion was
///    natural.
///
/// [`PipelineConfig`]: crate::pipeline::interfile::PipelineConfig
/// [`CursorAdvance`]: crate::pipeline::interfile::CursorAdvance
pub async fn run_with_store_and_client<C: TelegramClient>(
    cfg: &AppConfig,
    args: &BackfillArgs,
    client: &C,
    store: &Store,
) -> Result<()> {
    use crate::pipeline::interfile::{self, CursorAdvance, Job, JobOutcome, OutcomeKind};

    client
        .connect_and_warm()
        .await
        .context("connect_and_warm")?;

    // 1. Resolve chat reference → chat_id.
    let chat_id: i64 = if let Ok(n) = args.chat.parse::<i64>() {
        n
    } else {
        let r = if let Some(stripped) = args.chat.strip_prefix('@') {
            ChatRef::Username(stripped.to_string())
        } else {
            ChatRef::Username(args.chat.clone())
        };
        client
            .resolve_chat(&r)
            .await
            .with_context(|| format!("backfill: resolve {:?}", args.chat))?
    };

    // 2. Decide starting max_id from --resume. Done BEFORE output-chat
    //    resolution and pipeline construction so a `--resume` against an
    //    already-completed cursor returns Ok(()) without spinning up the
    //    pipeline.
    let mut max_id: Option<i32> = if args.resume {
        let st = store
            .backfill_cursor(chat_id)
            .with_context(|| format!("backfill_cursor chat={chat_id}"))?
            .ok_or_else(|| anyhow!("--resume but no prior backfill_state for chat {chat_id}"))?;
        if st.completed_at.is_some() {
            tracing::info!(
                chat_id,
                "backfill: prior run already complete, nothing to do",
            );
            return Ok(());
        }
        // `next_msg_id` is `i64` in the schema; `iter_history` wants `i32`.
        // Narrowing must be fallible per the project's "no `as` for narrowing"
        // rule.
        Some(i32::try_from(st.next_msg_id).with_context(|| {
            format!(
                "backfill: cursor next_msg_id {} overflows i32 for iter_history",
                st.next_msg_id,
            )
        })?)
    } else {
        None
    };

    // 3. Resolve --since cutoff (CLI wins over TOML).
    let since_str: Option<&str> = args.since.as_deref().or(cfg.backfill.since.as_deref());
    let since_unix: Option<i64> = match since_str {
        None => None,
        Some(s) => Some(
            chrono::DateTime::parse_from_rfc3339(s)
                .with_context(|| format!("backfill: parse --since {s:?} as RFC-3339"))?
                .timestamp(),
        ),
    };

    // 4. Resolve output chat ONCE (spec §11.2 public-chat gate). Symmetric
    //    with `cmd::watch`; a long-running backfill into a public chat is
    //    exactly the operator footgun the gate exists for.
    let target_chat_id = crate::cmd::fetch::resolve_output_chat_for_watch(
        cfg,
        args.confirm_public,
        client,
    )
    .await
    .context("backfill: resolve output chat")?
    .ok_or_else(|| anyhow!("backfill: telegram.output.{{chat,chat_id}} unset"))?;

    // 5. Build PipelineConfig and the (jobs_tx, jobs_rx) channel.
    let pcfg = crate::cmd::watch::pipeline_config_from_app(cfg, target_chat_id);
    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(2);

    // 6. CursorAdvance callback: advance_backfill on every outcome, and
    //    record_dead_letter on Failed before advancing past the poison
    //    message. The callback owns an Arc-cloned Store handle; the Arc
    //    is cheap (no SQLite-side cost) and shares the same Mutex<Connection>
    //    as the caller's `store` reference.
    let store_arc: std::sync::Arc<Store> = std::sync::Arc::new(store.clone_handle());
    let cb_store = store_arc.clone();
    let advance: CursorAdvance = std::sync::Arc::new(move |o: JobOutcome| {
        let chat_id = o.job.source_chat_id;
        let msg_id  = o.job.source_msg_id;
        let title   = format!("chat:{chat_id}");
        match &o.kind {
            OutcomeKind::Uploaded { .. } | OutcomeKind::Deduped { .. } => {
                if let Err(e) =
                    cb_store.advance_backfill(chat_id, &title, i64::from(msg_id))
                {
                    tracing::error!(
                        ?e,
                        chat_id,
                        msg_id,
                        "backfill: advance_backfill failed",
                    );
                }
            }
            OutcomeKind::Failed { error } => {
                let err = anyhow::anyhow!(error.clone());
                if let Err(e) = cb_store.record_dead_letter(
                    chat_id,
                    msg_id,
                    None,
                    &o.job.info.original_name,
                    o.job.info.size_bytes,
                    crate::cmd::watch::classify_format(&o.job.info),
                    crate::cmd::watch::classify_stage(&err),
                    &crate::cmd::watch::one_line(&err),
                ) {
                    tracing::error!(
                        ?e,
                        chat_id,
                        msg_id,
                        "backfill: record_dead_letter failed",
                    );
                }
                // Cursor MUST advance past the poison pill so a subsequent
                // --resume doesn't re-attempt it forever.
                if let Err(e) =
                    cb_store.advance_backfill(chat_id, &title, i64::from(msg_id))
                {
                    tracing::error!(
                        ?e,
                        chat_id,
                        msg_id,
                        "backfill: advance past dead-letter failed",
                    );
                }
            }
        }
    });

    // 7. Run the orchestrator concurrently with the feeder. The
    //    orchestrator borrows `&C` and `&Store`, so `tokio::join!` over
    //    async blocks (not `tokio::spawn`) avoids `'static` bounds.
    let pipeline_fut = async {
        interfile::run(client, Some(store_arc.as_ref()), &pcfg, jobs_rx, advance).await
    };

    let page_size = cfg.backfill.page_size.max(1);
    let limit_bound: u64 = args.limit.map(u64::from).unwrap_or(u64::MAX);

    // Track WHY the feed loop exits so the post-run `complete_backfill`
    // decision is precise: only natural exhaustion (next iter_history page
    // empty) marks the run complete. `--since`, `--limit`, and orchestrator
    // death all leave the cursor open for `--resume`.
    let mut total_dispatched: u64 = 0;
    let mut terminated_via_cutoff:   bool = false;
    let mut terminated_via_limit:    bool = false;
    let mut terminated_via_pipeline: bool = false;

    // Feed loop owns `jobs_tx` so it is dropped on exit, signalling EOF to
    // the orchestrator so Stage 1 can drain.
    let feed_then_close = async {
        let result: Result<()> = async {
            loop {
                let page = client
                    .iter_history(chat_id, max_id, page_size)
                    .await
                    .with_context(|| format!("iter_history chat={chat_id} max={max_id:?}"))?;
                if page.is_empty() {
                    // Natural exhaustion — no other terminated_via_* flag is set.
                    return Ok(());
                }
                let mut should_stop = false;
                for info in page {
                    if let Some(cut) = since_unix {
                        if info.date <= cut {
                            terminated_via_cutoff = true;
                            should_stop = true;
                            break;
                        }
                    }
                    if total_dispatched >= limit_bound {
                        terminated_via_limit = true;
                        should_stop = true;
                        break;
                    }

                    max_id = Some(info.msg_id);
                    let job = Job {
                        source_chat_id: info.chat_id,
                        source_msg_id:  info.msg_id,
                        info,
                    };
                    if jobs_tx.send(job).await.is_err() {
                        // Orchestrator hung up; cooperate by exiting cleanly.
                        terminated_via_pipeline = true;
                        should_stop = true;
                        break;
                    }
                    total_dispatched += 1;
                }
                if should_stop {
                    return Ok(());
                }
            }
        }
        .await;
        drop(jobs_tx);
        result
    };

    let (feed_res, run_res) = tokio::join!(feed_then_close, pipeline_fut);
    feed_res?;
    run_res?;

    // 8. Mark complete iff exhaustion was natural — none of `--since`,
    //    `--limit`, or orchestrator-death triggered the stop.
    let completed_naturally =
        !terminated_via_cutoff && !terminated_via_limit && !terminated_via_pipeline;
    if completed_naturally {
        store
            .complete_backfill(chat_id)
            .with_context(|| format!("complete_backfill chat={chat_id}"))?;
        tracing::info!(
            chat_id,
            total_dispatched,
            "backfill: run complete (history exhausted)",
        );
    } else {
        tracing::info!(
            chat_id,
            total_dispatched,
            terminated_via_cutoff,
            terminated_via_limit,
            terminated_via_pipeline,
            "backfill: run is resumable (cursor not finalized)",
        );
    }

    Ok(())
}
