//! `backfill` subcommand. Phase 9: walk a single chat's history backwards
//! from the most recent message (or `--resume`'s `backfill_cursor`)
//! towards either the channel beginning or a `--since` UTC cutoff.
//!
//! Pagination is via `iter_history(max_id, page_size)`; per-message
//! dispatch through [`crate::cmd::fetch::run_with_store_and_client`] reuses
//! the dedup, format detection, and upload retry path used by `fetch` and
//! `watch`. The `backfill_state` table records `(chat_id, next_msg_id)` so
//! a `--limit`-truncated run can be resumed; once history is exhausted (or
//! the `--since` cutoff is hit) `complete_backfill` stamps `completed_at`.

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
/// 3. Compute `since_unix` from `args.since.or(cfg.backfill.since)` via
///    `chrono::DateTime::parse_from_rfc3339`.
/// 4. Compute the starting `next_max: Option<i32>`. With `--resume` this is
///    `backfill_state.next_msg_id` (errors out if the row is missing; logs
///    and exits Ok if it is already complete). Without `--resume` it is
///    `None` (start at the most recent message).
/// 5. Loop: page `iter_history(chat_id, next_max, page_size)`. An empty
///    page is "history exhausted" → break with `completed_naturally = true`.
///    For each message in the page:
///      - If `info.date <= since_unix` (when `--since` is set) → break with
///        `completed_naturally = true` (cutoff hit).
///      - Otherwise dispatch through
///        [`crate::cmd::fetch::run_with_store_and_client`] with
///        `confirm_public = false` (backfill never auto-confirms public
///        uploads; users must set `chat_id` explicitly in TOML).
///      - On Ok: increment `processed`, advance the cursor, and break the
///        pages loop if `--limit` has been reached.
///      - On Err: log and continue. The cursor is **not** advanced for a
///        failed message so a subsequent `--resume` re-processes it.
///
///    After the page is fully consumed, advance `next_max` to the oldest
///    `msg_id` seen so the next page returns strictly older messages.
/// 6. If the loop terminated naturally (empty page or `--since` hit), stamp
///    `complete_backfill`. A `--limit` truncation leaves the cursor open
///    for resume.
pub async fn run_with_store_and_client<C: TelegramClient>(
    cfg: &AppConfig,
    args: &BackfillArgs,
    client: &C,
    store: &Store,
) -> Result<()> {
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

    // 2. Resolve --since cutoff (CLI wins over TOML).
    let since_str: Option<&str> = args.since.as_deref().or(cfg.backfill.since.as_deref());
    let since_unix: Option<i64> = match since_str {
        None => None,
        Some(s) => Some(
            chrono::DateTime::parse_from_rfc3339(s)
                .with_context(|| format!("backfill: parse --since {s:?} as RFC-3339"))?
                .timestamp(),
        ),
    };

    // 3. Decide starting max_id from --resume.
    let mut next_max: Option<i32> = if args.resume {
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
        // `next_msg_id` is `i64` in the schema (Telegram's reply chains
        // theoretically allow up to i32::MAX, but we widen on storage).
        // Narrowing back to `i32` for `iter_history` is fallible — surface
        // the overflow as an error rather than truncating silently.
        Some(i32::try_from(st.next_msg_id).with_context(|| {
            format!(
                "backfill: cursor next_msg_id {} overflows i32 for iter_history",
                st.next_msg_id,
            )
        })?)
    } else {
        None
    };

    // 4. Loop-invariant: chat_id never changes inside the page/message loops,
    //    so build the cursor title once. Mirrors the Task 8.3 hoist that the
    //    `cmd::watch` gap-fill pass adopted.
    let title = format!("chat:{chat_id}");

    let page_size = cfg.backfill.page_size.max(1);
    let mut processed: u32 = 0;
    let mut completed_naturally = false;
    let mut last_seen_msg_id: Option<i32> = None;

    'pages: loop {
        let page = client
            .iter_history(chat_id, next_max, page_size)
            .await
            .with_context(|| format!("iter_history chat={chat_id} max={next_max:?}"))?;
        if page.is_empty() {
            // History exhausted: no more document-bearing messages older
            // than `next_max` (or older than the most recent message on the
            // first iteration).
            completed_naturally = true;
            break;
        }
        for info in &page {
            // 4a. --since cutoff. Cutoff is exclusive: only messages whose
            //     date is strictly greater than `since_unix` are processed;
            //     the first message at-or-below the cutoff terminates the run.
            if let Some(cut) = since_unix {
                if info.date <= cut {
                    completed_naturally = true;
                    break 'pages;
                }
            }

            // 4b. Dispatch via cmd::fetch::run_with_store_and_client to reuse
            //     dedup + format detection + upload retry.
            let synth = crate::cmd::fetch::FetchArgs {
                link: None,
                chat: Some(info.chat_id),
                msg_id: Some(info.msg_id),
                no_upload: false,
                // backfill never auto-confirms public chats; the user must
                // pin a numeric `output.chat_id` in TOML for an unattended
                // backfill to upload.
                confirm_public: false,
            };
            match crate::cmd::fetch::run_with_store_and_client(cfg, &synth, client, Some(store))
                .await
            {
                Ok(()) => {
                    processed += 1;
                    last_seen_msg_id = Some(info.msg_id);
                    if let Err(e) = store.advance_backfill(chat_id, &title, i64::from(info.msg_id))
                    {
                        // A cursor-write failure is a *persistence* problem,
                        // not a per-message processing problem. Log and
                        // press on so the run can still report progress;
                        // the user can `--resume` from the prior cursor.
                        tracing::error!(
                            ?e,
                            chat_id,
                            msg_id = info.msg_id,
                            "backfill: advance_backfill write failed",
                        );
                    }
                    if let Some(lim) = args.limit {
                        if processed >= lim {
                            // --limit reached: stop without stamping
                            // completed_at so a follow-up --resume can
                            // continue from `next_msg_id`.
                            break 'pages;
                        }
                    }
                }
                Err(e) => {
                    // Per-message failures are logged + skipped without
                    // advancing the cursor. A subsequent `--resume`
                    // re-processes the failed message.
                    tracing::error!(
                        ?e,
                        chat_id,
                        msg_id = info.msg_id,
                        "backfill: per-message processing failed, continuing",
                    );
                }
            }
        }
        // Advance to the older page. `iter_history` returns newest-first, so
        // the *last* entry in the page is the oldest; using its msg_id as
        // the next `max_id` requests messages strictly older than it.
        next_max = page.last().map(|m| m.msg_id);
    }

    if completed_naturally {
        store
            .complete_backfill(chat_id)
            .with_context(|| format!("complete_backfill chat={chat_id}"))?;
        tracing::info!(
            chat_id,
            processed,
            last_seen_msg_id,
            "backfill: run complete",
        );
    } else {
        tracing::info!(
            chat_id,
            processed,
            last_seen_msg_id,
            "backfill: --limit reached, run is resumable",
        );
    }

    Ok(())
}
