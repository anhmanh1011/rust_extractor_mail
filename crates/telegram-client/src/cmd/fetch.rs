//! `tg-extract fetch` subcommand (spec §4.1, §6.3, §6.4, §8, §11.2).
//!
//! Entry points:
//! - [`run`]: real binary path -- builds a `GrammersClient` then delegates.
//! - [`run_with_client`]: generic over [`TelegramClient`] -- used by tests
//!   without a `Store` (back-compat with Phase 4-6 callers).
//! - [`run_with_store_and_client`]: generic over [`TelegramClient`] with an
//!   optional [`Store`](crate::store::Store) for sha256 dedup + lifecycle
//!   stamping (Phase 7).

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use clap::Args;
use extractor_core::Matcher;
use sha2::{Digest, Sha256};

use crate::config::{AppConfig, ExtractMode, Secrets};
use crate::output::{join_safe, sanitize};
use crate::pipeline::disk::disk_extract;
use crate::pipeline::stream::stream_extract;
use crate::pipeline::{detect_format, Format};
use crate::telegram::link_parser::{parse_message_link, MessageRef};
use crate::telegram::{ChatRef, MessageInfo, TelegramClient};

/// Arguments for `tg-extract fetch`.
#[derive(Args, Debug)]
pub struct FetchArgs {
    /// `t.me` message link (e.g. `https://t.me/c/1234567890/42`).
    #[arg(long, conflicts_with_all = ["chat", "msg_id"])]
    pub link: Option<String>,

    /// Numeric chat id (negative for channels/supergroups). Requires `--msg-id`.
    #[arg(long, requires = "msg_id")]
    pub chat: Option<i64>,

    /// Numeric message id within the chat. Requires `--chat`.
    #[arg(long = "msg-id", requires = "chat")]
    pub msg_id: Option<i32>,

    /// Do not upload the produced output to `telegram.output.chat`.
    #[arg(long, default_value_t = false)]
    pub no_upload: bool,

    /// Acknowledge that `telegram.output.chat` is a public chat (`@username`
    /// or any non-numeric handle). Required by spec §11.2 to avoid an
    /// accidental public credential leak.
    #[arg(long, default_value_t = false)]
    pub confirm_public: bool,
}

/// Subset of extractor stats the upload caption needs. Both extract paths
/// (`stream_extract` and `disk_extract`) return their own stats struct;
/// this is the common projection used to build [`crate::upload::caption::CaptionData`].
#[derive(Debug, Clone, Copy)]
struct CaptionStats {
    lines_scanned: u64,
    lines_matched: u64,
}

/// Top-level entry point invoked by `main.rs`. Constructs a real
/// [`GrammersClient`] and delegates to [`run_with_client`].
///
/// [`GrammersClient`]: crate::telegram::client::GrammersClient
pub async fn run(cfg: &AppConfig, secrets: &Secrets, args: &FetchArgs) -> Result<()> {
    let client = crate::telegram::client::GrammersClient::connect(
        secrets.api_id,
        &secrets.api_hash,
        Path::new(&cfg.telegram.session_path),
    )
    .await
    .context("GrammersClient::connect")?;
    run_with_client(cfg, args, &client).await
}

/// Generic fetch implementation without store wiring. Equivalent to calling
/// [`run_with_store_and_client`] with `store = None`. Preserved for back-compat
/// with Phase 4-6 callers (mostly tests); the binary path always wires a Store.
pub async fn run_with_client<C: TelegramClient>(
    cfg: &AppConfig,
    args: &FetchArgs,
    client: &C,
) -> Result<()> {
    run_with_store_and_client(cfg, args, client, None).await
}

/// Generic fetch implementation with optional [`Store`](crate::store::Store)
/// wiring (Phase 7). Usable from both the binary and tests.
///
/// Flow:
/// 1. Warm the client (no-op for `MockClient`; loads dialog cache for real client).
/// 2. Resolve `--link` or `(--chat, --msg-id)` to `(chat_id, msg_id)`.
/// 3. Fetch [`MessageInfo`] for the target message.
/// 4. Open a `download_stream` and peek the first chunk for format detection.
/// 5. Tee the chunk stream: one branch feeds the extractor, the other a
///    Sha256 hasher task. The final hex sha keys the `files` row.
/// 6. Route the stream through `stream_extract` for `.txt`/`.gz`, or delegate
///    to [`run_zip_path`] for `.zip` (disk-spill path).
/// 7. Write matched lines to `<output_dir>/<chat_id>/<msg_id>_<sanitized_name>.out`.
/// 8. If a `store` is provided: `try_enqueue` short-circuits on
///    `AlreadyDone` (delete out_path; no upload), proceeds on
///    `InProgress`/`New`, and stamps `mark_downloading` →
///    `mark_downloaded` → `mark_extracted` → `mark_uploaded`.
/// 9. Optionally upload that output to `cfg.telegram.output.{chat,chat_id}`
///    via [`crate::pipeline::upload::run`] (skipped when `args.no_upload`
///    is set or no output chat is configured). Public-chat targets require
///    `--confirm-public` per spec §11.2. Failed uploads are persisted via
///    [`crate::store::Store::enqueue_failed_upload`].
///
/// [`MessageInfo`]: crate::telegram::MessageInfo
pub async fn run_with_store_and_client<C: TelegramClient>(
    cfg: &AppConfig,
    args: &FetchArgs,
    client: &C,
    store: Option<&crate::store::Store>,
) -> Result<()> {
    client
        .connect_and_warm()
        .await
        .context("connect_and_warm")?;

    let (chat_id, msg_id) = resolve_target(args, client).await?;
    let info = client
        .message_info(chat_id, msg_id)
        .await
        .context("message_info")?;

    // Per-source-file output path: <pipeline.output_dir>/<chat_id>/<msg_id>_<sanitized>.out
    let chat_dir = Path::new(&cfg.pipeline.output_dir).join(chat_id.to_string());
    std::fs::create_dir_all(&chat_dir).with_context(|| format!("mkdir {}", chat_dir.display()))?;
    let stem = strip_known_ext(&sanitize(&info.original_name));
    let out_filename = format!("{msg_id}_{stem}.out");
    let out_path = join_safe(&chat_dir, &out_filename)
        .with_context(|| format!("join_safe under {}", chat_dir.display()))?;

    // Open download stream and peek the first chunk for magic-byte detection.
    let mut chunks_in = client
        .download_stream(chat_id, msg_id)
        .await
        .context("download_stream")?;
    let first_chunk: Bytes = match chunks_in.recv().await {
        Some(Ok(b)) => b,
        Some(Err(e)) => return Err(e.context("first chunk from download_stream")),
        None => Bytes::new(),
    };
    let format = detect_format(&info.original_name, &first_chunk);
    let is_gzip = match format {
        Format::Txt => false,
        Format::Gz => true,
        Format::Zip => {
            return run_zip_path(
                cfg,
                args,
                client,
                store,
                chat_id,
                msg_id,
                info,
                out_path,
                first_chunk,
                chunks_in,
            )
            .await
        }
        Format::Unknown => bail!(
            "unknown format for {} (extension + magic both inconclusive)",
            info.original_name
        ),
    };

    // Tee: chunks fan out to (a) the extractor input pipe and (b) a Sha256
    // hashing task. Both branches see the already-peeked first chunk.
    let cap = cfg.pipeline.intra_file_channel_capacity;
    let (pipe_tx, pipe_rx) = tokio::sync::mpsc::channel::<Bytes>(cap);
    let (hash_tx, mut hash_rx) = tokio::sync::mpsc::channel::<Bytes>(cap);

    let hasher_handle = tokio::spawn(async move {
        let mut h = Sha256::new();
        while let Some(b) = hash_rx.recv().await {
            h.update(&b);
        }
        hex::encode(h.finalize())
    });

    let first = first_chunk.clone();
    let pipe_tx_for_first = pipe_tx.clone();
    let hash_tx_for_first = hash_tx.clone();
    tokio::spawn(async move {
        if !first.is_empty() {
            let _ = pipe_tx_for_first.send(first.clone()).await;
            let _ = hash_tx_for_first.send(first).await;
        }
        while let Some(item) = chunks_in.recv().await {
            match item {
                Ok(b) => {
                    if pipe_tx.send(b.clone()).await.is_err() {
                        return;
                    }
                    if hash_tx.send(b).await.is_err() {
                        return;
                    }
                }
                // Upstream error: stop pumping; downstream observes EOF.
                Err(_) => return,
            }
        }
    });

    // Run the extractor.
    let matcher = Arc::new(
        Matcher::new(&cfg.extract.key, mode_for_extract(cfg.extract.mode))
            .context("Matcher::new")?,
    );
    let writer = std::fs::File::create(&out_path)
        .with_context(|| format!("create {}", out_path.display()))?;
    let (file, stats) = stream_extract(
        pipe_rx,
        matcher,
        cfg.pipeline.max_line_bytes,
        writer,
        is_gzip,
    )
    .await
    .with_context(|| format!("stream_extract for {}", out_path.display()))?;
    drop(file);

    tracing::info!(
        chat_id = info.chat_id,
        msg_id = info.msg_id,
        file_name = %info.original_name,
        out = %out_path.display(),
        lines_scanned = stats.lines_scanned,
        lines_matched = stats.lines_matched,
        bytes_scanned = stats.bytes_scanned,
        "fetch complete (stream)",
    );

    let cap_stats = CaptionStats {
        lines_scanned: stats.lines_scanned,
        lines_matched: stats.lines_matched,
    };

    // Finalize hash and dedup.
    let sha = hasher_handle.await.context("hasher join")?;
    if let Some(s) = store {
        let meta = crate::store::FileMeta {
            sha256: sha.clone(),
            source_chat_id: chat_id,
            source_msg_id: msg_id,
            original_name: info.original_name.clone(),
            size_bytes: info.size_bytes,
            format: format_label(&format),
            matcher_key: cfg.extract.key.clone(),
            matcher_mode: match cfg.extract.mode {
                ExtractMode::Plain => "plain".into(),
                ExtractMode::Url => "url".into(),
            },
        };
        match s.try_enqueue(&meta).context("try_enqueue")? {
            crate::store::EnqueueResult::AlreadyDone => {
                tracing::info!(sha256 = %sha, "fetch: dedup hit (file already done)");
                let _ = std::fs::remove_file(&out_path);
                return Ok(());
            }
            crate::store::EnqueueResult::InProgress(state) => {
                tracing::warn!(
                    sha256 = %sha,
                    state = %state,
                    "fetch: another run is processing this file; proceeding (last-writer wins)",
                );
            }
            crate::store::EnqueueResult::New => {}
        }
        s.mark_downloading(&sha)?;
        s.mark_downloaded(&sha)?;
        s.mark_extracted(&sha, stats.lines_scanned, stats.lines_matched, &out_path)?;
    }

    // Phase 6: optional upload to telegram.output.chat. Order matters:
    // `resolve_output_chat` runs the public-chat gate FIRST, then resolves,
    // so users get a clear error before any network resolve is attempted.
    if !args.no_upload {
        if let Some(target_chat_id) = resolve_output_chat(cfg, args, client).await? {
            run_single_upload(
                client,
                cfg,
                &out_path,
                &info,
                &sha,
                chat_id,
                msg_id,
                cap_stats,
                target_chat_id,
                store,
            )
            .await?;
        }
    }
    // `--no-upload` deliberately leaves the row at status='uploading'
    // (the state mark_extracted transitions to). Marking it 'done' with
    // `output_msg_id=0` would (a) collide with any real future msg_id of 0
    // and (b) cause the next plain `fetch` of the same source to
    // short-circuit on AlreadyDone, even though the file was never
    // actually uploaded. By staying at 'uploading', a later `fetch`
    // (without --no-upload) lands on `InProgress(uploading)` above and
    // proceeds with the upload — which is the desired behavior for a
    // debug/audit-only invocation. Subsequent process restarts also pick
    // it up: `reset_in_flight` (Task 7.3) clears transient states back to
    // 'queued', so a re-run reproduces the upload from scratch.

    Ok(())
}

/// Zip-archive fetch path. Mirrors the txt/gz path but routes the chunk
/// stream into `disk_extract` (which spills the archive to a tempfile and
/// iterates its entries). The same tee pattern feeds a Sha256 hasher.
///
/// The shape is duplicated rather than abstracted because the post-extract
/// shape is identical to txt/gz; collapsing them would force a sum type
/// over `stream_extract` / `disk_extract` stats that adds more noise than
/// it removes.
#[allow(clippy::too_many_arguments)]
async fn run_zip_path<C: TelegramClient>(
    cfg: &AppConfig,
    args: &FetchArgs,
    client: &C,
    store: Option<&crate::store::Store>,
    chat_id: i64,
    msg_id: i32,
    info: MessageInfo,
    out_path: std::path::PathBuf,
    first_chunk: Bytes,
    mut chunks_in: tokio::sync::mpsc::Receiver<Result<Bytes>>,
) -> Result<()> {
    let cap = cfg.pipeline.intra_file_channel_capacity;
    let (pipe_tx, pipe_rx) = tokio::sync::mpsc::channel::<Bytes>(cap);
    let (hash_tx, mut hash_rx) = tokio::sync::mpsc::channel::<Bytes>(cap);

    let hasher_handle = tokio::spawn(async move {
        let mut h = Sha256::new();
        while let Some(b) = hash_rx.recv().await {
            h.update(&b);
        }
        hex::encode(h.finalize())
    });

    let first = first_chunk.clone();
    let pipe_tx_for_first = pipe_tx.clone();
    let hash_tx_for_first = hash_tx.clone();
    tokio::spawn(async move {
        if !first.is_empty() {
            let _ = pipe_tx_for_first.send(first.clone()).await;
            let _ = hash_tx_for_first.send(first).await;
        }
        while let Some(item) = chunks_in.recv().await {
            match item {
                Ok(b) => {
                    if pipe_tx.send(b.clone()).await.is_err() {
                        return;
                    }
                    if hash_tx.send(b).await.is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });

    // Run the zip-aware extractor.
    let matcher = Arc::new(
        Matcher::new(&cfg.extract.key, mode_for_extract(cfg.extract.mode))
            .context("Matcher::new")?,
    );
    let stats = disk_extract(
        pipe_rx,
        matcher,
        cfg.pipeline.max_line_bytes,
        cfg.pipeline.max_uncompressed_bytes,
        &out_path,
    )
    .await
    .with_context(|| format!("disk_extract for {}", out_path.display()))?;

    tracing::info!(
        chat_id = info.chat_id,
        msg_id = info.msg_id,
        file_name = %info.original_name,
        out = %out_path.display(),
        lines_scanned = stats.lines_scanned,
        lines_matched = stats.lines_matched,
        bytes_scanned = stats.bytes_scanned,
        entries_processed = stats.entries_processed,
        entries_skipped = stats.entries_skipped,
        "fetch complete (disk-spill)",
    );

    // Finalize hash and dedup. Same shape as txt/gz.
    let sha = hasher_handle.await.context("hasher join")?;
    if let Some(s) = store {
        let meta = crate::store::FileMeta {
            sha256: sha.clone(),
            source_chat_id: chat_id,
            source_msg_id: msg_id,
            original_name: info.original_name.clone(),
            size_bytes: info.size_bytes,
            format: "zip".into(),
            matcher_key: cfg.extract.key.clone(),
            matcher_mode: match cfg.extract.mode {
                ExtractMode::Plain => "plain".into(),
                ExtractMode::Url => "url".into(),
            },
        };
        match s.try_enqueue(&meta).context("try_enqueue")? {
            crate::store::EnqueueResult::AlreadyDone => {
                tracing::info!(sha256 = %sha, "fetch: dedup hit (file already done)");
                let _ = std::fs::remove_file(&out_path);
                return Ok(());
            }
            crate::store::EnqueueResult::InProgress(state) => {
                tracing::warn!(
                    sha256 = %sha,
                    state = %state,
                    "fetch: another run is processing this file; proceeding (last-writer wins)",
                );
            }
            crate::store::EnqueueResult::New => {}
        }
        s.mark_downloading(&sha)?;
        s.mark_downloaded(&sha)?;
        s.mark_extracted(&sha, stats.lines_scanned, stats.lines_matched, &out_path)?;
    }

    if !args.no_upload {
        if let Some(target_chat_id) = resolve_output_chat(cfg, args, client).await? {
            // disk_extract returns DiskExtractStats; the upload caption only
            // consumes lines_scanned + lines_matched, so adapt to CaptionStats.
            let cap_stats = CaptionStats {
                lines_scanned: stats.lines_scanned,
                lines_matched: stats.lines_matched,
            };
            run_single_upload(
                client,
                cfg,
                &out_path,
                &info,
                &sha,
                chat_id,
                msg_id,
                cap_stats,
                target_chat_id,
                store,
            )
            .await?;
        }
    }
    // See `run_with_store_and_client` for the `--no-upload` invariant comment.
    Ok(())
}

/// Drive a single output file through the upload pipeline and stamp the
/// resulting message id (or failed-upload row) onto `store`.
///
/// `pipeline::upload::run` requires `F: FnMut(...) + Send + 'static`, so the
/// failure callback cannot borrow `&Store`. We route failures through a
/// `std::sync::mpsc::channel` and persist them serially after `run` returns
/// — this is `cmd::fetch` which is one-shot, so we are NOT in any hot path.
#[allow(clippy::too_many_arguments)]
async fn run_single_upload<C: TelegramClient>(
    client: &C,
    cfg: &AppConfig,
    out_path: &Path,
    info: &MessageInfo,
    sha256: &str,
    source_chat_id: i64,
    source_msg_id: i32,
    stats: CaptionStats,
    target_chat_id: i64,
    store: Option<&crate::store::Store>,
) -> Result<()> {
    let caption_data = crate::upload::caption::CaptionData {
        original_name: info.original_name.clone(),
        source_chat_id,
        source_msg_id,
        matcher_key: cfg.extract.key.clone(),
        matcher_mode: match cfg.extract.mode {
            ExtractMode::Plain => "plain".into(),
            ExtractMode::Url => "url".into(),
        },
        size_bytes: info.size_bytes,
        lines_scanned: stats.lines_scanned,
        lines_matched: stats.lines_matched,
    };
    let job = crate::pipeline::upload::UploadJob {
        sha256: sha256.to_string(),
        output_path: out_path.to_path_buf(),
        caption: caption_data,
    };
    // `cmd::fetch` is a one-shot: a single source message produces a single
    // UploadJob, so 1-element channels are correct here. Phase 8/9 size
    // these from `cfg.pipeline.upload_channel_capacity`.
    let (jt, jr) = tokio::sync::mpsc::channel(1);
    let (ot, mut or) = tokio::sync::mpsc::channel(1);
    let upload_cfg = crate::pipeline::upload::UploadRunConfig {
        target_chat_id,
        upload_max_size_bytes: cfg.pipeline.upload_max_size_bytes,
        upload_rate_seconds: cfg.pipeline.upload_rate_seconds,
        retry: crate::pipeline::upload::RetryPolicy::default(),
    };
    jt.send(job).await.context("send upload job")?;
    drop(jt);

    // Failure callback: route through a sync mpsc since the closure is
    // `Send + 'static` and cannot borrow `&Store`.
    let (failed_tx, failed_rx) =
        std::sync::mpsc::channel::<(crate::pipeline::upload::UploadJob, String)>();
    let on_failed = move |job: crate::pipeline::upload::UploadJob, err: anyhow::Error| {
        let _ = failed_tx.send((job, format!("{err:#}")));
    };
    crate::pipeline::upload::run(client, jr, ot, &upload_cfg, on_failed)
        .await
        .context("upload run")?;
    while let Some(o) = or.recv().await {
        if let crate::pipeline::upload::UploadOutcome::Done {
            sha256: s,
            output_msg_ids,
        } = o
        {
            if let Some(st) = store {
                // `files.output_msg_id` is INTEGER (single column). For multi-part
                // uploads (file > telegram.upload.max_size_bytes), only the FIRST
                // part's msg_id is recorded — that's the "head" message; subsequent
                // parts are reachable via Telegram's reply chain or a `Part i/N`
                // search in the destination chat. We log the full vector for audit.
                // A schema-level fix (separate `output_message_ids` table) is out of
                // scope for Phase 7; revisit in Phase 10 (hardening) if needed.
                let head = output_msg_ids.first().copied().unwrap_or_else(|| {
                    tracing::error!(
                        sha256 = %s,
                        "Done outcome had empty output_msg_ids; recording 0 — investigate upload::run",
                    );
                    0
                });
                st.mark_uploaded(&s, head)?;
            }
            tracing::info!(?output_msg_ids, "fetch upload complete");
        }
    }
    while let Ok((job, err_str)) = failed_rx.try_recv() {
        if let Some(s) = store {
            s.enqueue_failed_upload(&job.sha256, &job.output_path, &err_str)
                .context("enqueue_failed_upload after run")?;
        } else {
            tracing::error!(
                sha256 = %job.sha256,
                error = %err_str,
                "upload failed (no Store wired — record dropped)",
            );
        }
    }
    Ok(())
}

/// Map a detected [`Format`] to its short tag stored in `files.format`.
fn format_label(f: &Format) -> String {
    match f {
        Format::Txt => "txt".into(),
        Format::Gz => "gz".into(),
        Format::Zip => "zip".into(),
        Format::Unknown => "unknown".into(),
    }
}

/// Resolve CLI arguments to a `(chat_id, msg_id)` pair.
///
/// Direct form (`--chat` + `--msg-id`) is returned immediately without any
/// network call. Link form (`--link`) is parsed by [`parse_message_link`] and
/// the public-username variant is resolved via `client.resolve_chat`.
async fn resolve_target<C: TelegramClient>(args: &FetchArgs, client: &C) -> Result<(i64, i32)> {
    if let (Some(chat), Some(msg_id)) = (args.chat, args.msg_id) {
        return Ok((chat, msg_id));
    }
    let link = args
        .link
        .as_deref()
        .ok_or_else(|| anyhow!("--link or (--chat + --msg-id) required"))?;
    let parsed = parse_message_link(link).with_context(|| format!("parse link {link}"))?;
    match parsed {
        MessageRef::Username { username, msg_id } => {
            let chat_id = client
                .resolve_chat(&ChatRef::Username(username))
                .await
                .with_context(|| format!("resolve_chat for link {link}"))?;
            Ok((chat_id, msg_id))
        }
        MessageRef::ChatId { chat_id, msg_id } => {
            // chat_id already in canonical -(1_000_000_000_000 + internal) form.
            Ok((chat_id, msg_id))
        }
    }
}

/// Resolve the configured output chat for the upload step, applying the
/// spec §11.2 public-chat safety gate.
///
/// Returns `Ok(None)` (skip upload) when neither `chat` nor `chat_id` is set,
/// or when `chat` is set to an empty/whitespace-only string.
///
/// Public-chat heuristic: a `chat` string is treated as public iff it starts
/// with `@` OR fails to parse as an `i64`. Numeric strings (e.g.
/// `"-1001234567890"`) are private references and are accepted without
/// `--confirm-public`. Anything else (`"@chan"`, `"my_channel"`, channel
/// titles, typos) is public and requires `args.confirm_public`.
async fn resolve_output_chat<C: TelegramClient>(
    cfg: &AppConfig,
    args: &FetchArgs,
    client: &C,
) -> Result<Option<i64>> {
    resolve_output_chat_inner(cfg, args.confirm_public, client).await
}

/// Sibling of [`resolve_output_chat`] used by `cmd::watch`, which doesn't
/// build a [`FetchArgs`] for the once-at-startup output-chat resolution.
/// Same semantics: public-chat heuristic + spec §11.2 gate.
pub async fn resolve_output_chat_for_watch<C: TelegramClient>(
    cfg:            &AppConfig,
    confirm_public: bool,
    client:         &C,
) -> Result<Option<i64>> {
    resolve_output_chat_inner(cfg, confirm_public, client).await
}

async fn resolve_output_chat_inner<C: TelegramClient>(
    cfg:            &AppConfig,
    confirm_public: bool,
    client:         &C,
) -> Result<Option<i64>> {
    if let Some(id) = cfg.telegram.output.chat_id {
        return Ok(Some(id));
    }
    let Some(name) = cfg.telegram.output.chat.as_deref() else {
        return Ok(None);
    };
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let looks_public = trimmed.starts_with('@') || trimmed.parse::<i64>().is_err();
    if looks_public && !confirm_public {
        bail!(
            "telegram.output.chat = {trimmed:?} looks public; pass --confirm-public to upload there \
             (spec §11.2: public outputs require explicit acknowledgement)",
        );
    }
    if let Ok(id) = trimmed.parse::<i64>() {
        return Ok(Some(id));
    }
    let resolved = client
        .resolve_chat(&ChatRef::Username(
            trimmed.trim_start_matches('@').to_string(),
        ))
        .await
        .context("resolve telegram.output.chat")?;
    Ok(Some(resolved))
}

/// Strip a single well-known extension suffix from a sanitized filename stem.
///
/// Matched suffixes: `.txt`, `.gz`, `.zip`, and their all-uppercase variants.
/// Returns the stem without the extension, or `name` unchanged if no match.
fn strip_known_ext(name: &str) -> String {
    for ext in [".txt", ".gz", ".zip", ".TXT", ".GZ", ".ZIP"] {
        if let Some(stem) = name.strip_suffix(ext) {
            return stem.to_string();
        }
    }
    name.to_string()
}

/// Map [`ExtractMode`] to the `extractor_core::Mode` expected by [`Matcher`].
fn mode_for_extract(m: ExtractMode) -> extractor_core::Mode {
    match m {
        ExtractMode::Plain => extractor_core::Mode::Plain,
        ExtractMode::Url => extractor_core::Mode::Url,
    }
}
