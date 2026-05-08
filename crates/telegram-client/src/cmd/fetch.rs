//! `tg-extract fetch` subcommand (spec §4.1, §8).
//!
//! Entry points:
//! - [`run`]: real binary path -- builds a `GrammersClient` then delegates.
//! - [`run_with_client`]: generic over [`TelegramClient`] -- used by tests.

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use clap::Args;
use extractor_core::Matcher;

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

/// Generic fetch implementation, usable from both the binary and tests.
///
/// Flow:
/// 1. Warm the client (no-op for `MockClient`; loads dialog cache for real client).
/// 2. Resolve `--link` or `(--chat, --msg-id)` to `(chat_id, msg_id)`.
/// 3. Fetch [`MessageInfo`] for the target message.
/// 4. Open a `download_stream` and peek the first chunk for format detection.
/// 5. Route the stream through `stream_extract` for `.txt`/`.gz`, or
///    `disk_extract` for `.zip` (disk-spill path).
/// 6. Write matched lines to `<output_dir>/<chat_id>/<msg_id>_<sanitized_name>.out`.
///
/// [`MessageInfo`]: crate::telegram::MessageInfo
pub async fn run_with_client<C: TelegramClient>(
    cfg: &AppConfig,
    args: &FetchArgs,
    client: &C,
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
    let stem = strip_known_ext(&sanitize(&info.file_name));
    let out_filename = format!("{msg_id}_{stem}.out");
    let out_path = join_safe(&chat_dir, &out_filename)
        .with_context(|| format!("join_safe under {}", chat_dir.display()))?;

    // Open download stream and peek the first chunk for magic-byte detection.
    let mut chunks = client
        .download_stream(chat_id, msg_id)
        .await
        .context("download_stream")?;
    let first_chunk: Bytes = match chunks.recv().await {
        Some(Ok(b)) => b,
        Some(Err(e)) => return Err(e.context("first chunk from download_stream")),
        None => Bytes::new(),
    };
    let format = detect_format(&info.file_name, &first_chunk);

    // Bridge the upstream Receiver<Result<Bytes>> to a plain Receiver<Bytes>,
    // re-prepending the already-peeked first chunk so the extractor sees the
    // full stream.
    let cap = cfg.pipeline.intra_file_channel_capacity;
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(cap);
    tokio::spawn(async move {
        if !first_chunk.is_empty() && tx.send(first_chunk).await.is_err() {
            return;
        }
        while let Some(item) = chunks.recv().await {
            match item {
                Ok(b) => {
                    if tx.send(b).await.is_err() {
                        return;
                    }
                }
                // Upstream error: stop pumping; downstream observes EOF.
                Err(_) => return,
            }
        }
    });

    let matcher = Arc::new(
        Matcher::new(&cfg.extract.key, mode_for_extract(cfg.extract.mode))
            .context("Matcher::new")?,
    );

    match format {
        Format::Txt => run_stream_path(cfg, &info, &out_path, rx, matcher, false).await,
        Format::Gz => run_stream_path(cfg, &info, &out_path, rx, matcher, true).await,
        Format::Zip => run_disk_path(cfg, &info, &out_path, rx, matcher).await,
        Format::Unknown => bail!(
            "unknown format for {} (extension + magic both inconclusive)",
            info.file_name
        ),
    }
}

/// Stream-extract path: in-memory pipeline through `stream_extract`.
/// Used for `.txt` (plain) and `.gz` (gzip) sources.
async fn run_stream_path(
    cfg: &AppConfig,
    info: &MessageInfo,
    out_path: &Path,
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    matcher: Arc<Matcher>,
    is_gzip: bool,
) -> Result<()> {
    let writer = std::fs::File::create(out_path)
        .with_context(|| format!("create {}", out_path.display()))?;
    let (_file, stats) = stream_extract(rx, matcher, cfg.pipeline.max_line_bytes, writer, is_gzip)
        .await
        .with_context(|| format!("stream_extract for {}", out_path.display()))?;
    // _file (the File handle) is dropped here, flushing and closing the fd.

    tracing::info!(
        chat_id   = info.chat_id,
        msg_id    = info.msg_id,
        file_name = %info.file_name,
        out       = %out_path.display(),
        lines_scanned = stats.lines_scanned,
        lines_matched = stats.lines_matched,
        bytes_scanned = stats.bytes_scanned,
        "fetch complete (stream)",
    );
    Ok(())
}

/// Disk-spill path: spool the archive to a tempfile, then iterate its entries
/// through `disk_extract`. Used for `.zip` sources.
async fn run_disk_path(
    cfg: &AppConfig,
    info: &MessageInfo,
    out_path: &Path,
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    matcher: Arc<Matcher>,
) -> Result<()> {
    let stats = disk_extract(
        rx,
        matcher,
        cfg.pipeline.max_line_bytes,
        cfg.pipeline.max_uncompressed_bytes,
        out_path,
    )
    .await
    .with_context(|| format!("disk_extract for {}", out_path.display()))?;

    tracing::info!(
        chat_id   = info.chat_id,
        msg_id    = info.msg_id,
        file_name = %info.file_name,
        out       = %out_path.display(),
        lines_scanned = stats.lines_scanned,
        lines_matched = stats.lines_matched,
        bytes_scanned = stats.bytes_scanned,
        entries_processed = stats.entries_processed,
        entries_skipped = stats.entries_skipped,
        "fetch complete (disk-spill)",
    );
    Ok(())
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
