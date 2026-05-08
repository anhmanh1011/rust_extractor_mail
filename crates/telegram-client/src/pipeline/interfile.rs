//! Inter-file 3-stage pipeline (spec §4.2).
//!
//! Pipeline shape:
//!
//! ```text
//! [Job Queue] cap=2 ─► [Stage 1: Download] cap=1 ─► [Stage 2: Extract+Write]
//!                                                              │ cap=2
//!                                                              ▼
//!                                                     [Stage 3: Upload] ─► outcomes (cap=2)
//! ```
//!
//! Each stage is a single tokio task. Channels are `tokio::sync::mpsc`; the
//! orchestrator (`run`) joins all three on completion and on cancellation.
//!
//! Stage 3 emits exactly one `JobOutcome` per finished `Job` and processes
//! outcomes in strict FIFO order, so `on_outcome` fires in the same order as
//! jobs entered the input channel — this is the property `cmd::watch` and
//! `cmd::backfill` rely on for cursor monotonicity.
//!
//! Task 10.1 lands the public surface only; `run` is a no-op drain.
//! Tasks 10.2 / 10.3 / 10.4 wire Stage 1 / Stage 2 / Stage 3 in turn.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use tempfile::NamedTempFile;
use tokio::sync::mpsc;

// `Store`, `MessageInfo`, and `TelegramClient` appear only as parameter or
// field types in the skeleton. They become live in Task 10.2 (download stage).
use crate::pipeline::format::{detect as detect_format, Format};
use crate::store::Store;
use crate::telegram::{MessageInfo, TelegramClient};

/// One unit of inter-file work supplied by the upstream subcommand
/// (`fetch` / `watch` / `backfill`).
#[derive(Debug, Clone)]
pub struct Job {
    /// Source chat for cursor accounting + dedup keying. Always negative
    /// for channels (`-100…`) per Telegram's id convention.
    pub source_chat_id: i64,
    /// Source message id for cursor accounting + dedup keying.
    pub source_msg_id: i32,
    /// Pre-resolved document metadata. Callers fetch this via
    /// `client.message_info(...)` so the orchestrator can route by
    /// `original_name` extension *before* the first byte is downloaded.
    pub info: MessageInfo,
}

/// Outcome of processing a single job, emitted by Stage 3 for cursor
/// callback consumption. Variant choice carries enough context for the
/// caller to decide whether to advance a cursor, log, or queue a retry.
#[derive(Debug, Clone)]
pub struct JobOutcome {
    /// The job that produced this outcome. Owned (not `&Job`) so the
    /// Stage-3 callback path can move it across `await` points.
    pub job: Job,
    /// Variant-specific result of processing `job`.
    pub kind: OutcomeKind,
}

/// Discriminator on `JobOutcome` describing how a job finished.
#[derive(Debug, Clone)]
pub enum OutcomeKind {
    /// Bytes downloaded, extracted, AND uploaded successfully. The
    /// `output_msg_ids` vector has one entry per upload part (typically
    /// one; multi-part only when output exceeds `upload_max_size_bytes`).
    Uploaded {
        /// SHA-256 of the original downloaded document, lowercase hex.
        sha256: String,
        /// Telegram output message ids assigned by the server, in upload order.
        output_msg_ids: Vec<i64>,
    },
    /// Stage 1 short-circuited via `Store::try_enqueue` returning
    /// `AlreadyDone`. No bytes were downloaded past the prefix needed
    /// for hash-then-dedup; no output was produced; no upload was
    /// attempted.
    Deduped {
        /// SHA-256 that matched an existing `done` row in `files`.
        sha256: String,
    },
    /// Permanent failure at any stage. The `error` is a single-line
    /// `format!("{e:#}")` rendering of the anyhow chain. Cursor callers
    /// MUST NOT advance past a failed message in v1 (a poison message
    /// is re-attempted on every restart until manually cleared; Chunk
    /// 6c introduces a dead-letter table that lets the cursor advance
    /// past it while preserving the row for post-mortem). Note: `Failed`
    /// is also the v1 surface for skipped uploads (e.g., a part > the
    /// `upload_max_size_bytes` cap that no split could resolve) — they
    /// are not retryable, so collapsing them here keeps the cursor-callback
    /// contract simple. A future `OutcomeKind::Skipped` variant could
    /// split them out if cursor advancement semantics need to differ.
    Failed {
        /// Single-line rendering of the anyhow error chain.
        error: String,
    },
}

/// Callback invoked by Stage 3 in strict FIFO order (one call per finished
/// `Job`). The callback runs on the Stage-3 task; long-blocking work in
/// the callback will stall Stage 3, so callers should keep it cheap
/// (e.g., a `Store::update_watch_cursor` or `Store::advance_backfill`
/// call backed by SQLite).
pub type CursorAdvance = Arc<dyn Fn(JobOutcome) + Send + Sync>;

/// Configuration knobs lifted from `AppConfig` for the orchestrator.
/// Pulled into a flat struct so tests can construct one without an
/// `AppConfig`.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Domain matcher key (e.g. `"gmail.com"`); forwarded to `extractor-core`.
    pub matcher_key: String,
    /// Matcher mode string: `"plain"` or `"url"`.
    pub matcher_mode: String,
    /// Directory under which extracted output files are written.
    pub output_dir: PathBuf,
    /// Maximum bytes per scanned line; over-long lines are dropped.
    pub max_line_bytes: usize,
    /// Zip-bomb guard: per-entry uncompressed cap for the disk-spill path.
    pub max_uncompressed_bytes: u64,
    /// Bounded channel capacity inside the intra-file streaming path.
    pub intra_file_channel_capacity: usize,
    /// Capacity of the inter-file Stage1→Stage2 channel. Spec §4.2: 1.
    pub inter_file_channel_capacity: usize,
    /// Capacity of the Stage2→Stage3 channel. Spec §4.2: 2.
    pub upload_channel_capacity: usize,
    /// Capacity of the Stage3→cursor-callback channel. Spec §4.2: 2.
    pub outcomes_channel_capacity: usize,
    /// Soft cap on a single uploaded part; larger outputs are split.
    pub upload_max_size_bytes: u64,
    /// Polite delay between consecutive uploads, in whole seconds.
    pub upload_rate_seconds: u64,
    /// Telegram chat id receiving extractor output messages.
    pub target_chat_id: i64,
}

/// Drive the inter-file pipeline to completion. Returns `Ok(())` when
/// `jobs_rx` is closed AND all three stages have drained AND the
/// outcomes channel is empty. The function returns `Err(_)` only on
/// fatal infrastructure failures (e.g., output dir cannot be created);
/// per-job errors are surfaced via `OutcomeKind::Failed`, not the
/// return value.
///
/// `store` is optional — when `None`, dedup short-circuit is skipped and
/// no `files` rows are written. Production callers always pass `Some`;
/// tests use `None` to exercise the pipe in isolation.
///
/// **Task 10.1 skeleton:** the body drains `jobs_rx` without doing any
/// work, so an empty stream is a no-op `Ok(())`. Stage 1 / 2 / 3 spawn
/// graphs land in Tasks 10.2-10.4.
pub async fn run<C: TelegramClient + ?Sized>(
    _client: &C,
    _store: Option<&Store>,
    _cfg: &PipelineConfig,
    mut jobs_rx: mpsc::Receiver<Job>,
    _on_outcome: CursorAdvance,
) -> Result<()> {
    // Skeleton: drain the input channel so the empty-stream test passes.
    // Replaced in Tasks 10.2 / 10.3 / 10.4 with the three-stage spawn graph.
    while jobs_rx.recv().await.is_some() {
        // swallow
    }
    Ok(())
}

/// Stage 1 → Stage 2 hand-off shape. The variant determines which intra-file
/// path Stage 2 takes (stream vs disk-spill, per spec §4.1).
#[derive(Debug)]
pub enum Stage1Out {
    /// `.txt` / `.gz` flow. `chunks_rx` is the live download stream; Stage 2
    /// consumes it directly. `first_chunk` is the prefix already read for
    /// format detection — Stage 2 must process it BEFORE pulling more from
    /// `chunks_rx`. `is_gzip` is true iff `format == Gz`.
    Stream {
        /// The job whose download produced this stream.
        job:           Job,
        /// Detected format (always `Txt` or `Gz` in this variant).
        format:        Format,
        /// `true` iff `format == Format::Gz`; cached so Stage 2 doesn't
        /// re-match the enum.
        is_gzip:       bool,
        /// Prefix already pulled off the download stream for format
        /// detection. Stage 2 must drain this before reading more from
        /// `chunks_rx`.
        first_chunk:   Bytes,
        /// Live download stream of remaining chunks. Each item is
        /// `Result<Bytes>` so Stage 2 can surface mid-download network
        /// failures as per-job `OutcomeKind::Failed`.
        chunks_rx:     mpsc::Receiver<Result<Bytes>>,
    },
    /// `.zip` flow. The temp file is fully written and ready to mmap.
    /// Drop semantics: when Stage 2 finishes, dropping `temp` deletes the
    /// underlying file; if Stage 2 is cancelled before reading, drop still
    /// fires here on send-side hangup.
    Disk {
        /// The job whose download produced this tempfile.
        job:    Job,
        /// Detected format (always `Zip` in this variant).
        format: Format,
        /// Owned tempfile holding the full downloaded payload. Deletes on
        /// drop; Stage 2 mmaps `temp.path()` then drops `temp` when done.
        temp:   NamedTempFile,
    },
    /// Stage 1 itself failed (e.g., download error, unknown format). The
    /// orchestrator forwards this to Stage 3 unchanged so the cursor
    /// callback fires in FIFO order even for early failures.
    Failed {
        /// The job that failed in Stage 1.
        job:   Job,
        /// Anyhow error chain describing the Stage-1 failure.
        error: anyhow::Error,
    },
}

/// Stage 1 task body. Pulls jobs from `jobs_rx`, opens the download stream
/// for each, peeks the first chunk for format detection, and forwards a
/// `Stage1Out` to `s1_tx`. Returns `Ok(())` when `jobs_rx` is closed and
/// the last forward completes; returns `Err(_)` only on infrastructure
/// failures (channel send to a hung-up receiver during stage shutdown is
/// treated as cooperative cancellation and returns Ok).
pub async fn download_stage<C: TelegramClient + ?Sized>(
    client:      &C,
    _cfg:        &PipelineConfig,
    mut jobs_rx: mpsc::Receiver<Job>,
    s1_tx:       mpsc::Sender<Stage1Out>,
) -> Result<()> {
    while let Some(job) = jobs_rx.recv().await {
        let chat = job.source_chat_id;
        let msg  = job.source_msg_id;

        // Open stream + peek first chunk for format detection.
        let mut chunks_in = match client.download_stream(chat, msg).await {
            Ok(rx) => rx,
            Err(e) => {
                if s1_tx.send(Stage1Out::Failed { job, error: e.context("download_stream") })
                    .await.is_err()
                {
                    return Ok(());
                }
                continue;
            }
        };
        let first = match chunks_in.recv().await {
            Some(Ok(b)) => b,
            Some(Err(e)) => {
                if s1_tx.send(Stage1Out::Failed { job, error: e.context("first chunk") })
                    .await.is_err()
                {
                    return Ok(());
                }
                continue;
            }
            None => Bytes::new(),
        };
        let format = detect_format(&job.info.original_name, &first);

        let send_res = match format {
            Format::Txt | Format::Gz => {
                let is_gzip = matches!(format, Format::Gz);
                s1_tx.send(Stage1Out::Stream {
                    job, format, is_gzip,
                    first_chunk: first,
                    chunks_rx:   chunks_in,
                }).await
            }
            Format::Zip => {
                match drain_to_tempfile(first, chunks_in).await {
                    Ok(temp) => s1_tx.send(Stage1Out::Disk { job, format, temp }).await,
                    Err(e)   => s1_tx.send(Stage1Out::Failed {
                        job, error: e.context("download zip → tempfile"),
                    }).await,
                }
            }
            Format::Unknown => {
                s1_tx.send(Stage1Out::Failed {
                    job,
                    error: anyhow::anyhow!("unknown format (extension + magic both inconclusive)"),
                }).await
            }
        };
        if send_res.is_err() {
            // Stage 2 hung up (cancellation). Cooperate by exiting cleanly.
            return Ok(());
        }
    }
    Ok(())
}

async fn drain_to_tempfile(
    first:        Bytes,
    mut chunks:   mpsc::Receiver<Result<Bytes>>,
) -> Result<NamedTempFile> {
    use tokio::io::AsyncWriteExt;
    let temp     = tempfile::NamedTempFile::new().context("NamedTempFile::new")?;
    let path     = temp.path().to_path_buf();
    let std_file = temp.reopen().context("reopen temp")?;
    let mut f    = tokio::fs::File::from_std(std_file);

    if !first.is_empty() {
        f.write_all(&first).await
            .with_context(|| format!("write first chunk to {}", path.display()))?;
    }
    while let Some(item) = chunks.recv().await {
        let b = item.context("zip download chunk")?;
        f.write_all(&b).await
            .with_context(|| format!("write chunk to {}", path.display()))?;
    }
    f.flush().await.context("flush tempfile")?;
    drop(f);    // close handle so Stage 2 can mmap the path
    Ok(temp)
}
