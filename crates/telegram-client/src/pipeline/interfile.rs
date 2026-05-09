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

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use tokio::sync::mpsc;

use extractor_core::ScanStats;

// `Store`, `MessageInfo`, and `TelegramClient` appear only as parameter or
// field types in the skeleton. They become live in Task 10.2 (download stage).
use crate::output::{join_safe, sanitize};
use crate::pipeline::format::{detect as detect_format, Format};
use crate::pipeline::stream::stream_extract;
use crate::pipeline::upload::{self, UploadJob, UploadOutcome, UploadRunConfig};
use crate::store::{EnqueueResult, FileMeta, Store};
use crate::telegram::{MessageInfo, TelegramClient};
use crate::upload::caption::CaptionData;

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
    /// Optional indicatif container. `None` means bars are suppressed
    /// (non-TTY, CI, daemon mode). Stages must check `is_some` before
    /// allocating bars; the no-bar path must be a true no-op.
    pub progress: Option<Arc<indicatif::MultiProgress>>,
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
/// Task 10.4: drives the three stages concurrently via `tokio::join!` over
/// async blocks (not `tokio::spawn`) — `&client` and `Option<&Store>` are
/// borrowed across all three stages and `spawn` would force `'static`,
/// requiring `Arc` cloning that the borrow already prevents.
pub async fn run<C: TelegramClient + ?Sized>(
    client:     &C,
    store:      Option<&Store>,
    cfg:        &PipelineConfig,
    jobs_rx:    mpsc::Receiver<Job>,
    on_outcome: CursorAdvance,
) -> Result<()> {
    let (s1_tx, s1_rx) = mpsc::channel::<Stage1Out>(cfg.inter_file_channel_capacity);
    let (s2_tx, s2_rx) = mpsc::channel::<Stage2Out>(cfg.upload_channel_capacity);

    // Stage 1: download. The `s1_tx` half is moved into the s1_handle
    // async block so the only live sender lives on Stage 1's task — when
    // download_stage returns, the sender is dropped and Stage 2's recv()
    // observes a clean channel close.
    let s1_handle = async move { download_stage(client, cfg, jobs_rx, s1_tx).await };
    let s2_handle = async move { extract_stage(store, cfg, s1_rx, s2_tx).await };
    let s3_handle = async move { upload_stage(client, cfg, s2_rx, on_outcome).await };

    let (r1, r2, r3) = tokio::join!(s1_handle, s2_handle, s3_handle);
    r1?;
    r2?;
    r3?;
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
    cfg:         &PipelineConfig,
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
                let label = format!("dl {chat}/{msg}");
                let pb = crate::pipeline::progress::make_bar(
                    cfg.progress.as_ref(),
                    &label,
                    Some(job.info.size_bytes),
                );
                let drain_res = drain_to_tempfile(first, chunks_in, &pb).await;
                pb.finish_and_clear();
                match drain_res {
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
    pb:           &indicatif::ProgressBar,
) -> Result<NamedTempFile> {
    use tokio::io::AsyncWriteExt;
    let temp     = tempfile::NamedTempFile::new().context("NamedTempFile::new")?;
    let path     = temp.path().to_path_buf();
    let std_file = temp.reopen().context("reopen temp")?;
    let mut f    = tokio::fs::File::from_std(std_file);

    if !first.is_empty() {
        let n = first.len() as u64;
        f.write_all(&first).await
            .with_context(|| format!("write first chunk to {}", path.display()))?;
        pb.inc(n);
    }
    while let Some(item) = chunks.recv().await {
        let b = item.context("zip download chunk")?;
        let n = b.len() as u64;
        f.write_all(&b).await
            .with_context(|| format!("write chunk to {}", path.display()))?;
        pb.inc(n);
    }
    f.flush().await.context("flush tempfile")?;
    drop(f);    // close handle so Stage 2 can mmap the path
    Ok(temp)
}

/// Stage 2 → Stage 3 hand-off shape.
#[derive(Debug)]
pub enum Stage2Out {
    /// Bytes hashed, scanned, written. Ready for upload.
    Ready {
        /// The job whose payload was extracted.
        job:            Job,
        /// SHA-256 of the original downloaded document, lowercase hex.
        sha256:         String,
        /// On-disk path to the materialized `.out` file ready for upload.
        output_path:    PathBuf,
        /// Total lines scanned by `extractor-core`.
        lines_scanned:  u64,
        /// Lines that matched the configured matcher.
        lines_matched:  u64,
        /// Detected format of the source document.
        format:         Format,
    },
    /// Hash showed dedup hit before extraction completed (or after, if the
    /// store was consulted post-hash). No output exists; Stage 3 forwards
    /// to `OutcomeKind::Deduped` directly without touching uploads.
    Deduped {
        /// The job that was deduped.
        job:    Job,
        /// SHA-256 that matched an existing `done` row in `files`.
        sha256: String,
    },
    /// Stage-2 failure. Forwarded to Stage 3 untouched.
    Failed {
        /// The job that failed in Stage 2.
        job:   Job,
        /// Anyhow error chain describing the Stage-2 failure.
        error: anyhow::Error,
    },
}

/// Stage 2 task body. Pulls `Stage1Out` items off `s1_rx`, performs the
/// per-format intra-file extract+write, and forwards a `Stage2Out` to
/// `s2_tx`. The function returns `Ok(())` when `s1_rx` is closed and the
/// last forward completes; a hung-up `s2_tx` is treated as cooperative
/// cancellation (returns `Ok(())`).
pub async fn extract_stage(
    store:        Option<&Store>,
    cfg:          &PipelineConfig,
    mut s1_rx:    mpsc::Receiver<Stage1Out>,
    s2_tx:        mpsc::Sender<Stage2Out>,
) -> Result<()> {
    while let Some(s1) = s1_rx.recv().await {
        let out = match s1 {
            Stage1Out::Stream { job, format, is_gzip, first_chunk, chunks_rx } =>
                handle_stream(store, cfg, job, format, is_gzip, first_chunk, chunks_rx).await,
            Stage1Out::Disk { job, format, temp } =>
                handle_disk(store, cfg, job, format, temp).await,
            Stage1Out::Failed { job, error } =>
                Stage2Out::Failed { job, error },
        };
        if s2_tx.send(out).await.is_err() {
            // Stage 3 hung up (cancellation). Cooperate.
            return Ok(());
        }
    }
    Ok(())
}

async fn handle_stream(
    store:       Option<&Store>,
    cfg:         &PipelineConfig,
    job:         Job,
    format:      Format,
    is_gzip:     bool,
    first_chunk: Bytes,
    mut chunks:  mpsc::Receiver<anyhow::Result<Bytes>>,
) -> Stage2Out {
    let (out_path, chat_dir) = match build_output_path(cfg, &job) {
        Ok(p)  => p,
        Err(e) => return Stage2Out::Failed { job, error: e },
    };
    if let Err(e) = std::fs::create_dir_all(&chat_dir) {
        return Stage2Out::Failed {
            job,
            error: anyhow::Error::new(e).context(format!("mkdir {}", chat_dir.display())),
        };
    }

    // Tee: feed (a) stream_extract pipeline, (b) sha256 hasher.
    let (pipe_tx, pipe_rx) = mpsc::channel::<Bytes>(cfg.intra_file_channel_capacity);
    let (hash_tx, mut hash_rx) = mpsc::channel::<Bytes>(cfg.intra_file_channel_capacity);

    let hasher = tokio::spawn(async move {
        let mut h = Sha256::new();
        while let Some(b) = hash_rx.recv().await { h.update(&b); }
        hex::encode(h.finalize())
    });

    let first = first_chunk.clone();
    let pipe_tx_first = pipe_tx.clone();
    let hash_tx_first = hash_tx.clone();
    let pb_dl = crate::pipeline::progress::make_bar(
        cfg.progress.as_ref(),
        &format!("dl {}/{}", job.source_chat_id, job.source_msg_id),
        Some(job.info.size_bytes),
    );
    let teer = tokio::spawn(async move {
        if !first.is_empty() {
            let n = first.len() as u64;
            if pipe_tx_first.send(first.clone()).await.is_err() { return; }
            if hash_tx_first.send(first).await.is_err()         { return; }
            pb_dl.inc(n);
        }
        while let Some(item) = chunks.recv().await {
            match item {
                Ok(b) => {
                    let n = b.len() as u64;
                    if pipe_tx.send(b.clone()).await.is_err() { return; }
                    if hash_tx.send(b).await.is_err()         { return; }
                    pb_dl.inc(n);
                }
                Err(_) => return,
            }
        }
        pb_dl.finish_and_clear();
    });

    let matcher = match make_matcher(cfg) {
        Ok(m)  => m,
        Err(e) => return Stage2Out::Failed { job, error: e },
    };
    let writer = match std::fs::File::create(&out_path) {
        Ok(f)  => f,
        Err(e) => return Stage2Out::Failed {
            job, error: anyhow::Error::new(e).context(format!("create {}", out_path.display())),
        },
    };
    let extract_res = stream_extract(pipe_rx, matcher, cfg.max_line_bytes, writer, is_gzip).await;
    let _ = teer.await;
    let stats = match extract_res {
        Ok((_file, s)) => s,
        Err(e) => return Stage2Out::Failed {
            job, error: e.context(format!("stream_extract {}", out_path.display())),
        },
    };
    let sha = match hasher.await {
        Ok(s) => s,
        Err(e) => return Stage2Out::Failed { job, error: anyhow::Error::new(e).context("hasher join") },
    };

    // Optional store dedup + transitions.
    if let Some(s) = store {
        match enqueue_and_advance(s, cfg, &job, &sha, &stats, &out_path, &format) {
            Ok(true)  => return Stage2Out::Deduped { job, sha256: sha },
            Ok(false) => {}
            Err(e)    => return Stage2Out::Failed { job, error: e },
        }
    }

    Stage2Out::Ready {
        job, sha256: sha, output_path: out_path,
        lines_scanned: stats.lines_scanned,
        lines_matched: stats.lines_matched,
        format,
    }
}

async fn handle_disk(
    store:   Option<&Store>,
    cfg:     &PipelineConfig,
    job:     Job,
    format:  Format,
    temp:    tempfile::NamedTempFile,
) -> Stage2Out {
    use std::io::Read;

    // 1. Hash the spilled compressed bytes for file-level dedup. The
    //    compressed stream is the canonical identity here — two zips
    //    that decompress to identical entries but have different DEFLATE
    //    levels are treated as distinct (matches the txt/gz path which
    //    hashes the raw download bytes pre-decompression).
    let temp_path = temp.path().to_path_buf();
    let sha = match tokio::task::spawn_blocking({
        let p = temp_path.clone();
        move || -> anyhow::Result<String> {
            let mut f = std::fs::File::open(&p)
                .with_context(|| format!("reopen spill {} for hashing", p.display()))?;
            let mut h = Sha256::new();
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = f.read(&mut buf).context("read spill")?;
                if n == 0 { break; }
                h.update(&buf[..n]);
            }
            Ok(hex::encode(h.finalize()))
        }
    }).await {
        Ok(Ok(s))  => s,
        Ok(Err(e)) => return Stage2Out::Failed { job, error: e.context("hash spill") },
        Err(e)     => return Stage2Out::Failed {
            job,
            error: anyhow::anyhow!("hash spill task panicked: {e}"),
        },
    };

    // 2. Build output path + matcher (mirrors handle_stream Steps 1–3).
    let (out_path, chat_dir) = match build_output_path(cfg, &job) {
        Ok(p)  => p,
        Err(e) => return Stage2Out::Failed { job, error: e },
    };
    if let Err(e) = std::fs::create_dir_all(&chat_dir) {
        return Stage2Out::Failed {
            job,
            error: anyhow::Error::new(e).context(format!("mkdir {}", chat_dir.display())),
        };
    }
    // `make_matcher` already returns `Arc<Matcher>`; do NOT wrap again.
    let matcher = match make_matcher(cfg) {
        Ok(m)  => m,
        Err(e) => return Stage2Out::Failed { job, error: e },
    };

    // 3. Bridge: disk_extract wants a `Receiver<Bytes>`; we have a
    //    NamedTempFile. Stream the spill into a synthetic receiver on a
    //    blocking thread so disk_extract's existing read pipeline is
    //    unchanged. blocking_send REQUIRES a multi-threaded runtime
    //    (`#[tokio::main]` defaults to multi-thread; tests must use
    //    `#[tokio::test(flavor = "multi_thread", worker_threads = N)]`).
    let (bridge_tx, bridge_rx) = mpsc::channel::<Bytes>(cfg.intra_file_channel_capacity);
    let temp_for_pump = temp_path.clone();
    let pump_join = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut f = std::fs::File::open(&temp_for_pump)
            .with_context(|| format!("reopen spill {} for pump", temp_for_pump.display()))?;
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = f.read(&mut buf).context("read spill chunk")?;
            if n == 0 { break; }
            let chunk = Bytes::copy_from_slice(&buf[..n]);
            if bridge_tx.blocking_send(chunk).is_err() {
                return Ok(());
            }
        }
        Ok(())
    });

    // 4. Run disk_extract.
    let extract_res = crate::pipeline::disk::disk_extract(
        bridge_rx,
        matcher,
        cfg.max_line_bytes,
        cfg.max_uncompressed_bytes,
        &out_path,
    ).await;

    // Surface pump panics AFTER disk_extract finishes (extract error wins).
    if let Err(e) = pump_join.await {
        if extract_res.is_ok() {
            return Stage2Out::Failed {
                job,
                error: anyhow::anyhow!("zip pump task panicked: {e}"),
            };
        }
    }

    let stats = match extract_res {
        Ok(s)  => s,
        Err(e) => return Stage2Out::Failed { job, error: e },
    };

    // 5. Optional store dedup + transitions. Reuse `enqueue_and_advance` so
    //    logging / cleanup / FileMeta construction stays symmetric with
    //    handle_stream. `disk_extract`'s `DiskExtractStats` carries
    //    `lines_scanned`/`lines_matched` plus extras we don't need; project
    //    to `extractor_core::ScanStats`.
    let scan_stats = extractor_core::ScanStats {
        lines_scanned: stats.lines_scanned,
        lines_matched: stats.lines_matched,
        bytes_scanned: stats.bytes_scanned,
    };
    if let Some(s) = store {
        match enqueue_and_advance(s, cfg, &job, &sha, &scan_stats, &out_path, &format) {
            Ok(true) => {
                drop(temp);
                return Stage2Out::Deduped { job, sha256: sha };
            }
            Ok(false) => {}
            Err(e)    => return Stage2Out::Failed { job, error: e },
        }
    }

    // 6. RAII drop of `temp` deletes the spill at function return.
    drop(temp);
    Stage2Out::Ready {
        job,
        sha256: sha,
        output_path: out_path,
        lines_scanned: stats.lines_scanned,
        lines_matched: stats.lines_matched,
        format,
    }
}

fn build_output_path(cfg: &PipelineConfig, job: &Job) -> anyhow::Result<(PathBuf, PathBuf)> {
    let chat_dir = cfg.output_dir.join(job.source_chat_id.to_string());
    let stem     = sanitize(&job.info.original_name);
    let stem     = strip_known_ext(&stem);
    let out_name = format!("{}_{}.out", job.source_msg_id, stem);
    let out_path = join_safe(&chat_dir, &out_name)
        .with_context(|| format!("join_safe under {}", chat_dir.display()))?;
    Ok((out_path, chat_dir))
}

fn strip_known_ext(name: &str) -> String {
    for ext in [".txt", ".gz", ".zip"] {
        if let Some(stripped) = name.strip_suffix(ext) { return stripped.into(); }
    }
    name.into()
}

fn make_matcher(cfg: &PipelineConfig) -> anyhow::Result<std::sync::Arc<extractor_core::Matcher>> {
    let mode = match cfg.matcher_mode.as_str() {
        "plain" => extractor_core::Mode::Plain,
        "url"   => extractor_core::Mode::Url,
        other   => anyhow::bail!("invalid matcher_mode {other:?}; expected 'plain' or 'url'"),
    };
    Ok(std::sync::Arc::new(
        extractor_core::Matcher::new(&cfg.matcher_key, mode)
            .context("Matcher::new")?,
    ))
}

/// Returns `Ok(true)` iff the row was already done (dedup short-circuit;
/// caller emits `Stage2Out::Deduped`). Returns `Ok(false)` to mean
/// "proceed to upload".
fn enqueue_and_advance(
    s:        &Store,
    cfg:      &PipelineConfig,
    job:      &Job,
    sha:      &str,
    stats:    &ScanStats,
    out_path: &Path,
    format:   &Format,
) -> anyhow::Result<bool> {
    let meta = FileMeta {
        sha256:         sha.to_string(),
        source_chat_id: job.source_chat_id,
        source_msg_id:  job.source_msg_id,
        original_name:  job.info.original_name.clone(),
        size_bytes:     job.info.size_bytes,
        format:         format_label(format).into(),
        matcher_key:    cfg.matcher_key.clone(),
        matcher_mode:   cfg.matcher_mode.clone(),
    };
    match s.try_enqueue(&meta).context("try_enqueue")? {
        EnqueueResult::AlreadyDone => {
            tracing::info!(sha256 = %sha, "interfile: dedup hit (file already done)");
            let _ = std::fs::remove_file(out_path);
            return Ok(true);
        }
        EnqueueResult::InProgress(state) => {
            tracing::warn!(sha256 = %sha, state = %state,
                "interfile: another run is processing this file; proceeding (last-writer wins)");
        }
        EnqueueResult::New => {}
    }
    s.mark_downloading(sha)?;
    s.mark_downloaded(sha)?;
    s.mark_extracted(sha, stats.lines_scanned, stats.lines_matched, out_path)?;
    Ok(false)
}

fn format_label(f: &Format) -> &'static str {
    match f {
        Format::Txt => "txt",
        Format::Gz  => "gz",
        Format::Zip => "zip",
        Format::Unknown => "unknown",
    }
}

/// Stage 3 task body. Drains `Stage2Out` items off `s2_rx` and either
/// (a) forwards `Failed` / `Deduped` directly to `on_outcome`, or
/// (b) for `Ready`, dispatches a single-job `pipeline::upload::run` and
/// folds its `UploadOutcome` into `JobOutcome` before invoking
/// `on_outcome`. Outcomes fire in strict FIFO input order — Stage 3
/// processes one `Stage2Out` at a time, awaiting `upload::run` to
/// completion before pulling the next item, so `cmd::watch` and
/// `cmd::backfill` cursor monotonicity holds.
pub async fn upload_stage<C: TelegramClient + ?Sized>(
    client:     &C,
    cfg:        &PipelineConfig,
    mut s2_rx:  mpsc::Receiver<Stage2Out>,
    on_outcome: CursorAdvance,
) -> Result<()> {
    let upload_cfg = UploadRunConfig {
        target_chat_id:        cfg.target_chat_id,
        upload_max_size_bytes: cfg.upload_max_size_bytes,
        upload_rate_seconds:   cfg.upload_rate_seconds,
        retry:                 upload::RetryPolicy::default(),
    };

    while let Some(s2) = s2_rx.recv().await {
        match s2 {
            Stage2Out::Failed { job, error } => {
                on_outcome(JobOutcome {
                    job,
                    kind: OutcomeKind::Failed { error: format!("{error:#}") },
                });
            }
            Stage2Out::Deduped { job, sha256 } => {
                on_outcome(JobOutcome {
                    job,
                    kind: OutcomeKind::Deduped { sha256 },
                });
            }
            Stage2Out::Ready {
                job, sha256, output_path,
                lines_scanned, lines_matched, format: _format,
            } => {
                // Upload bar: presence signals "this file is mid-upload",
                // length is the prepared output size. The grammers v0.7
                // upload API does not expose a per-chunk callback, so the
                // bar does not move during the upload itself; v1.1 will
                // thread a real callback. `finish_and_clear` after
                // upload::run drops it cleanly.
                let total_up = std::fs::metadata(&output_path).map(|m| m.len()).ok();
                let label_up = format!(
                    "up {}",
                    output_path
                        .file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                );
                let pb_up = crate::pipeline::progress::make_bar(
                    cfg.progress.as_ref(),
                    &label_up,
                    total_up,
                );
                pb_up.set_message("uploading");

                let (in_tx, in_rx)       = mpsc::channel::<UploadJob>(1);
                let (out_tx, mut out_rx) = mpsc::channel::<UploadOutcome>(1);
                let caption = CaptionData {
                    original_name:  job.info.original_name.clone(),
                    source_chat_id: job.source_chat_id,
                    source_msg_id:  job.source_msg_id,
                    matcher_key:    cfg.matcher_key.clone(),
                    matcher_mode:   cfg.matcher_mode.clone(),
                    size_bytes:     job.info.size_bytes,
                    lines_scanned,
                    lines_matched,
                };
                if in_tx
                    .send(UploadJob {
                        sha256:      sha256.clone(),
                        output_path: output_path.clone(),
                        caption,
                    })
                    .await
                    .is_err()
                {
                    pb_up.finish_and_clear();
                    on_outcome(JobOutcome {
                        job,
                        kind: OutcomeKind::Failed {
                            error: "upload channel closed before send".into(),
                        },
                    });
                    continue;
                }
                drop(in_tx);

                // Capture the permanent-failure error from `upload::run`'s
                // `on_failed` callback so the resulting `OutcomeKind::Failed`
                // surfaces the real error chain rather than the generic
                // "no outcome" sentinel. `upload::run` requires
                // `F: FnMut + Send + 'static`, so the closure owns a clone
                // of the slot.
                let captured_err: Arc<Mutex<Option<anyhow::Error>>> =
                    Arc::new(Mutex::new(None));
                let captured_err_cb = captured_err.clone();
                let on_failed = move |_j: UploadJob, e: anyhow::Error| {
                    *captured_err_cb.lock().unwrap() = Some(e);
                };
                let upload_run = upload::run(client, in_rx, out_tx, &upload_cfg, on_failed);
                let drainer    = async { out_rx.recv().await };
                let (upload_res, outcome_opt) = tokio::join!(upload_run, drainer);
                pb_up.finish_and_clear();
                if let Err(e) = upload_res {
                    on_outcome(JobOutcome {
                        job,
                        kind: OutcomeKind::Failed { error: format!("{e:#}") },
                    });
                    continue;
                }
                let kind = match outcome_opt {
                    Some(UploadOutcome::Done { sha256, output_msg_ids }) =>
                        OutcomeKind::Uploaded { sha256, output_msg_ids },
                    Some(UploadOutcome::Skipped { sha256, reason }) =>
                        OutcomeKind::Failed {
                            error: format!("upload skipped ({reason}) for {sha256}"),
                        },
                    None => {
                        // `out_tx` was dropped without a `Done`/`Skipped` send.
                        // That can only happen via the `on_failed` path inside
                        // `upload::run`, so the captured error is the real
                        // diagnostic; fall back to the generic sentinel only
                        // if the slot is unexpectedly empty.
                        let err = captured_err.lock().unwrap().take();
                        let msg = err
                            .map(|e| format!("{e:#}"))
                            .unwrap_or_else(|| {
                                "upload produced no outcome (permanent failure)".into()
                            });
                        OutcomeKind::Failed { error: msg }
                    }
                };
                on_outcome(JobOutcome { job, kind });
            }
        }
    }
    Ok(())
}
