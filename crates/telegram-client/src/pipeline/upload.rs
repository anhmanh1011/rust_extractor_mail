//! Upload-stage primitives. `upload_with_retry` drives a single
//! (chat, path, caption) to completion or to budget-exhaustion.
//! `run` (Task 6.5) orchestrates a stream of `UploadJob`s.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};

use crate::telegram::TelegramClient;

/// Retry policy. `initial_backoff` is doubled on each retry, capped at
/// `max_backoff`. The actual sleep is `backoff * (1 ± jitter_ratio)`.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (inclusive of the first try).
    pub max_attempts:    u32,
    /// Backoff applied after the first failure, doubled per retry.
    pub initial_backoff: Duration,
    /// Hard ceiling on the per-attempt backoff.
    pub max_backoff:     Duration,
    /// Jitter ratio applied to each sleep: `0.10` = ±10%. `0.0` disables
    /// jitter; tests use 0 to keep timing deterministic.
    pub jitter_ratio:    f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts:    5,
            initial_backoff: Duration::from_secs(2),
            max_backoff:     Duration::from_secs(60),
            jitter_ratio:    0.10,
        }
    }
}

/// Drive a single upload until success or budget exhaustion.
///
/// Classification of errors (case-insensitive substring on the formatted
/// chain): `FLOOD_WAIT`, `TIMEOUT`, `CONNECTION`, `TEMPORARY`, `RATE_LIMIT`
/// → transient. Everything else → permanent (return immediately, no wait).
///
/// Implementer note: substring matching avoids importing grammers typed
/// errors so the seam stays mock-friendly.
pub async fn upload_with_retry<C: TelegramClient + ?Sized>(
    client: &C,
    chat_id: i64,
    local_path: &Path,
    caption: Option<&str>,
    policy: &RetryPolicy,
) -> Result<i64> {
    let mut backoff = policy.initial_backoff;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=policy.max_attempts {
        match client.upload_file(chat_id, local_path, caption).await {
            Ok(id) => return Ok(id),
            Err(e) => {
                let msg = format!("{e:#}");
                if !is_transient(&msg) {
                    return Err(e.context("permanent upload error"));
                }
                tracing::warn!(
                    attempt,
                    max = policy.max_attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %msg,
                    "transient upload error, retrying after backoff",
                );
                let sleep_for = jittered(backoff, policy.jitter_ratio);
                tokio::time::sleep(sleep_for).await;
                backoff = (backoff * 2).min(policy.max_backoff);
                last_err = Some(e);
            }
        }
    }
    Err(anyhow!(
        "upload retry budget exhausted after {} attempts: {}",
        policy.max_attempts,
        last_err.map(|e| format!("{e:#}")).unwrap_or_else(|| "unknown".into()),
    )
    .context("max_attempts reached"))
}

fn is_transient(msg: &str) -> bool {
    let m = msg.to_ascii_uppercase();
    m.contains("FLOOD_WAIT")
        || m.contains("TIMEOUT")
        || m.contains("CONNECTION")
        || m.contains("TEMPORARY")
        || m.contains("RATE_LIMIT")
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn jittered(base: Duration, ratio: f64) -> Duration {
    if ratio <= 0.0 {
        return base;
    }
    // Cheap pseudo-jitter from the system clock — we do NOT pull in `rand`
    // for this; the timing only needs to be "not lock-step".
    let nanos_seed = std::time::Instant::now().elapsed().subsec_nanos() as f64;
    let frac = (nanos_seed / 1_000_000_000.0).fract(); // 0.0..1.0
    let factor = 1.0 + (frac * 2.0 - 1.0) * ratio; // 1±ratio
    Duration::from_nanos(((base.as_nanos() as f64) * factor).max(0.0) as u64)
}

// ── Task 6.4: split_for_upload ────────────────────────────────────────────────

/// Slice a file into part files, each `<= cap_bytes`, breaking on the last
/// `\n` before the cap. Returns the list of part paths in order. If the
/// file is already `<= cap_bytes`, returns `vec![original_path]` (no copy).
///
/// Input may end without a trailing `\n`; the final line is written as-is
/// to the last part provided it does not by itself exceed `cap_bytes`.
///
/// Side effects: creates `<orig>.part01`, `<orig>.part02`, … next to `path`.
/// On error, partially-written part files are left in place — callers
/// clean them up if appropriate (typically Phase 6 logs and proceeds; the
/// local `out_path` is the source of truth).
///
/// # Errors
///
/// - `"cap_bytes must be > 0"` when `cap_bytes == 0`.
/// - `"line longer than cap"` when a single line exceeds `cap_bytes` (cannot
///   split on a line boundary).
/// - I/O errors propagated with `anyhow::Context` from `metadata`/`open`/
///   `create`/`fill_buf`/`write_all`/`flush`.
pub async fn split_for_upload(path: &Path, cap_bytes: u64) -> Result<Vec<PathBuf>> {
    if cap_bytes == 0 {
        anyhow::bail!("split_for_upload: cap_bytes must be > 0");
    }
    let total = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("metadata {}", path.display()))?
        .len();
    if total <= cap_bytes {
        return Ok(vec![path.to_path_buf()]);
    }
    let path_buf = path.to_path_buf();
    tokio::task::spawn_blocking(move || split_blocking(&path_buf, cap_bytes))
        .await
        .context("split_for_upload spawn_blocking join")?
}

#[allow(clippy::cast_possible_truncation)]
fn split_blocking(path: &Path, cap_bytes: u64) -> Result<Vec<PathBuf>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader, BufWriter, Write};

    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(64 * 1024, f);
    let mut parts: Vec<PathBuf> = Vec::new();
    let mut idx: u32 = 1;

    loop {
        let part = part_path(path, idx);
        let out = File::create(&part)
            .with_context(|| format!("create {}", part.display()))?;
        let mut writer = BufWriter::with_capacity(64 * 1024, out);
        let mut written: u64 = 0;
        let mut wrote_any_line = false;
        // Bytes written for the *current* in-progress line (no `\n` seen yet
        // for this stretch). Reset to 0 every time we land on a `\n`. Used to
        // detect lines that span multiple `fill_buf` chunks and exceed the cap.
        let mut accumulated_no_newline: u64 = 0;
        let mut eof = false;

        loop {
            let buf = reader.fill_buf().context("fill_buf")?;
            if buf.is_empty() {
                eof = true;
                break;
            }
            let remaining = cap_bytes.saturating_sub(written) as usize;
            if remaining == 0 {
                break;
            }
            let take = remaining.min(buf.len());
            let slice = &buf[..take];
            match memchr::memrchr(b'\n', slice) {
                Some(end_inclusive) => {
                    let upto = end_inclusive + 1;
                    writer.write_all(&slice[..upto]).context("write part")?;
                    written += upto as u64;
                    reader.consume(upto);
                    wrote_any_line = true;
                    accumulated_no_newline = 0;
                }
                None => {
                    if wrote_any_line {
                        // We already have at least one complete line in this
                        // part. Cut here, leaving the unconsumed bytes for the
                        // next part to read.
                        break;
                    }
                    if take < buf.len() {
                        // We hit the per-part cap (`remaining`) mid-buffer with
                        // no newline anywhere in `slice` AND more bytes still
                        // sit in `buf` — the line is genuinely longer than the
                        // cap.
                        anyhow::bail!(
                            "line longer than cap ({cap_bytes} B) at part {idx} of {}",
                            path.display(),
                        );
                    }
                    // `take == buf.len()`: the line may continue past the
                    // currently buffered chunk. Flush what we have, advance the
                    // reader, and try `fill_buf` again. If the next call yields
                    // an empty slice we're at EOF on a no-trailing-newline tail
                    // — that's allowed and handled by the post-loop check.
                    writer.write_all(slice).context("write part")?;
                    written += take as u64;
                    reader.consume(take);
                    accumulated_no_newline += take as u64;
                    if accumulated_no_newline > cap_bytes {
                        // Defensive: under the current invariants this is
                        // unreachable (`take <= remaining` keeps the running
                        // total at or below `cap_bytes`), but we keep the guard
                        // as a belt-and-braces line-length assertion.
                        anyhow::bail!(
                            "line longer than cap ({cap_bytes} B) at part {idx} of {}",
                            path.display(),
                        );
                    }
                }
            }
        }

        writer.flush().context("flush part")?;
        drop(writer);
        if written == 0 {
            std::fs::remove_file(&part).ok();
            break;
        }
        // If the part was filled mid-line (no newline ever landed) and there
        // are still bytes ahead, that means a single line exceeded `cap_bytes`
        // — bail rather than emit a part split on a non-line boundary.
        if !wrote_any_line && !eof {
            anyhow::bail!(
                "line longer than cap ({cap_bytes} B) at part {idx} of {}",
                path.display(),
            );
        }
        parts.push(part);
        idx += 1;

        if eof || reader.fill_buf().context("fill_buf eof check")?.is_empty() {
            break;
        }
    }

    Ok(parts)
}

fn part_path(orig: &Path, idx: u32) -> PathBuf {
    let mut s = orig.as_os_str().to_owned();
    s.push(format!(".part{idx:02}"));
    PathBuf::from(s)
}

// ── Task 6.5: run (job stream + on_failed callback) ───────────────────────────

/// One unit of upload work emitted by the extract stage.
///
/// `caption` is **structured data**, not a pre-rendered string: the
/// `Part i/N` line is added by `pipeline::upload::run` only when a
/// split occurs, and `caption::render` is invoked **per part** so
/// truncation to the 1024-char Telegram cap happens AFTER the Part
/// line is in place. Never post-concatenate `\nPart i/N` onto a
/// rendered caption — that bypasses the cap.
#[derive(Debug, Clone)]
pub struct UploadJob {
    /// Content hash of the source extraction; used as the dedup/identity key
    /// for `UploadOutcome` correlation downstream.
    pub sha256:      String,
    /// Local path to the output file produced by the extract stage. May be
    /// split into multiple part files by `split_for_upload` before upload.
    pub output_path: PathBuf,
    /// Structured caption data; rendered per part so the 1024-char cap is
    /// honored after `Part i/N` is appended.
    pub caption:     crate::upload::caption::CaptionData,
}

/// Result emitted on the outbound channel for downstream observers.
#[derive(Debug, Clone)]
pub enum UploadOutcome {
    /// Successful upload. `output_msg_ids` lists the Telegram message ids
    /// produced by uploading the (possibly split) output, in part order.
    Done {
        /// Source extraction hash carried over from the originating `UploadJob`.
        sha256:         String,
        /// Output Telegram message ids, one per part, in upload order.
        output_msg_ids: Vec<i64>,
    },
    /// Upload was skipped (e.g. dedup hit). Currently unused by `run`; kept
    /// for forward-compatibility with the Phase 7 dedup wiring.
    Skipped {
        /// Source extraction hash carried over from the originating `UploadJob`.
        sha256: String,
        /// Human-readable reason the upload was skipped.
        reason: String,
    },
}

/// Runtime configuration for [`run`].
#[derive(Debug, Clone)]
pub struct UploadRunConfig {
    /// Telegram chat id receiving the uploaded outputs.
    pub target_chat_id:        i64,
    /// Per-part size cap in bytes; outputs exceeding this are split via
    /// [`split_for_upload`] before upload.
    pub upload_max_size_bytes: u64,
    /// Inter-job pacing in seconds: after a successful job emits, sleep this
    /// long before pulling the next job. `0` disables pacing. Failures do
    /// NOT trigger this sleep — `run` proceeds immediately to the next job.
    pub upload_rate_seconds:   u64,
    /// Retry policy passed to [`upload_with_retry`] for each part.
    pub retry:                 RetryPolicy,
}

/// Drain `jobs` and upload each output. Emits `UploadOutcome` per job.
/// Permanent failures invoke `on_failed(job, err)` and are NOT emitted to
/// `outcomes` (the caller — Phase 7 — re-queues them via `failed_uploads`).
///
/// # Errors
///
/// Currently always returns `Ok(())` after the input channel closes; the
/// `Result` return is preserved so future error paths (e.g. fatal client
/// connection loss) can be surfaced without an API break.
pub async fn run<C, F>(
    client:    &C,
    mut jobs:  tokio::sync::mpsc::Receiver<UploadJob>,
    outcomes:  tokio::sync::mpsc::Sender<UploadOutcome>,
    cfg:       &UploadRunConfig,
    mut on_failed: F,
) -> Result<()>
where
    C: TelegramClient + ?Sized,
    // `+ 'static` is required because callers pass `run` into
    // `tokio::spawn(async move { run(...).await })`; the resulting
    // future captures `on_failed` and must satisfy `Future + 'static`.
    F: FnMut(UploadJob, anyhow::Error) + Send + 'static,
{
    while let Some(job) = jobs.recv().await {
        let res = upload_job(client, &job, cfg).await;
        match res {
            Ok(ids) => {
                if outcomes
                    .send(UploadOutcome::Done {
                        sha256:         job.sha256.clone(),
                        output_msg_ids: ids,
                    })
                    .await
                    .is_err()
                {
                    // Receiver hung up — the consumer is gone, stop draining.
                    break;
                }
                if cfg.upload_rate_seconds > 0 {
                    tokio::time::sleep(Duration::from_secs(cfg.upload_rate_seconds)).await;
                }
            }
            Err(e) => {
                tracing::warn!(
                    sha256 = %job.sha256,
                    output = %job.output_path.display(),
                    error = %format!("{e:#}"),
                    "upload job failed permanently",
                );
                on_failed(job, e);
                // do NOT pace after a failure — proceed to next job immediately.
            }
        }
    }
    Ok(())
}

async fn upload_job<C: TelegramClient + ?Sized>(
    client: &C,
    job:    &UploadJob,
    cfg:    &UploadRunConfig,
) -> Result<Vec<i64>> {
    let parts = split_for_upload(&job.output_path, cfg.upload_max_size_bytes)
        .await
        .with_context(|| format!("split {}", job.output_path.display()))?;
    let n = parts.len() as u32;
    let mut ids = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        // Render PER PART so the 1024-char truncation in `caption::render`
        // sees the final caption (including any `Part i/N` line) and the
        // resulting text never exceeds Telegram's hard cap.
        let (pi, pt) = if n > 1 { (Some(i as u32 + 1), Some(n)) } else { (None, None) };
        let input = job.caption.input(pi, pt);
        let cap = crate::upload::caption::render(&input);
        let id = upload_with_retry(client, cfg.target_chat_id, part, Some(&cap), &cfg.retry)
            .await
            .with_context(|| format!("upload part {} of {}", i + 1, n))?;
        ids.push(id);
    }
    Ok(ids)
}
