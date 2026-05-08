//! Upload-stage primitives. `upload_with_retry` drives a single
//! (chat, path, caption) to completion or to budget-exhaustion.
//! `run` (Task 6.5) orchestrates a stream of `UploadJob`s.

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Result};

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

use anyhow::Context as _;
use std::path::PathBuf;

/// Slice a file into part files, each `<= cap_bytes`, breaking on the last
/// `\n` before the cap. Returns the list of part paths in order. If the
/// file is already `<= cap_bytes`, returns `vec![original_path]` (no copy).
///
/// Side effects: creates `<orig>.part01`, `<orig>.part02`, … next to `path`.
/// On error, partially-written part files are left in place — callers
/// clean them up if appropriate (typically Phase 6 logs and proceeds; the
/// local `out_path` is the source of truth).
///
/// # Errors
///
/// - `"line longer than cap"` when a single line exceeds `cap_bytes` (cannot
///   split on a line boundary).
/// - I/O errors propagated with `anyhow::Context` from `metadata`/`open`/
///   `create`/`fill_buf`/`write_all`/`flush`.
pub async fn split_for_upload(path: &Path, cap_bytes: u64) -> Result<Vec<PathBuf>> {
    let total = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("metadata {}", path.display()))?
        .len();
    if total <= cap_bytes {
        return Ok(vec![path.to_path_buf()]);
    }
    let path_buf = path.to_path_buf();
    let cap = cap_bytes;
    tokio::task::spawn_blocking(move || split_blocking(&path_buf, cap))
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

        loop {
            let buf = reader.fill_buf().context("fill_buf")?;
            if buf.is_empty() {
                break;
            }
            let remaining = cap_bytes.saturating_sub(written) as usize;
            if remaining == 0 {
                break;
            }
            let take = remaining.min(buf.len());
            let slice = &buf[..take];
            let last_nl = memchr::memrchr(b'\n', slice);
            match last_nl {
                Some(end_inclusive) => {
                    let upto = end_inclusive + 1;
                    writer.write_all(&slice[..upto]).context("write part")?;
                    written += upto as u64;
                    reader.consume(upto);
                    wrote_any_line = true;
                }
                None => {
                    if !wrote_any_line {
                        anyhow::bail!(
                            "line longer than cap ({cap_bytes} B) at part {idx} of {}",
                            path.display(),
                        );
                    }
                    break;
                }
            }
        }

        writer.flush().context("flush part")?;
        drop(writer);
        if written == 0 {
            std::fs::remove_file(&part).ok();
            break;
        }
        parts.push(part);
        idx += 1;

        if reader.fill_buf().context("fill_buf eof check")?.is_empty() {
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
