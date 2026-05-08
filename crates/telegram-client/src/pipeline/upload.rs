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
