//! `tg-extract retry-uploads` — drain failed_uploads through the upload stage.
//!
//! Issue 6 fix: instead of bypassing `pipeline::upload::run` with
//! `upload_with_retry` and `Some("")`, this command reconstructs the original
//! `CaptionData` by JOINing `failed_uploads → files`. All eight caption
//! source-of-truth fields (original_name, source_chat_id, source_msg_id,
//! matcher_key, matcher_mode, size_bytes, lines_scanned, lines_matched) live
//! in the `files` row, which `cmd::fetch` populated via `mark_extracted`
//! BEFORE attempting upload. So even if upload crashed, the caption is
//! reconstructable — no need to widen the `failed_uploads` schema.
//!
//! Issue 8 fix: the caller passes `&Store`. Opening a second connection
//! against the same WAL-mode DB while `main.rs` holds the primary handle is
//! wasteful and risks lock contention; the entry point used by `main.rs`
//! is `run_with_store_and_client(&store, ...)`. The `cfg`-only convenience
//! form is dropped.

use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::pipeline::upload::{UploadJob, UploadOutcome, UploadRunConfig};
use crate::store::Store;
use crate::telegram::TelegramClient;
use crate::upload::caption::CaptionData;

/// Materialised join row: `failed_uploads.sha256` × `files.*`.
#[derive(Debug, Clone)]
struct RetryRow {
    sha256:      String,
    output_path: PathBuf,
    caption:     CaptionData,
}

fn list_retry_rows(store: &Store) -> Result<Vec<RetryRow>> {
    let conn = store.lock();
    let mut stmt = conn.prepare(
        "SELECT
             fu.sha256,
             fu.output_path,
             f.original_name,
             f.source_chat_id,
             f.source_msg_id,
             f.matcher_key,
             f.matcher_mode,
             f.size_bytes,
             COALESCE(f.lines_scanned, 0),
             COALESCE(f.lines_matched, 0)
           FROM failed_uploads fu
           JOIN files f ON f.sha256 = fu.sha256
          ORDER BY fu.last_attempt_at ASC",
    ).context("prepare retry-uploads JOIN")?;
    let rows = stmt.query_map([], |r| {
        Ok(RetryRow {
            sha256:      r.get(0)?,
            output_path: PathBuf::from(r.get::<_, String>(1)?),
            caption: CaptionData {
                original_name:  r.get(2)?,
                source_chat_id: r.get(3)?,
                source_msg_id:  r.get(4)?,
                matcher_key:    r.get(5)?,
                matcher_mode:   r.get(6)?,
                size_bytes:     u64::try_from(r.get::<_, i64>(7)?).unwrap_or(0),
                lines_scanned:  u64::try_from(r.get::<_, i64>(8)?).unwrap_or(0),
                lines_matched:  u64::try_from(r.get::<_, i64>(9)?).unwrap_or(0),
            },
        })
    }).context("query retry-uploads JOIN")?;
    let mut out = Vec::new();
    for r in rows { out.push(r.context("row")?); }
    Ok(out)
}

/// Drain pending `failed_uploads` rows back through `pipeline::upload::run`.
///
/// On success: `mark_uploaded(sha, head_msg_id)` then `clear_failed_upload(sha)`.
/// On permanent failure: `enqueue_failed_upload(sha, path, err_str)` (the
/// UPSERT increments `attempts`).
/// If the output_path is missing (operator deleted it): clear the row and skip
/// — there is nothing to retry.
pub async fn run_with_store_and_client<C: TelegramClient>(
    store:  &Store,
    client: &C,
    cfg:    &UploadRunConfig,
) -> Result<()> {
    let rows = list_retry_rows(store).context("list retry rows")?;
    if rows.is_empty() {
        tracing::info!("retry-uploads: nothing pending");
        return Ok(());
    }
    tracing::info!(count = rows.len(), "retry-uploads: starting drain");

    for row in rows {
        if !row.output_path.exists() {
            tracing::warn!(
                sha256 = %row.sha256,
                path   = %row.output_path.display(),
                "retry-uploads: output_path missing — clearing failed row",
            );
            let _ = store.clear_failed_upload(&row.sha256);
            continue;
        }

        // Route through the same `pipeline::upload::run` helper as
        // `cmd::fetch::run_single_upload`: we get the >2 GB split + per-part
        // `Part i/N` caption rendering for free. Single-element channels
        // because each retry row is a one-shot job.
        let (jt, jr)     = tokio::sync::mpsc::channel::<UploadJob>(1);
        let (ot, mut or) = tokio::sync::mpsc::channel::<UploadOutcome>(1);
        let job = UploadJob {
            sha256:      row.sha256.clone(),
            output_path: row.output_path.clone(),
            caption:     row.caption.clone(),
        };
        jt.send(job).await.context("send retry upload job")?;
        drop(jt);

        // Same `+ 'static` constraint as cmd::fetch: bus failures out via
        // sync mpsc, drain after `run` returns.
        let (failed_tx, failed_rx) =
            std::sync::mpsc::channel::<(UploadJob, String)>();
        let on_failed = move |j: UploadJob, e: anyhow::Error| {
            let _ = failed_tx.send((j, format!("{e:#}")));
        };
        let result = crate::pipeline::upload::run(client, jr, ot, cfg, on_failed).await;

        // Drain the outcome channel (Done outcomes mark the row 'done').
        let mut succeeded = false;
        while let Some(o) = or.recv().await {
            if let UploadOutcome::Done { sha256: s, output_msg_ids } = o {
                let head = output_msg_ids.first().copied().unwrap_or_else(|| {
                    tracing::error!(sha256 = %s, "retry: Done outcome had empty \
                        output_msg_ids; recording 0 — investigate upload::run");
                    0
                });
                store.mark_uploaded(&s, head).with_context(|| format!("mark_uploaded sha256={s}"))?;
                store.clear_failed_upload(&s).with_context(|| format!("clear_failed_upload sha256={s}"))?;
                succeeded = true;
            }
        }

        // Drain the failure bus.
        while let Ok((j, err_str)) = failed_rx.try_recv() {
            store.enqueue_failed_upload(&j.sha256, &j.output_path, &err_str)
                .with_context(|| format!("enqueue_failed_upload sha256={}", j.sha256))?;
        }

        // If `run` itself errored (transport-level), record it under the row
        // we just consumed. `succeeded` short-circuits this so a race where
        // both Done and a transient `run` error fire doesn't double-count.
        if !succeeded {
            if let Err(e) = result {
                store.enqueue_failed_upload(
                    &row.sha256, &row.output_path, &format!("{e:#}"),
                ).with_context(|| format!("enqueue_failed_upload sha256={}", row.sha256))?;
            }
        }
    }
    Ok(())
}
