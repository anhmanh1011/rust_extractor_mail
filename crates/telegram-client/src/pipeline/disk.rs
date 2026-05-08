//! Disk-spill extraction path (spec §4.1, §11.2).
//!
//! 1. Spill the `Bytes` stream to a `NamedTempFile` (RAII delete).
//! 2. Open as `zip::ZipArchive`.
//! 3. For each entry:
//!     - skip non-text (extension not in {.txt, .gz}).
//!     - feed decompressed bytes into Scanner against the merged output.
//!     - track cumulative uncompressed bytes; abort archive on cap breach.
//! 4. Drop the tempfile (delete) at the end of scope.

use std::fs::OpenOptions;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::mpsc::sync_channel;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use extractor_core::{Matcher, ScanStats, Scanner};

use crate::pipeline::sink::WriterSink;

const TEMP_PREFIX: &str = "tg-extract-spill-";
const READ_BUFFER: usize = 64 * 1024;
const SPILL_BRIDGE_CAP: usize = 4;

/// Aggregate counters returned by [`disk_extract`] across all entries
/// processed in a single archive.
#[derive(Debug, Default, Clone, Copy)]
pub struct DiskExtractStats {
    /// Total scanned lines summed across every accepted entry.
    pub lines_scanned: u64,
    /// Total matched lines emitted to the merged output sink.
    pub lines_matched: u64,
    /// Total uncompressed bytes fed through the scanner.
    pub bytes_scanned: u64,
    /// Number of entries whose body was decompressed and scanned.
    pub entries_processed: u32,
    /// Number of entries skipped because their extension was not `.txt`/`.gz`.
    pub entries_skipped: u32,
}

/// Extract matching lines from a zipped byte stream into `out_path`.
///
/// The download is spilled to a `tempfile::NamedTempFile` (deleted by RAII),
/// re-opened as a `zip::ZipArchive`, and each `.txt`/`.gz` entry is fed
/// through the same `Scanner` infrastructure used by [`crate::pipeline::stream::stream_extract`].
/// Entries whose extension is neither `.txt` nor `.gz` (case-insensitive) are
/// silently skipped.
///
/// `max_uncompressed_bytes` is enforced **cumulatively across the entire
/// archive**, not per-entry. This blocks adversarial archives whose
/// individual entries are each just under a per-entry cap.
///
/// Path-traversal-named entries (e.g. `../../etc/passwd`) are neutralised:
/// the entry's bytes still feed the merged output (which lives at the
/// caller-supplied `out_path`), but no filesystem path is ever constructed
/// from `entry.name()`.
pub async fn disk_extract<P: AsRef<Path>>(
    mut chunks: tokio::sync::mpsc::Receiver<Bytes>,
    matcher: Arc<Matcher>,
    max_line_bytes: usize,
    max_uncompressed_bytes: u64,
    out_path: P,
) -> Result<DiskExtractStats> {
    let out_path = out_path.as_ref().to_path_buf();

    // 1. Create the tempfile on a blocking thread.
    let spill = tokio::task::spawn_blocking(|| -> Result<tempfile::NamedTempFile> {
        tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempfile()
            .context("tempfile create")
    })
    .await
    .context("spill create task panicked")??;

    // 2. Spill download bytes via a bridge thread — never sync-write on the
    //    tokio reactor. Pattern mirrors `pipeline::stream::stream_extract`.
    //    The bridge thread owns a duped fd of the tempfile; the original
    //    `NamedTempFile` handle stays on the tokio side so its RAII Drop
    //    runs after the extract closure completes.
    let writer_fd = spill.as_file().try_clone().context("dup spill fd")?;
    let (bridge_tx, bridge_rx) = sync_channel::<Bytes>(SPILL_BRIDGE_CAP);
    let writer_join = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut w = writer_fd;
        while let Ok(c) = bridge_rx.recv() {
            w.write_all(&c).context("write spill")?;
        }
        w.flush().context("flush spill")
    });

    while let Some(c) = chunks.recv().await {
        let tx = bridge_tx.clone();
        let send_res = tokio::task::spawn_blocking(move || tx.send(c))
            .await
            .context("spill bridge join")?;
        if send_res.is_err() {
            // Writer thread bailed; stop pumping. The error surfaces from
            // the writer_join below.
            break;
        }
    }
    drop(bridge_tx);
    writer_join.await.context("spill writer join")??;

    // 3. Open + extract on a blocking thread (zip + scan are CPU work).
    //    The `spill` `NamedTempFile` is moved into this closure so its
    //    Drop (which deletes the file) runs after the archive is closed.
    let cleanup_path = out_path.clone();
    let join = tokio::task::spawn_blocking(move || -> Result<DiskExtractStats> {
        let mut spill = spill;
        spill
            .as_file_mut()
            .seek(SeekFrom::Start(0))
            .context("seek 0")?;
        let reader = BufReader::with_capacity(READ_BUFFER, spill.reopen().context("reopen spill")?);
        let mut archive = zip::ZipArchive::new(reader).context("ZipArchive::new")?;

        let writer = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&out_path)
            .with_context(|| format!("create {}", out_path.display()))?;
        let mut sink = WriterSink::new(writer);

        let mut total = DiskExtractStats::default();
        let mut cumulative_uncompressed: u64 = 0;

        // Deref the Arc<Matcher> once for the closure scope; Scanner<'m>
        // borrows it for each entry's iteration.
        let matcher_ref: &Matcher = &matcher;

        for i in 0..archive.len() {
            // Inspect entry name without trusting it as a path.
            let name = match archive.by_index_raw(i) {
                Ok(e) => e.name().to_string(),
                Err(e) => {
                    tracing::warn!("zip by_index_raw({i}): {e}");
                    continue;
                }
            };
            // Take the basename (last path component) separator-agnostically
            // so the extension check works for both Unix ("a/b.txt") and
            // Windows ("a\\b.txt") entry names. This is name-only inspection
            // — no filesystem path is ever built from `name`.
            let basename = name.rsplit(['/', '\\']).next().unwrap_or(&name);
            let lower = basename.to_ascii_lowercase();
            let is_gzip = lower.ends_with(".gz");
            let is_txt = lower.ends_with(".txt");
            // Extension-less basenames (no `.`) are treated as text-like.
            // Path-traversal-named entries such as `../../etc/passwd` land
            // here — the bytes still feed the merged output but no path is
            // built from the entry's name.
            let extless = !lower.contains('.');
            if !is_gzip && !is_txt && !extless {
                total.entries_skipped += 1;
                continue;
            }

            let mut entry = archive
                .by_index(i)
                .with_context(|| format!("by_index({i})"))?;
            let mut scanner = Scanner::with_max_line(matcher_ref, max_line_bytes);
            let mut entry_stats = ScanStats::default();

            if is_gzip {
                let mut decoder = flate2::read::GzDecoder::new(&mut entry);
                cumulative_uncompressed = scan_into(
                    &mut decoder,
                    &mut scanner,
                    &mut sink,
                    &mut entry_stats,
                    cumulative_uncompressed,
                    max_uncompressed_bytes,
                )?;
            } else {
                cumulative_uncompressed = scan_into(
                    &mut entry,
                    &mut scanner,
                    &mut sink,
                    &mut entry_stats,
                    cumulative_uncompressed,
                    max_uncompressed_bytes,
                )?;
            }

            let s = scanner
                .finish(&mut sink)
                .context("scanner.finish (entry)")?;
            entry_stats += s;

            total.lines_scanned += entry_stats.lines_scanned;
            total.lines_matched += entry_stats.lines_matched;
            total.bytes_scanned += entry_stats.bytes_scanned;
            total.entries_processed += 1;
        }

        sink.flush().context("flush sink")?;
        // RAII: spill goes out of scope here → file deleted.
        Ok(total)
    });

    let inner = join.await.context("disk extract task panicked")?;
    match inner {
        Ok(stats) => Ok(stats),
        Err(e) => {
            // Best-effort: remove the (possibly partial) merged output so
            // the next attempt does not append to confusing leftovers.
            // Failure to remove (e.g. file never created) is not fatal.
            if let Err(rm_err) = std::fs::remove_file(&cleanup_path) {
                if rm_err.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        "failed to remove partial output {}: {}",
                        cleanup_path.display(),
                        rm_err,
                    );
                }
            }
            Err(e)
        }
    }
}

fn scan_into<R: Read, W: Write>(
    src: &mut R,
    scanner: &mut Scanner,
    sink: &mut WriterSink<W>,
    stats: &mut ScanStats,
    mut cumulative: u64,
    cap: u64,
) -> Result<u64> {
    let mut buf = vec![0u8; READ_BUFFER];
    loop {
        let n = src.read(&mut buf).context("decompress entry read")?;
        if n == 0 {
            return Ok(cumulative);
        }
        cumulative = cumulative.saturating_add(n as u64);
        if cumulative > cap {
            bail!("max_uncompressed_bytes breach (zip bomb): {cumulative} > {cap}");
        }
        let s = scanner
            .feed(&buf[..n], sink)
            .context("scanner.feed (entry)")?;
        *stats += s;
    }
}
