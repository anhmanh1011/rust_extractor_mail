//! Spec §9.2 line 602 (`pipeline_zip.rs`): tempfile zip with N entries;
//! assert tempfile deleted post-run; assert per-archive cumulative cap.

use std::sync::Arc;

use bytes::Bytes;
use extractor_core::{Matcher, Mode};
use telegram_client::pipeline::disk::disk_extract;

const MAX_LINE_BYTES: usize = 64 * 1024;
const TEN_GB: u64 = 10 * 1024 * 1024 * 1024;

fn build_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    use std::io::Write;
    use zip::write::FileOptions;
    let mut buf: Vec<u8> = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut buf);
        let mut zip = zip::ZipWriter::new(cursor);
        let opts = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        for (name, body) in entries {
            zip.start_file(*name, opts).unwrap();
            zip.write_all(body).unwrap();
        }
        zip.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn three_text_entries_extracted_in_order() {
    let zip_bytes = build_zip(&[
        ("a.txt", b"target.com:a@a.com:p1\nnoise\n"),
        ("b.txt", b"target.com:b@b.com:p2\nnoise\n"),
        ("c.txt", b"noise\nnoise\n"),
    ]);

    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from(zip_bytes)).await.unwrap();
    drop(tx);

    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("merged.out");
    let stats = disk_extract(rx, m, MAX_LINE_BYTES, TEN_GB, &out)
        .await
        .unwrap();

    assert_eq!(stats.lines_matched, 2);
    assert_eq!(stats.entries_processed, 3);

    let body = std::fs::read(&out).unwrap();
    // Order: per-entry, in archive order.
    assert_eq!(body, b"a@a.com:p1\nb@b.com:p2\n");
}

#[tokio::test]
async fn nontext_entries_skipped_without_failing() {
    let zip_bytes = build_zip(&[
        ("a.txt", b"target.com:hit@x.com:p\n"),
        ("ignored.bin", b"\x00\x01\x02\xff"),
        ("ignored.jpg", b"jpeg-noise"),
    ]);

    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from(zip_bytes)).await.unwrap();
    drop(tx);

    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("merged.out");
    let stats = disk_extract(rx, m, MAX_LINE_BYTES, TEN_GB, &out)
        .await
        .unwrap();
    assert_eq!(stats.entries_processed, 1);
    assert_eq!(stats.entries_skipped, 2);
    assert_eq!(std::fs::read(&out).unwrap(), b"hit@x.com:p\n");
}

#[tokio::test]
async fn zip_bomb_per_archive_cumulative_cap_breached_aborts() {
    // Two 4 KiB entries, cap at 6 KiB cumulative — the second entry
    // breaches mid-decode and must abort.
    let body = vec![b'A'; 4096];
    let zip_bytes = build_zip(&[("e1.txt", &body), ("e2.txt", &body)]);

    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from(zip_bytes)).await.unwrap();
    drop(tx);

    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("merged.out");
    let err = disk_extract(rx, m, MAX_LINE_BYTES, 6 * 1024, &out)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("max_uncompressed_bytes") || msg.contains("zip bomb"),
        "expected bomb cap error, got: {msg}",
    );
}

#[tokio::test]
async fn tempfile_is_deleted_after_success() {
    // We can only check that no tempfile lingers in the OS temp dir
    // matching our prefix. Use a unique prefix the disk_extract honours.
    let zip_bytes = build_zip(&[("a.txt", b"target.com:hit@x.com:p\n")]);
    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from(zip_bytes)).await.unwrap();
    drop(tx);

    let snap_before = list_temp_prefix("tg-extract-spill-");
    let tmp = tempfile::tempdir().unwrap();
    let _ = disk_extract(rx, m, MAX_LINE_BYTES, TEN_GB, &tmp.path().join("o.out"))
        .await
        .unwrap();
    let snap_after = list_temp_prefix("tg-extract-spill-");
    assert_eq!(snap_before, snap_after, "tempfile leaked");
}

fn list_temp_prefix(prefix: &str) -> Vec<std::path::PathBuf> {
    let dir = std::env::temp_dir();
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for entry in rd.flatten() {
            if entry.file_name().to_string_lossy().starts_with(prefix) {
                v.push(entry.path());
            }
        }
    }
    v.sort();
    v
}

#[tokio::test]
async fn entry_with_traversal_filename_is_neutralised() {
    // The zip writer happily creates entries named "../../etc/passwd".
    // The extractor MUST NOT write outside the configured directory; the
    // entry is logged but not skipped — its lines still feed the merged
    // output (which lives inside the safe path). What matters is that no
    // path is constructed for the entry itself.
    let zip_bytes = build_zip(&[("../../etc/passwd", b"target.com:hit@x.com:p\n")]);
    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from(zip_bytes)).await.unwrap();
    drop(tx);

    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("merged.out");
    let _ = disk_extract(rx, m, MAX_LINE_BYTES, TEN_GB, &out)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), b"hit@x.com:p\n");
    // No file under /etc, no file at tmp/../etc, etc. (negative assertion
    // is structural — disk_extract never opens an entry-named file.)
}

#[tokio::test]
async fn aborted_extract_removes_partial_output() {
    use std::io::Write;
    use zip::write::FileOptions;
    let body_e1 = b"target.com:hit1@x.com:p1\ntarget.com:hit2@x.com:p2\n";
    let body_e2 = vec![b'A'; 8 * 1024];
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("a.txt", opts).unwrap();
        zip.write_all(body_e1).unwrap();
        zip.start_file("b.txt", opts).unwrap();
        zip.write_all(&body_e2).unwrap();
        zip.finish().unwrap();
    }
    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from(buf)).await.unwrap();
    drop(tx);
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("partial.out");
    let cap_below_e2 = 4 * 1024;
    let err = disk_extract(rx, m, MAX_LINE_BYTES, cap_below_e2, &out)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("max_uncompressed_bytes"));
    assert!(
        !out.exists(),
        "partial output {} must be removed on abort",
        out.display(),
    );
}
