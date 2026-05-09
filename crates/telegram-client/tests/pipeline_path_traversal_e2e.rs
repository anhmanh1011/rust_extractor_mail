//! Phase-10 regression. A zip whose entry name is `../../etc/passwd`
//! must NOT cause `interfile::run` to write a file outside `output_dir`.
//! The matched lines from the entry SHOULD still appear in the merged
//! output (`<output_dir>/<chat>/<msg>_<stem>.out`) — only the entry name
//! itself is rejected as a path component.

use std::path::Path;
use std::sync::Arc;

use telegram_client::pipeline::interfile::{
    self, CursorAdvance, Job, JobOutcome,
};
use telegram_client::store::Store;
use telegram_client::telegram::mock::MockClient;
use telegram_client::telegram::MessageInfo;

mod common;
use common::cfg_with_dir;

fn build_traversal_zip() -> Vec<u8> {
    use std::io::Write;
    let cur = std::io::Cursor::new(Vec::new());
    let mut zw = zip::ZipWriter::new(cur);
    let opts: zip::write::FileOptions =
        zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
    zw.start_file("../../etc/passwd", opts).unwrap();
    zw.write_all(b"target.com:hit@x.com:p\n").unwrap();
    zw.finish().unwrap().into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traversal_entry_name_does_not_escape_output_dir() {
    use telegram_client::telegram::mock::UploadOutcome as MockUploadOutcome;

    let store_dir = tempfile::tempdir().unwrap();
    let out_dir   = tempfile::tempdir().unwrap();
    let store     = Store::open(&store_dir.path().join("s.db")).unwrap();
    let zipb      = build_traversal_zip();
    let zipb_len  = u64::try_from(zipb.len()).unwrap();

    let mock = Arc::new(
        MockClient::new()
            .with_document(
                MessageInfo {
                    chat_id: -100, msg_id: 9,
                    original_name: "evil.zip".into(),
                    size_bytes:    zipb_len,
                    mime:          Some("application/zip".into()),
                    date: 0,
                },
                zipb,
            )
            .script_upload(vec![MockUploadOutcome::Ok(50_002)]),
    );

    let cfg = {
        let mut c = cfg_with_dir(out_dir.path().to_path_buf());
        c.matcher_key = "target.com".into();
        c
    };

    let (jobs_tx, jobs_rx) = tokio::sync::mpsc::channel::<Job>(2);
    let info = mock.messages.lock().unwrap()[&(-100i64, 9_i32)].0.clone();
    jobs_tx.send(Job { source_chat_id: -100, source_msg_id: 9, info })
        .await.unwrap();
    drop(jobs_tx);

    let advance: CursorAdvance = Arc::new(|_o: JobOutcome| { /* noop */ });
    interfile::run(mock.as_ref(), Some(&store), &cfg, jobs_rx, advance)
        .await.expect("traversal-named entries must NOT poison the orchestrator");

    // (1) The merged out-file lives at the expected path.
    let expected = out_dir.path().join("-100").join("9_evil.out");
    assert_eq!(std::fs::read(&expected).unwrap(), b"hit@x.com:p\n");

    // (2) Recursive walk of output_dir finds exactly one regular file. This
    //     is the LOAD-BEARING assertion — it works on every platform.
    let mut files = Vec::<std::path::PathBuf>::new();
    walk(out_dir.path(), &mut files);
    assert_eq!(
        files.len(), 1,
        "expected exactly 1 regular file under output_dir, found {files:#?}",
    );
    assert_eq!(files[0], expected);

    // (3) Negative assertions at known escape paths (defense in depth).
    let parent_etc_passwd = out_dir.path().join("..").join("etc").join("passwd");
    assert!(
        !parent_etc_passwd.exists(),
        "{} exists; traversal escaped via parent-relative path",
        parent_etc_passwd.display(),
    );
    let in_dir_etc_passwd = out_dir.path().join("etc").join("passwd");
    assert!(
        !in_dir_etc_passwd.exists(),
        "{} exists; traversal escaped (relative-resolved form)",
        in_dir_etc_passwd.display(),
    );

    // (4) Soft Linux-only check: if /etc/passwd exists and was just modified,
    //     SOMETHING got through to the system file. Vacuously skips on
    //     sandboxes/Windows where /etc/passwd is absent or unreadable.
    if let Ok(md) = std::fs::metadata("/etc/passwd") {
        if let Ok(mtime) = md.modified() {
            let recent = std::time::SystemTime::now()
                .duration_since(mtime)
                .map(|d| d < std::time::Duration::from_secs(60))
                .unwrap_or(false);
            assert!(!recent, "/etc/passwd was modified in the last 60s — traversal escaped");
        }
    }
}

fn walk(root: &Path, out: &mut Vec<std::path::PathBuf>) {
    if let Ok(rd) = std::fs::read_dir(root) {
        for entry in rd.flatten() {
            let p = entry.path();
            // `metadata()` follows symlinks; use `symlink_metadata()` so we
            // detect symlinks themselves rather than their targets.
            let md = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            // Symlinks: if a traversal landed via symlink, treat as escape.
            if md.is_symlink() {
                panic!("symlink found at {}; traversal escaped via symlink", p.display());
            }
            if md.is_dir()  { walk(&p, out); }
            if md.is_file() { out.push(p);   }
        }
    }
}
