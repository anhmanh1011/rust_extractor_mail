//! Spec §11.2 (path traversal): `sanitize` strips separators / `..` / control
//! bytes; `join_safe` re-asserts the result lives under `root`. The
//! adversarial sweep (`path_traversal.rs`) lives in Phase 10.

use std::path::PathBuf;
use telegram_client::output::{join_safe, sanitize};

#[test]
fn sanitize_strips_path_separators() {
    assert_eq!(sanitize("a/b.txt"), "a_b.txt");
    assert_eq!(sanitize("a\\b.txt"), "a_b.txt");
}

#[test]
fn sanitize_strips_dotdot_segments() {
    assert_eq!(sanitize("../etc/passwd"), "_etc_passwd");
    assert_eq!(sanitize("..\\..\\boot.ini"), "_boot.ini");
}

#[test]
fn sanitize_strips_control_and_nul_bytes() {
    let dirty = "ab\x00c\n.txt";
    let clean = sanitize(dirty);
    assert!(!clean.contains('\0'));
    assert!(!clean.contains('\n'));
    assert_eq!(clean, "ab_c_.txt");
}

#[test]
fn sanitize_replaces_empty_with_placeholder() {
    assert_eq!(sanitize(""), "unnamed");
    assert_eq!(sanitize("..."), "unnamed");
    assert_eq!(sanitize("///"), "unnamed");
}

#[test]
fn sanitize_truncates_to_192_chars_preserving_extension() {
    let long = "a".repeat(500);
    let dirty = format!("{long}.txt");
    let clean = sanitize(&dirty);
    assert!(clean.len() <= 192, "got len={}", clean.len());
    assert!(clean.ends_with(".txt"));
}

#[test]
fn join_safe_rejects_absolute_input() {
    let root = std::env::temp_dir();
    let err = join_safe(&root, "/etc/passwd").unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("path") || msg.contains("absolute") || msg.contains("escape"),
        "unexpected error: {msg}"
    );
}

#[test]
fn join_safe_rejects_escape_via_dotdot() {
    let root = std::env::temp_dir();
    let err = join_safe(&root, "../escape.txt").unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("escape") || msg.contains("traversal") || msg.contains("path"),
        "unexpected error: {msg}"
    );
}

#[test]
fn join_safe_returns_path_under_root_for_clean_name() {
    let tmp = tempfile::tempdir().unwrap();
    let p: PathBuf = join_safe(tmp.path(), "dump.out").unwrap();
    assert!(p.starts_with(tmp.path()));
    assert_eq!(p.file_name().unwrap(), "dump.out");
}
