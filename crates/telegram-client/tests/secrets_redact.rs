//! Spec §7.4 §9.2 — secrets must never appear verbatim in any debug or
//! tracing output.
//!
//! This file currently covers the `Debug` redaction surface. The end-to-end
//! tracing-capture assertion that depends on `observability::SecretScrubLayer`
//! lands in Task 2.5; see that task for the third test.

use telegram_client::config::Secrets;

#[test]
fn debug_redacts_api_hash() {
    let s = Secrets {
        api_id: 12345,
        api_hash: "deadbeef0123456789abcdef0123456789".into(),
    };
    let dbg = format!("{s:?}");
    assert!(dbg.contains("12345"), "api_id should be visible: {dbg}");
    assert!(
        !dbg.contains("deadbeef"),
        "api_hash literal MUST be redacted: {dbg}"
    );
    assert!(
        dbg.contains("redacted") || dbg.contains("****"),
        "Debug output should mark redaction explicitly: {dbg}"
    );
}

#[test]
fn display_is_not_implemented() {
    // Display is intentionally absent for Secrets — compile check only.
    let s = Secrets { api_id: 1, api_hash: "x".repeat(32) };
    let _ = format!("{s:?}"); // Debug works
    // The following line MUST fail to compile if uncommented:
    // let _ = format!("{s}");
}
