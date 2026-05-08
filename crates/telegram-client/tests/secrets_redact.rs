//! Spec §7.4 §9.2 — secrets must never appear verbatim in any debug or
//! tracing output.

use std::sync::{Arc, Mutex};
use telegram_client::config::Secrets;
use telegram_client::observability::SecretScrubLayer;
use tracing_subscriber::{fmt, prelude::*};

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

/// In-memory writer used to capture formatted log output.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Capture {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}
impl<'a> fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer { self.clone() }
}

/// Spec §9.2 names `secrets_redact.rs` as THE secret-leak test. Beyond Debug,
/// assert that emitting a `tracing::info!(api_hash = …)` event through the
/// configured fmt + SecretScrubLayer pipeline never lets the literal hex
/// reach the formatter.
#[test]
fn tracing_event_with_api_hash_field_does_not_leak_value() {
    let cap = Capture::default();
    let buf = cap.0.clone();
    let subscriber = tracing_subscriber::registry().with(
        fmt::layer()
            .with_writer(cap)
            .with_ansi(false)
            .with_target(false)
            .with_level(false)
            .fmt_fields(SecretScrubLayer::new()),
    );
    let _g = tracing::subscriber::set_default(subscriber);

    let s = Secrets { api_id: 7, api_hash: "feedface0123456789abcdef0123456789".into() };
    // Both string-valued field and Debug-formatted struct must be redacted.
    tracing::info!(api_hash = %s.api_hash, "secrets loaded");
    tracing::info!(secrets = ?s, "secrets loaded (debug)");

    let out = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(!out.contains("feedface"),
        "api_hash literal must NOT appear in tracing output: {out}");
    assert!(out.contains("redacted"),
        "redaction marker missing in tracing output: {out}");
}
