use std::sync::{Arc, Mutex};
use telegram_client::observability::SecretScrubLayer;
use tracing_subscriber::{fmt, layer::SubscriberExt};

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
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer { self.clone() }
}

#[test]
fn redacts_fields_whose_name_matches_secret_pattern() {
    let cap = Capture::default();
    let buf = cap.0.clone();
    // SecretScrubLayer is a FormatFields, NOT a Layer<S>. It plugs into the
    // fmt layer via .fmt_fields(...).
    let subscriber = tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(cap)
                .with_ansi(false)
                .with_target(false)
                .with_level(true)
                .fmt_fields(SecretScrubLayer::new()),
        );
    let _g = tracing::subscriber::set_default(subscriber);

    tracing::info!(api_hash = "deadbeef0123456789abcdef0123456789", "loaded");
    tracing::info!(password = "hunter2", session_token = "abcd", greeting = "hello");
    // Non-string secrets MUST also be redacted (i64, bool, debug):
    tracing::info!(api_hash = 0xCAFEBABE_u64, oauth_token = true, "loaded numeric");
    let s = String::from("topsecret_payload");
    tracing::info!(secret_blob = ?s, normal = "visible_norm", "debug-formatted");

    let formatted = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(!formatted.contains("deadbeef"),         "api_hash str leaked: {formatted}");
    assert!(!formatted.contains("hunter2"),          "password leaked: {formatted}");
    assert!(!formatted.contains("abcd"),             "session_token leaked: {formatted}");
    assert!(!formatted.contains("3405691582"),       "api_hash numeric (u64=0xCAFEBABE) leaked: {formatted}");
    assert!(!formatted.contains("0xcafebabe"),       "api_hash hex leaked: {formatted}");
    assert!(!formatted.contains("topsecret_payload"),"secret_blob debug leaked: {formatted}");
    assert!(formatted.contains("hello"),             "non-secret 'greeting' must NOT be redacted: {formatted}");
    assert!(formatted.contains("visible_norm"),      "non-secret 'normal' must NOT be redacted: {formatted}");
    assert!(formatted.contains("redacted"),          "redaction marker missing: {formatted}");
}
