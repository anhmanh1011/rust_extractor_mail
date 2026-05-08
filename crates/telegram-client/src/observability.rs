//! Tracing init + indicatif progress + secret scrub layer. Spec §7.4 §10.1.
//!
//! Design: `SecretScrubLayer` plays a SINGLE role — it is a custom
//! `FormatFields` impl that rewrites field values whose KEY matches a
//! secret-name pattern, BEFORE the formatter writes them out. It is wired
//! into both the console and file fmt layers via `.fmt_fields(...)`.
//! It is NOT also a `Layer<S>` (an earlier draft had a no-op Layer impl
//! that confused readers — removed).
//!
//! The visitor covers all `Visit::record_*` overloads so that redaction
//! applies regardless of the field type (str, i64, u64, f64, bool, error,
//! debug). Anything with a default `Visit` impl would have leaked through.

use std::path::Path;
use tracing::field::{Field, Visit};

/// Drop-guard for the optional non-blocking file appender worker.
pub struct LogGuard(#[allow(dead_code)] pub Option<tracing_appender::non_blocking::WorkerGuard>);

/// Field-formatter that redacts values for secret-named keys.
/// Use via `tracing_subscriber::fmt::layer().fmt_fields(SecretScrubLayer::new())`.
#[derive(Default, Clone)]
pub struct SecretScrubLayer;

impl SecretScrubLayer {
    /// Construct a fresh `SecretScrubLayer`.
    pub fn new() -> Self { Self }

    /// Match against `(?i)hash|key|secret|token|password|auth` (per spec §7.4).
    pub fn is_secret_key(name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        ["hash", "key", "secret", "token", "password", "auth"]
            .iter()
            .any(|needle| lower.contains(needle))
    }
}

/// Visitor used by the `FormatFields` impl. Rewrites secret-named values to
/// `<redacted>` for EVERY `Visit::record_*` overload — non-string values
/// (i64, bool, etc.) MUST be redacted too, otherwise an event like
/// `tracing::info!(api_hash = 12345)` would slip past as a numeric leaf.
pub struct RedactingVisitor<'w> {
    writer: tracing_subscriber::fmt::format::Writer<'w>,
    first: bool,
}

impl<'w> RedactingVisitor<'w> {
    fn write_kv(&mut self, name: &str, value: &dyn std::fmt::Display) {
        let sep = if self.first { self.first = false; "" } else { " " };
        let _ = write!(self.writer, "{sep}{name}={value}");
    }
    fn write_kv_debug(&mut self, name: &str, value: &dyn std::fmt::Debug) {
        let sep = if self.first { self.first = false; "" } else { " " };
        let _ = write!(self.writer, "{sep}{name}={value:?}");
    }
    fn redact(&mut self, name: &str) {
        let sep = if self.first { self.first = false; "" } else { " " };
        let _ = write!(self.writer, "{sep}{name}=<redacted>");
    }
}

impl<'w> Visit for RedactingVisitor<'w> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv(field.name(), &value) }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv_debug(field.name(), value) }
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv(field.name(), &value) }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv(field.name(), &value) }
    }
    fn record_i128(&mut self, field: &Field, value: i128) {
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv(field.name(), &value) }
    }
    fn record_u128(&mut self, field: &Field, value: u128) {
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv(field.name(), &value) }
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv(field.name(), &value) }
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv(field.name(), &value) }
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        // Errors may carry secrets in their Display impl; redact if name matches.
        if SecretScrubLayer::is_secret_key(field.name()) { self.redact(field.name()) }
        else { self.write_kv(field.name(), &value) }
    }
}

impl<'writer> tracing_subscriber::fmt::FormatFields<'writer> for SecretScrubLayer {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        writer: tracing_subscriber::fmt::format::Writer<'writer>,
        fields: R,
    ) -> std::fmt::Result {
        let mut visitor = RedactingVisitor { writer, first: true };
        fields.record(&mut visitor);
        Ok(())
    }
}

/// Initialize tracing. Returns a `LogGuard` that must be held for the
/// lifetime of the program (the file appender's worker is non-blocking).
pub fn init(level: &str, format: &str, file: Option<&Path>, rotation: &str) -> LogGuard {
    use tracing_subscriber::{fmt, layer::Layer, prelude::*, EnvFilter, Registry};

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));

    let console_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_ansi(supports_color())
        .fmt_fields(SecretScrubLayer::new());

    let (file_layer, guard) = if let Some(path) = file {
        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(dir).ok();
        let stem = path.file_name().and_then(|s| s.to_str()).unwrap_or("app.log");
        let appender = match rotation {
            "daily"  => tracing_appender::rolling::daily(dir, stem),
            "hourly" => tracing_appender::rolling::hourly(dir, stem),
            _        => tracing_appender::rolling::never(dir, stem),
        };
        let (nb, guard) = tracing_appender::non_blocking(appender);
        let layer = fmt::layer()
            .with_writer(nb)
            .with_ansi(false)
            .fmt_fields(SecretScrubLayer::new());
        // The boxed dyn-trait dance keeps the `Option` shape uniform.
        let layer: Box<dyn Layer<Registry> + Send + Sync> =
            if format == "json" { Box::new(layer.json()) } else { Box::new(layer) };
        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    // Compose: Registry -> file_layer (if any) -> env_filter -> console_layer.
    // The boxed file layer is parameterised over `Registry`, so it must be
    // attached *before* any other typed layer to satisfy `Layer<Registry>`.
    // `Option<L>: Layer<S>` when `L: Layer<S>`, so `None` becomes a no-op.
    tracing_subscriber::registry()
        .with(file_layer)
        .with(env_filter)
        .with(console_layer)
        .init();

    LogGuard(guard)
}

fn supports_color() -> bool {
    use std::io::IsTerminal;
    std::io::stderr().is_terminal()
}
