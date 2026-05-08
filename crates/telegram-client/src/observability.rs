//! Tracing init + indicatif progress. Filled in Task 2.5.

/// RAII guard returned by [`init`] that flushes the non-blocking tracing
/// appender on drop. Filled in Task 2.5.
pub struct LogGuard(#[allow(dead_code)] pub Option<tracing_appender::non_blocking::WorkerGuard>);

/// Initialise the global tracing subscriber. Filled in Task 2.5.
pub fn init(_level: &str, _format: &str, _file: Option<&std::path::Path>, _rotation: &str) -> LogGuard {
    unimplemented!("Task 2.5")
}
