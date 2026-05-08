//! 3-stage pipeline orchestration (spec §4.2).
pub mod coordinator;
pub mod disk;
pub mod format;
pub mod stream;

pub use format::{detect as detect_format, Format};

/// Per-file work item that flows through the pipeline. Filled in Task 4.x.
#[derive(Debug)]
pub struct FileJob { /* chat_id, msg_id, name, size, sha, ... */ }
