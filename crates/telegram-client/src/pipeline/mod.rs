//! 3-stage pipeline orchestration.
pub mod coordinator;
pub mod format;
pub mod stream;
pub mod disk;

/// Per-file work item that flows through the pipeline. Filled in Task 4.x.
#[derive(Debug)]
pub struct FileJob { /* chat_id, msg_id, name, size, sha, ... */ }
