//! Per-source-file writer + path sanitize. Filled in Task 4.x.
use std::path::{Path, PathBuf};

/// Sanitize a filename, stripping path separators and reserved characters.
/// Filled in Task 10.x.
pub fn sanitize(_name: &str) -> String { unimplemented!("Task 10.x") }
/// Join `name` under `root` while rejecting traversal. Filled in Task 10.x.
pub fn join_safe(_root: &Path, _name: &str) -> anyhow::Result<PathBuf> { unimplemented!("Task 10.x") }
