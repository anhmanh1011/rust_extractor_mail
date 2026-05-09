//! Shared helpers for integration tests under `tests/`. Cargo auto-includes
//! files under `tests/common/mod.rs` in every integration test crate that
//! does `mod common;`. The `#![allow(dead_code)]` suppresses warnings in
//! the test binaries that only use a subset of these helpers.

#![allow(dead_code)]

use telegram_client::pipeline::interfile::PipelineConfig;

/// Default `PipelineConfig` used by integration tests under `tests/`. The
/// inter-file channel capacity is the spec §4.2 value (1) so tests pin the
/// production cap; tests that want to relax it for higher throughput
/// override the field after calling this helper.
pub fn cfg_with_dir(dir: std::path::PathBuf) -> PipelineConfig {
    PipelineConfig {
        matcher_key:                 "gmail.com".into(),
        matcher_mode:                "plain".into(),
        output_dir:                  dir,
        max_line_bytes:              64 * 1024,
        max_uncompressed_bytes:      10 * 1024 * 1024 * 1024,
        intra_file_channel_capacity: 4,
        inter_file_channel_capacity: 1,
        upload_channel_capacity:     2,
        outcomes_channel_capacity:   2,
        upload_max_size_bytes:       2 * 1024 * 1024 * 1024,
        upload_rate_seconds:         0,
        target_chat_id:              42,
        progress:                    None,
    }
}
