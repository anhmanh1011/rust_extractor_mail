//! Config loader, env override, path expansion. Filled in Task 2.3.
use std::path::PathBuf;

/// Application configuration loaded from TOML. Filled in Task 2.3.
#[derive(Debug)]
pub struct AppConfig { /* fields land in Task 2.3 */ }
/// Secrets pulled from env / OS keystore. Filled in Task 2.4.
#[derive(Debug)]
pub struct Secrets { /* api_id, api_hash; redacting Debug in Task 2.4 */ }

/// Load `AppConfig` from the given TOML path. Filled in Task 2.3.
pub fn load(_path: &std::path::Path) -> anyhow::Result<AppConfig> {
    unimplemented!("Task 2.3")
}
/// Load `Secrets` from environment / keystore. Filled in Task 2.4.
pub fn load_secrets() -> anyhow::Result<Secrets> {
    unimplemented!("Task 2.4")
}
/// Expand `~`-prefixed paths and environment variables. Filled in Task 2.3.
pub fn expand_path(_p: &str) -> PathBuf {
    unimplemented!("Task 2.3")
}

/// Extract mode (plain text vs URL-format lines). Real impl in Task 2.3.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum ExtractMode {
    /// `<domain>:<txt1>:<txt2>` lines.
    Plain,
    /// `<url>:<email>:<password>` lines.
    Url,
}
