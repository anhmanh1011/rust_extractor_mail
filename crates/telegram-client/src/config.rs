//! Config loader: TOML + env var overrides + path expansion.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Top-level configuration aggregate parsed from the TOML config file.
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// Telegram credentials, session, and download tuning.
    pub telegram: TelegramSection,
    /// Pipeline directories, channel capacities, and size limits.
    pub pipeline: PipelineSection,
    /// Extraction mode and matching key.
    pub extract:  ExtractSection,
    /// Optional `watch` mode channels (defaults to empty list).
    #[serde(default)]
    pub watch:    WatchSection,
    /// Optional `backfill` mode tuning (defaults provided).
    #[serde(default)]
    pub backfill: BackfillSection,
    /// Logging configuration (level, format, rotation).
    pub log:      LogSection,
}

/// Section: Telegram credentials, session storage, and download tuning.
#[derive(Debug, Clone, Deserialize)]
pub struct TelegramSection {
    /// Path to the grammers session file (after `~` expansion).
    pub session_path: String,
    /// Number of concurrent chunk downloads per file.
    #[serde(default = "default_concurrent_chunks")]
    pub download_concurrent_chunks: usize,
    /// Output channel target (chat handle or numeric chat_id).
    pub output: OutputSection,
}

fn default_concurrent_chunks() -> usize { 4 }

/// Section: target chat for uploaded result files. Either `chat` or `chat_id`
/// must be set; both being absent is rejected by validation.
#[derive(Debug, Clone, Deserialize)]
pub struct OutputSection {
    /// Public username / handle, e.g. `"@my_results_channel"`.
    pub chat:    Option<String>,
    /// Numeric chat id (often a negative `-100…` channel id).
    pub chat_id: Option<i64>,
}

/// Section: pipeline working directories, channel capacities, and size guards.
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineSection {
    /// Scratch directory for in-flight files (after `~` expansion).
    pub work_dir: String,
    /// Final destination directory for completed result files.
    pub output_dir: String,
    /// Bytes per streamed download chunk; must be ≥ 64 KiB.
    pub chunk_bytes: usize,
    /// Bounded capacity of the intra-file (downloader → extractor) channel.
    pub intra_file_channel_capacity: usize,
    /// Bounded capacity of the inter-file (extractor → writer) channel.
    pub inter_file_channel_capacity: usize,
    /// Bounded capacity of the writer → uploader channel.
    pub upload_channel_capacity: usize,
    /// Maximum line length the extractor will buffer; must be ≥ 1024.
    pub max_line_bytes: usize,
    /// Minimum seconds between upload attempts (per spec rate-limit).
    pub upload_rate_seconds: u64,
    /// Maximum upload size in bytes; rotates the result file when exceeded.
    pub upload_max_size_bytes: u64,
    /// Zip-bomb guard: maximum decompressed bytes per archive entry.
    pub max_uncompressed_bytes: u64,
}

/// Extraction mode (selects which line layout the parser expects).
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ExtractMode {
    /// Plain mode: lines look like `domain:txt1:txt2`
    Plain,
    /// URL mode: lines look like `<URL>:<email>:<password>`
    Url,
}

/// Section: extraction mode and the substring key to match against.
#[derive(Debug, Clone, Deserialize)]
pub struct ExtractSection {
    /// Extraction mode (`"plain"` or `"url"`).
    pub mode: ExtractMode,
    /// Substring to look for inside each candidate line.
    pub key: String,
}

/// Section: list of `watch` mode channels. Defaults to an empty list.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WatchSection {
    /// One entry per `[[watch.channel]]` table in TOML.
    #[serde(default, rename = "channel")]
    pub channels: Vec<WatchChannel>,
}

/// A single watched channel, optionally overriding `[extract]` for that channel.
#[derive(Debug, Clone, Deserialize)]
pub struct WatchChannel {
    /// Public handle, e.g. `"@dump_channel_a"`.
    pub chat:    Option<String>,
    /// Numeric chat id alternative to `chat`.
    pub chat_id: Option<i64>,
    /// Optional per-channel extract override.
    #[serde(default)]
    pub extract: Option<ExtractSection>,
}

/// Section: `backfill` mode tuning (history pagination + cutoff).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BackfillSection {
    /// Number of messages requested per page during backfill.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// RFC3339 lower bound; messages older than this are skipped.
    pub since: Option<String>,
}
fn default_page_size() -> u32 { 100 }

/// Section: logging level, format, and rotation policy.
#[derive(Debug, Clone, Deserialize)]
pub struct LogSection {
    /// `tracing` level filter (e.g. `"info"`, `"debug"`).
    pub level:    String,
    /// Output format: `"human"` or `"json"`.
    pub format:   String,   // "human" | "json"
    /// Optional log file path (after `~` expansion); `None` means stderr only.
    pub file:     Option<String>,
    /// Rotation cadence: `"never"`, `"daily"`, or `"hourly"`.
    pub rotation: String,   // "never" | "daily" | "hourly"
}

/// Load `AppConfig` from `path`, applying env overrides, validation, and
/// `~` expansion on path-like fields.
pub fn load(path: &Path) -> Result<AppConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading config from {}", path.display()))?;
    let mut cfg: AppConfig = toml::from_str(&raw)
        .with_context(|| format!("parsing config TOML at {}", path.display()))?;

    // Apply env overrides (precedence: env > toml). Only `RUST_TG_SESSION`
    // is handled here. Other env vars from spec §7.2 are processed at their
    // respective layers:
    //   - RUST_TG_CONFIG → clap (`#[arg(env = "RUST_TG_CONFIG")]` on Cli::config)
    //   - RUST_LOG       → tracing (EnvFilter::try_from_default_env)
    //   - TG_API_ID/HASH → load_secrets() in Task 2.4
    if let Ok(s) = std::env::var("RUST_TG_SESSION") {
        cfg.telegram.session_path = s;
    }

    validate(&cfg)?;

    // Expand ~ in path-like fields.
    cfg.telegram.session_path = expand_path(&cfg.telegram.session_path).to_string_lossy().into();
    cfg.pipeline.work_dir     = expand_path(&cfg.pipeline.work_dir).to_string_lossy().into();
    cfg.pipeline.output_dir   = expand_path(&cfg.pipeline.output_dir).to_string_lossy().into();
    if let Some(f) = cfg.log.file.as_ref() {
        cfg.log.file = Some(expand_path(f).to_string_lossy().into());
    }

    Ok(cfg)
}

fn validate(cfg: &AppConfig) -> Result<()> {
    if cfg.telegram.output.chat.is_none() && cfg.telegram.output.chat_id.is_none() {
        return Err(anyhow!("[telegram.output] must specify either `chat = \"@name\"` or `chat_id = -100...`"));
    }
    if cfg.pipeline.chunk_bytes < 64 * 1024 {
        return Err(anyhow!("[pipeline.chunk_bytes] must be ≥ 64 KiB; got {}", cfg.pipeline.chunk_bytes));
    }
    if cfg.pipeline.max_line_bytes < 1024 {
        return Err(anyhow!("[pipeline.max_line_bytes] must be ≥ 1024; got {}", cfg.pipeline.max_line_bytes));
    }
    match cfg.log.format.as_str() {
        "human" | "json" => {}
        s => return Err(anyhow!("[log.format] must be 'human' or 'json'; got {s:?}")),
    }
    match cfg.log.rotation.as_str() {
        "never" | "daily" | "hourly" => {}
        s => return Err(anyhow!("[log.rotation] must be 'never'|'daily'|'hourly'; got {s:?}")),
    }
    if cfg.extract.key.is_empty() {
        return Err(anyhow!("[extract.key] must not be empty"));
    }
    Ok(())
}

/// Expand a leading `~` to the user's home directory.
///
/// Supported forms:
/// - `~/foo/bar` → `<home>/foo/bar`
/// - `~`        → `<home>`
///
/// NOT supported (returned verbatim):
/// - `~user/foo` (other-user expansion — non-portable, reject early in spec)
/// - mid-string `~` like `foo/~/bar`
/// - bare relative paths like `./out` are returned as-is (caller resolves
///   relative to CWD).
pub fn expand_path(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    } else if p == "~" {
        if let Some(home) = dirs::home_dir() {
            return home;
        }
    }
    PathBuf::from(p)
}

/// Telegram API credentials loaded from env. `api_hash` is redacted from
/// `Debug` output to prevent log leaks. `Display` is intentionally not
/// implemented so `{}` formatting cannot embed the value either. Spec §7.4.
#[derive(Clone)]
pub struct Secrets {
    /// Telegram API id (from `TG_API_ID`).
    pub api_id: i32,
    /// Telegram API hash (from `TG_API_HASH`, 32 ASCII hex chars).
    pub api_hash: String,
}

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Secrets")
            .field("api_id", &self.api_id)
            .field("api_hash", &"<redacted>")
            .finish()
    }
}

/// Load `Secrets` from the `TG_API_ID` and `TG_API_HASH` environment vars.
pub fn load_secrets() -> Result<Secrets> {
    let api_id = std::env::var("TG_API_ID")
        .context("TG_API_ID not set")?
        .parse::<i32>()
        .context("TG_API_ID must be an integer")?;
    let api_hash = std::env::var("TG_API_HASH").context("TG_API_HASH not set")?;
    if api_hash.len() != 32 || !api_hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("TG_API_HASH must be 32 hex chars"));
    }
    Ok(Secrets { api_id, api_hash })
}
