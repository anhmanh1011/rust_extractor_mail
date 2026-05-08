//! Render the per-output caption attached to each Telegram upload.
//!
//! Constraints (spec §11 / §10.2):
//! - Must NOT contain any matched line content (no credentials in chat history).
//! - Must fit inside Telegram's 1024-char caption limit.
//! - Must include enough provenance for a recipient to find the source.

const TELEGRAM_CAPTION_LIMIT_CHARS: usize = 1024;

/// Borrowing input to [`render`]; assembled per-call so the same
/// [`CaptionData`] can produce different captions per upload part.
#[derive(Debug)]
pub struct CaptionInput<'a> {
    /// Source filename as Telegram named it on the source message.
    pub original_name:  &'a str,
    /// Telegram chat id of the source message.
    pub source_chat_id: i64,
    /// Telegram message id of the source message.
    pub source_msg_id:  i32,
    /// Matcher key (e.g. domain name for `plain`, URL host for `url`).
    pub matcher_key:    &'a str,
    /// Matcher mode discriminator: `"plain"` or `"url"`.
    pub matcher_mode:   &'a str,
    /// Original (uncompressed) source size in bytes.
    pub size_bytes:     u64,
    /// Total lines scanned across the source.
    pub lines_scanned:  u64,
    /// Lines that matched the matcher.
    pub lines_matched:  u64,
    /// 1-based part index when output is split across multiple uploads.
    pub part_index:     Option<u32>,
    /// Total number of parts when output is split.
    pub part_total:     Option<u32>,
}

/// Owned form of [`CaptionInput`], suitable for crossing async/spawn
/// boundaries. `UploadJob` carries this; `pipeline::upload::run` calls
/// `data.input(part_index, part_total)` per part to construct a borrowing
/// [`CaptionInput`] and feeds it to [`render`]. This is the ONLY way
/// captions are produced — never post-concatenate `"\nPart i/N"` onto a
/// rendered caption (that bypasses the 1024-char truncation).
#[derive(Debug, Clone)]
pub struct CaptionData {
    /// Source filename as Telegram named it on the source message.
    pub original_name:  String,
    /// Telegram chat id of the source message.
    pub source_chat_id: i64,
    /// Telegram message id of the source message.
    pub source_msg_id:  i32,
    /// Matcher key (e.g. domain for `plain`, URL host for `url`).
    pub matcher_key:    String,
    /// Matcher mode discriminator: `"plain"` or `"url"`.
    pub matcher_mode:   String,
    /// Original (uncompressed) source size in bytes.
    pub size_bytes:     u64,
    /// Total lines scanned across the source.
    pub lines_scanned:  u64,
    /// Lines that matched the matcher.
    pub lines_matched:  u64,
}

impl CaptionData {
    /// Build a borrowing [`CaptionInput`] for a specific part of a split
    /// upload (or `None`/`None` for a single-part upload).
    pub fn input<'a>(
        &'a self,
        part_index: Option<u32>,
        part_total: Option<u32>,
    ) -> CaptionInput<'a> {
        CaptionInput {
            original_name:  &self.original_name,
            source_chat_id: self.source_chat_id,
            source_msg_id:  self.source_msg_id,
            matcher_key:    &self.matcher_key,
            matcher_mode:   &self.matcher_mode,
            size_bytes:     self.size_bytes,
            lines_scanned:  self.lines_scanned,
            lines_matched:  self.lines_matched,
            part_index,
            part_total,
        }
    }
}

/// Render a Telegram caption for an uploaded output file.
///
/// The result is guaranteed to be at most [`TELEGRAM_CAPTION_LIMIT_CHARS`]
/// `char`s long; if the natural rendering would overflow, it is truncated
/// from the right and an ellipsis `…` is appended.
pub fn render(input: &CaptionInput<'_>) -> String {
    let mut s = String::with_capacity(512);
    s.push_str("Source: ");
    s.push_str(input.original_name);
    s.push('\n');
    s.push_str(&format!("Chat: {}  Msg: {}\n", input.source_chat_id, input.source_msg_id));
    s.push_str(&format!("Match: {} ({})\n", input.matcher_key, input.matcher_mode));
    s.push_str(&format!("Size: {}\n", human_bytes(input.size_bytes)));
    s.push_str(&format!(
        "Scanned: {}  Matched: {}\n",
        with_thousands(input.lines_scanned),
        with_thousands(input.lines_matched),
    ));
    if let (Some(i), Some(n)) = (input.part_index, input.part_total) {
        s.push_str(&format!("Part {i}/{n}\n"));
    }
    truncate_to_chars(s, TELEGRAM_CAPTION_LIMIT_CHARS)
}

fn truncate_to_chars(mut s: String, limit_chars: usize) -> String {
    if s.chars().count() <= limit_chars { return s; }
    let cut: String = s.chars().take(limit_chars.saturating_sub(1)).collect();
    s.clear();
    s.push_str(&cut);
    s.push('…');
    s
}

fn human_bytes(n: u64) -> String {
    const KB: u64 = 1_000;
    const MB: u64 = 1_000_000;
    const GB: u64 = 1_000_000_000;
    if n >= GB { format!("{:.1} GB", n as f64 / GB as f64) }
    else if n >= MB { format!("{:.1} MB", n as f64 / MB as f64) }
    else if n >= KB { format!("{:.1} KB", n as f64 / KB as f64) }
    else { format!("{n} B") }
}

fn with_thousands(n: u64) -> String {
    let raw = n.to_string();
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len() + raw.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 { out.push(','); }
        out.push(*b as char);
    }
    out
}
