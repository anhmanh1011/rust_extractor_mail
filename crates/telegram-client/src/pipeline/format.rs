//! Format detection for downloaded Telegram documents.
//!
//! Magic bytes win over extension when the head buffer is long enough.
//! See spec §4.1 (intra-file paths) for routing rules: a misnamed `.txt`
//! that is actually gzip MUST be routed to the gzip decoder, otherwise
//! the line scanner would treat compressed bytes as text.

/// Detected container/encoding of a fetched document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Plain UTF-8 / ASCII text — feed directly to `Scanner`.
    Txt,
    /// Gzip-compressed text — wrap stream in `flate2::read::MultiGzDecoder`.
    Gz,
    /// ZIP archive — disk-spill path (`pipeline::disk`) handles it.
    Zip,
    /// Neither extension nor magic bytes matched a supported format.
    Unknown,
}

const GZIP_MAGIC: [u8; 2] = [0x1F, 0x8B];
const ZIP_LOCAL_HEADER: [u8; 4] = [0x50, 0x4B, 0x03, 0x04];

/// Detect the format of a document from its filename plus an optional
/// head sample.
///
/// Routing precedence:
/// 1. ZIP magic (4 bytes `PK\x03\x04`) wins absolutely.
/// 2. GZIP magic (2 bytes `0x1F 0x8B`) wins absolutely.
/// 3. Otherwise, lowercase extension chooses (`.txt`/`.gz`/`.zip`).
/// 4. Otherwise, [`Format::Unknown`].
///
/// `head` may be empty; in that case only the extension is consulted.
pub fn detect(name: &str, head: &[u8]) -> Format {
    if head.len() >= ZIP_LOCAL_HEADER.len() && head[..ZIP_LOCAL_HEADER.len()] == ZIP_LOCAL_HEADER {
        return Format::Zip;
    }
    if head.len() >= GZIP_MAGIC.len() && head[..GZIP_MAGIC.len()] == GZIP_MAGIC {
        return Format::Gz;
    }
    let lower = name.to_ascii_lowercase();
    if lower.ends_with(".txt") {
        Format::Txt
    } else if lower.ends_with(".gz") {
        Format::Gz
    } else if lower.ends_with(".zip") {
        Format::Zip
    } else {
        Format::Unknown
    }
}
