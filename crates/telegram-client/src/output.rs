//! Per-source-file output writer + path sanitiser.
//!
//! Spec §11.2 (Path traversal): `sanitize(name)` strips path separators,
//! `..` segments, NUL/control bytes, and Windows-reserved characters;
//! `join_safe` rejects any input that contains a traversal or absolute
//! component up-front, then sanitises and re-asserts containment under
//! `root` as defence-in-depth.

use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, Result};

// NTFS / most-Linux file-name cap is 255 bytes. We reserve ~63 bytes for the
// `<chat_id>/<msg_id>_` prefix added by `cmd::fetch` so the final on-disk
// path never bumps the OS limit. Truncation below counts chars rather than
// bytes — non-ASCII names may end up slightly under the byte budget, which
// is the safe direction.
const MAX_FILENAME: usize = 192;
const PLACEHOLDER: &str = "unnamed";

/// Reduce an arbitrary user-supplied name to a leaf filename safe for
/// concatenation under the configured `output_dir`.
///
/// Path separators are normalised to `/`, then segments equal to ``,
/// `.`, or `..` collapse to a single `_` placeholder so traversal
/// attempts leave a visible mark instead of vanishing silently. The
/// remaining bytes are scrubbed of NUL, control chars `<0x20`, and
/// Windows-reserved `:*?"<>|`. Leading/trailing dots and whitespace are
/// trimmed, and an empty result falls back to [`PLACEHOLDER`].
///
/// If the result still exceeds [`MAX_FILENAME`] chars, it is truncated
/// while preserving the extension (last `.`-suffix).
pub fn sanitize(name: &str) -> String {
    let normalized = name.replace('\\', "/");
    let mut cleaned: Vec<&str> = Vec::new();
    let mut prev_was_bad = false;
    for seg in normalized.split('/') {
        let bad = seg.is_empty() || seg == "." || seg == "..";
        if bad {
            if !prev_was_bad {
                cleaned.push("");
                prev_was_bad = true;
            }
        } else {
            cleaned.push(seg);
            prev_was_bad = false;
        }
    }
    let joined = cleaned.join("_");

    let mut out = String::with_capacity(joined.len());
    for c in joined.chars() {
        if c.is_control()
            || c == '\0'
            || c == ':'
            || c == '*'
            || c == '?'
            || c == '"'
            || c == '<'
            || c == '>'
            || c == '|'
        {
            out.push('_');
        } else {
            out.push(c);
        }
    }

    let trimmed = out.trim_matches(|c: char| c == '.' || c.is_whitespace());
    if trimmed.is_empty() {
        return PLACEHOLDER.to_string();
    }

    if trimmed.len() <= MAX_FILENAME {
        return trimmed.to_string();
    }

    if let Some(dot) = trimmed.rfind('.') {
        let ext = &trimmed[dot..];
        if ext.len() < MAX_FILENAME {
            let head_budget = MAX_FILENAME - ext.len();
            let mut head: String = trimmed[..dot].chars().take(head_budget).collect();
            head.push_str(ext);
            return head;
        }
    }
    trimmed.chars().take(MAX_FILENAME).collect()
}

/// Join `name` under `root`, returning a path guaranteed to live inside
/// `root`. Adversarial inputs are rejected up-front rather than silently
/// rewritten — silent rewriting would mask attacks in logs.
///
/// Errors:
/// - input contained an absolute root or drive prefix (`/foo`, `C:\foo`)
/// - input contained any `..` (parent-dir) component
/// - resolved path would escape `root` (defence-in-depth in case
///   [`sanitize`] ever has a bug)
pub fn join_safe(root: &Path, name: &str) -> Result<PathBuf> {
    let raw = Path::new(name);
    for comp in raw.components() {
        match comp {
            Component::RootDir | Component::Prefix(_) => {
                return Err(anyhow!("absolute path rejected: {}", name));
            }
            Component::ParentDir => {
                return Err(anyhow!("path traversal escape rejected: {}", name));
            }
            _ => {}
        }
    }

    let safe = sanitize(name);
    let candidate = root.join(&safe);
    if !candidate.starts_with(root) {
        return Err(anyhow!("path escape detected after sanitize: {}", name));
    }
    Ok(candidate)
}
