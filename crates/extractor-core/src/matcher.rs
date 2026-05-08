//! Domain-aware suffix matcher and per-line parsing.

#![allow(dead_code)] // implementation lands in Task 1.2

/// Parsing mode for a line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// `domain:txt1:txt2`
    Plain,
    /// `<scheme>://<host>[/path]:<email>:<password>`
    Url,
}

/// Errors when constructing a [`Matcher`].
#[derive(Debug, thiserror::Error)]
pub enum MatcherError {
    /// Key was empty.
    #[error("key must not be empty")]
    Empty,
    /// Key contained whitespace.
    #[error("key must not contain whitespace")]
    Whitespace,
    /// Key started or ended with a dot.
    #[error("key must not start or end with '.'")]
    EdgeDot,
    /// Key contained non-ASCII bytes.
    #[error("key must be ASCII")]
    NonAscii,
}

/// Domain-aware matcher. Construct once, use across many lines.
#[derive(Debug, Clone)]
pub struct Matcher {
    key: Box<[u8]>,
    mode: Mode,
}

impl Matcher {
    /// Construct a new matcher.
    pub fn new(key: &str, mode: Mode) -> Result<Self, MatcherError> {
        if key.is_empty() {
            return Err(MatcherError::Empty);
        }
        if !key.is_ascii() {
            return Err(MatcherError::NonAscii);
        }
        if key.bytes().any(|b| b.is_ascii_whitespace()) {
            return Err(MatcherError::Whitespace);
        }
        if key.starts_with('.') || key.ends_with('.') {
            return Err(MatcherError::EdgeDot);
        }
        let lower: Vec<u8> = key.bytes().map(|b| b.to_ascii_lowercase()).collect();
        Ok(Self {
            key: lower.into_boxed_slice(),
            mode,
        })
    }

    /// Returns Some(rest-after-first-colon) on match, None on miss.
    #[inline]
    pub fn match_line<'a>(&self, line: &'a [u8]) -> Option<&'a [u8]> {
        match self.mode {
            Mode::Plain => self.match_plain(line),
            Mode::Url   => self.match_url(line),
        }
    }

    #[inline]
    fn match_plain<'a>(&self, line: &'a [u8]) -> Option<&'a [u8]> {
        // Find the first ':'. The field BEFORE it is the candidate domain.
        let colon = memchr::memchr(b':', line)?;
        let field = &line[..colon];
        if matches_suffix(field, &self.key) {
            // emit everything after the colon (txt1:txt2)
            Some(&line[colon + 1..])
        } else {
            None
        }
    }

    /// URL-mode line match. Verbatim port of `extract_url_match` from
    /// `crates/extract-mail/src/main.rs:184-224`.
    #[inline]
    fn match_url<'a>(&self, line: &'a [u8]) -> Option<&'a [u8]> {
        // 1. Locate "://" — anything without a scheme is not a URL line.
        let scheme_sep = memchr::memmem::find(line, b"://")?;
        let host_start = scheme_sep + 3;
        if host_start >= line.len() {
            return None;
        }

        // 2. Host = the run of [a-zA-Z0-9.-] starting at host_start; stops at
        //    '/', ':', '?', '#', or any other non-host byte.
        let mut host_end = host_start;
        while host_end < line.len() {
            let b = line[host_end];
            if b.is_ascii_alphanumeric() || b == b'.' || b == b'-' {
                host_end += 1;
            } else {
                break;
            }
        }
        let host = &line[host_start..host_end];

        // 3. Domain-aware suffix match against the key.
        if host.len() < self.key.len() || !matches_suffix(host, &self.key) {
            return None;
        }

        // 4. Find <email>:<password> as the LAST two ':'-separated fields.
        //    Reading from the right is robust against ':' inside the URL
        //    (port, path, query). Layout: `<URL>:<email>:<password>`, so the
        //    second-to-last ':' marks the start of <email>.
        let last_colon = memchr::memrchr(b':', line)?;
        if last_colon == 0 {
            return None;
        }
        let second_last_colon = memchr::memrchr(b':', &line[..last_colon])?;
        // Both colons must lie strictly after the host. Otherwise the line has
        // no <email>:<password> tail (only a URL with optional port), and we
        // reject by returning None — matching the original's behavior.
        if second_last_colon < host_end {
            return None;
        }
        Some(&line[second_last_colon + 1..])
    }

    /// The canonical (lowercased ASCII) key bytes.
    pub fn key(&self) -> &[u8] { &self.key }

    /// The configured mode.
    pub fn mode(&self) -> Mode { self.mode }
}

/// Domain-aware suffix match: `field` must equal `key` or end with `.<key>`,
/// case-insensitive (key is already lowercase; we lower the field byte-wise).
fn matches_suffix(field: &[u8], key: &[u8]) -> bool {
    if field.len() < key.len() {
        return false;
    }
    let tail = &field[field.len() - key.len()..];
    if !eq_ascii_ci(tail, key) {
        return false;
    }
    if field.len() == key.len() {
        return true;
    }
    // boundary char must be '.'
    field[field.len() - key.len() - 1] == b'.'
}

fn eq_ascii_ci(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.to_ascii_lowercase() == *y)
}
