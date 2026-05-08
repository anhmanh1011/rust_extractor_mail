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
    pub fn new(_key: &str, _mode: Mode) -> Result<Self, MatcherError> {
        unimplemented!("Task 1.2")
    }

    /// Returns `Some(rest_after_match)` if `line` matches, else `None`.
    #[inline]
    pub fn match_line<'a>(&self, _line: &'a [u8]) -> Option<&'a [u8]> {
        unimplemented!("Task 1.2")
    }

    /// The canonical (lowercased ASCII) key bytes.
    pub fn key(&self) -> &[u8] { &self.key }

    /// The configured mode.
    pub fn mode(&self) -> Mode { self.mode }
}
