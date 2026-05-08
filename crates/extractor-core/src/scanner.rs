//! Byte-stream scanner that emits matched line slices.

#![allow(dead_code)] // implementation lands in Task 1.4

use crate::matcher::Matcher;

/// Sink that receives matched line bytes (without trailing newline).
pub trait LineSink {
    /// Sink-specific error type.
    type Error;
    /// Emit one matched line. Returning `Err` aborts scanning.
    fn emit(&mut self, line: &[u8]) -> Result<(), Self::Error>;
}

/// Aggregate stats over a scan.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScanStats {
    /// Lines observed (including non-matching).
    pub lines_scanned: u64,
    /// Lines emitted to the sink.
    pub lines_matched: u64,
    /// Bytes fed to the scanner.
    pub bytes_scanned: u64,
}

impl std::ops::AddAssign for ScanStats {
    fn add_assign(&mut self, rhs: Self) {
        self.lines_scanned += rhs.lines_scanned;
        self.lines_matched += rhs.lines_matched;
        self.bytes_scanned += rhs.bytes_scanned;
    }
}

/// Errors from scanning.
#[derive(Debug, thiserror::Error)]
pub enum ScanError<E> {
    /// A single line exceeded the configured `max_line` cap.
    #[error("line exceeds max_line ({0} bytes)")]
    LineTooLong(usize),
    /// The sink returned an error.
    #[error("sink error: {0}")]
    Sink(E),
}

/// The scanner. Holds a reference to the matcher and a small carry buffer.
#[derive(Debug)]
pub struct Scanner<'m> {
    matcher: &'m Matcher,
    carry: Vec<u8>,
    max_line: usize,
}

impl<'m> Scanner<'m> {
    /// Default cap for a single line: 64 KiB.
    pub const DEFAULT_MAX_LINE: usize = 64 * 1024;

    /// Construct with default `max_line`.
    pub fn new(matcher: &'m Matcher) -> Self {
        Self::with_max_line(matcher, Self::DEFAULT_MAX_LINE)
    }

    /// Construct with custom `max_line`.
    pub fn with_max_line(matcher: &'m Matcher, max_line: usize) -> Self {
        Self {
            matcher,
            carry: Vec::with_capacity(4096),
            max_line,
        }
    }

    /// Feed a chunk. Lines split across chunks are stitched via internal
    /// carry buffer.
    pub fn feed<S: LineSink>(
        &mut self,
        chunk: &[u8],
        sink: &mut S,
    ) -> Result<ScanStats, ScanError<S::Error>> {
        let mut stats = ScanStats {
            bytes_scanned: chunk.len() as u64,
            ..Default::default()
        };

        // Walk through `chunk` finding LF boundaries.
        let mut cursor = 0usize;

        while let Some(rel_lf) = memchr::memchr(b'\n', &chunk[cursor..]) {
            let lf = cursor + rel_lf;
            // Build the logical line: carry + chunk[cursor..lf]
            let line_len = self.carry.len() + (lf - cursor);
            if line_len > self.max_line {
                return Err(ScanError::LineTooLong(line_len));
            }

            let matched_or_none = if self.carry.is_empty() {
                self.match_and_emit(&chunk[cursor..lf], sink)
            } else {
                // Stitch: carry + chunk[cursor..lf]. Use mem::take to move
                // the carry buffer out so we can borrow it as `&[u8]` while
                // calling `&self`-methods, then put the (cleared) buffer
                // back so its allocation is reused on the next stitched line.
                self.carry.extend_from_slice(&chunk[cursor..lf]);
                let line_buf = std::mem::take(&mut self.carry);
                let res = self.match_and_emit(&line_buf, sink);
                self.carry = line_buf;
                self.carry.clear();
                res
            };
            match matched_or_none {
                Ok(matched) => {
                    stats.lines_scanned += 1;
                    if matched {
                        stats.lines_matched += 1;
                    }
                }
                Err(e) => return Err(e),
            }
            cursor = lf + 1;
        }

        // Tail: bytes after the last LF (or all of chunk if no LF) → carry
        if cursor < chunk.len() {
            let tail = &chunk[cursor..];
            if self.carry.len() + tail.len() > self.max_line {
                return Err(ScanError::LineTooLong(self.carry.len() + tail.len()));
            }
            self.carry.extend_from_slice(tail);
        }
        Ok(stats)
    }

    /// Flush the final partial line, if any. End-of-stream call.
    ///
    /// Contract: `finish` consumes the carry buffer and replaces it with an
    /// empty `Vec` (no preallocated capacity). If a caller wants to reuse
    /// this `Scanner` for another independent stream, they must construct a
    /// fresh `Scanner` rather than reusing the post-`finish` instance — the
    /// empty `carry` will reallocate on first `feed` if a partial line spans
    /// chunks. For the streaming pipeline (one stream per `Scanner`) this is
    /// a non-issue.
    pub fn finish<S: LineSink>(
        &mut self,
        sink: &mut S,
    ) -> Result<ScanStats, ScanError<S::Error>> {
        let mut stats = ScanStats::default();
        if self.carry.is_empty() {
            return Ok(stats);
        }
        let line = std::mem::take(&mut self.carry);
        match self.match_and_emit(&line, sink)? {
            true  => stats.lines_matched = 1,
            false => {}
        }
        stats.lines_scanned = 1;
        Ok(stats)
    }

    /// Returns Ok(true) if matched & emitted; Ok(false) if not matched;
    /// Err on sink failure.
    fn match_and_emit<S: LineSink>(
        &self,
        line: &[u8],
        sink: &mut S,
    ) -> Result<bool, ScanError<S::Error>> {
        match self.matcher.match_line(line) {
            Some(rest) => {
                sink.emit(rest).map_err(ScanError::Sink)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Convenience: scan a complete buffer in one shot.
    pub fn scan_all<S: LineSink>(
        &mut self,
        buf: &[u8],
        sink: &mut S,
    ) -> Result<ScanStats, ScanError<S::Error>> {
        let mut stats = self.feed(buf, sink)?;
        stats += self.finish(sink)?;
        Ok(stats)
    }
}

impl<W: std::io::Write> LineSink for W {
    type Error = std::io::Error;
    fn emit(&mut self, line: &[u8]) -> Result<(), Self::Error> {
        self.write_all(line)?;
        self.write_all(b"\n")
    }
}
