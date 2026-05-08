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
        _chunk: &[u8],
        _sink: &mut S,
    ) -> Result<ScanStats, ScanError<S::Error>> {
        unimplemented!("Task 1.4")
    }

    /// Flush the final partial line (if any) — call exactly once when the
    /// stream ends.
    pub fn finish<S: LineSink>(
        &mut self,
        _sink: &mut S,
    ) -> Result<ScanStats, ScanError<S::Error>> {
        unimplemented!("Task 1.4")
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
