//! Buffered file sink for matched lines (spec §4.1: 1 MiB `BufWriter`).
//!
//! The scanner thread owns a `WriterSink<W>` for the duration of a single
//! source file. Each matched line is appended verbatim followed by `\n`,
//! letting downstream tools split on newlines without ambiguity. On end
//! of stream the caller invokes [`WriterSink::into_inner`] which flushes
//! the buffer and hands back the underlying writer.

use std::io::{self, BufWriter, Write};

use extractor_core::LineSink;

const SINK_BUFFER_BYTES: usize = 1 << 20; // 1 MiB per spec §4.1

/// Adapter from `extractor_core::LineSink` to any `std::io::Write`.
///
/// Wraps the writer in a [`BufWriter`] sized [`SINK_BUFFER_BYTES`] so
/// per-line `write_all` calls coalesce into ~1 MiB OS-level writes.
pub struct WriterSink<W: Write> {
    inner: BufWriter<W>,
}

impl<W: Write> WriterSink<W> {
    /// Wrap `w` in a 1 MiB-buffered LineSink.
    pub fn new(w: W) -> Self {
        Self {
            inner: BufWriter::with_capacity(SINK_BUFFER_BYTES, w),
        }
    }

    /// Flush any pending buffered bytes and recover the underlying writer.
    /// Used by the stream stage to close the output file with a guaranteed
    /// flush before returning [`extractor_core::ScanStats`] to the caller.
    pub fn into_inner(self) -> io::Result<W> {
        self.inner.into_inner().map_err(|e| e.into_error())
    }

    /// Force any pending buffered bytes to the underlying writer without
    /// consuming the sink.
    pub fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> LineSink for WriterSink<W> {
    type Error = io::Error;
    fn emit(&mut self, line: &[u8]) -> Result<(), Self::Error> {
        self.inner.write_all(line)?;
        self.inner.write_all(b"\n")
    }
}
