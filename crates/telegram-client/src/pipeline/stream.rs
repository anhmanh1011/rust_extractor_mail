//! Stream extraction path (spec §4.1, §5.3).
//!
//! Bridge: `tokio::sync::mpsc<Bytes>` (cap=4)
//!   → `tokio::task::spawn_blocking`
//!   → `std::sync::mpsc::sync_channel<Bytes>(4)`
//!   → dedicated `std::thread` running `Scanner`
//!
//! Invariants:
//! - The tokio side NEVER calls `scanner.feed`.
//! - The std::thread NEVER calls any tokio API.
//! - On `chunks.recv()` returning `None` we drop the bridge sender; the
//!   scanner thread observes `Err(RecvError)`, runs `scanner.finish()`,
//!   flushes, and returns `(W, ScanStats)`.

use std::io::{Read, Write};
use std::sync::mpsc::{sync_channel, Receiver as StdReceiver, RecvError};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use extractor_core::{Matcher, ScanStats, Scanner};
use tokio::sync::mpsc as tokio_mpsc;

use crate::pipeline::sink::WriterSink;

const BRIDGE_CAPACITY: usize = 4;
const GZ_READ_BUFFER_BYTES: usize = 64 * 1024;

/// Extract matching lines from a stream of byte chunks into `writer`.
///
/// `is_gzip == true` wraps the chunk stream in a [`flate2::read::GzDecoder`]
/// before feeding the scanner. The decoder runs on the same `std::thread`
/// as the scanner, so decompression is also off the tokio reactor.
///
/// On clean shutdown — i.e. `chunks.recv()` returning `None` — the scanner
/// thread runs `scanner.finish()`, flushes the [`WriterSink`], and the
/// underlying writer is returned alongside [`ScanStats`].
pub async fn stream_extract<W>(
    mut chunks: tokio_mpsc::Receiver<Bytes>,
    matcher: Arc<Matcher>,
    max_line_bytes: usize,
    writer: W,
    is_gzip: bool,
) -> Result<(W, ScanStats)>
where
    W: Write + Send + 'static,
{
    let (bridge_tx, bridge_rx) = sync_channel::<Bytes>(BRIDGE_CAPACITY);

    let worker = tokio::task::spawn_blocking(move || -> Result<(W, ScanStats)> {
        let mut sink = WriterSink::new(writer);
        let mut scanner = Scanner::with_max_line(&matcher, max_line_bytes);
        let stats = if is_gzip {
            run_gz(bridge_rx, &mut scanner, &mut sink)?
        } else {
            run_plain(bridge_rx, &mut scanner, &mut sink)?
        };
        let inner = sink.into_inner().context("flush sink writer")?;
        Ok((inner, stats))
    });

    let pump = tokio::spawn(async move {
        while let Some(c) = chunks.recv().await {
            let tx = bridge_tx.clone();
            let send_res = tokio::task::spawn_blocking(move || tx.send(c)).await;
            match send_res {
                Ok(Ok(())) => {}
                Ok(Err(_)) => break, // worker died / dropped its receiver
                Err(join_err) => {
                    return Err(anyhow::anyhow!("bridge pump join: {join_err}"));
                }
            }
        }
        drop(bridge_tx);
        Ok::<_, anyhow::Error>(())
    });

    let pump_res = pump.await.context("bridge pump task panicked")?;
    let (out, stats) = worker.await.context("scanner thread panicked")??;
    pump_res?;
    Ok((out, stats))
}

fn run_plain<W: Write>(
    rx: StdReceiver<Bytes>,
    scanner: &mut Scanner,
    sink: &mut WriterSink<W>,
) -> Result<ScanStats> {
    let mut total = ScanStats::default();
    loop {
        match rx.recv() {
            Ok(buf) => {
                let s = scanner.feed(&buf, sink).context("scanner.feed")?;
                accumulate(&mut total, &s);
            }
            Err(RecvError) => {
                let s = scanner.finish(sink).context("scanner.finish")?;
                accumulate(&mut total, &s);
                return Ok(total);
            }
        }
    }
}

fn run_gz<W: Write>(
    rx: StdReceiver<Bytes>,
    scanner: &mut Scanner,
    sink: &mut WriterSink<W>,
) -> Result<ScanStats> {
    let mut decoder = flate2::read::GzDecoder::new(ChannelReader::new(rx));
    let mut buf = vec![0u8; GZ_READ_BUFFER_BYTES];
    let mut total = ScanStats::default();
    loop {
        let n = decoder.read(&mut buf).context("gz decode")?;
        if n == 0 {
            let s = scanner.finish(sink).context("scanner.finish")?;
            accumulate(&mut total, &s);
            return Ok(total);
        }
        let s = scanner.feed(&buf[..n], sink).context("scanner.feed")?;
        accumulate(&mut total, &s);
    }
}

fn accumulate(total: &mut ScanStats, delta: &ScanStats) {
    total.lines_scanned += delta.lines_scanned;
    total.lines_matched += delta.lines_matched;
    total.bytes_scanned += delta.bytes_scanned;
}

/// `Read`-implementing adapter over `std::sync::mpsc::Receiver<Bytes>`.
/// Holds at most one `Bytes` of in-flight residue between calls.
struct ChannelReader {
    rx: StdReceiver<Bytes>,
    buf: Bytes,
}

impl ChannelReader {
    fn new(rx: StdReceiver<Bytes>) -> Self {
        Self {
            rx,
            buf: Bytes::new(),
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.buf.is_empty() {
            match self.rx.recv() {
                Ok(b) => self.buf = b,
                Err(_) => return Ok(0),
            }
        }
        let n = std::cmp::min(self.buf.len(), out.len());
        out[..n].copy_from_slice(&self.buf[..n]);
        let _ = self.buf.split_to(n);
        Ok(n)
    }
}
