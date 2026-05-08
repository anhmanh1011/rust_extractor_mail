//! extract-mail — local-file CLI for extracting credential records.

use anyhow::{Context, Result};
use clap::Parser;
use extractor_core::{Matcher, Mode, Scanner};
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::io::{stdout, BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short, long)]
    file: PathBuf,

    #[arg(short, long)]
    key: String,

    #[arg(short, long)]
    output: Option<PathBuf>,

    #[arg(short = 'j', long, default_value_t = 0)]
    jobs: usize,

    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    chunk_size: usize,

    /// URL mode (lines: <url>:<email>:<password>)
    #[arg(long)]
    url: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.jobs > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.jobs)
            .build_global()
            .context("rayon configure")?;
    }

    let file = File::open(&args.file)
        .with_context(|| format!("open {}", args.file.display()))?;
    // SAFETY: file must not be modified while mapped (documented mmap invariant).
    let mmap = unsafe { Mmap::map(&file) }
        .with_context(|| format!("mmap {}", args.file.display()))?;
    #[cfg(unix)]
    let _ = mmap.advise(memmap2::Advice::Sequential);

    let mode = if args.url { Mode::Url } else { Mode::Plain };
    let matcher = Matcher::new(&args.key, mode)
        .with_context(|| format!("invalid key: {:?}", args.key))?;

    let chunks = split_into_line_chunks(&mmap, args.chunk_size);

    let parts: Vec<Vec<u8>> = chunks
        .par_iter()
        .map(|&(start, end)| -> Result<Vec<u8>> {
            let mut out = Vec::with_capacity(64 * 1024);
            let mut scanner = Scanner::new(&matcher);
            scanner
                .scan_all(&mmap[start..end], &mut out)
                .map_err(|e| anyhow::anyhow!("scan: {e}"))?;
            Ok(out)
        })
        .collect::<Result<_>>()?;

    let writer: Box<dyn Write> = match &args.output {
        Some(p) => Box::new(BufWriter::with_capacity(
            1 << 20,
            File::create(p).with_context(|| format!("create {}", p.display()))?,
        )),
        None => Box::new(BufWriter::with_capacity(1 << 20, stdout().lock())),
    };
    write_parts(writer, &parts)?;

    Ok(())
}

/// Partition `data` into newline-aligned half-open byte ranges.
/// Each chunk is at least `target_size` bytes (except possibly the last).
fn split_into_line_chunks(data: &[u8], target_size: usize) -> Vec<(usize, usize)> {
    if data.is_empty() {
        return Vec::new();
    }
    let target_size = target_size.max(64 * 1024);
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < data.len() {
        let mut end = (start + target_size).min(data.len());
        if end < data.len() {
            // advance to next '\n' (inclusive) so chunk ends on line boundary
            match memchr::memchr(b'\n', &data[end..]) {
                Some(rel) => end += rel + 1,
                None => end = data.len(),
            }
        }
        out.push((start, end));
        start = end;
    }
    out
}

fn write_parts<W: Write>(mut writer: W, parts: &[Vec<u8>]) -> std::io::Result<()> {
    for part in parts {
        writer.write_all(part)?;
    }
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_empty() {
        assert!(split_into_line_chunks(b"", 64).is_empty());
    }

    #[test]
    fn split_no_newline() {
        let chunks = split_into_line_chunks(b"abcd", 1);
        assert_eq!(chunks, vec![(0, 4)]);
    }

    #[test]
    fn split_multiple_chunks_align_on_newline() {
        let data = b"aaaa\nbbbb\ncccc\ndddd\n";
        let chunks = split_into_line_chunks(data, 1);
        // Each chunk at least 64 KiB → entire data is one chunk because
        // target_size is clamped to 64 KiB.
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], (0, data.len()));
    }
}
