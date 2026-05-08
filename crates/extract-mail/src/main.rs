use anyhow::{Context, Result};
use clap::Parser;
use memchr::{memchr, memmem, memrchr};
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::io::{stdout, BufWriter, Write};
use std::path::PathBuf;

/// Extract `txt1:txt2` from lines of the form `domain:txt1:txt2` where
/// `domain` equals the key or is a subdomain of it (domain-aware suffix
/// match: key="gmail.com" matches "gmail.com" and "mail.gmail.com" but not
/// "xgmail.com").
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Input file path
    #[arg(short, long)]
    file: PathBuf,

    /// Domain key to match against the field before the first `:`.
    /// Matches the field exactly OR any subdomain (`*.<key>`).
    #[arg(short, long)]
    key: String,

    /// Output file (defaults to stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of worker threads (0 = auto = number of CPU cores)
    #[arg(short = 'j', long, default_value_t = 0)]
    jobs: usize,

    /// Minimum chunk size in bytes for a parallel worker (default: 4 MiB)
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    chunk_size: usize,

    /// URL mode: parse lines as `<URL>:<email>:<password>` (URL contains
    /// `://`). The host portion of the URL is suffix-matched against `--key`,
    /// and the output is `<email>:<password>` for matching lines.
    #[arg(long)]
    url: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.jobs > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.jobs)
            .build_global()
            .context("failed to configure rayon thread pool")?;
    }

    let file = File::open(&args.file)
        .with_context(|| format!("failed to open {}", args.file.display()))?;

    // SAFETY: caller must ensure the file is not concurrently modified while
    // mapped. This is a documented invariant of memmap2::Mmap and is the
    // standard cost of using mmap for read-only file processing.
    let mmap = unsafe { Mmap::map(&file) }
        .with_context(|| format!("failed to mmap {}", args.file.display()))?;

    // Hint the kernel that we'll do a sequential scan — boosts read-ahead.
    #[cfg(unix)]
    let _ = mmap.advise(memmap2::Advice::Sequential);

    let data: &[u8] = &mmap;
    let key = args.key.as_bytes();

    let chunks = split_into_line_chunks(data, args.chunk_size);

    // Each worker produces its own output buffer. Keeping per-chunk buffers
    // means workers never block each other on a shared writer, and we
    // preserve input order trivially because we collect into a Vec.
    let parts: Vec<Vec<u8>> = if args.url {
        chunks
            .par_iter()
            .map(|&(start, end)| process_chunk_url(&data[start..end], key))
            .collect()
    } else {
        chunks
            .par_iter()
            .map(|&(start, end)| process_chunk(&data[start..end], key))
            .collect()
    };

    let writer: Box<dyn Write> = match &args.output {
        Some(path) => {
            let f = File::create(path)
                .with_context(|| format!("failed to create {}", path.display()))?;
            Box::new(BufWriter::with_capacity(1 << 20, f))
        }
        None => Box::new(BufWriter::with_capacity(1 << 20, stdout().lock())),
    };

    write_parts(writer, &parts).context("failed to write output")?;
    Ok(())
}

/// Split the buffer into chunks of approximately `target_size` bytes,
/// guaranteeing that each chunk ends on a newline boundary so workers
/// never see a partial line.
fn split_into_line_chunks(data: &[u8], target_size: usize) -> Vec<(usize, usize)> {
    if data.is_empty() {
        return Vec::new();
    }
    let target_size = target_size.max(1);
    let mut chunks = Vec::with_capacity(data.len() / target_size + 1);
    let mut start = 0usize;
    while start < data.len() {
        let tentative_end = start.saturating_add(target_size).min(data.len());
        let end = if tentative_end >= data.len() {
            data.len()
        } else {
            // Extend forward to the next newline so the chunk owns the full line.
            match memchr(b'\n', &data[tentative_end..]) {
                Some(rel) => tentative_end + rel + 1,
                None => data.len(),
            }
        };
        chunks.push((start, end));
        start = end;
    }
    chunks
}

/// Scan a chunk line-by-line and append matching `txt2:txt3` segments
/// (followed by `\n`) into a fresh output buffer.
fn process_chunk(chunk: &[u8], key: &[u8]) -> Vec<u8> {
    // Heuristic preallocation: matches are usually a small fraction of the
    // input. Start at 1/64 of chunk size to amortize re-allocation.
    let mut out = Vec::with_capacity(chunk.len() / 64);
    let mut pos = 0usize;
    while pos < chunk.len() {
        let line_end = match memchr(b'\n', &chunk[pos..]) {
            Some(rel) => pos + rel,
            None => chunk.len(),
        };
        let line = &chunk[pos..line_end];

        if let Some(colon) = memchr(b':', line) {
            // Domain-aware suffix match: the field before ':' must equal `key`
            // exactly, OR end with `.<key>` (so "gmail.com" matches
            // "mail.gmail.com" but not "xgmail.com").
            // Length pre-check rejects too-short fields without touching memory.
            if colon >= key.len() && matches_domain_suffix(&line[..colon], key) {
                out.extend_from_slice(&line[colon + 1..]);
                out.push(b'\n');
            }
        }

        pos = line_end + 1;
    }
    out
}

/// URL-mode chunk processor. Treats each line as `<URL>:<email>:<password>`,
/// extracts the host from the URL, and emits `<email>:<password>` if the host
/// matches the given key (domain-aware suffix).
fn process_chunk_url(chunk: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunk.len() / 64);
    let mut pos = 0usize;
    while pos < chunk.len() {
        let line_end = match memchr(b'\n', &chunk[pos..]) {
            Some(rel) => pos + rel,
            None => chunk.len(),
        };
        let line = &chunk[pos..line_end];

        if let Some(extracted) = extract_url_match(line, key) {
            out.extend_from_slice(extracted);
            out.push(b'\n');
        }

        pos = line_end + 1;
    }
    out
}

/// For a line of the form `<scheme>://<host>[/path…]:<email>:<password>`,
/// return `<email>:<password>` if the host matches `key` (domain-aware
/// suffix). Returns `None` for malformed or non-matching lines.
fn extract_url_match<'a>(line: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    // Locate "://" — anything without a scheme is not a URL line.
    let scheme_sep = memmem::find(line, b"://")?;
    let host_start = scheme_sep + 3;
    if host_start >= line.len() {
        return None;
    }

    // Host ends at the first byte that cannot appear in a hostname:
    // anything outside [a-zA-Z0-9.-]. Stops at '/', ':', '?', '#', etc.
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
    if host.len() < key.len() || !matches_domain_suffix(host, key) {
        return None;
    }

    // Find email:password as the LAST two ':' separated fields. Reading from
    // the right is robust against URLs that contain extra ':' (port, path,
    // query). The line layout is:
    //     <URL>:<email>:<password>
    // So the second-to-last ':' marks the start of <email>.
    let last_colon = memrchr(b':', line)?;
    if last_colon == 0 {
        return None;
    }
    let second_last_colon = memrchr(b':', &line[..last_colon])?;
    // Both colons must lie strictly after the host (otherwise the line has no
    // <email>:<password> tail at all — it would be just the URL).
    if second_last_colon < host_end {
        return None;
    }
    Some(&line[second_last_colon + 1..])
}

/// Domain-aware suffix match.
///
/// Returns `true` when `field` is exactly equal to `key`, or when `field`
/// ends with `.<key>` (so the boundary lies on a dot — preventing
/// `"xgmail.com"` from matching `"gmail.com"`).
///
/// `field.len() >= key.len()` is a precondition (callers verify this with
/// the cheap length check before calling).
#[inline]
fn matches_domain_suffix(field: &[u8], key: &[u8]) -> bool {
    debug_assert!(field.len() >= key.len());
    if field.len() == key.len() {
        return field == key;
    }
    // field.len() > key.len(): the byte just before the suffix must be '.'
    let split = field.len() - key.len();
    field[split - 1] == b'.' && &field[split..] == key
}

fn write_parts<W: Write>(mut writer: W, parts: &[Vec<u8>]) -> std::io::Result<()> {
    for part in parts {
        if !part.is_empty() {
            writer.write_all(part)?;
        }
    }
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(input: &str, key: &str) -> String {
        let chunks = split_into_line_chunks(input.as_bytes(), 16);
        let parts: Vec<Vec<u8>> = chunks
            .iter()
            .map(|&(s, e)| process_chunk(&input.as_bytes()[s..e], key.as_bytes()))
            .collect();
        let mut out = Vec::new();
        for p in &parts {
            out.extend_from_slice(p);
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn matches_exact_domain() {
        let input = "gmail.com:pw1:meta\nyahoo.com:pw2:meta\ngmail.com:pw3:meta\n";
        assert_eq!(run(input, "gmail.com"), "pw1:meta\npw3:meta\n");
    }

    #[test]
    fn matches_subdomain() {
        // mail.gmail.com is a subdomain of gmail.com → match
        let input = "mail.gmail.com:a:b\ngmail.com:c:d\nfoo.bar.gmail.com:e:f\n";
        assert_eq!(run(input, "gmail.com"), "a:b\nc:d\ne:f\n");
    }

    #[test]
    fn rejects_non_subdomain_suffix() {
        // "xgmail.com" ends with "gmail.com" textually but is NOT a subdomain
        // (boundary is not a dot) → must NOT match
        let input = "xgmail.com:a:b\nnot-gmail.com:c:d\ngmail.com:e:f\n";
        assert_eq!(run(input, "gmail.com"), "e:f\n");
    }

    #[test]
    fn rejects_partial_or_unrelated() {
        let input = "gmail.co:a:b\ngmail.commerce:c:d\nyahoo.com:e:f\n";
        assert_eq!(run(input, "gmail.com"), "");
    }

    #[test]
    fn dot_is_required_boundary() {
        // direct equality on length-match path
        assert!(matches_domain_suffix(b"gmail.com", b"gmail.com"));
        // proper subdomain
        assert!(matches_domain_suffix(b"mail.gmail.com", b"gmail.com"));
        assert!(matches_domain_suffix(b"a.b.c.gmail.com", b"gmail.com"));
        // boundary is not '.' → reject
        assert!(!matches_domain_suffix(b"xgmail.com", b"gmail.com"));
        assert!(!matches_domain_suffix(b"-gmail.com", b"gmail.com"));
    }

    #[test]
    fn handles_missing_trailing_newline() {
        let input = "gmail.com:a:b";
        assert_eq!(run(input, "gmail.com"), "a:b\n");
    }

    #[test]
    fn skips_lines_without_colon() {
        let input = "garbage\ngmail.com:a:b\n\n";
        assert_eq!(run(input, "gmail.com"), "a:b\n");
    }

    #[test]
    fn empty_input() {
        let input = "";
        assert_eq!(run(input, "gmail.com"), "");
    }

    fn run_url(input: &str, key: &str) -> String {
        let chunks = split_into_line_chunks(input.as_bytes(), 16);
        let parts: Vec<Vec<u8>> = chunks
            .iter()
            .map(|&(s, e)| process_chunk_url(&input.as_bytes()[s..e], key.as_bytes()))
            .collect();
        let mut out = Vec::new();
        for p in &parts {
            out.extend_from_slice(p);
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn url_mode_basic() {
        let input = "\
http://br.linkedin.com/:alice@x.com:pwd1
https://www.google.com:bob@y.com:pwd2
http://in.linkedin.com/feed:carol@z.com:pwd3
";
        assert_eq!(
            run_url(input, "linkedin.com"),
            "alice@x.com:pwd1\ncarol@z.com:pwd3\n"
        );
    }

    #[test]
    fn url_mode_rejects_pseudo_subdomain() {
        // "com.linkedin.android" ends with ".android", not ".linkedin.com"
        let input = "http://com.linkedin.android:eve@hack.com:pwd\n\
                     http://br.linkedin.com/:alice@x.com:pwd1\n";
        assert_eq!(run_url(input, "linkedin.com"), "alice@x.com:pwd1\n");
    }

    #[test]
    fn url_mode_handles_port_and_path() {
        let input = "https://mail.linkedin.com:8443/path/to/page:user@x.com:secret\n";
        assert_eq!(run_url(input, "linkedin.com"), "user@x.com:secret\n");
    }

    #[test]
    fn url_mode_skips_garbage_lines() {
        let input = "junk line no scheme\n\
                     http://gmail.com/:a@b.com:pwd\n\
                     incomplete:line\n";
        assert_eq!(run_url(input, "gmail.com"), "a@b.com:pwd\n");
    }

    #[test]
    fn chunk_split_preserves_lines() {
        // Force many chunks; output must be identical to single-pass.
        let input: String = (0..1000)
            .map(|i| format!("a.gmail.com:val{}:extra\nyahoo.com:x:y\n", i))
            .collect();
        let small_chunks = run(&input, "gmail.com");
        // Single chunk reference
        let one = process_chunk(input.as_bytes(), b"gmail.com");
        assert_eq!(small_chunks.as_bytes(), one.as_slice());
    }
}
