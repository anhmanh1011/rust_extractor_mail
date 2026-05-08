//! Generator for synthetic test data of the form `domain:txt1:txt2`.
//!
//! Designed to be I/O bound: a small set of domains is rotated, each line
//! is built from a tiny LCG so we don't pay for `rand` overhead, and output
//! is streamed through a 4 MiB BufWriter.

use anyhow::{Context, Result};
use clap::Parser;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(about = "Generate synthetic domain:txt1:txt2 test data")]
struct Args {
    /// Output file path
    #[arg(short, long)]
    output: PathBuf,

    /// Number of lines to generate
    #[arg(short, long)]
    lines: u64,

    /// Target domain for guaranteed hits (gets injected at fixed cadence)
    #[arg(long, default_value = "target.com")]
    target: String,

    /// Insert a target hit every N lines (0 = never)
    #[arg(long, default_value_t = 1_000_000)]
    hit_every: u64,
}

const DOMAINS: &[&str] = &[
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
    "protonmail.com", "aol.com", "live.com", "mail.com", "zoho.com",
    "yandex.com", "gmx.com", "fastmail.com", "tutanota.com", "hey.com",
    "example.com", "example.org", "test.com", "company.com", "corp.net",
    "alpha.io", "beta.io", "gamma.io", "delta.io", "epsilon.io",
    "acme.com", "globex.com", "initech.com", "umbrella.com", "wayne.com",
    "stark.com", "oscorp.com", "lexcorp.com", "weyland.com", "tyrell.com",
    "cyberdyne.com", "soylent.com", "hooli.com", "pied-piper.com", "dunder.com",
    "vandelay.com", "kramerica.com", "ollivanders.com", "gringotts.com", "hogwarts.edu",
    "starfleet.org", "tardis.tv", "rebel.org", "empire.gov", "republic.gov",
    "atlantis.gov", "valhalla.no", "olympus.gr", "asgard.no", "midgard.no",
    "shire.nz", "mordor.tk", "rohan.uk", "gondor.uk", "rivendell.elf",
    "cyber.io", "secure.io", "vault.io", "fortress.io", "shield.io",
    "nova.io", "stellar.io", "lunar.io", "solar.io", "comet.io",
    "matrix.io", "neo.io", "morpheus.io", "trinity.io", "zion.io",
    "skynet.ai", "deepmind.ai", "openai.io", "anthropic.io", "mistral.ai",
    "vector.ai", "tensor.ai", "neural.ai", "synaptic.ai", "cortex.ai",
    "delta-corp.com", "omega-corp.com", "sigma-corp.com", "phi-corp.com", "psi-corp.com",
    "north.com", "south.com", "east.com", "west.com", "central.com",
    "first.io", "second.io", "third.io", "fourth.io", "fifth.io",
    "alpha-tech.io", "beta-tech.io", "gamma-tech.io", "delta-tech.io", "epsilon-tech.io",
];

fn main() -> Result<()> {
    let args = Args::parse();

    let file = File::create(&args.output)
        .with_context(|| format!("failed to create {}", args.output.display()))?;
    // 4 MiB buffer — large enough that write() syscalls don't dominate.
    let mut w = BufWriter::with_capacity(4 << 20, file);

    let target = args.target.as_bytes();
    let start = Instant::now();
    let mut lcg: u64 = 0x9E3779B97F4A7C15; // golden ratio constant as seed

    let mut total_bytes: u64 = 0;
    let mut buf = Vec::with_capacity(96);

    for i in 0..args.lines {
        buf.clear();

        // Decide whether this is a guaranteed hit.
        let is_hit = args.hit_every > 0 && i % args.hit_every == 0;

        // LCG step (Numerical Recipes parameters)
        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);

        if is_hit {
            buf.extend_from_slice(target);
        } else {
            let domain = DOMAINS[(lcg as usize >> 33) % DOMAINS.len()];
            buf.extend_from_slice(domain.as_bytes());
        }
        buf.push(b':');

        // txt1 = "u<i>"
        write_u64(&mut buf, b'u', i);
        buf.push(b':');

        // txt2 = "p<lcg>"
        write_u64(&mut buf, b'p', lcg);
        buf.push(b'\n');

        w.write_all(&buf)?;
        total_bytes += buf.len() as u64;

        if i > 0 && i % 50_000_000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            eprintln!(
                "  [gen] {} lines, {:.2} GB, {:.1} M lines/s",
                i,
                total_bytes as f64 / (1u64 << 30) as f64,
                i as f64 / elapsed / 1e6
            );
        }
    }

    w.flush()?;
    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "[gen] done: {} lines, {:.2} GB, {:.2}s ({:.1} M lines/s, {:.0} MB/s)",
        args.lines,
        total_bytes as f64 / (1u64 << 30) as f64,
        elapsed,
        args.lines as f64 / elapsed / 1e6,
        total_bytes as f64 / elapsed / (1u64 << 20) as f64,
    );
    Ok(())
}

/// Append `prefix` followed by the decimal representation of `n` to `buf`,
/// without going through the formatter (saves allocations).
fn write_u64(buf: &mut Vec<u8>, prefix: u8, n: u64) {
    buf.push(prefix);
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut v = n;
    if v == 0 {
        buf.push(b'0');
        return;
    }
    while v > 0 {
        i -= 1;
        tmp[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    buf.extend_from_slice(&tmp[i..]);
}
