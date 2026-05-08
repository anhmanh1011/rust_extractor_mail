# extract_mail

A high-throughput Rust CLI for extracting matching records from very large
colon-delimited text files (multi-GB scale).

Designed for line-oriented inputs of the form:

- **Plain mode:**  `domain:txt1:txt2`
- **URL mode:**    `<scheme>://<host>[/path…]:<email>:<password>`

The tool reads the file via memory-mapped I/O, scans it in parallel across
all CPU cores using SIMD-accelerated byte search, and emits matching
records line-by-line.

---

## Features

- **Memory-mapped I/O** (`memmap2`) — no userspace copy, kernel manages paging
- **SIMD byte search** (`memchr`) — finds `\n` and `:` near memory bandwidth
- **Data-parallel** (`rayon`) — chunks the file on newline boundaries and
  scans them concurrently; output preserves input order
- **Domain-aware suffix match** — `gmail.com` matches `gmail.com` and
  `mail.gmail.com`, but **not** `xgmail.com` or `gmail.com.vn`
- **URL mode** — robust parser for `<URL>:<email>:<password>` lines,
  extracting the host from the URL before suffix-matching
- **Buffered streaming output** — 1 MiB `BufWriter` to keep syscalls cheap
- **Zero-copy hot path** — no `String` allocation per line
- **~960 MB/s** sustained throughput on Apple Silicon NVMe (disk-bound)

---

## Build

Requires Rust 1.70+ (install via [rustup](https://rustup.rs)).

```bash
cargo build --release
```

The binary is produced at `./target/release/extract_mail`.

---

## Usage

### Plain mode (`domain:txt1:txt2`)

```bash
./target/release/extract_mail -f data.txt -k gmail.com -o gmail.out
```

For a line `gmail.com:user@x.com:pass`, the output is `user@x.com:pass`.

### URL mode (`<URL>:<email>:<password>`)

```bash
./target/release/extract_mail --url -f dump.txt -k linkedin.com -o linkedin.out
```

For a line `http://br.linkedin.com/:alice@x.com:pwd1`, the output is
`alice@x.com:pwd1`. The host portion of the URL is extracted and
suffix-matched against the key, so subdomains (`br.linkedin.com`,
`mail.linkedin.com`) match while pseudo-suffixes (`com.linkedin.android`)
are correctly rejected.

### Flags

| Flag | Required | Description |
|---|:---:|---|
| `-f, --file <PATH>` | yes | Input file |
| `-k, --key <DOMAIN>` | yes | Domain to match (exact or subdomain) |
| `-o, --output <PATH>` | no | Output file (default: stdout) |
| `--url` | no | Switch to URL-format parsing |
| `-j, --jobs <N>` | no | Worker threads (0 = all cores) |
| `--chunk-size <BYTES>` | no | Per-worker chunk size (default 4 MiB) |

Run `--help` for the full list.

---

## Match semantics

Both plain and URL modes use **domain-aware suffix matching**: the field
must equal the key exactly, or end with `.<key>` (boundary must be a dot).

| Field value | `--key gmail.com` | Match? |
|---|---|:---:|
| `gmail.com` | exact | ✅ |
| `mail.gmail.com` | subdomain | ✅ |
| `foo.bar.gmail.com` | deep subdomain | ✅ |
| `xgmail.com` | wrong boundary | ❌ |
| `gmail.com.vn` | extra suffix | ❌ |
| `gmail.commerce` | not subdomain | ❌ |

---

## Performance

Synthetic benchmark on Apple M-series, 16 GB RAM, NVMe SSD:

| Lines | Size | Cold cache | Warm cache | Throughput |
|---:|---:|---:|---:|---:|
| 10M | 394 MB | 0.27s | — | ~1.5 GB/s |
| 100M | 3.9 GB | 3.26s | 0.23s | ~1.2 GB/s cold / 17 GB/s warm |
| 500M | 20 GB | 20.88s | 19.63s | ~960 MB/s (disk-bound) |

Scaling with `-j N` (10-core machine, 500M lines, common key):

| `-j` | Real time |
|---:|---:|
| 1 | 32.92s |
| 2 | 28.43s |
| 8 | 23.98s |
| 10 | **21.50s** |

Sub-linear scaling because the bottleneck is sequential disk read, not
CPU. CPU work is roughly **32 M lines / core / second**.

---

## Algorithm

For each line:

1. Find the next `\n` with `memchr` (SIMD).
2. **Plain mode:** find the first `:`; reject by length (`colon == key.len()`
   path) or boundary; if matched, emit `&line[colon+1..]`.
3. **URL mode:** locate `://`; extract host as the run of `[a-zA-Z0-9.-]`
   bytes after; suffix-match host; if matched, scan from the right for
   the last two `:` to slice off `<email>:<password>`.

Workers operate on disjoint, newline-aligned chunks; per-worker output
buffers are concatenated by the main thread to preserve input order.

---

## Tests

```bash
cargo test --release
```

13 unit tests cover exact match, subdomain, pseudo-subdomain rejection,
URL parsing (port + path), garbage-line skipping, and chunk-split
invariance.

---

## Synthetic data generator

A second binary `generate` emits realistic test data:

```bash
./target/release/generate -o data/big.txt -l 100000000 \
  --target target.com --hit-every 1000000
```

Generates 100 M lines of `domain:txt1:txt2` rotating across ~100 domains,
with a guaranteed hit on `target.com` every `--hit-every` lines.

---

## Security notes

- **Never commit credential files.** `.gitignore` excludes `*.txt`, `*.out`,
  and common dump file names by default.
- The tool writes plaintext output. Delete output files (`rm` or `srm`)
  immediately after use if they contain sensitive data.
- Tool reads the file with `mmap` — content is processed entirely in
  user space; no network calls are made.

---

## License

MIT (or your choice — add a `LICENSE` file).
