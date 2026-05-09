# extractor-core

Zero-I/O scanning + matching primitives shared between `extract-mail`
(local CLI) and `telegram-client` (Telegram pipeline binary).

`#![forbid(unsafe_code)]` workspace-wide.

## What this crate does

- Domain-aware suffix matching (`gmail.com` matches `mail.gmail.com`
  but not `xgmail.com` or `gmail.com.vn`)
- Streaming line scanner that operates on `&[u8]` chunks — no I/O,
  no allocation per line
- `feed`/`finish` API so callers can plug arbitrary readers (memory-
  mapped files, HTTP bodies, gzip streams, zip entries) without
  re-implementing the matching logic
- Property-tested chunk-split invariance (`scan_all == feed+finish`)

## What this crate does NOT do

- No file open / mmap / network I/O — that's the consumer's job
- No output buffering — consumers wrap a `BufWriter` themselves
- No format detection (`.txt` / `.gz` / `.zip`) — see `telegram-client`'s
  `pipeline::format` module for that

## Build

```bash
cargo build -p extractor-core --release
cargo test  -p extractor-core --release
```

## Public API

See `cargo doc -p extractor-core --open` for the full surface. Key
entry points:

- `Scanner::new(config)` — build a stateful scanner
- `Scanner::scan_all(input, sink)` — single-shot over an in-memory slice
- `Scanner::feed(chunk, sink)` + `Scanner::finish(sink)` — chunked feed
- `Mode::Plain` (`domain:txt1:txt2`) and `Mode::Url`
  (`<scheme>://<host>[/path]:<email>:<password>`)

The scanner emits `&[u8]` line slices to a caller-provided `LineSink`;
no `String` allocation per line.

## Coverage

Property test in `tests/chunked_feed.rs` ensures scanner output is
chunk-boundary invariant. Coverage target ≥ 90% lines.

## License

MIT.
