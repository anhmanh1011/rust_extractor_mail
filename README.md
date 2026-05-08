# extractor_mail workspace

A Rust workspace that extracts credential records from very large
domain-keyed text dumps, with two binaries:

- **`extract-mail`** — the original local-file CLI (mmap + SIMD + rayon, ~960 MB/s)
- **`tg-extract`** — Telegram pipeline: streams large files from channels,
  extracts in real time, uploads results back to a user-controlled channel

Shared logic lives in **`extractor-core`**.

## Build

```bash
cargo build --release
# binaries at target/release/extract-mail and target/release/tg-extract
```

## Test

```bash
cargo test --workspace --release
```

## Crates

| Crate             | Type | Purpose                              |
| ----------------- | ---- | ------------------------------------ |
| `extractor-core`  | lib  | Zero-I/O scanner + matcher           |
| `extract-mail`    | bin  | Local-file CLI                       |
| `telegram-client` | bin  | `tg-extract` — Telegram pipeline     |

See `docs/superpowers/specs/2026-05-07-telegram-large-file-pipeline-design.md`
for the full design.

## Security

Never commit `config.toml`, `.env*`, or `*.session`. The session file is a
bearer credential equivalent to your Telegram account.
