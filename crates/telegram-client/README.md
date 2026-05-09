# telegram-client (binary `tg-extract`)

Pipeline that downloads large credential dumps from a Telegram channel,
extracts matching records via `extractor-core`, and uploads the filtered
output to a target channel. Designed for files in the 500 MB – 2 GB range.

`#![forbid(unsafe_code)]` workspace-wide.

## Quick start

```bash
# 1. Configure (see config.toml.example at the workspace root)
cp config.toml.example config.toml
${EDITOR:-vi} config.toml
export TG_API_ID=<your numeric id>
export TG_API_HASH=<your 32-hex-char string>

# 2. First-run auth (interactive)
cargo run --release -p telegram-client -- auth

# 3. Discover channels
cargo run --release -p telegram-client -- chats --filter dump

# 4. Pull a single big file
cargo run --release -p telegram-client -- fetch \
    --link https://t.me/somechannel/12345 \
    --key gmail.com

# 5. Long-running tail
cargo run --release -p telegram-client -- watch --duration-seconds 86400

# 6. Backfill an entire channel
cargo run --release -p telegram-client -- backfill <chat-id> --limit 100

# 7. Drain pending upload retries
cargo run --release -p telegram-client -- retry-uploads

# 8. Aggregate stats
cargo run --release -p telegram-client -- stats
```

## Architecture

3-stage inter-file pipeline (spec §4.2):

```
sources -> [Stage 1: download] -> [Stage 2: extract+write] -> [Stage 3: upload] -> target chat
              (cap=N concurrent)     (cap=1 — serialized)         (cap=N retries)
```

- Stage 1 streams `.txt` / `.gz` from grammers; spills `.zip` to a
  per-job tempfile (delete-on-drop).
- Stage 2 runs `extractor-core::Scanner` and writes one `.out` file
  per source.
- Stage 3 uploads with retry, captioned with provenance metadata.
- A `Store` (SQLite, bundled) tracks file-level dedup, watch / backfill
  cursors, failed uploads, and a dead-letter audit table.

## Subcommands

| Subcommand | Purpose |
|---|---|
| `auth` | First-run interactive login. Stores session at the configured `session_path` (chmod 0600 on Unix). |
| `chats` | List dialog channels, optionally filtered by name. |
| `join` | Join a channel by invite link. |
| `fetch` | Pull a single message's attached file. |
| `watch` | Subscribe to one or more channels and pull new files for `--duration-seconds`. |
| `backfill` | Pull historical files in a channel up to `--limit`, with `--since` and `--resume`. |
| `retry-uploads` | Drain the `failed_uploads` queue. |
| `stats` | Aggregate counts, per-channel breakdown, last 10 dead-letter errors, failed-upload queue depth. |

## Configuration

See `config.toml.example` at the workspace root (annotated). Secrets:

- `TG_API_ID`, `TG_API_HASH` — env vars (NEVER in `config.toml`)
- Session file — `chmod 0600` on Unix; on Windows relies on the
  per-user profile dir's ACL

## Operational warnings

- **Use a throwaway Telegram account.** Automation behavior may
  trigger account ban (spec §13).
- **Never run alongside Telegram Desktop with the same session file.**
  Concurrent file locks corrupt the session (Windows-specific).
- **Output files contain credentials in plaintext.** Delete after
  exfiltration. The `.gitignore` blocks `*.out` by default.

## Testing

```bash
cargo test -p telegram-client --release
```

CI gates: `cargo test`, `cargo clippy -- -D warnings`,
`cargo audit --deny warnings`. Live Telegram is **not** in CI; the
`MockClient` covers all integration scenarios under `tests/`.

## License

MIT.
