# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-05-09

### Added

- `extractor-core` crate: zero-I/O streaming scanner shared between
  `extract-mail` and `telegram-client`. Property-tested chunk-split
  invariance (`scan_all == feed+finish`).
- `extract-mail` crate: refactored v0 CLI, behavior-preserving fork
  on top of `extractor-core`. No perf regression vs. the v0
  `rust_extractor_mail/` archive.
- `telegram-client` crate (binary `tg-extract`):
  - Subcommands: `auth`, `chats`, `join`, `fetch`, `watch`,
    `backfill`, `retry-uploads`, `stats`.
  - 3-stage inter-file pipeline (spec §4.2): download → extract+write
    → upload. Stream path for `.txt` / `.gz`, disk-spill for `.zip`.
  - SQLite-backed file dedup, watch / backfill cursors, failed-upload
    queue, dead-letter audit trail. Single writer + WAL + foreign
    keys, opened once at startup with `Store::reset_in_flight()` to
    recover transient-state rows from a previous interrupted run.
  - Auto-reconnect on grammers `Disconnected` (capped exponential
    backoff).
  - `MultiProgress` download + upload bars (TTY only — daemon / cron
    suppresses bars at the source via `IsTerminal`).

### Hardening

- Path-traversal sanitizer for zip entries and output filenames
  (`output.rs` strips path components before writing under
  `output_dir`).
- `max_uncompressed_bytes` cap (zip-bomb defence — the only such
  guard, per spec §11.2).
- `SecretScrubLayer` for tracing — redacts fields whose name matches
  `(?i)hash|key|secret|token|password|auth|session`.
- `Secrets` `Debug` prints `<redacted>` for `api_hash`; intentionally
  no `Display` impl.
- Session file `chmod 0600` on Unix.
- `forbid(unsafe_code)` workspace-wide.
- CI `cargo audit --deny warnings` gate
  (`severity_threshold = low`).

### Tests

- `extractor-core`: ≥ 90% line coverage, including a `proptest`
  shrinking against chunk-split divergence.
- `telegram-client`: ≥ 70% line coverage. Mock-driven integration
  suite (no live Telegram in CI) covering 3-stage backpressure,
  inter-file cap=1, zip-bomb regression, path-traversal regression,
  log-leakage regression, retry queue, secrets redaction, and
  rolling-appender smoke.

### Known limitations (deferred to v1.x)

- No resumable downloads — Ctrl-C mid-2 GB file = redownload.
  (Spec §14, +1 day to add chunk-offset persistence.)
- No record-level dedup across runs — only file-level (sha256).
- No multi-account session pool — one account's rate limits cap
  throughput.
- Live-TG smoke tests are manual (`auth` → throwaway account →
  fetch 1.5 GB). See spec §9.3 for the recipe.
- Path-traversal regression test only exercises POSIX `../`. The
  Phase-4 unit suite covers Windows `\\..\\` separately. A Windows
  integration variant is a v1.1 follow-up.
- `pipeline_log_leakage_e2e` asserts negative on the human formatter
  only; the JSON formatter shares the same `SecretScrubLayer` but
  does not have an independent integration assertion. v1.1 follow-up.
- `stats` does NOT show dedup hit count.
  `EnqueueResult::AlreadyDone` short-circuits before any row is
  written, so the count is not persisted in `files`. Operator infers
  it from the gap between `fetch` log lines and `stats` total. v1.1
  follow-up: dedicated dedup counter table.
- `MultiProgress` upload bar uses per-attempt-tick fallback —
  granular byte tracking needs a chunked send hook from grammers.
- `observability_rotation.rs` does not validate rotation cadence —
  that is `tracing-appender`'s contract. v1 trusts the dependency.

### Operational warnings

- Use a throwaway Telegram account (spec §13: low-likelihood,
  critical-impact ban risk).
- Do not run alongside Telegram Desktop on Windows with the same
  session file — concurrent locks corrupt the session.
- Output `.out` files are plaintext credentials; delete after
  exfiltration. `.gitignore` blocks them by default.
- Never commit `config.toml`, `.env*`, or `*.session` —
  `.gitignore` already covers these.

[1.0.0]: https://example.invalid/tag/v1.0.0
