# Telegram Large-File Extraction Pipeline — Design Spec

**Status**: Draft for review
**Author**: Đào Đức Mạnh + Claude (brainstorming session)
**Date**: 2026-05-07
**Scope**: Single project, ~13 dev-days

---

## 1. Overview

A Rust application (`telegram-client`) that streams large files (500 MB – 2 GB)
from Telegram channels (public and private), runs domain-targeted credential
extraction on them in real time, and uploads the extracted results back to a
user-controlled Telegram channel. The extraction logic is shared with the
existing `extract-mail` CLI by refactoring the current single-binary repo into
a Cargo workspace.

The new tool, the existing CLI, and the shared logic live as three crates in
one workspace:

```
extractor_mail/                      ← workspace root
├── Cargo.toml                       ← [workspace]
└── crates/
    ├── extractor-core/              ← lib (zero-I/O extraction logic)
    ├── extract-mail/                ← bin (refactored from current main.rs)
    └── telegram-client/             ← bin (new)
```

## 2. Goals / Non-goals

### Goals
- Stream `.txt` and `.gz` files from Telegram with zero disk spill on the
  source file path. Memory peak per in-flight file is independent of file
  size (~12 MiB).
- Process `.zip` files via parallel-chunk download to a tempfile, mmap, then
  delete the tempfile immediately after the last entry is extracted.
- Run the existing extractor's algorithm (SIMD `memchr` + suffix-match) on
  the streamed/mmapped bytes via a shared crate, with no algorithmic regression.
- Three operational modes via subcommands: `fetch` (one-shot), `watch`
  (daemon subscribing to channel updates), `backfill` (walk channel history).
- Persist dedup state, processing log, and per-channel cursors in SQLite
  (bundled, no external dependency).
- Upload extracted results to a configured Telegram channel with a metadata
  caption (source channel, msg id, hits, hit rate, sha256 prefix).
- Cross-platform Rust (Windows + Linux + macOS), pure-Rust dependencies
  (no C/C++ build steps).

### Non-goals (v1)
- Resuming a partially-downloaded file across crashes (re-download from
  scratch is acceptable for 2 GB / ~5 min worst case).
- Nested archives (zip-in-zip).
- Real-time alerts / webhooks / external observability backends (Prometheus,
  OTel) — `tracing` to stderr+file is enough.
- Record-level dedup (hash every credential pair across runs). File-level
  sha256 dedup only.
- Dialog-based interactive UI; this is a CLI/daemon.

## 3. Architecture

### 3.1 Crate boundaries

```
telegram-client ──depends on── extractor-core
extract-mail    ──depends on── extractor-core
```

`extractor-core` is **pure logic, zero I/O**. It accepts `&[u8]` chunks and
emits matched line slices via a caller-provided sink trait. It knows nothing
about files, mmap, network, or output destinations.

### 3.2 Workspace `Cargo.toml`

```toml
[workspace]
resolver = "2"
members  = ["crates/extractor-core", "crates/extract-mail", "crates/telegram-client"]

[workspace.package]
edition      = "2021"
rust-version = "1.75"

[workspace.dependencies]
anyhow              = "1"
thiserror           = "1"
tokio               = { version = "1", features = ["rt-multi-thread", "macros", "fs", "sync", "signal"] }
tracing             = "0.1"
tracing-subscriber  = { version = "0.3", features = ["env-filter", "json"] }
tracing-appender    = "0.2"
indicatif           = "0.17"
clap                = { version = "4", features = ["derive"] }
serde               = { version = "1", features = ["derive"] }
toml                = "0.8"
memchr              = "2"
memmap2             = "0.9"
rayon               = "1"
bytes               = "1"
flate2              = { version = "1", default-features = false, features = ["rust_backend"] }
zip                 = { version = "0.6", default-features = false, features = ["deflate"] }
# Pure-Rust constraint: flate2 pinned to `rust_backend` (miniz_oxide); zip uses
# flate2's deflate without zlib-sys. No C/C++ build steps anywhere.
sha2                = "0.10"
rusqlite            = { version = "0.31", features = ["bundled"] }
grammers-client     = "0.6"
grammers-session    = "0.6"
grammers-tl-types   = "0.6"
futures             = "0.3"
tempfile            = "3"
proptest            = "1"   # dev-dependencies

[profile.release]
opt-level       = 3
lto             = "fat"
codegen-units   = 1
panic           = "abort"
strip           = true
```

### 3.3 Module layout (telegram-client)

```
src/
├── main.rs                 ← clap entry, dispatch
├── config.rs               ← AppConfig, Secrets, validation, path expansion
├── observability.rs        ← tracing init, indicatif progress, secret scrubber
├── telegram/
│   ├── mod.rs              ← TelegramClient trait (for mocking)
│   ├── client.rs           ← grammers wrapper, login, dialog warm-up
│   └── download.rs         ← parallel chunk download, format-detect helpers
├── pipeline/
│   ├── mod.rs              ← FileJob, FileMeta, ScanStats
│   ├── coordinator.rs      ← 3-stage orchestrator (download → extract → upload)
│   ├── format.rs           ← detect: txt | gz | zip (extension + magic bytes)
│   ├── stream.rs           ← txt/gz: mpsc<Bytes> → Scanner (in std::thread)
│   └── disk.rs             ← zip: tempfile → mmap → Scanner
├── store/
│   ├── mod.rs
│   ├── schema.sql
│   └── repo.rs             ← rusqlite, dedup, watch/backfill cursors
├── output.rs               ← per-source-file writer + path sanitize
└── cmd/
    ├── auth.rs             ← interactive phone → code → session
    ├── join.rs             ← accept invite link
    ├── chats.rs            ← list dialogs (helper to find chat_id)
    ├── fetch.rs            ← single message link / chat+msg_id
    ├── watch.rs            ← daemon: dialog updates → enqueue
    ├── backfill.rs         ← iter_messages from newest to since-cutoff
    ├── retry_uploads.rs
    └── stats.rs            ← read DB, print aggregate
```

## 4. Data flow

### 4.1 Two intra-file paths

```
Telegram document ──► Format detector ──┬─► STREAM PATH    (.txt, .gz)
                                         └─► DISK-SPILL PATH (.zip)
```

**STREAM PATH** (`.txt`, `.gz`) — never touches disk:

```
[Downloader (tokio)] ──Bytes(1MB)──► [Decoder (flate2/none)] ──Bytes──► [Scanner (std::thread)]
                       cap=4                                     cap=4              │
                                                                                    ▼
                                                              [BufWriter (1 MiB internal buf) → out/<ch>/<msg>_<name>.out (size unbounded)]
```

- Channels are `tokio::sync::mpsc<Bytes>` with capacity 4 (4 MiB peak in
  flight). `send().await` provides natural backpressure.
- Scanner runs on a dedicated `std::thread` (not tokio task), receives
  chunks via a bridge `std::sync::mpsc::sync_channel`. CPU-bound work
  must not block the tokio reactor.
- Line alignment: the chunk boundary may cut a line. The `Scanner` keeps a
  carry-over buffer up to `max_line_bytes` (default 64 KiB).

**DISK-SPILL PATH** (`.zip`):

```
[Downloader] ──► [tempfile::NamedTempFile (RAII delete)] ──► [zip::ZipArchive]
                                                                  │
                                                                  ▼ for each text/gz entry
                                                         [Scanner → BufWriter → out file]
```

- The tempfile is explicitly dropped (and thus deleted) immediately after
  the last entry is processed and the writer is flushed.
- Disk peak: one file ≈ 2 GB.
- Memory peak: ~16 MiB (zip read buffer + scan buffer + writer buffer).
  File contents are not held in memory.

### 4.2 Inter-file 3-stage pipeline

```
[Job Queue] ──cap=2──► [Stage 1: Download] ──cap=1──► [Stage 2: Extract+Write] ──cap=2──► [Stage 3: Upload]
                                                                                                  │
                                                                                                  ▼
                                                                                          [output_chat]
```

- Each stage is a single tokio task. Channel capacity = 1 between Stage 1
  and Stage 2 is the critical backpressure point: it allows download(N+1)
  to overlap extract(N).
  - For the **disk-spill path** (`.zip`), cap=1 also bounds disk peak to
    one fully-downloaded tempfile sitting between stages (worst case
    ~2 GB), independent of queue depth.
  - For the **stream path** (`.txt`/`.gz`), no fully-downloaded artefact
    sits between stages — Stage 1 and Stage 2 hand off live byte chunks
    via the intra-file mpsc described in §4.1, and Stage 1 finishes only
    when Stage 2 has consumed the last chunk. The cap=1 on the inter-file
    channel still gates *which file index* Stage 2 is currently working
    on.
- Stage 3 capacity = 2 because uploads are usually small (output << input)
  and rate-limited by Telegram (≥3 s spacing per upload anyway).

### 4.3 Cancellation

`tokio::signal::ctrl_c()` triggers a `CancellationToken`. Each stage observes
it and returns `Err(Cancelled)`. Tempfiles are deleted by RAII drop.
In-flight DB rows transition from `downloading`/`extracting` back to `queued`
on next startup (see §6.4 recovery).

## 5. Component contracts

### 5.1 `extractor-core` public API

```rust
// crates/extractor-core/src/lib.rs

pub mod matcher;
pub mod scanner;

pub use matcher::{Matcher, Mode, MatcherError};
pub use scanner::{Scanner, ScanStats, ScanError, LineSink};
```

```rust
// matcher.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode { Plain, Url }

#[derive(Debug, Clone)]
pub struct Matcher { /* ... */ }

impl Matcher {
    pub fn new(key: &str, mode: Mode) -> Result<Self, MatcherError>;
    /// Returns Some(<rest-after-match>) if the line matches, else None.
    /// Plain: returns &line[colon+1..]
    /// Url:   returns &line[<email_start>..] (email:password)
    #[inline]
    pub fn match_line<'a>(&self, line: &'a [u8]) -> Option<&'a [u8]>;
}
```

```rust
// scanner.rs
pub trait LineSink {
    type Error;
    fn emit(&mut self, line: &[u8]) -> Result<(), Self::Error>;
}

// Blanket impl for io::Write — emit appends '\n'.
// Note: this blanket impl covers `Vec<u8>`, `BufWriter<File>`, `&mut File`,
// etc., because all of them implement `Write`. Coherence rules forbid a
// downstream concrete impl on any third-party type that *also* implements
// `Write`; if such a sink is ever needed (e.g., a structured `Vec<Match>`
// collector), wrap it in a newtype that does NOT implement `Write` and
// add a direct `LineSink` impl on the newtype.
impl<W: std::io::Write> LineSink for W { /* ... */ }

pub struct Scanner<'m> { /* matcher, carry, max_line */ }

impl<'m> Scanner<'m> {
    pub fn new(matcher: &'m Matcher) -> Self;          // max_line=64KiB
    pub fn with_max_line(matcher: &'m Matcher, n: usize) -> Self;

    pub fn feed<S: LineSink>(&mut self, chunk: &[u8], sink: &mut S)
        -> Result<ScanStats, ScanError<S::Error>>;

    pub fn finish<S: LineSink>(&mut self, sink: &mut S)
        -> Result<ScanStats, ScanError<S::Error>>;

    pub fn scan_all<S: LineSink>(&mut self, buf: &[u8], sink: &mut S)
        -> Result<ScanStats, ScanError<S::Error>>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanStats {
    pub lines_scanned: u64,
    pub lines_matched: u64,
    pub bytes_scanned: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ScanError<E> {
    #[error("line exceeds max_line ({0} bytes)")]
    LineTooLong(usize),
    #[error("sink error: {0}")]
    Sink(E),
}
```

**Invariant** (test-enforced): for any byte buffer `B` and any partition
`B = B0 ++ B1 ++ ... ++ Bn`, sequentially calling
`feed(B0); feed(B1); ...; feed(Bn); finish()` produces the same emitted
sequence and stats as `scan_all(B)`.

### 5.2 `extract-mail` consumer (refactored)

The existing single-file `main.rs` (~14 KB) shrinks to ~150 LOC: clap flags,
mmap, rayon split on newline-aligned chunks, per-chunk `Scanner::scan_all`
into a per-thread `Vec<u8>` sink, then merge in input order to the writer.
The 13 existing unit tests move to `extractor-core` (logic-only) and form
the regression baseline.

### 5.3 `telegram-client` consumer (streaming)

```rust
async fn stream_extract(
    mut chunks: tokio::sync::mpsc::Receiver<Bytes>,
    matcher: Arc<Matcher>,
    out_path: PathBuf,
) -> Result<ScanStats> {
    // 1. Open output BufWriter on a blocking thread (spawn_blocking).
    // 2. Spawn a dedicated `std::thread` (NOT tokio task): it owns the
    //    Scanner and the BufWriter, and consumes from a
    //    `std::sync::mpsc::sync_channel::<Bytes>(4)` receiver. This thread
    //    is the only side that calls scanner.feed/finish/flush.
    // 3. Bridge: on the tokio side, await `chunks.recv().await` and forward
    //    each Bytes via `tx.send(c)` — `send` on a sync_channel CAN block
    //    when full, so we wrap each forward in `tokio::task::spawn_blocking`
    //    OR use `tx.try_send` + yield. We choose `spawn_blocking` for
    //    simplicity; full-channel events are rare (the upstream tokio
    //    channel already provides backpressure).
    // 4. Drop `tx` when chunks.recv returns None → triggers scanner finish.
    // 5. Join the scan thread (via spawn_blocking + JoinHandle) and return
    //    ScanStats.
    //
    // The blocking direction: tokio task NEVER calls `scanner.feed` directly;
    // tokio task NEVER blocks on `tx.send`. The std::thread NEVER calls any
    // tokio API. This isolates CPU-bound work from the runtime.
}
```

## 6. Persistence

### 6.1 Why SQLite (bundled)

- Single file, atomic via WAL, query-able for `stats`.
- `rusqlite` with `bundled` feature compiles SQLite from source — no system
  libsqlite3 dependency, clean Windows build.
- Mature, well-known by reviewers; not a research bet.

### 6.2 Schema (`crates/telegram-client/src/store/schema.sql`)

```sql
CREATE TABLE IF NOT EXISTS files (
    sha256             TEXT PRIMARY KEY,        -- 64-char hex
    source_chat_id     INTEGER NOT NULL,
    source_msg_id      INTEGER NOT NULL,
    original_name      TEXT    NOT NULL,
    size_bytes         INTEGER NOT NULL,
    format             TEXT    NOT NULL,        -- 'txt' | 'gz' | 'zip'
    matcher_key        TEXT    NOT NULL,
    matcher_mode       TEXT    NOT NULL,        -- 'plain' | 'url'
    discovered_at      INTEGER NOT NULL,
    download_done_at   INTEGER,
    extract_done_at    INTEGER,
    upload_done_at     INTEGER,
    lines_scanned      INTEGER,
    lines_matched      INTEGER,
    output_path        TEXT,
    output_msg_id      INTEGER,
    status             TEXT    NOT NULL,        -- 'queued'|'downloading'|'extracting'|'uploading'|'done'|'failed'
    error              TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_source ON files(source_chat_id, source_msg_id);

CREATE TABLE IF NOT EXISTS watch_state (
    chat_id      INTEGER PRIMARY KEY,
    chat_title   TEXT    NOT NULL,
    last_msg_id  INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS backfill_state (
    chat_id       INTEGER PRIMARY KEY,
    chat_title    TEXT    NOT NULL,
    next_msg_id   INTEGER NOT NULL,
    started_at    INTEGER NOT NULL,
    completed_at  INTEGER,
    updated_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS failed_uploads (
    sha256           TEXT PRIMARY KEY,
    output_path      TEXT NOT NULL,
    error            TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 1,
    last_attempt_at  INTEGER NOT NULL,
    FOREIGN KEY (sha256) REFERENCES files(sha256)
);

CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);
INSERT OR IGNORE INTO schema_version VALUES (1);
```

### 6.3 `Store` API (selected)

```rust
pub enum EnqueueResult { New, AlreadyDone, InProgress(String) }

impl Store {
    pub fn open(path: &Path) -> Result<Self>;            // runs migrations, sets WAL
    pub fn try_enqueue(&self, file: &FileMeta) -> Result<EnqueueResult>;
    pub fn mark_downloaded(&self, sha256: &str) -> Result<()>;
    pub fn mark_extracted(&self, sha256: &str, stats: ScanStats, out: &Path) -> Result<()>;
    pub fn mark_uploaded(&self, sha256: &str, output_msg_id: i64) -> Result<()>;
    pub fn mark_failed(&self, sha256: &str, err: &str) -> Result<()>;
    pub fn watch_cursor(&self, chat_id: i64) -> Result<Option<i64>>;
    pub fn update_watch_cursor(&self, chat_id: i64, title: &str, last: i64) -> Result<()>;
    pub fn backfill_cursor(&self, chat_id: i64) -> Result<Option<BackfillState>>;
    pub fn advance_backfill(&self, chat_id: i64, next: i64) -> Result<()>;
    pub fn complete_backfill(&self, chat_id: i64) -> Result<()>;
    pub fn enqueue_failed_upload(&self, sha: &str, p: &Path, err: &str) -> Result<()>;
    pub fn pending_failed_uploads(&self) -> Result<Vec<FailedUpload>>;
    pub fn reset_in_flight(&self) -> Result<usize>;     // recovery
    pub fn list_pending_uploads(&self) -> Result<Vec<UploadJob>>;
}
```

Concurrency: `Mutex<rusqlite::Connection>` is sufficient — write traffic is
low (a handful of statements per file).

### 6.4 Recovery on startup

```text
1. SELECT files WHERE status IN ('downloading','extracting'):
     UPDATE → 'queued'.  These will re-run from scratch.
2. SELECT files WHERE status = 'uploading':
     output_path exists on disk → re-queue to upload stage.
3. SELECT files WHERE status = 'queued':
     → process normally.
```

## 7. Configuration & secrets

### 7.1 TOML (`config.toml`)

```toml
[telegram]
session_path                = "~/.config/tg-extract/session.session"
download_concurrent_chunks  = 4

[telegram.output]
chat = "@my_results_channel"     # OR  chat_id = -1001234567890

[pipeline]
work_dir                      = "~/.local/share/tg-extract"
output_dir                    = "./out"
chunk_bytes                   = 1048576
intra_file_channel_capacity   = 4
inter_file_channel_capacity   = 1
upload_channel_capacity       = 2
max_line_bytes                = 65536
upload_rate_seconds           = 3
upload_max_size_bytes         = 2147483648
max_uncompressed_bytes        = 10737418240   # 10 GB zip-bomb guard

[extract]
mode = "plain"                   # "plain" | "url"
key  = "gmail.com"

[[watch.channel]]
chat = "@dump_channel_a"

[[watch.channel]]
chat_id = -1001999888777
# extract = { mode = "url", key = "linkedin.com" }   # optional override

[backfill]
page_size = 100
since     = "2024-01-01T00:00:00Z"

[log]
level    = "info"
format   = "human"               # "human" | "json"
file     = "~/.local/share/tg-extract/log/app.log"
rotation = "daily"               # "never" | "daily" | "hourly"
```

### 7.2 Environment (precedence: env > toml > default)

```
TG_API_ID         (required)   integer api_id from my.telegram.org
TG_API_HASH       (required)   32-char hex api_hash
RUST_TG_CONFIG    (optional)   path to config.toml (default ./config.toml)
RUST_TG_SESSION   (optional)   override telegram.session_path
RUST_LOG          (optional)   tracing EnvFilter
```

### 7.3 Private channel access

- Public channels resolve via `client.resolve_username("@name")`.
- Private channels require the account to be a member; `chat_id` is supplied
  directly (e.g. `-1001234567890`).
- On startup `connect_and_warm()` runs `iter_dialogs()` once to populate the
  per-chat `access_hash` cache in the session file.
- Message links of the form `https://t.me/c/<internal_id>/<msg_id>` are
  parsed: `chat_id = -100<internal_id>`.
- A `join <invite_link>` subcommand calls `client.accept_invitation_link()`
  for headless onboarding to private channels.
- A `chats [--filter <substr>]` subcommand prints all dialogs (id, type,
  title, username) so the user can copy `chat_id` into config.

### 7.4 Secrets handling (security-critical)

- `Secrets { api_id, api_hash }` has a custom `Debug` that redacts
  `api_hash`.
- The session file is `chmod 0600` after creation on Unix
  (`std::os::unix::fs::PermissionsExt::set_mode(0o600)`).
  On Windows there is no group/world bit equivalent reachable from `std::fs`;
  the file inherits the parent dir's ACL, which under per-user profile
  directories is already restricted to the current user. Implementation:
  best-effort skip on Windows with a one-line `tracing::info!` noting the
  reliance on profile-directory ACLs. Hardening Windows ACLs (e.g., via
  `windows-acl`) is out of scope for v1.
- A `SecretScrubLayer` for `tracing_subscriber` redacts any field whose
  name matches `(?i)hash|key|secret|token|password|auth` — value replaced
  with `<redacted>`.
- `.gitignore` blocks `config.toml`, `.env*`, `session*`, `*.session`,
  `out/`, `work_dir/`, `*.log`, `*.tmp`, `target/`. A `config.toml.example`
  is committed instead of `config.toml`.

### 7.5 First-run flow

`auth` subcommand: prompts phone (international), sends code, prompts code,
prompts 2FA password (if enabled), saves session to configured path with
`0600` perms, prints logged-in username for confirmation.

## 8. CLI surface

```rust
#[derive(Parser)]
struct Cli {
    #[arg(short, long, env = "RUST_TG_CONFIG", default_value = "config.toml")]
    config: PathBuf,
    #[arg(short = 'k', long)] key: Option<String>,
    #[arg(long, value_enum)] mode: Option<Mode>,
    #[command(subcommand)] cmd: Cmd,
}

enum Cmd {
    Auth(AuthArgs),
    Join { invite_link: String },
    Chats { #[arg(long)] filter: Option<String> },
    Fetch(FetchArgs),         // --link <url>  OR  --chat <ref> --msg-id <n>
    Watch(WatchArgs),         // optional override list of chats; --duration-seconds
    Backfill(BackfillArgs),   // <chat>; --since; --limit; --resume
    RetryUploads,
    Stats,
}
```

Binary name: **`tg-extract`** (chosen for brevity + clarity; alternates
considered and rejected: `tg-extract` too long, `tgex` cryptic,
`tgx` not self-explanatory). The crate is still named `telegram-client` in
`Cargo.toml`; only the produced binary is `tg-extract` (set via
`[[bin]] name = "tg-extract"` in `crates/telegram-client/Cargo.toml`).

## 9. Testing strategy

### 9.1 `extractor-core`

| Test                        | Type                          |
| --------------------------- | ----------------------------- |
| `plain_match.rs`            | unit (golden table)           |
| `url_match.rs`              | unit (port/path/query/IPv6)   |
| `boundary.rs`               | unit (suffix-match correctness) |
| `chunked_feed.rs`           | property (`proptest`) — chunk-split invariant |
| `carry_overflow.rs`         | unit                          |
| `empty_inputs.rs`           | unit                          |
| `sink_error.rs`             | unit                          |
| `canon_key.rs`              | unit                          |

The chunk-split-invariant property test is the linchpin: it shrinks any
divergence between `scan_all` and `feed+finish` to a minimal counterexample.

Coverage target: **≥ 90%** lines.

### 9.2 `telegram-client`

| Test                        | Notes                              |
| --------------------------- | ---------------------------------- |
| `format_detect.rs`          | extension + magic bytes            |
| `store_repo.rs`             | in-memory SQLite, migrations, dedup, monotonic cursors, `reset_in_flight` |
| `link_parser.rs`            | `t.me/X/N`, `t.me/c/X/N`, `tg://`  |
| `pipeline_stream.rs`        | mocked mpsc<Bytes> → asserts output bytes |
| `pipeline_zip.rs`           | tempfile zip with 3 entries; assert tempfile deleted post-run |
| `pipeline_3stage.rs`        | mock `TelegramClient` trait; assert ordering, backpressure |
| `upload_retry.rs`           | mock 429 → backoff → `failed_uploads` row |
| `secrets_redact.rs`         | tracing capture; assert no `api_hash` literal |
| `config_validation.rs`      | bad TOML, missing required field, `~` expansion |
| `path_traversal.rs`         | `../../etc/passwd` filename → safe target |
| `zip_bomb.rs`               | 1 MiB zip → 1 GB uncompressed → reject |

Live Telegram is **not** in CI: client interactions sit behind
`trait TelegramClient`; tests use a `MockClient`. Coverage target: **≥ 70%**.

### 9.3 Manual smoke tests (per release)

1. `auth` against a throwaway TG account.
2. `fetch` a 1.5 GB `.gz` from a public channel; verify output upload.
3. `watch` a private channel for ~5 min; verify dedup on a duplicate post.
4. `backfill` a small channel with `--limit 5`.
5. `Ctrl-C` mid-extract; restart; verify recovery completes the file.

## 10. Observability

### 10.1 Tracing

`tracing` + `tracing-subscriber` with two layers:
- console (stderr, human formatter)
- file (JSON formatter, optional, with daily/hourly rotation via
  `tracing-appender`)

A custom `SecretScrubLayer` filters secret-named fields (regex above)
before either layer formats them.

### 10.2 KPIs (emitted as span fields, queryable from JSON logs)

- `download_seconds`, `download_throughput_mbs`
- `extract_seconds`, `lines_per_sec`, `hits_per_sec`, `hit_rate`
- `upload_seconds`, `upload_size_bytes`
- counters: `files_processed`, `files_deduped`, `files_failed`
- error events: `network`, `parse`, `write`, with span context

### 10.3 Progress UI

`indicatif::MultiProgress` for download + upload bars when stderr is a TTY
(via `IsTerminal`). Suppressed in non-TTY mode (e.g., redirected to file).

### 10.4 `stats` subcommand

Reads from SQLite, prints aggregate counts, per-channel breakdown, and the
last 10 errors. Useful for daemon mode.

## 11. Security

### 11.1 Threat model

The user runs this tool on their own machine with their own Telegram
account. Inputs are partially trusted (other Telegram users can craft files
intended to harm the consumer). Outputs include credential-bearing data and
must not leak via logs.

### 11.2 Hardening checklist

| Surface                     | Mitigation                                                   |
| --------------------------- | ------------------------------------------------------------ |
| `api_hash` / `session`      | env-only secrets; redacting `Debug`; tracing scrub layer; `chmod 0600`; startup warning on lax perms |
| Path traversal (filename)   | `sanitize(name)` strips `/`, `\`, `..`, control bytes; assert `final_path.starts_with(output_dir)` after join |
| Disk exhaustion (zip bomb)  | per-entry uncompressed size check; `max_uncompressed_bytes` cap is **per-archive cumulative** (sum of all entries decoded so far); breach aborts that archive and marks file failed; pipeline continues with next file |
| Pathological line length    | `max_line_bytes` cap → `ScanError::LineTooLong`; file marked failed; pipeline continues |
| SQL injection               | `rusqlite` parameterized queries only; lint review forbids string-format SQL |
| Tempfile races              | `tempfile::NamedTempFile` (`O_EXCL` / `CREATE_NEW`)          |
| Log leakage of credentials  | scanner emits via `LineSink` only, never `tracing!`; tracing carries metadata only |
| Output channel misconfig    | warn if `output.chat` resolves to a public username; require `--confirm-public` flag to upload to public chats |
| Dependency CVEs             | `cargo audit` in CI; pinned versions; review changelog on bump |

### 11.3 Documented user warnings (README)

- "Use a throwaway Telegram account." Account behavior automated through
  MTProto can attract review by Telegram.
- "Never share `session.session`." It is a bearer credential equivalent to
  the account.
- "Output may contain credentials." Treat the local `out/` directory and
  the configured output channel as sensitive.

## 12. Milestones

Phasing assumes one developer.

| Phase | Scope                                                                           | Days |
| ----- | ------------------------------------------------------------------------------- | ---- |
| 0     | Workspace setup; relocate `extract-mail` (still building green)                 | 0.5  |
| 1     | Extract logic into `extractor-core`; refactor `extract-mail`; tests pass; no perf regression | 1.5  |
| 2     | `telegram-client` skeleton: deps, config loader, secrets, CLI subcommands stubbed, logging init | 1.0  |
| 3     | Auth, `chats`, `join`; dialog warm-up; session lifecycle                        | 1.0  |
| 4     | Stream pipeline (`.txt`/`.gz`); `fetch` end-to-end                              | 2.0  |
| 5     | Disk-spill (`.zip`); cleanup                                                    | 1.0  |
| 6     | Upload stage; caption; >2GB split; retry queue                                  | 1.0  |
| 7     | SQLite store; recovery on startup                                               | 1.0  |
| 8     | Watch mode; update handler; cursor                                              | 1.5  |
| 9     | Backfill mode; pagination; since cutoff; resume                                 | 0.5  |
| 10    | Hardening (path sanitize, zip-bomb, secrets scrubber, perms); security tests    | 1.0  |
| 11    | Observability polish; `stats` subcommand; progress bars                         | 0.5  |
| 12    | Docs (README per crate, `config.toml.example`, CHANGELOG)                       | 0.5  |

**Total**: ~13 dev-days of focused work. The schedule has **no built-in
buffer**; realistic calendar time is 15-18 days assuming ~70-80% deep-work
throughput, plus contingency for grammers quirks discovered at runtime
(parallel-chunk stability is the largest unknown). Phases 8-9 (watch /
backfill) are the primary deferral candidates if cuts are needed: a v1.0
shipping with `fetch` only is a usable tool. Critical path: 0 → 1 → 2 → 4
→ 6 → 7.

## 13. Risks

| Risk                                                   | Likelihood | Impact   | Mitigation |
| ------------------------------------------------------ | :--------: | :------: | ---------- |
| `grammers` parallel-chunk download unstable on 2 GB    | Medium     | High     | `--no-parallel-chunks` fallback; smoke test on 1.5 GB real file |
| Telegram FLOOD_WAIT on free account                    | High       | Medium   | grammers auto-handles; logged each occurrence; visible in `stats` |
| Account ban for automation behavior                    | Low        | Critical | Conservative defaults; "use throwaway account" warning |
| Disk full from queued `.zip` files                     | Medium     | High     | Backpressure cap=1 between stages; pre-flight disk check; immediate tempfile delete |
| RAM swell from pathological line                       | Low        | Medium   | `max_line_bytes` cap + abort                                 |
| Windows file locking conflict with TG Desktop          | Medium     | Medium   | Documented: do not run alongside TG Desktop with same session |
| `sled` instability (avoided)                           | —          | —        | Choose `rusqlite` instead                                    |

## 14. Open questions / future work

- **Resumable downloads**: if 2 GB reruns become painful, add chunk-offset
  persistence (estimated +1 day).
- **Record-level dedup**: bloom filter or rolling hash store for extracted
  credentials across runs (tradeoff: GBs of state, false-positive risk).
- **Multi-account**: rotate among N accounts to bypass per-account rate
  limits. Requires session pool, scheduling.
- **Output formats**: emit `.jsonl` with metadata alongside plaintext.
- **TUI**: ratatui dashboard for `watch` mode. Defer; `stats` + tracing is
  enough.

## 15. Decisions log (from brainstorming)

| #  | Decision                                                                                  |
| -- | ----------------------------------------------------------------------------------------- |
| 1  | Pipeline-integrated downloader+extractor (vs separate downloader)                         |
| 2  | `grammers` (pure Rust MTProto) — not Bot API, not TDLib                                   |
| 3  | Mixed formats `.txt`/`.gz`/`.zip`; size 500 MB – 2 GB                                     |
| 4  | Three subcommands: `fetch`, `watch`, `backfill`; public + private channels                |
| 5  | Cargo workspace with `extractor-core` shared lib                                          |
| 6  | 2-stage pipeline → expanded to 3 stages with upload                                       |
| 7  | Per-source-file output; file-level sha256 dedup                                           |
| 8  | SQLite (rusqlite, bundled); simple-restart recovery (no resumable downloads in v1)        |
| 9  | Hybrid secrets: TOML for non-secrets, env for `api_*`, session file with `0600`; interactive `auth` |
| 10 | `tracing` + `tracing-appender` + `indicatif`; KPIs: download throughput, file count, dedup skip count, errors with context, extract throughput |
| 11 | Output upload to a Telegram channel owned by the user; local output files retained        |
| 12 | Parallel chunk download in `grammers` (default 4 concurrent chunks)                       |
