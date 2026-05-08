BEGIN;
CREATE TABLE IF NOT EXISTS files (
    sha256             TEXT PRIMARY KEY,
    source_chat_id     INTEGER NOT NULL,
    source_msg_id      INTEGER NOT NULL,
    original_name      TEXT    NOT NULL,
    size_bytes         INTEGER NOT NULL,
    format             TEXT    NOT NULL,
    matcher_key        TEXT    NOT NULL,
    matcher_mode       TEXT    NOT NULL,
    discovered_at      INTEGER NOT NULL,
    download_done_at   INTEGER,
    extract_done_at    INTEGER,
    upload_done_at     INTEGER,
    lines_scanned      INTEGER,
    lines_matched      INTEGER,
    output_path        TEXT,
    output_msg_id      INTEGER,
    status             TEXT    NOT NULL,
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

-- ─────────────────────────── v2 migration ───────────────────────────
-- Forensic destination for jobs whose source is unrecoverable (corrupt
-- download bytes, zip-bomb cap, path-traversal entry, OOM-at-extract).
-- Distinct from failed_uploads, which holds RETRYABLE upload errors.
CREATE TABLE IF NOT EXISTS dead_letter (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_chat_id  INTEGER NOT NULL,
    source_msg_id   INTEGER NOT NULL,
    sha256          TEXT,
    original_name   TEXT    NOT NULL,
    size_bytes      INTEGER NOT NULL,
    format          TEXT    NOT NULL,
    stage           TEXT    NOT NULL,
    error           TEXT    NOT NULL,
    recorded_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dead_letter_source ON dead_letter(source_chat_id, source_msg_id);

INSERT OR IGNORE INTO schema_version VALUES (2);
COMMIT;
