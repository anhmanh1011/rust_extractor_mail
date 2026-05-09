//! Phase-11 smoke: the rolling appender wired in Phase 2 actually writes
//! to disk when given a path + rotation policy. Test runs in a tempdir to
//! avoid polluting the working tree. The test does NOT validate rotation
//! cadence (daily/hourly) — that is `tracing-appender`'s contract, not
//! ours. We only validate that *some* file appears and contains our line.

use telegram_client::observability;

#[test]
fn rolling_appender_writes_to_tempdir() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("tg.log");

    {
        let _guard = observability::init(
            "info",
            "json",
            Some(log_path.as_path()),
            "daily",
        );
        tracing::info!(probe = "phase11_rotation_smoke",
                       "rolling-appender smoke event");
        // Drop _guard at scope end — flushes the non-blocking writer worker.
    }

    // tracing-appender names files as `<stem>.YYYY-MM-DD` for daily.
    // Walk the tempdir and confirm at least one file contains our probe.
    let mut found = false;
    for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
        let p = entry.path();
        if !p.is_file() { continue; }
        let body = std::fs::read_to_string(&p).unwrap_or_default();
        if body.contains("phase11_rotation_smoke") {
            found = true;
            break;
        }
    }
    assert!(
        found,
        "no log file under {:?} contained the probe — \
         rotation wiring may be broken",
        dir.path(),
    );
}
