// PM2 process file for tg-extract.
//
// Usage:
//   pm2 start ecosystem.config.cjs        # start (or reload) all apps
//   pm2 status
//   pm2 logs tg-extract-watch             # tail merged stdout+stderr
//   pm2 stop tg-extract-watch             # graceful shutdown (SIGTERM → grammers)
//   pm2 reload tg-extract-watch           # restart with new binary after `cargo build`
//
// CommonJS (.cjs) on purpose: PM2 spawns the file with `require()`, which
// would fail with `package.json: "type": "module"`. Keep this file CJS even
// if package.json switches to ESM later.

const path = require('path');
const ROOT = __dirname;

module.exports = {
  apps: [
    {
      name:        'tg-extract-watch',
      script:      path.join(ROOT, 'scripts', 'run-watch.sh'),
      interpreter: 'bash',
      cwd:         ROOT,

      // Single instance — `watch` holds an exclusive Telegram session
      // (one MTProto auth key per process), so fork/cluster modes would
      // race on the session file and grammers would log out on second
      // login. Spec §7: session is a bearer credential, one writer.
      instances:   1,
      exec_mode:   'fork',

      // Auto-restart policy. The binary exits cleanly on Ctrl-C / SIGTERM
      // and on `--duration-seconds`, so we want:
      //   - restart on unexpected crash (FLOOD_WAIT spirals, panic, OOM)
      //   - NOT restart when the operator stops with `pm2 stop`
      autorestart:        true,
      max_restarts:       10,            // give up after 10 crashes in window
      min_uptime:         '30s',         // <30s alive = "crash", not "ran"
      restart_delay:      5_000,         // 5s between restarts → respect Telegram
      exponential_backoff_restart_delay: 0, // linear delay; 5s is enough

      // OOM guard. Spec §13 puts steady-state at <100 MB for the streaming
      // path; the disk-spill zip flow plus a 2 GB output file can spike
      // higher transiently. 2 GB is a generous ceiling that catches leaks
      // without false-positives on the spill path.
      max_memory_restart: '2G',

      // Logs. Both stdout and stderr go through tracing's stderr writer;
      // PM2 captures them. tracing-appender ALSO writes to ./logs/app.log*
      // per config.toml — the PM2 log is for operator tails, the
      // appender log is for retention/grep.
      out_file:           path.join(ROOT, 'logs', 'pm2-out.log'),
      error_file:         path.join(ROOT, 'logs', 'pm2-err.log'),
      merge_logs:         true,         // unified stream across restarts
      time:               true,         // prefix each line with timestamp

      // No env block here on purpose. scripts/run-watch.sh sources .env so
      // TG_API_ID/TG_API_HASH never appear in this config file (which is
      // typically committed) or in `pm2 env` output.

      // Graceful shutdown. tg-extract installs a Ctrl-C handler that
      // drains in-flight chunks and flushes the SQLite WAL; give it room
      // before PM2 SIGKILLs.
      kill_timeout:       30_000,
    },
  ],
};
