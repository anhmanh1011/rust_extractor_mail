#!/usr/bin/env bash
# Per-job stage timing report. Pulls `stage[123].*` events from tracing's
# rolling appender (the canonical source) and prints one row per
# (chat_id, msg_id) showing elapsed ms / throughput per stage so it is
# obvious which step dominates.
#
# Usage:
#   scripts/timing-report.sh                  # all log files in ./logs/
#   scripts/timing-report.sh logs/app.log.*   # explicit
#
# Why not parse pm2-err.log: pm2 prepends its own timestamp + pid header
# that breaks key=value alignment. The tracing appender writes the
# structured form directly.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if (( $# == 0 )); then
  set -- logs/app.log.*
fi

gawk '
function field(line, name,   re, m) {
  re = "(^|[ \t])" name "=([^ ]+)"
  if (match(line, re, m)) return m[2]
  return ""
}

/stage1\.download: starting/ {
  id = field($0, "chat_id") "/" field($0, "msg_id")
  ev[id, "name"]  = field($0, "name")
  ev[id, "size"]  = field($0, "size_bytes")
  ev[id, "s1_t0"] = $1
  seen[id] = 1
}
/stage1\.download: handing off/ {
  id = field($0, "chat_id") "/" field($0, "msg_id")
  ev[id, "s1_handoff_ms"] = field($0, "stage1_handoff_ms")
}
/stage1\.download: zip drained/ {
  id = field($0, "chat_id") "/" field($0, "msg_id")
  ev[id, "s1_zip_ms"]  = field($0, "elapsed_ms")
  ev[id, "s1_zip_kbps"]= field($0, "kbps")
}
/stage2\.extract: done/ {
  id = field($0, "chat_id") "/" field($0, "msg_id")
  ev[id, "s2_ms"]   = field($0, "elapsed_ms")
  ev[id, "s2_kbps"] = field($0, "kbps")
  ev[id, "lines_s"] = field($0, "lines_scanned")
  ev[id, "lines_m"] = field($0, "lines_matched")
}
/stage3\.upload: done/ {
  id = field($0, "chat_id") "/" field($0, "msg_id")
  ev[id, "s3_ms"]    = field($0, "elapsed_ms")
  ev[id, "s3_parts"] = field($0, "parts")
}
/stage3\.upload: skipping/ {
  id = field($0, "chat_id") "/" field($0, "msg_id")
  ev[id, "s3_ms"] = "skip"
}
/stage[123]\..*(failed|skipping|unknown)/ {
  id = field($0, "chat_id") "/" field($0, "msg_id")
  ev[id, "failed"] = 1
}

END {
  fmt = "%-14s  %10s  %7s  %8s  %12s  %10s  %8s  %s\n"
  printf fmt, "chat/msg", "src_MB", "s1_ms", "s2_s", "s2_MBps", "s3_ms", "out_st", "name"
  printf fmt, "--------", "------", "-----", "----", "-------", "-----", "------", "----"
  for (id in seen) {
    name = ev[id, "name"]; size = ev[id, "size"] + 0
    s1   = ev[id, "s1_handoff_ms"]; if (s1 == "") s1 = ev[id, "s1_zip_ms"]
    s2   = ev[id, "s2_ms"];        s3 = ev[id, "s3_ms"]
    kbps = ev[id, "s2_kbps"] + 0
    src_mb = (size > 0) ? sprintf("%.1f", size / 1048576.0) : "?"
    s2_s   = (s2 != "" && s2 != "skip") ? sprintf("%.1f", s2 / 1000.0) : (s2 == "" ? "running" : s2)
    mbps   = (kbps > 0) ? sprintf("%.1f", kbps / 1024.0) : "-"
    s3_show= (s3 == "") ? (ev[id, "failed"] ? "fail" : "pending") : s3
    status = (ev[id, "failed"]) ? "FAIL" : ((s3 == "") ? "wait" : "done")
    printf fmt, id, src_mb, s1, s2_s, mbps, s3_show, status, name
  }
}
' "$@"
