# Performance baseline

Dataset: `bench_data/big.txt` (10M lines, 413.24 MB)
Machine: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz, Windows 11
Date:    2026-05-08
Build:   `cargo build --release --bin extract-mail` at commit `eb97bee`

| Variant       | Warm-cache real time | Throughput (MB/s) |
|---------------|----------------------|-------------------|
| Pre-refactor  | n/a (archive consumed in Phase 0; no baseline available) | n/a |
| Post-refactor | 0.0758 s | 5450.55 |

Three timed runs (post-refactor, after one untimed warmup):
- Run 1: 0.0847019 s
- Run 2: 0.0758171 s
- Run 3: 0.0753037 s

**Comparison vs README claim:** README claims ~960 MB/s on a 500M-line dataset on different hardware; this 10M-line measurement on the current machine (Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz) is well above the absolute floor (>= 900 MB/s warm-cache) defined in Task 1.7 Step 4 / Task 1.8 Step 5. The unusually high MB/s here reflects the smaller dataset fitting entirely in the OS page cache after the warmup pass; an apples-to-apples comparison to the README's 500M-line number would require a much larger dataset that exceeds RAM.
