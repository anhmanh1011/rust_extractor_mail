//! Phase-11 helper. A thin wrapper that returns a [`ProgressBar`] either
//! attached to a [`MultiProgress`] (TTY case) or [`ProgressBar::hidden()`]
//! (no-op case). Stages call this once per job and `inc` on each chunk —
//! the same code path covers both branches.

use std::sync::Arc;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// Build a length-aware bar. `len` is the total expected bytes; if `None`
/// (e.g. unknown upload length), the bar runs as a spinner. When `mp` is
/// `None` the returned bar is hidden — every `inc`/`finish` call is a
/// cheap no-op so callers do not need to special-case the off path.
pub fn make_bar(
    mp: Option<&Arc<MultiProgress>>,
    label: &str,
    len: Option<u64>,
) -> ProgressBar {
    let pb = match (mp, len) {
        (Some(mp), Some(n)) => mp.add(ProgressBar::new(n)),
        (Some(mp), None)    => mp.add(ProgressBar::new_spinner()),
        (None,     _)       => ProgressBar::hidden(),
    };
    let style = ProgressStyle::with_template(
        "{prefix:.bold} [{elapsed_precise}] [{bar:40.cyan/blue}] \
         {bytes:>10}/{total_bytes:<10} {msg}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("=>-");
    pb.set_style(style);
    pb.set_prefix(label.to_string());
    pb
}
