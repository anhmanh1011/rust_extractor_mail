//! tg-extract: Telegram large-file extraction pipeline.

#![warn(missing_docs)]
#![allow(dead_code)] // will shrink as Phase 2-12 fill in bodies

pub mod config;
pub mod observability;
pub mod output;
pub mod telegram;
pub mod pipeline;
pub mod store;
pub mod cmd;

// Convenience re-exports — the canonical paths still live under `telegram::*`,
// these just shorten the most common imports for downstream callers.
pub use telegram::{ChatRef, Dialog, DialogKind, MessageInfo, TelegramClient};
pub use telegram::mock::MockClient;
