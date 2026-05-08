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
