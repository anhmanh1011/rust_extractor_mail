//! Zero-I/O domain extraction logic.
//!
//! This crate provides:
//!   - [`Matcher`]: domain-aware suffix matcher with two parsing modes
//!     (plain `domain:txt1:txt2` and URL `<url>:<email>:<password>`).
//!   - [`Scanner`]: byte-stream scanner that emits matched line slices
//!     to a caller-provided [`LineSink`]. Supports both single-shot
//!     (`scan_all`) and chunked-feed (`feed` + `finish`) operation,
//!     proven equivalent by a property test.
//!
//! All APIs are zero-allocation per line on the happy path.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod matcher;
pub mod scanner;

pub use matcher::{Matcher, MatcherError, Mode};
pub use scanner::{LineSink, ScanError, ScanStats, Scanner};
