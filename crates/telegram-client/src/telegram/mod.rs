//! Telegram MTProto wrapper.
pub mod client;
pub mod download;

/// Trait used by the pipeline so tests can substitute a `MockClient`.
/// Real impl wires to `grammers_client::Client`. Filled in Task 3.1.
pub trait TelegramClient: Send + Sync {
    /* methods land in Task 3.1 */
}
