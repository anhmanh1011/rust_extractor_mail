//! Telegram MTProto client abstraction.

use anyhow::Result;
use bytes::Bytes;

pub mod client;
pub mod download;
pub mod link_parser;
pub mod mock;

/// Dialog summary returned by `iter_dialogs`.
#[derive(Debug, Clone)]
pub struct Dialog {
    /// MTProto numeric chat id (negative for channels/supergroups).
    pub chat_id: i64,
    /// Whether the dialog is a user, group, or channel.
    pub kind: DialogKind,
    /// Human-readable title of the chat.
    pub title: String,
    /// Public `@username` (without the leading `@`), if any.
    pub username: Option<String>,
}

/// Coarse classification of a dialog peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogKind {
    /// One-to-one chat with another user.
    User,
    /// Basic or super-group chat.
    Group,
    /// Broadcast channel.
    Channel,
}

/// Identifies a chat for API calls. `Username` requires an extra resolve step.
#[derive(Debug, Clone)]
pub enum ChatRef {
    /// Public `@username` (without the leading `@`).
    Username(String),
    /// MTProto numeric chat id.
    ChatId(i64),
}

/// Summary of a media/document message.
#[derive(Debug, Clone)]
pub struct MessageInfo {
    /// MTProto chat id the message belongs to.
    pub chat_id: i64,
    /// Numeric message id within the chat.
    pub msg_id: i32,
    /// Original file name as reported by the sender.
    pub file_name: String,
    /// Document size in bytes.
    pub size: u64,
    /// MIME type, if the document carries one.
    pub mime: Option<String>,
}

/// Trait used by the pipeline so tests can substitute `MockClient`.
/// Real impl wires to `grammers_client::Client`. Bodies in Task 3.2.
///
/// Receiver consistency: every method takes `&self`. The pipeline shares
/// a single client across many concurrent download/upload tasks (Phase 4+)
/// so a `&mut self` warm-up method would force unnecessary serialization.
/// Internal state mutated by warm-up (grammers session, dialog cache)
/// lives behind interior mutability inside the implementation.
#[async_trait::async_trait]
pub trait TelegramClient: Send + Sync {
    /// Connect to Telegram, perform login if needed, and warm any internal caches.
    async fn connect_and_warm(&self) -> Result<()>;

    /// Enumerate dialogs (chats) reachable by the current session.
    async fn iter_dialogs(&self) -> Result<Vec<Dialog>>;

    /// Accept a `t.me/+...` invite link, joining the referenced chat.
    async fn join_invite_link(&self, link: &str) -> Result<()>;

    /// Resolve a `ChatRef` (username or chat id) to a numeric chat id.
    async fn resolve_chat(&self, r: &ChatRef) -> Result<i64>;

    /// Fetch metadata for a single message that carries a document.
    async fn message_info(&self, chat_id: i64, msg_id: i32) -> Result<MessageInfo>;

    /// Returns a stream of byte chunks for the document. Callers consume
    /// via `tokio::sync::mpsc::Receiver`. Implementation details (parallel
    /// chunk size, retries) are encapsulated.
    async fn download_stream(
        &self,
        chat_id: i64,
        msg_id: i32,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<Bytes>>>;

    /// Upload a local file to `target_chat_id` with an optional caption.
    async fn upload_file(
        &self,
        target_chat_id: i64,
        local_path: &std::path::Path,
        caption: Option<&str>,
    ) -> Result<()>;
}
