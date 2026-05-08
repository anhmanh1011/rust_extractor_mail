//! In-memory mock implementing `TelegramClient` for tests.

use super::*;
use anyhow::Result;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::mpsc;

/// Test fixture. Pre-populate via the `with_*` builders before passing into
/// the code under test.
pub struct MockClient {
    /// Dialogs returned by `iter_dialogs` and used for `resolve_chat` lookups.
    pub dialogs: Mutex<Vec<Dialog>>,
    /// Pre-seeded messages keyed by `(chat_id, msg_id)` with payload bytes.
    #[allow(clippy::type_complexity)]
    pub messages: Mutex<HashMap<(i64, i32), (MessageInfo, Vec<u8>)>>,
    /// Invite links the mock has accepted via `join_invite_link`.
    pub joined: Mutex<Vec<String>>,
    /// Files passed to `upload_file`, recorded as `(chat, path, caption)`.
    pub uploaded: Mutex<Vec<(i64, std::path::PathBuf, Option<String>)>>,
}

impl MockClient {
    /// Create an empty mock with no dialogs, messages, joins, or uploads.
    pub fn new() -> Self {
        Self {
            dialogs:  Mutex::new(Vec::new()),
            messages: Mutex::new(HashMap::new()),
            joined:   Mutex::new(Vec::new()),
            uploaded: Mutex::new(Vec::new()),
        }
    }

    /// Builder: append a dialog to the mock's dialog list.
    pub fn with_dialog(self, d: Dialog) -> Self {
        self.dialogs.lock().unwrap().push(d);
        self
    }

    /// Builder: register a downloadable document keyed by its `(chat_id, msg_id)`.
    pub fn with_document(self, info: MessageInfo, bytes: Vec<u8>) -> Self {
        self.messages.lock().unwrap().insert((info.chat_id, info.msg_id), (info, bytes));
        self
    }
}

impl Default for MockClient {
    fn default() -> Self { Self::new() }
}

#[async_trait::async_trait]
impl TelegramClient for MockClient {
    async fn connect_and_warm(&self) -> Result<()> { Ok(()) }

    async fn iter_dialogs(&self) -> Result<Vec<Dialog>> {
        Ok(self.dialogs.lock().unwrap().clone())
    }

    async fn join_invite_link(&self, link: &str) -> Result<()> {
        self.joined.lock().unwrap().push(link.into());
        Ok(())
    }

    async fn resolve_chat(&self, r: &ChatRef) -> Result<i64> {
        match r {
            ChatRef::ChatId(id) => Ok(*id),
            ChatRef::Username(name) => {
                self.dialogs.lock().unwrap().iter()
                    .find(|d| d.username.as_deref() == Some(name))
                    .map(|d| d.chat_id)
                    .ok_or_else(|| anyhow::anyhow!("mock: no dialog with username {name}"))
            }
        }
    }

    async fn message_info(&self, chat_id: i64, msg_id: i32) -> Result<MessageInfo> {
        self.messages.lock().unwrap().get(&(chat_id, msg_id))
            .map(|(info, _)| info.clone())
            .ok_or_else(|| anyhow::anyhow!("mock: no message {chat_id}/{msg_id}"))
    }

    async fn download_stream(
        &self,
        chat_id: i64,
        msg_id: i32,
    ) -> Result<mpsc::Receiver<Result<Bytes>>> {
        let bytes = self.messages.lock().unwrap().get(&(chat_id, msg_id))
            .map(|(_, b)| b.clone())
            .ok_or_else(|| anyhow::anyhow!("mock: no document {chat_id}/{msg_id}"))?;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for chunk in bytes.chunks(1024) {
                if tx.send(Ok(Bytes::copy_from_slice(chunk))).await.is_err() { break; }
            }
        });
        Ok(rx)
    }

    async fn upload_file(
        &self,
        target_chat_id: i64,
        local_path: &std::path::Path,
        caption: Option<&str>,
    ) -> Result<()> {
        self.uploaded.lock().unwrap().push((
            target_chat_id,
            local_path.into(),
            caption.map(String::from),
        ));
        Ok(())
    }
}
