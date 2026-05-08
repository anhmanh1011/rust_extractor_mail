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
    /// Per-`(chat_id, msg_id)` scripted chunk sequences for `download_stream`.
    ///
    /// Each entry is a `Vec` of `Result<Bytes>` items consumed (drained) on
    /// the first `download_stream` call for that key. After a scripted `Err`
    /// is forwarded the channel is closed immediately.
    #[allow(clippy::type_complexity)]
    pub download_scripts: Mutex<HashMap<(i64, i32), Vec<anyhow::Result<Bytes>>>>,
}

impl MockClient {
    /// Create an empty mock with no dialogs, messages, joins, uploads, or download scripts.
    pub fn new() -> Self {
        Self {
            dialogs: Mutex::new(Vec::new()),
            messages: Mutex::new(HashMap::new()),
            joined: Mutex::new(Vec::new()),
            uploaded: Mutex::new(Vec::new()),
            download_scripts: Mutex::new(HashMap::new()),
        }
    }

    /// Builder: append a dialog to the mock's dialog list.
    pub fn with_dialog(self, d: Dialog) -> Self {
        self.dialogs.lock().unwrap().push(d);
        self
    }

    /// Builder: register a downloadable document keyed by its `(chat_id, msg_id)`.
    ///
    /// When no `script_download` entry exists for the same key, `download_stream`
    /// falls back to chunking these bytes at 1 024 bytes per chunk. This
    /// preserves Phase-3 builder semantics for downstream tests.
    pub fn with_document(self, info: MessageInfo, bytes: Vec<u8>) -> Self {
        self.messages
            .lock()
            .unwrap()
            .insert((info.chat_id, info.msg_id), (info, bytes));
        self
    }

    /// Register a chunk-by-chunk download script for `(chat_id, msg_id)`.
    ///
    /// The script is consumed (drained) on the first `download_stream` call
    /// for that key pair. Any prior script for the same key is replaced.
    /// After a scripted `Err` entry is forwarded the channel is closed,
    /// even if further entries remain in the script.
    pub fn script_download(
        &self,
        chat_id: i64,
        msg_id: i32,
        chunks: Vec<anyhow::Result<Bytes>>,
    ) {
        self.download_scripts
            .lock()
            .unwrap()
            .insert((chat_id, msg_id), chunks);
    }
}

impl Default for MockClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TelegramClient for MockClient {
    async fn connect_and_warm(&self) -> Result<()> {
        Ok(())
    }

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
            ChatRef::Username(name) => self
                .dialogs
                .lock()
                .unwrap()
                .iter()
                .find(|d| d.username.as_deref() == Some(name))
                .map(|d| d.chat_id)
                .ok_or_else(|| anyhow::anyhow!("mock: no dialog with username {name}")),
        }
    }

    async fn message_info(&self, chat_id: i64, msg_id: i32) -> Result<MessageInfo> {
        self.messages
            .lock()
            .unwrap()
            .get(&(chat_id, msg_id))
            .map(|(info, _)| info.clone())
            .ok_or_else(|| anyhow::anyhow!("mock: no message {chat_id}/{msg_id}"))
    }

    /// Return a channel receiver that streams byte chunks for `(chat_id, msg_id)`.
    ///
    /// Resolution order:
    /// 1. **Script** — if `script_download` was called for this key, drain the
    ///    script and forward each entry; close the channel after any `Err` entry.
    /// 2. **Document** — if `with_document` was called for this key, chunk the
    ///    bytes at 1 024 bytes per chunk and forward them.
    /// 3. **Error** — return `Err` with a message containing `"no script"`.
    async fn download_stream(
        &self,
        chat_id: i64,
        msg_id: i32,
    ) -> Result<mpsc::Receiver<Result<Bytes>>> {
        // 1. Check scripted download first (drains the entry on first call).
        let script = self
            .download_scripts
            .lock()
            .unwrap()
            .remove(&(chat_id, msg_id));

        if let Some(script) = script {
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                for item in script {
                    let is_err = item.is_err();
                    if tx.send(item).await.is_err() {
                        break;
                    }
                    if is_err {
                        break; // close channel after scripted error
                    }
                }
                // `tx` dropped here → receiver observes `None`
            });
            return Ok(rx);
        }

        // 2. Fall back to `with_document` message bytes chunked at 1 024 bytes.
        let bytes = self
            .messages
            .lock()
            .unwrap()
            .get(&(chat_id, msg_id))
            .map(|(_, b)| b.clone());

        if let Some(bytes) = bytes {
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                for chunk in bytes.chunks(1024) {
                    if tx.send(Ok(Bytes::copy_from_slice(chunk))).await.is_err() {
                        break;
                    }
                }
            });
            return Ok(rx);
        }

        // 3. Neither script nor document registered — surface a clear error.
        Err(anyhow::anyhow!(
            "mock: no script or document for ({chat_id}, {msg_id})"
        ))
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
