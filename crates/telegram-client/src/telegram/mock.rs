//! In-memory mock implementing `TelegramClient` for tests.

use super::*;
use anyhow::Result;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::mpsc;

/// Scripted outcome for a single `upload_file` call on [`MockClient`].
///
/// Push outcomes via [`MockClient::script_upload`]; they are consumed FIFO.
/// When the queue is empty, `upload_file` succeeds and auto-allocates a
/// monotonically-increasing id from `MockClient::next_msg_id`.
#[derive(Debug, Clone)]
pub enum UploadOutcome {
    /// Succeed and assign this specific output message id.
    Ok(i64),
    /// Simulate a `FLOOD_WAIT_<n>` transient — caller should back off and retry.
    FloodWait {
        /// Seconds to wait per the simulated `FLOOD_WAIT` response.
        seconds: u32,
    },
    /// Permanent failure with the given message.
    Permanent(String),
}

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
    /// Files passed to `upload_file`, recorded as `(chat, path, caption, output_msg_id)`.
    #[allow(clippy::type_complexity)]
    pub uploaded: Mutex<Vec<(i64, std::path::PathBuf, Option<String>, i64)>>,
    /// Per-`(chat_id, msg_id)` scripted chunk sequences for `download_stream`.
    ///
    /// Each entry is a `Vec` of `Result<Bytes>` items consumed (drained) on
    /// the first `download_stream` call for that key. After a scripted `Err`
    /// is forwarded the channel is closed immediately.
    #[allow(clippy::type_complexity)]
    pub download_scripts: Mutex<HashMap<(i64, i32), Vec<anyhow::Result<Bytes>>>>,
    /// Monotonic id source for `upload_file` outputs.
    ///
    /// Used when no [`UploadOutcome::Ok`] is scripted via [`MockClient::script_upload`].
    /// Starts at 1 000 and increases by 1 for each auto-allocated call.
    /// Concurrent callers see distinct ids because `fetch_add` is atomic.
    next_msg_id: AtomicI64,
    /// Scripted upload outcomes, drained FIFO. Empty = success with auto-allocated id.
    upload_script: Mutex<std::collections::VecDeque<UploadOutcome>>,
    /// Per-chat scripted history. `iter_history` reads from here.
    /// Stored newest-first (high msg_id → low msg_id).
    pub history: Mutex<HashMap<i64, Vec<MessageInfo>>>,
    /// Scripted update batches (FIFO across all chats; consumer filters).
    /// Each call to `subscribe_updates` consumes the FRONT batch — that
    /// models a real Telegram stream which closes after each "burst" so
    /// the auto-reconnect loop can be exercised by scripting more than one
    /// batch.
    pub update_batches: Mutex<Vec<Vec<MessageInfo>>>,
    /// Counts calls to `subscribe_updates`. Tests assert this directly via
    /// [`MockClient::subscribe_calls`] to pin reconnect behavior.
    subscribe_call_count: AtomicUsize,
    /// Wall-clock delay each `download_stream` producer holds the in-flight
    /// slot before producing bytes. Default `Duration::ZERO`.
    download_delay: std::time::Duration,
    /// Currently-running download producer tasks. `Arc` so the counter can
    /// be cloned into spawned closures (raw `AtomicUsize` cannot move out of
    /// `&self`). Bumped on producer entry, decremented on producer drop.
    inflight_downloads: std::sync::Arc<AtomicUsize>,
    /// High-water mark of `inflight_downloads` over this mock's lifetime.
    /// Read via [`MockClient::inflight_observed`].
    max_inflight_downloads: std::sync::Arc<AtomicUsize>,
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
            next_msg_id: AtomicI64::new(1_000),
            upload_script: Mutex::new(std::collections::VecDeque::new()),
            history: Mutex::new(HashMap::new()),
            update_batches: Mutex::new(Vec::new()),
            subscribe_call_count: AtomicUsize::new(0),
            download_delay: std::time::Duration::ZERO,
            inflight_downloads: std::sync::Arc::new(AtomicUsize::new(0)),
            max_inflight_downloads: std::sync::Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Record a chat's scripted history newest-first.
    ///
    /// `iter_history` reads from this map. The page is sorted by msg_id
    /// descending so callers may pass any order — the order they observe
    /// from `iter_history` is always newest-first.
    pub fn script_history(&self, chat_id: i64, mut page: Vec<MessageInfo>) {
        page.sort_by_key(|m| std::cmp::Reverse(m.msg_id));
        self.history.lock().unwrap().insert(chat_id, page);
    }

    /// Record live updates that `subscribe_updates` will deliver as a single
    /// batch. Each call APPENDS one batch — the next `subscribe_updates`
    /// call drains the front batch and closes the channel, modelling a
    /// real Telegram stream that closes after a burst so the auto-reconnect
    /// loop can be exercised. Backwards-compatible thin wrapper over
    /// [`Self::script_updates_batches`].
    pub fn script_updates(&self, evts: Vec<MessageInfo>) {
        self.update_batches.lock().unwrap().push(evts);
    }

    /// Replace the queued update batches with the supplied list. Each batch
    /// is delivered by exactly one `subscribe_updates` call; the receiver
    /// closes after the batch's last item, so the consumer must
    /// re-subscribe to consume the next batch. Used to pin reconnect
    /// behavior in [`crate::cmd::watch::subscribe_with_reconnect`].
    pub fn script_updates_batches(&self, batches: Vec<Vec<MessageInfo>>) {
        *self.update_batches.lock().unwrap() = batches;
    }

    /// Number of times `subscribe_updates` has been called on this mock.
    pub fn subscribe_calls(&self) -> usize {
        self.subscribe_call_count.load(Ordering::Relaxed)
    }

    /// Push a sequence of upload outcomes consumed FIFO by `upload_file`.
    ///
    /// Chainable builder form (matches `with_dialog` / `with_document`).
    /// Calling this method replaces any prior script. Once the queue is
    /// exhausted, subsequent `upload_file` calls succeed with auto-allocated
    /// ids from the internal monotonic counter.
    pub fn script_upload(self, outcomes: Vec<UploadOutcome>) -> Self {
        *self.upload_script.lock().unwrap() = outcomes.into();
        self
    }

    /// Configure a wall-clock delay each `download_stream` producer holds the
    /// in-flight slot before producing bytes. Defaults to `Duration::ZERO`.
    /// Tests use this to widen the race-window when asserting on
    /// [`Self::inflight_observed`].
    pub fn download_delay(mut self, d: std::time::Duration) -> Self {
        self.download_delay = d;
        self
    }

    /// High-water mark of concurrent in-flight `download_stream` producer
    /// tasks observed over the lifetime of this mock. The counter is bumped
    /// at the start of each producer task and decremented on its drop, so
    /// the value reflects the maximum concurrency the orchestrator allowed
    /// — a regression in `inter_file_channel_capacity` enforcement is
    /// caught by asserting this is `<= cfg.inter_file_channel_capacity`.
    pub fn inflight_observed(&self) -> usize {
        self.max_inflight_downloads.load(Ordering::SeqCst)
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

    /// Register a message's metadata for `message_info()` lookups.
    ///
    /// Mirrors [`Self::with_document`] but takes `&self` so it composes with
    /// `Arc<MockClient>` (alongside `script_download`/`script_upload`). The
    /// stored payload bytes are empty — pair with `script_download` to drive
    /// chunk delivery.
    pub fn set_message(&self, chat_id: i64, msg_id: i32, info: MessageInfo) {
        self.messages
            .lock()
            .unwrap()
            .insert((chat_id, msg_id), (info, Vec::new()));
    }

    /// Register a chunk-by-chunk download script for `(chat_id, msg_id)`.
    ///
    /// The script is consumed (drained) on the first `download_stream` call
    /// for that key pair. Any prior script for the same key is replaced.
    /// After a scripted `Err` entry is forwarded the channel is closed,
    /// even if further entries remain in the script.
    pub fn script_download(&self, chat_id: i64, msg_id: i32, chunks: Vec<anyhow::Result<Bytes>>) {
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

/// RAII bump/decrement on a paired `Arc<AtomicUsize>`; updates `max`
/// monotonically on bump via a lock-free CAS loop. Owned form (no borrow)
/// so the guard can be moved into a `tokio::spawn` closure.
struct InflightGuardOwned {
    inflight: std::sync::Arc<AtomicUsize>,
}

impl InflightGuardOwned {
    fn enter(
        inflight: &std::sync::Arc<AtomicUsize>,
        max: &std::sync::Arc<AtomicUsize>,
    ) -> Self {
        let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
        let mut cur = max.load(Ordering::SeqCst);
        while now > cur {
            match max.compare_exchange_weak(cur, now, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(updated) => cur = updated,
            }
        }
        InflightGuardOwned {
            inflight: inflight.clone(),
        }
    }
}

impl Drop for InflightGuardOwned {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::SeqCst);
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
            // Clone the Arc'd counters + delay before moving into the spawn
            // so the guard's lifetime spans the entire producer task — not
            // just the prelude. See `InflightGuardOwned` doc-comment.
            let inflight = self.inflight_downloads.clone();
            let max = self.max_inflight_downloads.clone();
            let delay = self.download_delay;
            tokio::spawn(async move {
                // Guard MUST be the first statement so it covers `sleep`
                // too — otherwise concurrent producers can both be sleeping
                // outside the guard and `inflight_observed()` would report
                // 1 even when the orchestrator broke the cap.
                let _g = InflightGuardOwned::enter(&inflight, &max);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
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
            let inflight = self.inflight_downloads.clone();
            let max = self.max_inflight_downloads.clone();
            let delay = self.download_delay;
            tokio::spawn(async move {
                let _g = InflightGuardOwned::enter(&inflight, &max);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
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
    ) -> Result<i64> {
        let outcome = self.upload_script.lock().unwrap().pop_front();
        match outcome {
            Some(UploadOutcome::FloodWait { seconds }) => {
                anyhow::bail!("FLOOD_WAIT_{seconds}");
            }
            Some(UploadOutcome::Permanent(msg)) => {
                anyhow::bail!("permanent upload error: {msg}");
            }
            Some(UploadOutcome::Ok(id)) => {
                self.uploaded.lock().unwrap().push((
                    target_chat_id,
                    local_path.into(),
                    caption.map(String::from),
                    id,
                ));
                Ok(id)
            }
            None => {
                let id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
                self.uploaded.lock().unwrap().push((
                    target_chat_id,
                    local_path.into(),
                    caption.map(String::from),
                    id,
                ));
                Ok(id)
            }
        }
    }

    async fn iter_history(
        &self,
        chat_id: i64,
        max_id: Option<i32>,
        limit: u32,
    ) -> Result<Vec<MessageInfo>> {
        let h = self.history.lock().unwrap();
        let Some(page) = h.get(&chat_id) else {
            return Ok(Vec::new());
        };
        let limit_us = usize::try_from(limit).unwrap_or(usize::MAX);
        Ok(page
            .iter()
            .filter(|m| max_id.map_or(true, |x| m.msg_id < x))
            .take(limit_us)
            .cloned()
            .collect())
    }

    async fn subscribe_updates(
        &self,
        chat_ids: &[i64],
    ) -> Result<mpsc::Receiver<MessageInfo>> {
        // Count this subscription attempt so reconnect tests can pin the
        // expected number of re-subscribes.
        self.subscribe_call_count.fetch_add(1, Ordering::Relaxed);

        let want: std::collections::HashSet<i64> = chat_ids.iter().copied().collect();
        // Pop ONE batch from the front. If no batch is queued, deliver an
        // empty batch (channel closes immediately) — this lets a long
        // `--duration-seconds` test exit cleanly via the deadline.
        let queued = {
            let mut batches = self.update_batches.lock().unwrap();
            if batches.is_empty() {
                Vec::new()
            } else {
                batches.remove(0)
            }
        };
        let (tx, rx) = mpsc::channel::<MessageInfo>(32);
        tokio::spawn(async move {
            for evt in queued {
                if !want.contains(&evt.chat_id) {
                    continue;
                }
                if tx.send(evt).await.is_err() {
                    return;
                }
            }
            // tx dropped here → receiver sees None (batch exhausted; the
            // consumer must re-subscribe to drain the next batch).
        });
        Ok(rx)
    }
}
