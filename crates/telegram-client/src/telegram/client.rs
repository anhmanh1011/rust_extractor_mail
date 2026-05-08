//! Real `TelegramClient` implementation backed by the `grammers` crates.

use super::{ChatRef, Dialog, DialogKind, MessageInfo, TelegramClient};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use grammers_client::types::{Chat, Media};
use grammers_client::{Client, Config, InitParams, SignInError};
use grammers_session::Session;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

/// Real grammers-backed client. Construct via [`GrammersClient::connect`].
pub struct GrammersClient {
    pub(crate) client: Client,
    pub(crate) session_path: PathBuf,
}

impl GrammersClient {
    /// Connect using credentials + session file path. Loads existing session
    /// if present, else creates an empty one (login required separately via
    /// `auth` subcommand).
    pub async fn connect(api_id: i32, api_hash: &str, session_path: &Path) -> Result<Self> {
        let session = if session_path.exists() {
            Session::load_file(session_path)
                .with_context(|| format!("loading session from {}", session_path.display()))?
        } else {
            Session::new()
        };
        let client = Client::connect(Config {
            session,
            api_id,
            api_hash: api_hash.to_string(),
            params: InitParams {
                catch_up: false,
                ..Default::default()
            },
        })
        .await
        .context("grammers connect")?;
        Ok(Self {
            client,
            session_path: session_path.into(),
        })
    }

    /// Sign in with a phone number; returns `Ok(())` once the session is authorized.
    /// Fails if 2FA is required and `password` is `None`.
    pub async fn sign_in_with_code(
        &self,
        phone: &str,
        code: &str,
        password: Option<&str>,
    ) -> Result<()> {
        let token = self
            .client
            .request_login_code(phone)
            .await
            .context("request_login_code")?;
        match self.client.sign_in(&token, code).await {
            Ok(_) => Ok(()),
            Err(SignInError::PasswordRequired(pwt)) => {
                let p = password.ok_or_else(|| anyhow!("2FA enabled — password required"))?;
                self.client
                    .check_password(pwt, p)
                    .await
                    .map_err(|e| anyhow!("check_password: {e}"))?;
                Ok(())
            }
            Err(e) => Err(anyhow!("sign_in: {e}")),
        }
    }

    /// Persist the current session to disk (call after successful login).
    /// Sets `0o600` on Unix; on Windows logs a one-line note that perms are
    /// inherited from the profile directory's ACL.
    pub fn save_session(&self) -> Result<()> {
        self.client
            .session()
            .save_to_file(&self.session_path)
            .with_context(|| format!("saving session to {}", self.session_path.display()))?;
        // Lock down on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&self.session_path)?.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(&self.session_path, perms)?;
        }
        #[cfg(windows)]
        {
            tracing::info!(
                path = %self.session_path.display(),
                "Windows: session perms inherited from profile dir ACLs (no chmod equivalent)"
            );
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TelegramClient for GrammersClient {
    async fn connect_and_warm(&self) -> Result<()> {
        // Walk dialogs once to populate the per-chat access_hash cache.
        // grammers' `Client` is `Clone` and internally Arc-shared, so the
        // dialog iterator borrows the inner state via &self only.
        let mut iter = self.client.iter_dialogs();
        let mut count = 0usize;
        while let Some(_d) = iter.next().await.context("iter_dialogs")? {
            count += 1;
        }
        tracing::info!(dialogs = count, "warm-up complete");
        // Persist refreshed access_hash cache. save_session() takes &self;
        // the only mutated state is the on-disk session file.
        self.save_session()?;
        Ok(())
    }

    async fn iter_dialogs(&self) -> Result<Vec<Dialog>> {
        let mut out = Vec::new();
        let mut iter = self.client.iter_dialogs();
        while let Some(d) = iter.next().await.context("iter_dialogs")? {
            let chat = d.chat();
            let (kind, title, username) = match chat {
                Chat::User(u) => (
                    DialogKind::User,
                    u.full_name(),
                    u.username().map(String::from),
                ),
                Chat::Group(g) => (DialogKind::Group, g.title().to_string(), None),
                Chat::Channel(c) => (
                    DialogKind::Channel,
                    c.title().to_string(),
                    c.username().map(String::from),
                ),
            };
            out.push(Dialog {
                chat_id: chat.id(),
                kind,
                title,
                username,
            });
        }
        Ok(out)
    }

    async fn join_invite_link(&self, _link: &str) -> Result<()> {
        // grammers 0.7 gates `Client::accept_invite_link` behind the
        // `parse_invite_link` Cargo feature, which the workspace does not
        // enable (Phase 3 forbids dependency edits). Public chats can still
        // be joined via `resolve_chat` -> `join_chat`, but private invite
        // links are not yet wired. See plan note line 3721 + Phase 3 Task 3.2
        // dispatch report.
        Err(anyhow!(
            "join_invite_link: not supported in pinned grammers 0.7 \
             without the `parse_invite_link` feature — see plan note line 3721"
        ))
    }

    async fn resolve_chat(&self, r: &ChatRef) -> Result<i64> {
        match r {
            ChatRef::ChatId(id) => Ok(*id),
            ChatRef::Username(name) => {
                let chat = self
                    .client
                    .resolve_username(name.trim_start_matches('@'))
                    .await
                    .context("resolve_username")?
                    .ok_or_else(|| anyhow!("username not found: {name}"))?;
                Ok(chat.id())
            }
        }
    }

    async fn message_info(&self, chat_id: i64, msg_id: i32) -> Result<MessageInfo> {
        // grammers' message lookup goes through the chat handle: we need to
        // hydrate the chat first. For now (Phase 3) we re-walk dialogs each
        // call — fine for `auth`/`chats`/`join` which call this 0-1 times.
        // Phase 4 (download path) calls this per file, so a `(chat_id → Chat)`
        // cache populated by warm-up MUST be added there to avoid re-walking
        // dialogs for every download. Tracked in Phase 4 Task 4.x.
        let chat = self.find_chat(chat_id).await?;
        let msgs = self
            .client
            .get_messages_by_id(&chat, &[msg_id])
            .await
            .context("get_messages_by_id")?;
        let msg = msgs
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| anyhow!("no message {chat_id}/{msg_id}"))?;
        let media = msg
            .media()
            .ok_or_else(|| anyhow!("message {chat_id}/{msg_id} has no media"))?;
        let (file_name, size, mime) = doc_meta(&media)?;
        Ok(MessageInfo {
            chat_id,
            msg_id,
            file_name,
            size,
            mime,
        })
    }

    async fn download_stream(
        &self,
        chat_id: i64,
        msg_id: i32,
    ) -> Result<mpsc::Receiver<Result<Bytes>>> {
        use anyhow::Context as _;

        // 1. Resolve message to media reference.
        let chat = self
            .find_chat(chat_id)
            .await
            .context("resolve chat for download_stream")?;
        let msg = self
            .client
            .get_messages_by_id(&chat, &[msg_id])
            .await
            .context("get_messages_by_id")?
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| anyhow!("message {msg_id} not found in chat {chat_id}"))?;

        let media = msg
            .media()
            .ok_or_else(|| anyhow!("message {msg_id} has no media"))?;

        // 2. Wrap Media → Downloadable and build the grammers chunk iterator.
        //    Then convert to a futures::Stream via unfold so pump_chunks can
        //    drive it without capturing a mutable reference across closure calls.
        let downloadable = grammers_client::types::Downloadable::Media(media);
        let dl = self.client.iter_download(&downloadable);

        let chunk_stream = futures::stream::unfold(dl, |mut iter| async move {
            match iter.next().await {
                Ok(Some(chunk)) => Some((Ok(Bytes::from(chunk)), iter)),
                Ok(None) => None,
                Err(e) => Some((Err(anyhow::anyhow!("grammers iter_download: {e}")), iter)),
            }
        });

        let (tx, rx) = mpsc::channel(crate::telegram::download::INTRA_FILE_CAP);
        tokio::spawn(crate::telegram::download::pump_chunks(tx, chunk_stream));
        Ok(rx)
    }

    async fn upload_file(&self, chat: i64, path: &Path, caption: Option<&str>) -> Result<i64> {
        let target = self.find_chat(chat).await?;
        let mut file = tokio::fs::File::open(path).await.context("open upload")?;
        let size = file.metadata().await.context("upload metadata")?.len() as usize;
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("upload")
            .to_string();
        let uploaded = self
            .client
            .upload_stream(&mut file, size, name)
            .await
            .context("upload_stream")?;
        // `InputMessage::text(...)` is the only public way to set the caption;
        // `InputMessage::default().file(...)` would leave the text empty and
        // there's no public setter post-construction.
        let input_msg = grammers_client::InputMessage::text(caption.unwrap_or("")).file(uploaded);
        let sent = self
            .client
            .send_message(&target, input_msg)
            .await
            .context("send_message")?;
        // `Message::id()` returns i32 in grammers 0.7 — widen losslessly to i64.
        Ok(sent.id() as i64)
    }
}

impl GrammersClient {
    /// Helper: locate a `grammers Chat` by its numeric id (post warm-up).
    async fn find_chat(&self, chat_id: i64) -> Result<Chat> {
        let mut iter = self.client.iter_dialogs();
        while let Some(d) = iter.next().await.context("iter_dialogs")? {
            if d.chat().id() == chat_id {
                return Ok(d.chat().clone());
            }
        }
        Err(anyhow!(
            "chat_id {chat_id} not in dialogs — run `chats` first or `join` if private"
        ))
    }
}

/// Extract `(file_name, size, mime_type)` from a `Media::Document`.
/// Returns `Err` for non-document media (Photo, Sticker, Poll, etc.).
fn doc_meta(media: &Media) -> Result<(String, u64, Option<String>)> {
    match media {
        Media::Document(d) => Ok((
            d.name().to_string(),
            d.size().max(0) as u64,
            d.mime_type().map(|s| s.to_string()),
        )),
        other => Err(anyhow!("unsupported media kind for extraction: {other:?}")),
    }
}
