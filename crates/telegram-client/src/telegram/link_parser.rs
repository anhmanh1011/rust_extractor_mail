//! Parse t.me / tg:// message links into a typed reference.

use anyhow::{anyhow, Context, Result};

/// Typed reference to a Telegram message extracted from a link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageRef {
    /// Public channel: resolved via `client.resolve_username`.
    Username {
        /// Public `@username` of the channel/user (without the leading `@`).
        username: String,
        /// Numeric message id within the channel.
        msg_id: i32,
    },
    /// Private channel: chat_id is the t.me/c/<N> internal id with -100 prefix.
    ChatId {
        /// MTProto chat_id (already shifted by `-1_000_000_000_000`).
        chat_id: i64,
        /// Numeric message id within the chat.
        msg_id: i32,
    },
}

/// Parse a t.me message link. Accepted forms:
///   https://t.me/<username>/<msg_id>
///   https://t.me/c/<internal_id>/<msg_id>
///   tg://resolve?domain=<username>&post=<msg_id>
pub fn parse_message_link(s: &str) -> Result<MessageRef> {
    if let Some(rest) = s.strip_prefix("tg://resolve?") {
        let mut domain = None;
        let mut post   = None;
        for kv in rest.split('&') {
            let (k, v) = kv.split_once('=').ok_or_else(|| anyhow!("malformed query: {kv}"))?;
            match k {
                "domain" => domain = Some(v.to_string()),
                "post"   => post   = Some(v.parse::<i32>().context("post must be int")?),
                _ => {}
            }
        }
        let username = domain.ok_or_else(|| anyhow!("tg://resolve missing domain"))?;
        let msg_id   = post.ok_or_else(|| anyhow!("tg://resolve missing post"))?;
        return Ok(MessageRef::Username { username, msg_id });
    }

    let rest = s
        .strip_prefix("https://t.me/")
        .or_else(|| s.strip_prefix("http://t.me/"))
        .ok_or_else(|| anyhow!("not a t.me URL: {s}"))?;

    if let Some(after_c) = rest.strip_prefix("c/") {
        let mut parts = after_c.splitn(2, '/');
        let internal: i64 = parts.next().ok_or_else(|| anyhow!("missing chat segment"))?
            .parse().context("internal id must be int")?;
        let msg_id: i32  = parts.next().ok_or_else(|| anyhow!("missing msg_id"))?
            .parse().context("msg_id must be int")?;
        return Ok(MessageRef::ChatId { chat_id: -1_000_000_000_000_i64 - internal, msg_id });
    }

    let mut parts = rest.splitn(2, '/');
    let username = parts.next().ok_or_else(|| anyhow!("missing username"))?;
    if username.is_empty() { return Err(anyhow!("empty username")); }
    let msg_id: i32 = parts.next().ok_or_else(|| anyhow!("missing msg_id"))?
        .parse().context("msg_id must be int")?;
    Ok(MessageRef::Username { username: username.into(), msg_id })
}
