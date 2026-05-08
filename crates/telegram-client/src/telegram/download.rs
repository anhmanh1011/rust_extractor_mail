//! Streaming-download helper (spec §4.1, §5.3).
//!
//! pump_chunks drives any futures::Stream<Item = anyhow::Result<bytes::Bytes>>
//! into a bounded tokio::sync::mpsc. Backpressure: when the receiver is slow
//! the stream poll simply awaits on tx.send(...).await.

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt as _;

/// Channel capacity = config.pipeline.intra_file_channel_capacity (default 4).
pub const INTRA_FILE_CAP: usize = 4;

/// Drive a Stream<Item = Result<Bytes>> into a tokio mpsc sender.
///
/// Pulls each item from stream and forwards it through tx. Stops early if the
/// receiver is dropped (tx.send returns Err). On a stream error the Err item
/// is forwarded and the sender is closed.
///
/// The stream does not need to be Unpin; it is pinned internally.
pub async fn pump_chunks<S>(tx: tokio::sync::mpsc::Sender<Result<Bytes>>, stream: S)
where
    S: futures::Stream<Item = Result<Bytes>>,
{
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let is_err = item.is_err();
        if tx.send(item).await.is_err() {
            return; // receiver dropped — stop early
        }
        if is_err {
            return; // transport error forwarded — close channel
        }
    }
}