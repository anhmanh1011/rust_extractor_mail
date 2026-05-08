//! Spec §9.2 line 601 (`pipeline_stream.rs`): mocked mpsc<Bytes> → output bytes.

use std::sync::Arc;

use bytes::Bytes;
use extractor_core::{Matcher, Mode};
use telegram_client::pipeline::stream::stream_extract;

const MAX_LINE_BYTES: usize = 64 * 1024;

#[tokio::test]
async fn plain_text_single_chunk_emits_only_matches() {
    let m = Arc::new(Matcher::new("gmail.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from_static(
        b"gmail.com:alice@x.com:p1\n\
          yahoo.com:bob@y.com:p2\n\
          mail.gmail.com:carol@z.com:p3\n",
    ))
    .await
    .unwrap();
    drop(tx);

    let buf: Vec<u8> = Vec::new();
    let (out, stats) = stream_extract(rx, m, MAX_LINE_BYTES, buf, false).await.unwrap();
    assert_eq!(out, b"alice@x.com:p1\ncarol@z.com:p3\n");
    assert_eq!(stats.lines_matched, 2);
}

#[tokio::test]
async fn plain_text_chunk_split_mid_line_does_not_lose_lines() {
    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from_static(b"target.com:user@x.com")).await.unwrap();
    tx.send(Bytes::from_static(
        b":secret\nother.com:noise:noise\ntarget.com:b",
    ))
    .await
    .unwrap();
    tx.send(Bytes::from_static(b"@y.com:s2\n")).await.unwrap();
    drop(tx);

    let (out, stats) = stream_extract(rx, m, MAX_LINE_BYTES, Vec::new(), false)
        .await
        .unwrap();
    assert_eq!(out, b"user@x.com:secret\nb@y.com:s2\n");
    assert_eq!(stats.lines_matched, 2);
}

#[tokio::test]
async fn plain_text_unterminated_final_line_still_processed() {
    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Bytes::from_static(b"target.com:no@trailing.nl:pwd"))
        .await
        .unwrap();
    drop(tx);

    let (out, stats) = stream_extract(rx, m, MAX_LINE_BYTES, Vec::new(), false)
        .await
        .unwrap();
    assert_eq!(out, b"no@trailing.nl:pwd\n");
    assert_eq!(stats.lines_matched, 1);
}

#[tokio::test]
async fn gzip_chunk_decodes_and_extracts() {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(
        b"target.com:a@a.com:p1\n\
          noise.com:b@b.com:p2\n\
          target.com:c@c.com:p3\n",
    )
    .unwrap();
    let gz = enc.finish().unwrap();

    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mid = gz.len() / 2;
    tx.send(Bytes::copy_from_slice(&gz[..mid])).await.unwrap();
    tx.send(Bytes::copy_from_slice(&gz[mid..])).await.unwrap();
    drop(tx);

    let (out, stats) = stream_extract(rx, m, MAX_LINE_BYTES, Vec::new(), true)
        .await
        .unwrap();
    assert_eq!(out, b"a@a.com:p1\nc@c.com:p3\n");
    assert_eq!(stats.lines_matched, 2);
}

#[tokio::test]
async fn line_too_long_returns_error() {
    let m = Arc::new(Matcher::new("target.com", Mode::Plain).unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mut huge = Vec::with_capacity(200_000);
    huge.extend_from_slice(b"target.com:");
    huge.extend(std::iter::repeat(b'A').take(150_000));
    tx.send(Bytes::from(huge)).await.unwrap();
    drop(tx);

    let err = stream_extract(rx, m, 64 * 1024, Vec::new(), false)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("LineTooLong") || msg.contains("max_line"),
        "expected line-too-long error, got: {msg}",
    );
}
