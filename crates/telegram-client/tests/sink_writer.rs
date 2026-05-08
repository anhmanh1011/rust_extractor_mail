//! Spec §9.2 (`sink_writer.rs`): buffered LineSink emits each line plus
//! a trailing newline; `into_inner` flushes pending writes.

use extractor_core::LineSink;
use telegram_client::pipeline::sink::WriterSink;

#[test]
fn emit_writes_line_then_newline() {
    let buf: Vec<u8> = Vec::new();
    let mut sink = WriterSink::new(buf);
    sink.emit(b"alice@x.com:pwd").unwrap();
    sink.emit(b"bob@y.com:pwd2").unwrap();
    let out = sink.into_inner().unwrap();
    assert_eq!(out, b"alice@x.com:pwd\nbob@y.com:pwd2\n");
}

#[test]
fn emit_handles_empty_line() {
    let buf: Vec<u8> = Vec::new();
    let mut sink = WriterSink::new(buf);
    sink.emit(b"").unwrap();
    let out = sink.into_inner().unwrap();
    assert_eq!(out, b"\n");
}

#[test]
fn into_inner_flushes_pending_buffered_writes() {
    // Vec<u8> is unbuffered itself, but BufWriter holds writes until full
    // or flushed. into_inner() must trigger the flush.
    let buf: Vec<u8> = Vec::new();
    let mut sink = WriterSink::new(buf);
    for _ in 0..1024 {
        sink.emit(b"x").unwrap();
    }
    let out = sink.into_inner().unwrap();
    assert_eq!(out.len(), 2 * 1024); // 1024 × ("x" + "\n")
}
