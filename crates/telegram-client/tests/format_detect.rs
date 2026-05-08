//! Test list per spec §9.2 line 599 (`format_detect.rs`).

use telegram_client::pipeline::format::{detect, Format};

#[test]
fn extension_only_txt() {
    assert_eq!(detect("dump.txt", &[]), Format::Txt);
}

#[test]
fn extension_only_gz() {
    assert_eq!(detect("dump.gz", &[]), Format::Gz);
}

#[test]
fn extension_only_zip() {
    assert_eq!(detect("dump.zip", &[]), Format::Zip);
}

#[test]
fn extension_unknown_returns_unknown() {
    assert_eq!(detect("dump.bin", &[]), Format::Unknown);
}

#[test]
fn magic_bytes_gzip_overrides_txt_extension() {
    // 0x1F 0x8B is the gzip magic. A file named .txt that is actually gzip
    // (e.g. someone renamed a dump) MUST be detected as Gz so the decoder
    // engages — otherwise we'd write binary garbage to the output.
    let head = [0x1F, 0x8B, 0x08, 0x00];
    assert_eq!(detect("misnamed.txt", &head), Format::Gz);
}

#[test]
fn magic_bytes_zip_overrides_gz_extension() {
    // 0x50 0x4B 0x03 0x04 is the local-file-header signature.
    let head = [0x50, 0x4B, 0x03, 0x04];
    assert_eq!(detect("weird.gz", &head), Format::Zip);
}

#[test]
fn ascii_head_with_txt_extension_stays_txt() {
    let head = b"hello world\n";
    assert_eq!(detect("dump.txt", head), Format::Txt);
}

#[test]
fn case_insensitive_extension() {
    assert_eq!(detect("DUMP.TXT", &[]), Format::Txt);
    assert_eq!(detect("Dump.Gz", &[]), Format::Gz);
    assert_eq!(detect("dump.ZIP", &[]), Format::Zip);
}

#[test]
fn empty_head_short_circuit_to_extension() {
    // No magic bytes available → must use extension.
    assert_eq!(detect("dump.txt", &[]), Format::Txt);
    assert_eq!(detect("dump.gz", &[]), Format::Gz);
}

#[test]
fn three_byte_head_too_short_for_zip_magic_falls_back_to_extension() {
    // Zip magic is 4 bytes. Less than that → fall back to extension.
    let head = [0x50, 0x4B, 0x03];
    assert_eq!(detect("dump.gz", &head), Format::Gz);
}
