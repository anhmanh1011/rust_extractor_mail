use telegram_client::pipeline::upload::split_for_upload;

#[tokio::test]
async fn no_split_when_under_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("small.out");
    std::fs::write(&p, b"alice@x.com:p1\nbob@y.com:p2\n").unwrap();

    let parts = split_for_upload(&p, 1 << 20).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0], p);
}

#[tokio::test]
async fn splits_three_parts_at_line_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("big.out");
    let mut buf = Vec::new();
    for i in 0..6 {
        buf.extend_from_slice(format!("user{i:02}@xx.com:p\n").as_bytes()); // 16 B
    }
    assert_eq!(buf.len(), 96);
    std::fs::write(&p, &buf).unwrap();

    let parts = split_for_upload(&p, 32).await.unwrap();
    assert_eq!(parts.len(), 3, "expected 3 parts; got {parts:?}");

    let mut total = Vec::new();
    for part in &parts {
        let bytes = std::fs::read(part).unwrap();
        assert!(bytes.len() <= 32, "part {} = {} B exceeds cap", part.display(), bytes.len());
        assert!(bytes.ends_with(b"\n"), "part must end on \\n: {part:?}");
        total.extend_from_slice(&bytes);
    }
    assert_eq!(total, buf, "concatenation of parts must equal original");
}

#[tokio::test]
async fn pathological_long_line_returns_err() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("long.out");
    let line: Vec<u8> = std::iter::repeat(b'x').take(64).chain([b'\n']).collect();
    std::fs::write(&p, &line).unwrap();

    let err = split_for_upload(&p, 32).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("line longer than cap"),
        "expected 'line longer than cap' classification: {msg}");
}

#[tokio::test]
async fn splits_no_trailing_newline_input() {
    // Input without a trailing '\n'; cap forces 2 parts. Verify no data loss
    // and that the final part is written as-is (no synthesized newline).
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("notrail.out");
    let original: &[u8] = b"aaa\nbbb\nccc";
    std::fs::write(&p, original).unwrap();

    let parts = split_for_upload(&p, 8).await.unwrap();
    assert_eq!(parts.len(), 2, "expected 2 parts; got {parts:?}");

    let mut total = Vec::new();
    for part in &parts {
        let bytes = std::fs::read(part).unwrap();
        assert!(bytes.len() <= 8, "part {} = {} B exceeds cap", part.display(), bytes.len());
        total.extend_from_slice(&bytes);
    }
    assert_eq!(total, original, "concatenation of parts must equal original");

    let last = std::fs::read(parts.last().unwrap()).unwrap();
    assert!(!last.ends_with(b"\n"),
        "last part should preserve the original (no trailing newline): {last:?}");
}

#[tokio::test]
async fn cap_zero_returns_err() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("any.out");
    std::fs::write(&p, b"alice@x.com:p1\n").unwrap();

    let err = split_for_upload(&p, 0).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("cap_bytes must be > 0"),
        "expected 'cap_bytes must be > 0' classification: {msg}");
}
