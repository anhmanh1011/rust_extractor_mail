use extractor_core::{Matcher, Mode, Scanner, ScanError};

#[test]
fn line_exceeding_max_line_returns_error() {
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    let mut s = Scanner::with_max_line(&m, 32);
    let mut out: Vec<u8> = Vec::new();
    let huge = b"gmail.com:a:".to_vec();
    let mut input = huge.clone();
    input.extend(std::iter::repeat(b'b').take(64));
    let r = s.scan_all(&input, &mut out);
    assert!(matches!(r, Err(ScanError::LineTooLong(_))));
}
