use extractor_core::{Matcher, Mode, Scanner};

fn run(matcher: &Matcher, input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut s = Scanner::new(matcher);
    s.scan_all(input, &mut out).unwrap();
    out
}

#[test]
fn empty_input_emits_nothing() {
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    assert_eq!(run(&m, b""), b"");
}

#[test]
fn only_newlines_emits_nothing() {
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    assert_eq!(run(&m, b"\n\n\n"), b"");
}

#[test]
fn missing_trailing_newline_still_processed() {
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    assert_eq!(
        run(&m, b"gmail.com:a:b"),
        b"a:b\n"
    );
}

#[test]
fn crlf_treated_as_lf_with_carriage_return_kept_visible() {
    // Our scanner splits on '\n' only — '\r' (if present) is part of the line
    // and the matcher will (correctly) not match because '\r' breaks the
    // host-byte run / suffix check. This documents current behavior.
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    let out = run(&m, b"gmail.com:a:b\r\nother:x:y\r\n");
    // Line 1 is "gmail.com:a:b\r" — first colon at index 9, field "gmail.com"
    // matches; emitted slice is "a:b\r"
    assert_eq!(out, b"a:b\r\n");
}
