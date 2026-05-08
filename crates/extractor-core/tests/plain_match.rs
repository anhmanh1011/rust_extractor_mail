use extractor_core::{Matcher, MatcherError, Mode};

#[test]
fn new_rejects_empty_key() {
    let r = Matcher::new("", Mode::Plain);
    assert!(matches!(r, Err(MatcherError::Empty)));
}

#[test]
fn new_rejects_whitespace() {
    let r = Matcher::new("gma il.com", Mode::Plain);
    assert!(matches!(r, Err(MatcherError::Whitespace)));
}

#[test]
fn new_rejects_edge_dot() {
    assert!(matches!(Matcher::new(".gmail.com", Mode::Plain), Err(MatcherError::EdgeDot)));
    assert!(matches!(Matcher::new("gmail.com.", Mode::Plain), Err(MatcherError::EdgeDot)));
}

#[test]
fn new_rejects_non_ascii() {
    let r = Matcher::new("gmãil.com", Mode::Plain);
    assert!(matches!(r, Err(MatcherError::NonAscii)));
}

#[test]
fn key_is_lowercased() {
    let m = Matcher::new("GMAIL.com", Mode::Plain).unwrap();
    assert_eq!(m.key(), b"gmail.com");
}

#[test]
fn plain_emits_rest_after_first_colon() {
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    let line = b"gmail.com:user@x.com:pass1234";
    assert_eq!(m.match_line(line), Some(&b"user@x.com:pass1234"[..]));
}

#[test]
fn plain_no_colon_returns_none() {
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    assert_eq!(m.match_line(b"gmail.com"), None);
}

#[test]
fn plain_field_too_short_returns_none() {
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    assert_eq!(m.match_line(b"x.com:user:pass"), None);
}
