use extractor_core::{Matcher, Mode};

fn matches(key: &str, field: &str) -> bool {
    let m = Matcher::new(key, Mode::Plain).unwrap();
    let line = format!("{field}:user:pass");
    m.match_line(line.as_bytes()).is_some()
}

#[test]
fn exact_match() {
    assert!(matches("gmail.com", "gmail.com"));
}

#[test]
fn subdomain_match() {
    assert!(matches("gmail.com", "mail.gmail.com"));
    assert!(matches("gmail.com", "foo.bar.gmail.com"));
}

#[test]
fn wrong_boundary_rejected() {
    // Boundary char must be '.', not alphanumeric or hyphen.
    assert!(!matches("gmail.com", "xgmail.com"));     // alphanumeric boundary
    assert!(!matches("gmail.com", "x-gmail.com"));    // hyphen boundary
    assert!(!matches("gmail.com", "-gmail.com"));     // hyphen at start
    assert!(!matches("gmail.com", "not-gmail.com"));  // hyphen mid-word
}

#[test]
fn extra_suffix_rejected() {
    assert!(!matches("gmail.com", "gmail.com.vn"));
}

#[test]
fn pseudo_subdomain_rejected() {
    assert!(!matches("gmail.com", "gmail.commerce"));
}

#[test]
fn case_insensitive_field() {
    assert!(matches("gmail.com", "Mail.Gmail.COM"));
}
