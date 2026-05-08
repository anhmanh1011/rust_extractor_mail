use extractor_core::{Matcher, Mode};

fn m(key: &str) -> Matcher {
    Matcher::new(key, Mode::Url).unwrap()
}

#[test]
fn url_basic() {
    let line = b"http://br.linkedin.com/:alice@x.com:pwd1";
    assert_eq!(
        m("linkedin.com").match_line(line),
        Some(&b"alice@x.com:pwd1"[..])
    );
}

#[test]
fn url_with_port_and_path() {
    let line = b"https://login.example.com:8443/auth/login:user@x:p4ss";
    assert_eq!(
        m("example.com").match_line(line),
        Some(&b"user@x:p4ss"[..])
    );
}

#[test]
fn url_no_path() {
    let line = b"https://x.com:u:p";
    assert_eq!(m("x.com").match_line(line), Some(&b"u:p"[..]));
}

#[test]
fn url_pseudo_suffix_rejected() {
    let line = b"http://example.com.attacker.tld/:user:pass";
    assert_eq!(m("example.com").match_line(line), None);
}

#[test]
fn url_no_scheme_returns_none() {
    let line = b"example.com/:user:pass";
    assert_eq!(m("example.com").match_line(line), None);
}

#[test]
fn url_too_few_colons_returns_none() {
    let line = b"http://example.com/:user";
    assert_eq!(m("example.com").match_line(line), None);
}

#[test]
fn url_empty_host_returns_none() {
    let line = b"http://:user:pass";
    assert_eq!(m("example.com").match_line(line), None);
}

#[test]
fn url_garbage_line_returns_none() {
    let line = b"this is not a url at all";
    assert_eq!(m("example.com").match_line(line), None);
}
