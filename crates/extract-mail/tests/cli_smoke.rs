use std::io::Write;
use std::process::Command;
use tempfile::NamedTempFile;

#[test]
fn plain_mode_extracts_matches() {
    let mut input = NamedTempFile::new().unwrap();
    writeln!(input, "gmail.com:alice:pwd1").unwrap();
    writeln!(input, "yahoo.com:bob:pwd2").unwrap();
    writeln!(input, "mail.gmail.com:carol:pwd3").unwrap();
    input.flush().unwrap();

    let bin = env!("CARGO_BIN_EXE_extract-mail");
    let output = Command::new(bin)
        .arg("-f").arg(input.path())
        .args(["-k", "gmail.com"])
        .output()
        .expect("run extract-mail");

    assert!(output.status.success(), "stderr: {}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("alice:pwd1"));
    assert!(stdout.contains("carol:pwd3"));
    assert!(!stdout.contains("bob:pwd2"));
}

#[test]
fn url_mode_extracts_matches() {
    let mut input = NamedTempFile::new().unwrap();
    writeln!(input, "https://br.linkedin.com/:alice@x:p1").unwrap();
    writeln!(input, "http://yahoo.com/:bob:p2").unwrap();
    input.flush().unwrap();

    let bin = env!("CARGO_BIN_EXE_extract-mail");
    let output = Command::new(bin)
        .arg("--url")
        .arg("-f").arg(input.path())
        .args(["-k", "linkedin.com"])
        .output()
        .expect("run extract-mail");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("alice@x:p1"));
    assert!(!stdout.contains("bob"));
}
