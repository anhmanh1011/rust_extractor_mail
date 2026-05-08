use std::io::Write;
use telegram_client::config;
use tempfile::NamedTempFile;

fn write_toml(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

#[test]
fn loads_minimal_valid_config() {
    let f = write_toml(r#"
        [telegram]
        session_path = "/tmp/session.session"
        download_concurrent_chunks = 4
        [telegram.output]
        chat = "@results"
        [pipeline]
        work_dir = "/tmp/work"
        output_dir = "./out"
        chunk_bytes = 1048576
        intra_file_channel_capacity = 4
        inter_file_channel_capacity = 1
        upload_channel_capacity = 2
        max_line_bytes = 65536
        upload_rate_seconds = 3
        upload_max_size_bytes = 2147483648
        max_uncompressed_bytes = 10737418240
        [extract]
        mode = "plain"
        key  = "gmail.com"
        [log]
        level = "info"
        format = "human"
        rotation = "never"
    "#);
    let cfg = config::load(f.path()).unwrap();
    assert_eq!(cfg.extract.key, "gmail.com");
    assert_eq!(cfg.extract.mode, config::ExtractMode::Plain);
    assert_eq!(cfg.pipeline.chunk_bytes, 1_048_576);
}

#[test]
fn rejects_missing_required_section() {
    let f = write_toml("[telegram]\nsession_path = \"/tmp/s\"\n");
    let r = config::load(f.path());
    assert!(r.is_err(), "expected error: missing [extract]/[pipeline]");
}

#[test]
fn rejects_invalid_mode() {
    let f = write_toml(r#"
        [telegram]
        session_path = "/tmp/s"
        [telegram.output]
        chat = "@x"
        [pipeline]
        work_dir = "/tmp/w"
        output_dir = "./out"
        chunk_bytes = 1048576
        intra_file_channel_capacity = 4
        inter_file_channel_capacity = 1
        upload_channel_capacity = 2
        max_line_bytes = 65536
        upload_rate_seconds = 3
        upload_max_size_bytes = 1
        max_uncompressed_bytes = 1
        [extract]
        mode = "bogus"
        key  = "x.com"
        [log]
        level = "info"
        format = "human"
        rotation = "never"
    "#);
    let r = config::load(f.path());
    assert!(r.is_err());
}

#[test]
fn expands_tilde_in_paths() {
    // home() must be available — sanity-check the helper directly.
    let p = config::expand_path("~/foo");
    let home = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("/"));
    assert!(p.starts_with(&home), "expansion did not resolve ~: {p:?}");
    assert!(p.ends_with("foo"));
}

#[test]
fn output_chat_xor_chat_id_required() {
    // Both empty → reject.
    let f = write_toml(r#"
        [telegram]
        session_path = "/tmp/s"
        [telegram.output]
        [pipeline]
        work_dir = "/tmp/w"
        output_dir = "./out"
        chunk_bytes = 1048576
        intra_file_channel_capacity = 4
        inter_file_channel_capacity = 1
        upload_channel_capacity = 2
        max_line_bytes = 65536
        upload_rate_seconds = 3
        upload_max_size_bytes = 1
        max_uncompressed_bytes = 1
        [extract]
        mode = "plain"
        key  = "x.com"
        [log]
        level = "info"
        format = "human"
        rotation = "never"
    "#);
    assert!(config::load(f.path()).is_err());
}
