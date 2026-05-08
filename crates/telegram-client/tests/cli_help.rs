use std::process::Command;

#[test]
fn root_help_lists_all_subcommands() {
    let bin = env!("CARGO_BIN_EXE_tg-extract");
    let out = Command::new(bin).arg("--help").output().expect("run --help");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    for sub in ["auth", "join", "chats", "fetch", "watch", "backfill", "retry-uploads", "stats"] {
        assert!(stdout.contains(sub), "--help missing subcommand: {sub}\n{stdout}");
    }
}

#[test]
fn auth_subcommand_help_works() {
    let bin = env!("CARGO_BIN_EXE_tg-extract");
    let out = Command::new(bin).args(["auth", "--help"]).output().expect("run auth --help");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn fetch_link_and_chat_msg_id_are_mutually_exclusive() {
    let bin = env!("CARGO_BIN_EXE_tg-extract");
    let out = Command::new(bin)
        .args(["fetch", "--link", "https://t.me/c/1/2", "--chat", "@x", "--msg-id", "3"])
        .output().expect("run fetch with conflict");
    assert!(!out.status.success(), "expected clap to reject conflicting args");
}
