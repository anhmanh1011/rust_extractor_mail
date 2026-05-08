use anyhow::Result;
use clap::Parser;
use telegram_client::cmd::Cli;

fn main() -> Result<()> {
    let cli = Cli::parse();
    // observability + cmd dispatch land in Task 2.5 / 2.6.
    // For now: just verify clap parsed cleanly.
    let _ = cli;
    Ok(())
}
