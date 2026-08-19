#![allow(clippy::print_stderr)]

use clap::Parser;
use codex_arg0::Arg0DispatchPaths;
use codex_arg0::arg0_dispatch_or_else;
use codex_config::LoaderOverrides;
use codex_tui::ExitReason;
use codex_tui::SessionViewerOptions;
use codex_tui::run_session_viewer;
use codex_utils_cli::CliConfigOverrides;

/// Read-only, conversation-only viewer for a saved Codex session.
#[derive(Debug, Parser)]
#[command(name = "codex-viewer", version)]
struct Cli {
    /// Session id (UUID) to display.
    #[arg(value_name = "SESSION_ID")]
    session_id: String,

    /// Disable alternate screen mode and preserve terminal scrollback.
    #[arg(long, default_value_t = false)]
    no_alt_screen: bool,

    #[clap(flatten)]
    config_overrides: CliConfigOverrides,
}

fn main() -> anyhow::Result<()> {
    arg0_dispatch_or_else(|arg0_paths: Arg0DispatchPaths| async move {
        let cli = Cli::parse();
        let exit_info = run_session_viewer(
            SessionViewerOptions {
                session_id: cli.session_id,
                no_alt_screen: cli.no_alt_screen,
                config_overrides: cli.config_overrides,
            },
            arg0_paths,
            LoaderOverrides::default(),
        )
        .await?;
        if let ExitReason::Fatal(message) = exit_info.exit_reason {
            eprintln!("ERROR: {message}");
            std::process::exit(1);
        }
        Ok(())
    })
}

#[cfg(test)]
#[path = "session_viewer_tests.rs"]
mod tests;
