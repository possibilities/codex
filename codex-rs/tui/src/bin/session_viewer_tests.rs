use super::*;
use pretty_assertions::assert_eq;

#[test]
fn parses_required_session_id_and_no_alt_screen() {
    let cli = Cli::try_parse_from([
        "codex-viewer",
        "00000000-0000-4000-8000-000000000123",
        "--no-alt-screen",
    ])
    .expect("valid viewer arguments");

    assert_eq!(
        (
            cli.session_id,
            cli.no_alt_screen,
            cli.config_overrides.raw_overrides,
        ),
        (
            Some("00000000-0000-4000-8000-000000000123".to_string()),
            true,
            Vec::<String>::new(),
        )
    );
}

#[test]
fn requires_session_id() {
    assert!(Cli::try_parse_from(["codex-viewer"]).is_err());
}

#[test]
fn voice_input_and_follow_are_explicit() {
    let cli =
        Cli::try_parse_from(["codex-viewer", "--voice-jsonl", "voice.jsonl", "--follow"]).unwrap();
    assert_eq!(
        (cli.session_id, cli.voice_jsonl, cli.follow),
        (None, Some("voice.jsonl".into()), true)
    );
    assert!(Cli::try_parse_from(["codex-viewer", "--follow", "session"]).is_err());
    assert!(
        Cli::try_parse_from(["codex-viewer", "session", "--voice-jsonl", "voice.jsonl"]).is_err()
    );
}
