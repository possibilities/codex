//! Local-file-only TUI path: no app-server, model session, or rollout writes.
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use crossterm::event::KeyCode;
use crossterm::event::KeyEventKind;
use crossterm::event::KeyModifiers;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Stylize;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Widget;
use tokio_stream::StreamExt;

use super::SessionViewerOptions;
use super::viewport::ConversationViewport;
use super::voice_transcript::VoiceFile;
use crate::AppExitInfo;
use crate::ExitReason;
use crate::TerminalRestoreGuard;
use crate::keymap::RuntimeKeymap;
use crate::legacy_core::config::ConfigBuilder;
use crate::tui::Tui;
use crate::tui::TuiEvent;

pub(super) async fn run(path: PathBuf, options: SessionViewerOptions) -> io::Result<AppExitInfo> {
    let mut source = VoiceFile::open(&path)?;
    source.refresh()?;
    let config = ConfigBuilder::default()
        .cli_overrides(
            options
                .config_overrides
                .parse_overrides()
                .map_err(io::Error::other)?,
        )
        .build()
        .await?;
    let keymap = RuntimeKeymap::from_config(&config.tui_keymap)
        .map_err(io::Error::other)?
        .pager;
    let mut viewport = ConversationViewport::new(source.transcript.cells(), keymap);
    let initialized = crate::tui::init()?;
    let mut restore = TerminalRestoreGuard::new();
    let mut tui = Tui::new(
        initialized.terminal,
        initialized.enhanced_keys_supported,
        initialized.stderr_guard,
    );
    tui.set_alt_screen_enabled(crate::determine_alt_screen_mode(
        options.no_alt_screen,
        config.tui_alternate_screen,
    ));
    let result = run_loop(&mut tui, &mut viewport, &mut source, options.follow).await;
    tui.pause_events();
    if tui.is_alt_screen_active() {
        let _ = super::set_mouse_capture(&mut tui, /*enabled*/ false);
        let _ = tui.leave_alt_screen();
    }
    restore.restore_silently();
    result?;
    Ok(AppExitInfo {
        token_usage: crate::token_usage::TokenUsage::default(),
        thread_id: None,
        resume_hint: None,
        update_action: None,
        exit_reason: ExitReason::UserRequested,
        disconnect_info: None,
    })
}

async fn run_loop(
    tui: &mut Tui,
    viewport: &mut ConversationViewport,
    source: &mut VoiceFile,
    follow: bool,
) -> io::Result<()> {
    tui.enter_alt_screen()?;
    if tui.is_alt_screen_active() {
        super::set_mouse_capture(tui, /*enabled*/ true)?;
    }
    let mut events = tui.event_stream();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut loaded = false;
    let mut dirty = true;
    loop {
        if dirty {
            let status = if source.partial_tail() {
                "Voice · incomplete JSONL tail · q / Ctrl+C quit"
            } else if follow {
                "Voice · following recording · q / Ctrl+C quit"
            } else {
                "Voice · saved recording · q / Ctrl+C quit"
            };
            tui.draw(u16::MAX, |frame| {
                render(viewport, status, frame.area(), frame.buffer);
            })?;
            dirty = false;
        }
        tokio::select! {
            _ = tick.tick() => {
                if follow || !loaded {
                    let partial = source.partial_tail();
                    let changed = source.refresh()?;
                    loaded = source.caught_up()?;
                    if loaded && !follow { source.transcript.interrupt(); }
                    if changed || !follow { viewport.replace_cells(source.transcript.cells()); }
                    dirty = changed || !follow || partial != source.partial_tail();
                }
            }
            event = events.next() => match event {
                Some(TuiEvent::Key(key)) => {
                    if (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) && key.kind != KeyEventKind::Release)
                        || viewport.handle_key(key) { break; }
                    dirty = true;
                }
                Some(TuiEvent::Draw | TuiEvent::Resize(_) | TuiEvent::Resume) => dirty = true,
                Some(TuiEvent::Paste(_) | TuiEvent::FocusGained | TuiEvent::FocusLost) => {},
                None => break,
            }
        }
    }
    Ok(())
}

pub(super) fn render(
    viewport: &mut ConversationViewport,
    status: &str,
    area: Rect,
    buffer: &mut Buffer,
) {
    let content = Rect::new(area.x, area.y, area.width, area.height.saturating_sub(1));
    viewport.render(content, buffer);
    Paragraph::new(status.dim()).render(
        Rect::new(
            area.x,
            area.bottom().saturating_sub(1),
            area.width,
            u16::from(area.height > 0),
        ),
        buffer,
    );
}
