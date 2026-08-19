//! Standalone, read-only conversation viewer for saved Codex sessions.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::Turn;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_exec_server::EnvironmentManager;
use codex_protocol::ThreadId;
use codex_rollout::StateDbHandle;
use codex_state::log_db;
use codex_utils_cli::CliConfigOverrides;
use color_eyre::eyre::Result;
use crossterm::event::DisableMouseCapture;
use crossterm::event::EnableMouseCapture;
use crossterm::execute;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Stylize;
use ratatui::widgets::Clear;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Widget;
use tokio::time::MissedTickBehavior;
use tokio_stream::StreamExt;

use crate::AppExitInfo;
use crate::AppServerTarget;
use crate::ExitReason;
use crate::app_server_session::AppServerSession;
use crate::keymap::RuntimeKeymap;
use crate::legacy_core::config::Config;
use crate::thread_transcript::RawReasoningVisibility;
use crate::thread_transcript::TranscriptCells;
use crate::thread_transcript::load_session_thread;
use crate::thread_transcript::thread_to_transcript_cells;
use crate::token_usage::TokenUsage;
use crate::tui::Tui;
use crate::tui::TuiEvent;

use self::rollout_watcher::LocalRolloutUpdate;
use self::rollout_watcher::LocalRolloutWatcher;
use self::transcript::turn_to_transcript_cells;
use self::viewport::ConversationViewport;

mod rollout_watcher;
mod transcript;
mod viewport;

const LIVE_REFRESH_INTERVAL: Duration = Duration::from_millis(250);
const ROLLOUT_REFRESH_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct SessionViewerOptions {
    pub session_id: String,
    pub no_alt_screen: bool,
    pub config_overrides: CliConfigOverrides,
}

/// Launches the standalone viewer without exposing it as a `codex` subcommand.
pub async fn run_session_viewer(
    options: SessionViewerOptions,
    arg0_paths: Arg0DispatchPaths,
    loader_overrides: LoaderOverrides,
) -> io::Result<AppExitInfo> {
    let mut cli = crate::Cli::try_parse_from(["codex-session-viewer"]).map_err(io::Error::other)?;
    cli.view_session_id = Some(options.session_id);
    cli.no_alt_screen = options.no_alt_screen;
    cli.config_overrides = options.config_overrides;
    crate::run_main(
        cli,
        arg0_paths,
        loader_overrides,
        /*explicit_remote_endpoint*/ None,
    )
    .await
}

struct SessionViewer {
    thread: Thread,
    config: Config,
    rendered_turn_cells: Vec<RenderedTurnCells>,
    viewport: ConversationViewport,
}

type RenderedTurnCells = TranscriptCells;

impl SessionViewer {
    fn new(thread: Thread, config: Config) -> Result<Self> {
        let keymap = RuntimeKeymap::from_config(&config.tui_keymap)
            .map_err(color_eyre::eyre::Report::msg)?
            .pager;
        let rendered_turn_cells = thread
            .turns
            .iter()
            .map(|turn| turn_to_transcript_cells(&thread, turn, &config))
            .collect::<Vec<_>>();
        let cells = flattened_transcript_cells(&thread, &config, &rendered_turn_cells);
        Ok(Self {
            thread,
            config,
            rendered_turn_cells,
            viewport: ConversationViewport::new(cells, keymap),
        })
    }

    fn replace_turns(&mut self, turns: Vec<Turn>) -> bool {
        let visible_change = self.thread.turns.len() != turns.len()
            || self
                .thread
                .turns
                .iter()
                .zip(&turns)
                .any(|(existing, replacement)| {
                    existing.id != replacement.id || existing.items != replacement.items
                });
        if !visible_change {
            self.thread.turns = turns;
            return false;
        }

        let previous_turns = std::mem::replace(&mut self.thread.turns, turns);
        let previous_cells = std::mem::take(&mut self.rendered_turn_cells);
        self.rendered_turn_cells = self
            .thread
            .turns
            .iter()
            .enumerate()
            .map(|(index, turn)| {
                if previous_turns
                    .get(index)
                    .is_some_and(|previous| previous == turn)
                {
                    previous_cells.get(index).cloned().unwrap_or_default()
                } else {
                    turn_to_transcript_cells(&self.thread, turn, &self.config)
                }
            })
            .collect();
        self.viewport.replace_cells(flattened_transcript_cells(
            &self.thread,
            &self.config,
            &self.rendered_turn_cells,
        ));
        true
    }

    fn merge_latest_turn(&mut self, turn: Turn) -> bool {
        if let Some(index) = self
            .thread
            .turns
            .iter()
            .position(|existing| existing.id == turn.id)
        {
            if self.thread.turns[index] == turn {
                return false;
            }
            let previous_turn = std::mem::replace(&mut self.thread.turns[index], turn);
            let items_changed = previous_turn.items != self.thread.turns[index].items;
            if !items_changed {
                return false;
            }
            self.rendered_turn_cells[index] =
                turn_to_transcript_cells(&self.thread, &self.thread.turns[index], &self.config);
        } else {
            let cells = turn_to_transcript_cells(&self.thread, &turn, &self.config);
            self.thread.turns.push(turn);
            self.rendered_turn_cells.push(cells);
        }
        self.viewport.replace_cells(flattened_transcript_cells(
            &self.thread,
            &self.config,
            &self.rendered_turn_cells,
        ));
        true
    }

    fn apply_history_changes(&mut self, changes: Vec<ThreadHistoryChangeSet>) -> bool {
        let mut visible_change = false;
        let mut changed_turn_ids = Vec::new();
        for changes in changes {
            for turn_id in changes.removed_turn_ids {
                if let Some(index) = self.thread.turns.iter().position(|turn| turn.id == turn_id) {
                    self.thread.turns.remove(index);
                    self.rendered_turn_cells.remove(index);
                    visible_change = true;
                }
            }
            for change in changes.changed_turns {
                let index = if let Some(index) = self
                    .thread
                    .turns
                    .iter()
                    .position(|turn| turn.id == change.turn_id)
                {
                    index
                } else {
                    self.thread.turns.push(Turn {
                        id: change.turn_id.clone(),
                        items: Vec::new(),
                        items_view: codex_app_server_protocol::TurnItemsView::Full,
                        status: change.status.clone(),
                        error: None,
                        started_at: None,
                        completed_at: None,
                        duration_ms: None,
                    });
                    self.rendered_turn_cells.push(Vec::new());
                    self.thread.turns.len() - 1
                };
                let turn = &mut self.thread.turns[index];
                turn.status = change.status;
                turn.error = change.error;
                turn.started_at = change.started_at;
                turn.completed_at = change.completed_at;
                turn.duration_ms = change.duration_ms;
            }
            for change in changes.changed_items {
                let index = if let Some(index) = self
                    .thread
                    .turns
                    .iter()
                    .position(|turn| turn.id == change.turn_id)
                {
                    index
                } else {
                    self.thread.turns.push(Turn {
                        id: change.turn_id.clone(),
                        items: Vec::new(),
                        items_view: codex_app_server_protocol::TurnItemsView::Full,
                        status: codex_app_server_protocol::TurnStatus::InProgress,
                        error: None,
                        started_at: None,
                        completed_at: None,
                        duration_ms: None,
                    });
                    self.rendered_turn_cells.push(Vec::new());
                    self.thread.turns.len() - 1
                };
                let turn = &mut self.thread.turns[index];
                if let Some(existing) = turn
                    .items
                    .iter_mut()
                    .find(|item| item.id() == change.item.id())
                {
                    if existing == &change.item {
                        continue;
                    }
                    *existing = change.item;
                } else {
                    turn.items.push(change.item);
                }
                if !changed_turn_ids.contains(&change.turn_id) {
                    changed_turn_ids.push(change.turn_id);
                }
                visible_change = true;
            }
        }

        for turn_id in changed_turn_ids {
            if let Some(index) = self.thread.turns.iter().position(|turn| turn.id == turn_id) {
                self.rendered_turn_cells[index] =
                    turn_to_transcript_cells(&self.thread, &self.thread.turns[index], &self.config);
            }
        }
        if visible_change {
            self.viewport.replace_cells(flattened_transcript_cells(
                &self.thread,
                &self.config,
                &self.rendered_turn_cells,
            ));
        }
        visible_change
    }
}

fn flattened_transcript_cells(
    thread: &Thread,
    config: &Config,
    rendered_turn_cells: &[RenderedTurnCells],
) -> TranscriptCells {
    let cells = rendered_turn_cells
        .iter()
        .flat_map(|cells| cells.iter().cloned())
        .collect::<TranscriptCells>();
    if !cells.is_empty() {
        return cells;
    }

    let mut empty_thread = thread.clone();
    empty_thread.turns.clear();
    thread_to_transcript_cells(empty_thread, raw_reasoning_visibility(config), Some(config))
}

fn raw_reasoning_visibility(config: &Config) -> RawReasoningVisibility {
    if config.show_raw_agent_reasoning {
        RawReasoningVisibility::Visible
    } else {
        RawReasoningVisibility::Hidden
    }
}

fn render_loading(area: Rect, buf: &mut Buffer) {
    Clear.render(area, buf);
    let loading_area = Rect::new(
        area.x,
        area.y.saturating_add(area.height.saturating_sub(1) / 2),
        area.width,
        u16::from(area.height > 0),
    );
    Paragraph::new("Loading conversation…".dim())
        .centered()
        .render(loading_area, buf);
}

pub(crate) fn draw_loading(tui: &mut Tui) -> io::Result<()> {
    tui.draw(u16::MAX, |frame| {
        render_loading(frame.area(), frame.buffer);
    })
}

fn set_mouse_capture(tui: &mut Tui, enabled: bool) -> io::Result<()> {
    if enabled {
        execute!(tui.terminal.backend_mut(), EnableMouseCapture)
    } else {
        execute!(tui.terminal.backend_mut(), DisableMouseCapture)
    }
}

fn merge_latest_turn(turns: &mut Vec<Turn>, turn: Turn) -> bool {
    if let Some(existing) = turns.iter_mut().find(|existing| existing.id == turn.id) {
        if existing == &turn {
            return false;
        }
        *existing = turn;
    } else {
        turns.push(turn);
    }
    true
}

async fn load_initial_thread(
    app_server: &mut AppServerSession,
    thread_id: ThreadId,
) -> io::Result<(Thread, Option<LocalRolloutWatcher>)> {
    let mut thread = app_server
        .thread_read(thread_id, /*include_turns*/ false)
        .await
        .map_err(io::Error::other)?;
    let mut rollout_watcher = LocalRolloutWatcher::for_thread(&thread);
    let mut rollout_watcher_failed = false;
    let loaded_local_rollout = if let Some(watcher) = rollout_watcher.as_mut() {
        match watcher.load_turns_if_changed().await {
            Ok(Some(LocalRolloutUpdate::Replace(turns))) => {
                thread.turns = turns;
                true
            }
            Ok(Some(LocalRolloutUpdate::Changes(_))) => {
                return Err(io::Error::other(
                    "session viewer rollout watcher produced deltas before its initial snapshot",
                ));
            }
            Ok(None) => true,
            Err(error) => {
                tracing::debug!(
                    path = %watcher.path.display(),
                    %error,
                    "session viewer initial rollout load failed"
                );
                rollout_watcher_failed = true;
                false
            }
        }
    } else {
        false
    };
    if !loaded_local_rollout {
        thread = load_session_thread(app_server, thread_id).await?;
    }
    if !loaded_local_rollout
        && let Some(turn) = app_server
            .latest_thread_turn(thread_id)
            .await
            .map_err(io::Error::other)?
    {
        merge_latest_turn(&mut thread.turns, turn);
    }
    if rollout_watcher_failed {
        rollout_watcher = None;
    }
    Ok((thread, rollout_watcher))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_app(
    cli: crate::Cli,
    arg0_paths: Arg0DispatchPaths,
    loader_overrides: LoaderOverrides,
    strict_config: bool,
    app_server_target: AppServerTarget,
    remote_cwd_override: Option<std::path::PathBuf>,
    config: Config,
    cli_kv_overrides: Vec<(String, toml::Value)>,
    cloud_config_bundle: CloudConfigBundleLoader,
    feedback: codex_feedback::CodexFeedback,
    log_db: Option<log_db::LogDbLayer>,
    state_db: Option<StateDbHandle>,
    environment_manager: Arc<EnvironmentManager>,
    startup_draft: crate::startup_draft::StartupDraft,
) -> Result<AppExitInfo> {
    let Some(session_id) = cli.view_session_id.as_deref() else {
        return Err(color_eyre::eyre::eyre!(
            "standalone viewer requires a session id"
        ));
    };
    let thread_id = match ThreadId::from_string(session_id) {
        Ok(thread_id) => thread_id,
        Err(error) => return Ok(AppExitInfo::fatal(format!("Invalid session ID: {error}"))),
    };
    let (mut tui, mut terminal_restore_guard, mut startup_draft) = startup_draft.into_parts();
    let app_server = startup_draft
        .run_until(
            &mut tui,
            crate::start_app_server(
                &app_server_target,
                arg0_paths,
                config.clone(),
                cli_kv_overrides,
                loader_overrides,
                strict_config,
                cloud_config_bundle,
                feedback,
                log_db,
                state_db,
                environment_manager,
            ),
        )
        .await??;
    let mut app_server = AppServerSession::new(app_server, app_server_target.thread_params_mode())
        .with_startup_config(&config)
        .with_remote_cwd_override(remote_cwd_override);
    drop(startup_draft);

    tui.set_alt_screen_enabled(crate::determine_alt_screen_mode(
        cli.no_alt_screen,
        config.tui_alternate_screen,
    ));
    let mut mouse_capture_enabled = false;
    let setup_result = tui.enter_alt_screen().and_then(|()| {
        if tui.is_alt_screen_active() {
            set_mouse_capture(&mut tui, /*enabled*/ true)?;
            mouse_capture_enabled = true;
        }
        draw_loading(&mut tui)
    });
    if let Err(error) = setup_result {
        cleanup_viewer(
            &mut tui,
            app_server,
            mouse_capture_enabled,
            &mut terminal_restore_guard,
        )
        .await;
        return Err(error.into());
    }

    let (thread, rollout_watcher) = match load_initial_thread(&mut app_server, thread_id).await {
        Ok(loaded) => loaded,
        Err(error) => {
            cleanup_viewer(
                &mut tui,
                app_server,
                mouse_capture_enabled,
                &mut terminal_restore_guard,
            )
            .await;
            return Ok(AppExitInfo::fatal(format!(
                "No saved session found with ID {session_id}: {error}"
            )));
        }
    };
    let mut viewer = match SessionViewer::new(thread, config) {
        Ok(viewer) => viewer,
        Err(error) => {
            cleanup_viewer(
                &mut tui,
                app_server,
                mouse_capture_enabled,
                &mut terminal_restore_guard,
            )
            .await;
            return Err(error);
        }
    };
    let run_result = run_event_loop(
        &mut tui,
        &mut app_server,
        thread_id,
        &mut viewer,
        rollout_watcher,
    )
    .await;
    cleanup_viewer(
        &mut tui,
        app_server,
        mouse_capture_enabled,
        &mut terminal_restore_guard,
    )
    .await;
    run_result?;
    Ok(AppExitInfo {
        token_usage: TokenUsage::default(),
        thread_id: Some(thread_id),
        resume_hint: None,
        update_action: None,
        exit_reason: ExitReason::UserRequested,
    })
}

async fn cleanup_viewer(
    tui: &mut Tui,
    app_server: AppServerSession,
    mouse_capture_enabled: bool,
    terminal_restore_guard: &mut crate::TerminalRestoreGuard,
) {
    tui.pause_events();
    if mouse_capture_enabled && let Err(error) = set_mouse_capture(tui, /*enabled*/ false) {
        tracing::warn!(%error, "failed to disable session viewer mouse capture");
    }
    if let Err(error) = app_server.shutdown().await {
        tracing::warn!(%error, "failed to shut down session viewer app server");
    }
    if let Err(error) = crate::tui::discard_pending_terminal_input() {
        tracing::warn!(%error, "failed to discard pending session viewer input");
    }
    if tui.is_alt_screen_active() {
        if let Err(error) = tui.leave_alt_screen() {
            tracing::warn!(%error, "failed to leave session viewer alternate screen");
        } else if let Err(error) = tui.terminal.clear_visible_screen() {
            tracing::warn!(%error, "failed to clear session viewer startup frame");
        }
    }
    terminal_restore_guard.restore_silently();
}

async fn run_event_loop(
    tui: &mut Tui,
    app_server: &mut AppServerSession,
    thread_id: ThreadId,
    viewer: &mut SessionViewer,
    rollout_watcher: Option<LocalRolloutWatcher>,
) -> io::Result<()> {
    let mut events = tui.event_stream();
    let mut live_refresh = tokio::time::interval(LIVE_REFRESH_INTERVAL);
    live_refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    live_refresh.tick().await;
    tui.draw(u16::MAX, |frame| {
        viewer.viewport.render(frame.area(), frame.buffer);
    })?;
    let (rollout_tx, mut rollout_rx) = tokio::sync::mpsc::channel(1);
    let rollout_task = rollout_watcher.map(|mut watcher| {
        tokio::spawn(async move {
            let mut refresh = tokio::time::interval(ROLLOUT_REFRESH_INTERVAL);
            refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
            refresh.tick().await;
            loop {
                refresh.tick().await;
                match watcher.load_turns_if_changed().await {
                    Ok(Some(update)) => {
                        if rollout_tx.send(update).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => tracing::debug!(
                        path = %watcher.path.display(),
                        %error,
                        "session viewer rollout refresh failed"
                    ),
                }
            }
        })
    });
    let poll_live_turn = rollout_task.is_none();

    let result = loop {
        tokio::select! {
            event = events.next() => {
                let Some(event) = event else {
                    break Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "terminal input stream closed",
                    ));
                };
                match event {
                    TuiEvent::Key(key) if viewer.viewport.handle_key(key) => break Ok(()),
                    TuiEvent::Key(_) => {
                        tui.frame_requester().schedule_frame();
                    }
                    TuiEvent::Draw | TuiEvent::Resize(_) | TuiEvent::Resume => {
                        let draw_result = tui.draw(u16::MAX, |frame| {
                            viewer.viewport.render(frame.area(), frame.buffer);
                        });
                        if let Err(error) = draw_result {
                            break Err(error);
                        }
                    }
                    TuiEvent::Paste(_) => {}
                }
            }
            _ = live_refresh.tick(), if poll_live_turn => {
                match app_server.latest_thread_turn(thread_id).await {
                    Ok(Some(turn)) => {
                        if viewer.merge_latest_turn(turn) {
                            tui.frame_requester().schedule_frame();
                        }
                    }
                    Ok(None) => {}
                    Err(error) => tracing::debug!(%error, "session viewer live refresh failed"),
                }
            }
            update = rollout_rx.recv(), if rollout_task.is_some() => {
                match update {
                    Some(LocalRolloutUpdate::Replace(turns)) => {
                        if viewer.replace_turns(turns) {
                            tui.frame_requester().schedule_frame();
                        }
                    }
                    Some(LocalRolloutUpdate::Changes(changes)) => {
                        if viewer.apply_history_changes(changes) {
                            tui.frame_requester().schedule_frame();
                        }
                    }
                    None => break Err(io::Error::other(
                        "session viewer rollout watcher stopped unexpectedly",
                    )),
                }
            }
        }
    };
    if let Some(task) = rollout_task {
        task.abort();
    }
    result
}

#[cfg(test)]
#[path = "session_viewer_tests.rs"]
mod tests;
