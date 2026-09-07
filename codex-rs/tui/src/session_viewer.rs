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
use crate::legacy_core::config::Config;
use crate::thread_transcript::load_session_thread;
use crate::token_usage::TokenUsage;
use crate::tui::Tui;
use crate::tui::TuiEvent;

use self::rollout_watcher::LocalRolloutUpdate;
use self::rollout_watcher::LocalRolloutWatcher;
use self::viewer_state::PreparedTurnReplacement;
use self::viewer_state::SessionViewer;
use self::viewer_state::prepare_initial_viewer;
use self::viewer_state::prepare_turn_replacement;

mod rollout_watcher;
mod transcript;
mod viewer_state;
mod viewport;
mod voice_transcript;
mod voice_viewer;

const LIVE_REFRESH_INTERVAL: Duration = Duration::from_millis(250);
const ROLLOUT_REFRESH_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct SessionViewerOptions {
    pub session_id: Option<String>,
    pub voice_jsonl: Option<std::path::PathBuf>,
    pub follow: bool,
    pub no_alt_screen: bool,
    pub config_overrides: CliConfigOverrides,
}

/// Launches the standalone viewer without exposing it as a `codex` subcommand.
pub async fn run_session_viewer(
    options: SessionViewerOptions,
    arg0_paths: Arg0DispatchPaths,
    loader_overrides: LoaderOverrides,
) -> io::Result<AppExitInfo> {
    if let Some(path) = options.voice_jsonl.clone() {
        return voice_viewer::run(path, options).await;
    }
    let mut cli = crate::Cli::try_parse_from(["codex-viewer"]).map_err(io::Error::other)?;
    cli.view_session_id = options.session_id;
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

enum ViewerUpdate {
    Backfill {
        replacement: PreparedTurnReplacement,
        rollout_offset: u64,
    },
    Replace {
        replacement: PreparedTurnReplacement,
        rollout_offset: u64,
    },
    Changes {
        changes: Vec<ThreadHistoryChangeSet>,
        rollout_offset: u64,
    },
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
    if let Some(watcher) = rollout_watcher.as_mut() {
        match watcher.load_recent_turn().await {
            Ok(turns) => {
                thread.turns = turns;
            }
            Err(error) => {
                tracing::debug!(
                    path = %watcher.path.display(),
                    %error,
                    "session viewer recent rollout load failed"
                );
            }
        }
        return Ok((thread, rollout_watcher));
    }

    thread = load_session_thread(app_server, thread_id).await?;
    if let Some(turn) = app_server
        .latest_thread_turn(thread_id)
        .await
        .map_err(io::Error::other)?
    {
        merge_latest_turn(&mut thread.turns, turn);
    }
    Ok((thread, rollout_watcher))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_app(
    cli: crate::Cli,
    arg0_paths: Arg0DispatchPaths,
    loader_overrides: LoaderOverrides,
    strict_config: bool,
    mut app_server_target: AppServerTarget,
    remote_cwd_override: Option<std::path::PathBuf>,
    config: Config,
    cli_kv_overrides: Vec<(String, toml::Value)>,
    cloud_config_bundle: CloudConfigBundleLoader,
    feedback: codex_feedback::CodexFeedback,
    log_db: Option<log_db::LogDbLayer>,
    mut state_db: Option<StateDbHandle>,
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
                &mut app_server_target,
                arg0_paths,
                config.clone(),
                cli_kv_overrides,
                loader_overrides,
                strict_config,
                cloud_config_bundle,
                feedback,
                log_db,
                &mut state_db,
                environment_manager,
            ),
        )
        .await??;
    let mut app_server = AppServerSession::new(app_server, app_server_target.thread_params_mode())
        .with_local_codex_home(&config.codex_home)
        .with_remote_cwd_override(remote_cwd_override);
    let loaded = startup_draft
        .run_until(&mut tui, load_initial_thread(&mut app_server, thread_id))
        .await;
    let (thread, rollout_watcher) = match loaded {
        Ok(Ok(loaded)) => loaded,
        Ok(Err(error)) => {
            drop(startup_draft);
            cleanup_viewer(
                &mut tui,
                app_server,
                /*mouse_capture_enabled*/ false,
                &mut terminal_restore_guard,
            )
            .await;
            return Ok(AppExitInfo::fatal(format!(
                "No saved session found with ID {session_id}: {error}"
            )));
        }
        Err(error) => {
            drop(startup_draft);
            cleanup_viewer(
                &mut tui,
                app_server,
                /*mouse_capture_enabled*/ false,
                &mut terminal_restore_guard,
            )
            .await;
            return Err(error.into());
        }
    };
    let initial_width = tui
        .terminal
        .size()
        .map(|size| size.width.max(1))
        .unwrap_or(/*default*/ 80);
    let prepared_viewer = startup_draft
        .run_until(
            &mut tui,
            prepare_initial_viewer(thread, config, initial_width),
        )
        .await;
    let mut viewer = match prepared_viewer {
        Ok(Ok(viewer)) => viewer,
        Ok(Err(error)) => {
            drop(startup_draft);
            cleanup_viewer(
                &mut tui,
                app_server,
                /*mouse_capture_enabled*/ false,
                &mut terminal_restore_guard,
            )
            .await;
            return Err(error);
        }
        Err(error) => {
            drop(startup_draft);
            cleanup_viewer(
                &mut tui,
                app_server,
                /*mouse_capture_enabled*/ false,
                &mut terminal_restore_guard,
            )
            .await;
            return Err(error.into());
        }
    };
    drop(startup_draft);

    tui.set_alt_screen_enabled(crate::determine_alt_screen_mode(
        cli.no_alt_screen,
        viewer.config.tui_alternate_screen,
    ));
    let mut mouse_capture_enabled = false;
    let setup_result = tui.enter_alt_screen().and_then(|()| {
        if tui.is_alt_screen_active() {
            set_mouse_capture(&mut tui, /*enabled*/ true)?;
            mouse_capture_enabled = true;
        }
        tui.draw(u16::MAX, |frame| {
            viewer.viewport.render(frame.area(), frame.buffer);
        })
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
        disconnect_info: None,
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
    let mut rollout_tasks = Vec::new();
    if let Some(mut watcher) = rollout_watcher {
        let mut full_watcher = watcher.fresh();
        let full_tx = rollout_tx.clone();
        let full_thread = viewer.thread.clone();
        let full_config = viewer.config.clone();
        let full_width = tui.terminal.size()?.width.max(1);
        rollout_tasks.push(tokio::spawn(async move {
            match full_watcher.load_turns_if_changed().await {
                Ok(Some(LocalRolloutUpdate::Replace(turns))) => {
                    let rollout_offset = full_watcher.offset;
                    let replacement = tokio::task::spawn_blocking(move || {
                        prepare_turn_replacement(&full_thread, &full_config, turns, full_width)
                    })
                    .await;
                    match replacement {
                        Ok(replacement) => {
                            let _ = full_tx
                                .send(ViewerUpdate::Backfill {
                                    replacement,
                                    rollout_offset,
                                })
                                .await;
                        }
                        Err(error) => tracing::debug!(
                            %error,
                            "session viewer full history rendering failed"
                        ),
                    }
                }
                Ok(Some(LocalRolloutUpdate::Changes(_))) => tracing::debug!(
                    "session viewer full history load unexpectedly produced changes"
                ),
                Ok(None) => {}
                Err(error) => tracing::debug!(
                    path = %full_watcher.path.display(),
                    %error,
                    "session viewer full history load failed"
                ),
            }
        }));

        let live_thread = viewer.thread.clone();
        let live_config = viewer.config.clone();
        let live_width = full_width;
        let live_tx = rollout_tx.clone();
        rollout_tasks.push(tokio::spawn(async move {
            let mut refresh = tokio::time::interval(ROLLOUT_REFRESH_INTERVAL);
            refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
            refresh.tick().await;
            loop {
                refresh.tick().await;
                match watcher.load_turns_if_changed().await {
                    Ok(Some(LocalRolloutUpdate::Replace(turns))) => {
                        let rollout_offset = watcher.offset;
                        let thread = live_thread.clone();
                        let config = live_config.clone();
                        let replacement = tokio::task::spawn_blocking(move || {
                            prepare_turn_replacement(&thread, &config, turns, live_width)
                        })
                        .await;
                        let replacement = match replacement {
                            Ok(replacement) => replacement,
                            Err(error) => {
                                tracing::debug!(
                                    %error,
                                    "session viewer replacement rendering failed"
                                );
                                continue;
                            }
                        };
                        if live_tx
                            .send(ViewerUpdate::Replace {
                                replacement,
                                rollout_offset,
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(Some(LocalRolloutUpdate::Changes(changes))) => {
                        let rollout_offset = watcher.offset;
                        if live_tx
                            .send(ViewerUpdate::Changes {
                                changes,
                                rollout_offset,
                            })
                            .await
                            .is_err()
                        {
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
        }));
    }
    drop(rollout_tx);
    let poll_live_turn = rollout_tasks.is_empty();

    let result = loop {
        tokio::select! {
            biased;
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
                    TuiEvent::Paste(_) | TuiEvent::FocusGained | TuiEvent::FocusLost => {}
                }
            }
            _ = live_refresh.tick(), if poll_live_turn && viewer.viewport.is_following() => {
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
            update = rollout_rx.recv(), if !rollout_tasks.is_empty() && viewer.viewport.is_following() => {
                match update {
                    Some(ViewerUpdate::Backfill {
                        replacement,
                        rollout_offset,
                    }) => {
                        if viewer.backfill_prepared_turns(replacement, rollout_offset) {
                            tui.frame_requester().schedule_frame();
                        }
                    }
                    Some(ViewerUpdate::Replace {
                        replacement,
                        rollout_offset,
                    }) => {
                        viewer.rollout_offset = rollout_offset;
                        if viewer.install_prepared_turns(replacement) {
                            tui.frame_requester().schedule_frame();
                        }
                    }
                    Some(ViewerUpdate::Changes {
                        changes,
                        rollout_offset,
                    }) => {
                        if viewer.apply_history_changes(changes, rollout_offset) {
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
    for task in rollout_tasks {
        task.abort();
    }
    result
}

#[cfg(test)]
#[path = "session_viewer_tests.rs"]
mod tests;
