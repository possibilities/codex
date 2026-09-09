use super::*;

use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::build_turns_from_rollout_items;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::ThreadHistoryMode as CoreThreadHistoryMode;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyModifiers;
use insta::assert_snapshot;
use pretty_assertions::assert_eq;
use ratatui::style::Color;
use ratatui::style::Stylize;
use ratatui::text::Line;
use std::io::Write as _;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::history_cell::AgentMarkdownCell;
use crate::history_cell::HistoryCell;
use crate::history_cell::UserHistoryCell;
use crate::keymap::RuntimeKeymap;
use crate::legacy_core::config::ConfigBuilder;
use crate::thread_transcript::TranscriptCells;

use super::transcript::render_turn;
use super::transcript::rerender_turn_suffix;
use super::viewport::ConversationViewport;

#[derive(Debug)]
struct TestCell(String);

#[derive(Debug)]
struct CountingCell {
    text: String,
    height_calls: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct DiffStyleCell;

impl HistoryCell for TestCell {
    fn display_lines(&self, _width: u16) -> Vec<Line<'static>> {
        vec![self.0.clone().into()]
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        vec![self.0.clone().into()]
    }
}

impl HistoryCell for CountingCell {
    fn display_lines(&self, _width: u16) -> Vec<Line<'static>> {
        vec![self.text.clone().into()]
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        vec![self.text.clone().into()]
    }

    fn desired_height(&self, _width: u16) -> u16 {
        self.height_calls.fetch_add(1, Ordering::Relaxed);
        1
    }
}

impl HistoryCell for DiffStyleCell {
    fn display_lines(&self, _width: u16) -> Vec<Line<'static>> {
        vec![
            Line::from("+ added").green().on_blue(),
            Line::from("- removed").red().on_red(),
        ]
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        self.display_lines(/*width*/ 0)
    }
}

fn viewport(cells: TranscriptCells) -> ConversationViewport {
    ConversationViewport::new(cells, RuntimeKeymap::defaults().pager)
}

fn render(viewport: &mut ConversationViewport, width: u16, height: u16) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buffer = Buffer::empty(area);
    viewport.render(area, &mut buffer);
    (0..height)
        .map(|row| {
            (0..width)
                .map(|column| buffer[(column, row)].symbol())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string()
}

fn buffer_text(buffer: &Buffer, width: u16, height: u16) -> String {
    (0..height)
        .map(|row| {
            (0..width)
                .map(|column| buffer[(column, row)].symbol())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string()
}

fn buffer_background_map(buffer: &Buffer, width: u16, height: u16) -> String {
    (0..height)
        .map(|row| {
            (0..width)
                .map(|column| match buffer[(column, row)].bg {
                    Color::Blue => '+',
                    Color::Red => '-',
                    _ => '.',
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn write_rollout(path: &std::path::Path, items: Vec<RolloutItem>) {
    let contents = items
        .into_iter()
        .enumerate()
        .map(|(ordinal, item)| {
            serde_json::to_string(&RolloutLine {
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                ordinal: Some(ordinal as u64),
                item,
            })
            .expect("serialize rollout line")
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(path, contents).expect("write rollout");
}

fn turn(id: &str, text: &str) -> Turn {
    Turn {
        id: id.to_string(),
        items: vec![ThreadItem::AgentMessage {
            id: format!("message-{id}"),
            text: text.to_string(),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        }],
        items_view: TurnItemsView::Full,
        status: TurnStatus::InProgress,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
    }
}

#[test]
fn renders_only_the_conversation() {
    let cwd = std::env::current_dir().expect("current directory");
    let cells: TranscriptCells = vec![
        Arc::new(UserHistoryCell {
            spoken: false,
            message: "Show me the session without controls.".to_string(),
            text_elements: Vec::new(),
            local_image_paths: Vec::new(),
            remote_image_urls: Vec::new(),
        }),
        Arc::new(AgentMarkdownCell::new(
            "The viewer contains **only** conversation cells.".to_string(),
            &cwd,
        )),
    ];
    let mut viewport = viewport(cells);
    let height = viewport.content_height(/*width*/ 56) as u16;

    assert_snapshot!(
        "standalone_session_viewer_conversation_only",
        render(&mut viewport, /*width*/ 56, height)
    );
}

#[test]
fn renders_stable_loading_surface() {
    let width = 40;
    let height = 5;
    let area = Rect::new(0, 0, width, height);
    let mut buffer = Buffer::empty(area);

    render_loading(area, &mut buffer);

    assert_snapshot!(
        "standalone_session_viewer_loading",
        buffer_text(&buffer, width, height)
    );
}

#[tokio::test]
async fn renders_native_markdown_command_and_file_change_cells() {
    let temp_dir = tempfile::tempdir().expect("temporary Codex home");
    let config = ConfigBuilder::default()
        .codex_home(temp_dir.path().to_path_buf())
        .fallback_cwd(Some(temp_dir.path().to_path_buf()))
        .build()
        .await
        .expect("viewer config");
    let thread_id = ThreadId::new().to_string();
    let turn = Turn {
        id: "turn-native".to_string(),
        items: vec![
            ThreadItem::UserMessage {
                id: "user-native".to_string(),
                client_id: None,
                content: vec![codex_app_server_protocol::UserInput::Text {
                    text: "Make the session viewer feel like Codex.".to_string(),
                    text_elements: Vec::new(),
                }],
            },
            ThreadItem::CommandExecution {
                id: "command-native".to_string(),
                plugin_id: None,
                script_path: None,
                command: "cargo check -p codex-tui".to_string(),
                cwd: config.cwd.clone().into(),
                process_id: None,
                source: codex_app_server_protocol::CommandExecutionSource::Agent,
                status: codex_app_server_protocol::CommandExecutionStatus::Completed,
                command_actions: Vec::new(),
                aggregated_output: Some("Finished dev profile\n".to_string()),
                exit_code: Some(0),
                duration_ms: Some(1_250),
            },
            ThreadItem::FileChange {
                id: "patch-native".to_string(),
                changes: vec![codex_app_server_protocol::FileUpdateChange {
                    path: "tui/src/session_viewer.rs".to_string(),
                    kind: codex_app_server_protocol::PatchChangeKind::Update { move_path: None },
                    diff: "@@ -1 +1 @@\n-old viewer\n+native viewer\n".to_string(),
                }],
                status: codex_app_server_protocol::PatchApplyStatus::Completed,
            },
            ThreadItem::AgentMessage {
                id: "agent-native".to_string(),
                text: "Implemented **read-only** viewing with native history cells.".to_string(),
                phase: Some(codex_protocol::models::MessagePhase::FinalAnswer),
                memory_citation: None,
                delivery: None,
                questions: None,
            },
        ],
        items_view: TurnItemsView::Full,
        status: TurnStatus::Completed,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: Some(2_000),
    };
    let thread = Thread {
        id: thread_id.clone(),
        environments: None,
        model: None,
        reasoning_effort: None,
        originator: None,
        daybreak_enabled: None,
        extra: None,
        session_id: thread_id,
        forked_from_id: None,
        parent_thread_id: None,
        preview: "Make the session viewer feel like Codex.".to_string(),
        ephemeral: false,
        section: None,
        section_entered_at: None,
        project_id: None,
        history_mode: Default::default(),
        model_provider: "openai".to_string(),
        created_at: 1,
        updated_at: 2,
        recency_at: Some(2),
        status: codex_app_server_protocol::ThreadStatus::Idle,
        path: None,
        cwd: config.cwd.clone(),
        cli_version: "0.0.0".to_string(),
        source: codex_app_server_protocol::SessionSource::Unknown,
        can_accept_direct_input: None,
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        git_info: None,
        name: None,
        turns: vec![turn],
    };
    let cells = render_turn(&thread, &thread.turns[0], &config).cells;
    let mut native_viewport = viewport(cells);
    let height = native_viewport.content_height(/*width*/ 72) as u16;

    assert_snapshot!(
        "standalone_session_viewer_native_cells",
        render(&mut native_viewport, /*width*/ 72, height)
    );

    let mut previous_turn = thread.turns[0].clone();
    previous_turn.items.truncate(2);
    let previous_render = render_turn(&thread, &previous_turn, &config);
    let incremental_render = rerender_turn_suffix(
        &thread,
        &previous_turn,
        &previous_render,
        &thread.turns[0],
        &config,
    );
    let full_render = render_turn(&thread, &thread.turns[0], &config);
    assert!(
        previous_render
            .cells
            .iter()
            .zip(&incremental_render.cells)
            .all(|(previous, incremental)| Arc::ptr_eq(previous, incremental))
    );
    let mut incremental_viewport = viewport(incremental_render.cells);
    let mut full_viewport = viewport(full_render.cells);
    let full_rendered = render(&mut full_viewport, /*width*/ 72, height);
    assert_eq!(
        render(&mut incremental_viewport, /*width*/ 72, height),
        full_rendered,
    );

    let mut partial_thread = thread.clone();
    partial_thread.turns[0].items.drain(..2);
    let mut backfilled_viewer =
        SessionViewer::new(partial_thread, config.clone(), /*width*/ 72).expect("partial viewer");
    let replacement =
        prepare_turn_replacement(&thread, &config, thread.turns.clone(), /*width*/ 72);
    assert!(backfilled_viewer.backfill_prepared_turns(replacement, /*rollout_offset*/ 1,));
    assert_eq!(backfilled_viewer.thread, thread);
    assert_eq!(
        render(&mut backfilled_viewer.viewport, /*width*/ 72, height),
        full_rendered,
    );

    let mut newer_turn = thread.turns[0].clone();
    newer_turn.items.push(ThreadItem::AgentMessage {
        id: "agent-after-backfill".to_string(),
        text: "newer than the full snapshot".to_string(),
        phase: Some(codex_protocol::models::MessagePhase::Commentary),
        memory_citation: None,
        delivery: None,
        questions: None,
    });
    assert!(backfilled_viewer.merge_latest_turn(newer_turn.clone()));
    backfilled_viewer.rollout_offset = 2;
    let stale_replacement =
        prepare_turn_replacement(&thread, &config, thread.turns.clone(), /*width*/ 72);
    assert!(!backfilled_viewer.backfill_prepared_turns(stale_replacement, /*rollout_offset*/ 1,));
    assert_eq!(backfilled_viewer.thread.turns, vec![newer_turn]);
}

#[test]
fn diff_line_backgrounds_fill_the_viewer_width() {
    let width = 16;
    let height = 2;
    let area = Rect::new(0, 0, width, height);
    let mut buffer = Buffer::empty(area);
    let mut viewport = viewport(vec![Arc::new(DiffStyleCell)]);

    viewport.render(area, &mut buffer);

    assert_snapshot!(
        "standalone_session_viewer_diff_line_backgrounds",
        format!(
            "{}\n\nbackgrounds\n{}",
            buffer_text(&buffer, width, height),
            buffer_background_map(&buffer, width, height)
        )
    );
}

#[test]
fn live_updates_follow_only_while_scrolled_to_bottom() {
    let cells = (1..=6)
        .map(|index| Arc::new(TestCell(format!("message {index}"))) as Arc<dyn HistoryCell>)
        .collect();
    let mut viewport = viewport(cells);
    render(&mut viewport, /*width*/ 30, /*height*/ 4);
    let initial = (
        viewport.scroll_offset,
        viewport.max_scroll,
        viewport.follow_tail,
    );

    viewport.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
    render(&mut viewport, /*width*/ 30, /*height*/ 4);
    let manually_scrolled_offset = viewport.scroll_offset;
    viewport
        .cells
        .push(Arc::new(TestCell("new live message".to_string())));
    render(&mut viewport, /*width*/ 30, /*height*/ 4);
    let after_live_update = (viewport.scroll_offset, viewport.follow_tail);

    viewport.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    render(&mut viewport, /*width*/ 30, /*height*/ 4);
    let after_jump_bottom = (
        viewport.scroll_offset,
        viewport.max_scroll,
        viewport.follow_tail,
    );

    assert_eq!(
        (initial, after_live_update, after_jump_bottom),
        (
            (7, 7, true),
            (manually_scrolled_offset, false),
            (9, 9, true),
        )
    );
}

#[test]
fn unchanged_cells_keep_their_cached_layout_across_updates() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let appended_calls = Arc::new(AtomicUsize::new(0));
    let first = Arc::new(CountingCell {
        text: "first".to_string(),
        height_calls: Arc::clone(&first_calls),
    });
    let second = Arc::new(CountingCell {
        text: "second".to_string(),
        height_calls: Arc::clone(&second_calls),
    });
    let appended = Arc::new(CountingCell {
        text: "appended".to_string(),
        height_calls: Arc::clone(&appended_calls),
    });
    let mut viewport = viewport(vec![first.clone(), second.clone()]);

    viewport.content_height(/*width*/ 30);
    viewport.replace_cells(vec![first, second, appended]);
    viewport.content_height(/*width*/ 30);

    assert_eq!(
        (
            first_calls.load(Ordering::Relaxed),
            second_calls.load(Ordering::Relaxed),
            appended_calls.load(Ordering::Relaxed),
        ),
        (1, 1, 1)
    );
}

#[test]
fn prepared_layout_does_not_remeasure_stable_cells() {
    let height_calls = Arc::new(AtomicUsize::new(0));
    let cell = Arc::new(CountingCell {
        text: "prepared".to_string(),
        height_calls: Arc::clone(&height_calls),
    });
    let mut viewport = viewport(Vec::new());

    viewport.replace_cells_with_heights(vec![cell], /*width*/ 30, vec![Some(/*height*/ 1)]);
    viewport.content_height(/*width*/ 30);

    assert_eq!(height_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn latest_turn_replaces_in_place_without_duplicates() {
    let mut turns = vec![turn("turn-1", "partial")];
    let completed = turn("turn-1", "complete");

    assert!(merge_latest_turn(&mut turns, completed.clone()));
    assert!(!merge_latest_turn(&mut turns, completed.clone()));
    assert!(merge_latest_turn(&mut turns, turn("turn-2", "next")));
    assert_eq!(turns, vec![completed, turn("turn-2", "next")]);
}

#[tokio::test]
async fn local_rollout_watcher_only_reloads_changed_files() {
    let temp_dir = tempfile::tempdir().expect("temporary rollout directory");
    let path = temp_dir.path().join("rollout.jsonl");
    let turn_started = RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: "turn-live".to_string(),
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }));
    let user_message = RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: "watch this conversation".to_string(),
        ..Default::default()
    }));
    let initial_items = vec![turn_started.clone(), user_message.clone()];
    write_rollout(&path, initial_items.clone());
    let mut watcher = LocalRolloutWatcher::new(path.clone(), CoreThreadHistoryMode::Legacy);

    let initial = watcher
        .load_turns_if_changed()
        .await
        .expect("initial rollout load")
        .expect("initial turns");
    let LocalRolloutUpdate::Replace(mut initial) = initial else {
        panic!("initial watcher load should replace the transcript");
    };
    assert_eq!(initial, build_turns_from_rollout_items(&initial_items));
    assert_eq!(watcher.load_turns_if_changed().await.unwrap(), None);

    let agent_message = RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
        message: "new live reply".to_string(),
        phase: None,
        memory_citation: None,
        delivery: None,
        questions: None,
    }));
    let updated_items = vec![turn_started, user_message, agent_message];
    write_rollout(&path, updated_items.clone());
    let updated = watcher
        .load_turns_if_changed()
        .await
        .expect("updated rollout load")
        .expect("updated turns");
    let LocalRolloutUpdate::Changes(changes) = updated else {
        panic!("appended rollout items should produce deltas");
    };
    for change in changes {
        rollout_watcher::apply_changes(&mut initial, change);
    }

    assert_eq!(initial, build_turns_from_rollout_items(&updated_items));
}

#[tokio::test]
async fn local_rollout_watcher_seeds_from_only_the_latest_turn() {
    let temp_dir = tempfile::tempdir().expect("temporary rollout directory");
    let path = temp_dir.path().join("rollout.jsonl");
    let first_started = RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: "turn-old".to_string(),
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }));
    let first_message = RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: "older conversation".to_string(),
        ..Default::default()
    }));
    let latest_started = RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: "turn-latest".to_string(),
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }));
    let latest_message = RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: "latest conversation".to_string(),
        ..Default::default()
    }));
    let recent_items = vec![latest_started.clone(), latest_message.clone()];
    let initial_items = vec![
        first_started,
        first_message,
        latest_started.clone(),
        latest_message.clone(),
    ];
    write_rollout(&path, initial_items.clone());
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open rollout for partial append")
        .write_all(b"{\"timestamp\":")
        .expect("append partial rollout record");
    let mut watcher = LocalRolloutWatcher::new(path.clone(), CoreThreadHistoryMode::Legacy);

    let mut turns = watcher
        .load_recent_turn()
        .await
        .expect("load recent rollout turn");
    assert_eq!(turns, build_turns_from_rollout_items(&recent_items));

    let agent_message = RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
        message: "followed live".to_string(),
        phase: None,
        memory_citation: None,
        delivery: None,
        questions: None,
    }));
    let mut updated_items = initial_items;
    updated_items.push(agent_message.clone());
    write_rollout(&path, updated_items);
    let update = watcher
        .load_turns_if_changed()
        .await
        .expect("load appended rollout items")
        .expect("live rollout update");
    let LocalRolloutUpdate::Changes(changes) = update else {
        panic!("recently seeded watcher should process only appended changes");
    };
    for change in changes {
        rollout_watcher::apply_changes(&mut turns, change);
    }

    assert_eq!(
        turns,
        build_turns_from_rollout_items(
            &recent_items
                .into_iter()
                .chain(std::iter::once(agent_message))
                .collect::<Vec<_>>()
        )
    );
}

#[tokio::test]
async fn local_rollout_watcher_bounds_the_initial_active_turn() {
    let temp_dir = tempfile::tempdir().expect("temporary rollout directory");
    let path = temp_dir.path().join("rollout.jsonl");
    let mut items = vec![RolloutItem::EventMsg(EventMsg::TurnStarted(
        TurnStartedEvent {
            turn_id: "turn-large".to_string(),
            trace_id: None,
            started_at: None,
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        },
    ))];
    items.extend(
        (0..rollout_watcher::INITIAL_TURN_ITEM_LIMIT + 3).map(|index| {
            RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                message: format!("message {index}"),
                phase: None,
                memory_citation: None,
                delivery: None,
                questions: None,
            }))
        }),
    );
    write_rollout(&path, items.clone());
    let mut expected = build_turns_from_rollout_items(&items);
    let remove_count = expected[0]
        .items
        .len()
        .saturating_sub(rollout_watcher::INITIAL_TURN_ITEM_LIMIT);
    expected[0].items.drain(..remove_count);
    let mut watcher = LocalRolloutWatcher::new(path, CoreThreadHistoryMode::Legacy);

    let turns = watcher
        .load_recent_turn()
        .await
        .expect("load bounded recent rollout turn");

    assert_eq!(turns, expected);
}

#[tokio::test]
async fn paginated_rollout_watcher_projects_appended_agent_messages() {
    let temp_dir = tempfile::tempdir().expect("temporary rollout directory");
    let path = temp_dir.path().join("rollout.jsonl");
    let thread_id = ThreadId::new();
    let turn_started = RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: "turn-live".to_string(),
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }));
    write_rollout(&path, vec![turn_started.clone()]);
    let mut watcher = LocalRolloutWatcher::new(path.clone(), CoreThreadHistoryMode::Paginated);

    let initial = watcher
        .load_turns_if_changed()
        .await
        .expect("initial rollout load")
        .expect("initial turns");
    let LocalRolloutUpdate::Replace(mut turns) = initial else {
        panic!("initial watcher load should replace the transcript");
    };

    let agent_message =
        codex_protocol::items::TurnItem::AgentMessage(codex_protocol::items::AgentMessageItem {
            id: "message-live".to_string(),
            content: vec![codex_protocol::items::AgentMessageContent::Text {
                text: "new live commentary".to_string(),
            }],
            phase: Some(codex_protocol::models::MessagePhase::Commentary),
            memory_citation: None,
            delivery: None,
            questions: None,
        });
    let completed = RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: "turn-live".to_string(),
        item: agent_message,
        started_at_ms: Some(10),
        completed_at_ms: 20,
    }));
    write_rollout(&path, vec![turn_started, completed]);

    let updated = watcher
        .load_turns_if_changed()
        .await
        .expect("updated rollout load")
        .expect("updated turns");
    let LocalRolloutUpdate::Changes(changes) = updated else {
        panic!("appended rollout items should produce deltas");
    };
    for change in changes {
        rollout_watcher::apply_changes(&mut turns, change);
    }

    assert_eq!(
        turns[0].items,
        vec![ThreadItem::AgentMessage {
            id: "message-live".to_string(),
            text: "new live commentary".to_string(),
            phase: Some(codex_protocol::models::MessagePhase::Commentary),
            memory_citation: None,
            delivery: None,
            questions: None,
        }]
    );
}
