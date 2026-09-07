//! Native Codex history-cell reconstruction for the standalone viewer.

use std::sync::Arc;
use std::time::Duration;

use codex_app_server_protocol::CommandExecutionSource;
use codex_app_server_protocol::CommandExecutionStatus;
use codex_app_server_protocol::McpToolCallStatus;
use codex_app_server_protocol::PatchApplyStatus;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_protocol::ThreadId;

use crate::app_server_approval_conversions::file_update_changes_to_display;
use crate::exec_cell::CommandOutput;
use crate::exec_cell::ExecCell;
use crate::exec_cell::new_active_exec_command;
use crate::exec_command::split_command_string;
use crate::history_cell;
use crate::history_cell::HistoryCell;
use crate::history_cell::McpInvocation;
use crate::legacy_core::config::Config;
use crate::multi_agents::AgentMetadata;
use crate::thread_transcript::RawReasoningVisibility;
use crate::thread_transcript::TranscriptCells;
use crate::thread_transcript::thread_items_to_transcript_cells;

#[derive(Clone)]
pub(super) struct RenderedTurn {
    pub(super) cells: TranscriptCells,
    item_cell_starts: Vec<usize>,
}

pub(super) fn render_turn(thread: &Thread, turn: &Turn, config: &Config) -> RenderedTurn {
    let mut renderer = TurnTranscriptRenderer {
        thread,
        config,
        cells: Vec::new(),
        item_cell_starts: Vec::new(),
        pending_exec: None,
        pending_exec_start: None,
    };
    for item in &turn.items {
        renderer.push(item);
    }
    renderer.finish()
}

pub(super) fn rerender_turn_suffix(
    thread: &Thread,
    previous_turn: &Turn,
    previous_render: &RenderedTurn,
    turn: &Turn,
    config: &Config,
) -> RenderedTurn {
    let first_changed = previous_turn
        .items
        .iter()
        .zip(&turn.items)
        .position(|(previous, current)| previous != current)
        .unwrap_or_else(|| previous_turn.items.len().min(turn.items.len()));
    if first_changed == previous_turn.items.len() && first_changed == turn.items.len() {
        return previous_render.clone();
    }

    let mut start_item = first_changed;
    let changed_item_is_command = turn
        .items
        .get(start_item)
        .or_else(|| previous_turn.items.get(start_item))
        .is_some_and(is_command_execution);
    if changed_item_is_command {
        while start_item > 0
            && turn
                .items
                .get(start_item - 1)
                .or_else(|| previous_turn.items.get(start_item - 1))
                .is_some_and(is_command_execution)
        {
            start_item -= 1;
        }
    }

    let prefix_cell_count = previous_render
        .item_cell_starts
        .get(start_item)
        .copied()
        .unwrap_or(previous_render.cells.len());
    let mut renderer = TurnTranscriptRenderer {
        thread,
        config,
        cells: Vec::new(),
        item_cell_starts: Vec::new(),
        pending_exec: None,
        pending_exec_start: None,
    };
    for item in &turn.items[start_item..] {
        renderer.push(item);
    }
    let suffix = renderer.finish();
    let mut cells = previous_render.cells[..prefix_cell_count].to_vec();
    cells.extend(suffix.cells);
    let mut item_cell_starts = previous_render.item_cell_starts[..start_item].to_vec();
    item_cell_starts.extend(
        suffix
            .item_cell_starts
            .into_iter()
            .map(|start| prefix_cell_count.saturating_add(start)),
    );
    RenderedTurn {
        cells,
        item_cell_starts,
    }
}

fn is_command_execution(item: &ThreadItem) -> bool {
    matches!(item, ThreadItem::CommandExecution { .. })
}

struct TurnTranscriptRenderer<'a> {
    thread: &'a Thread,
    config: &'a Config,
    cells: TranscriptCells,
    item_cell_starts: Vec<usize>,
    pending_exec: Option<ExecCell>,
    pending_exec_start: Option<usize>,
}

impl TurnTranscriptRenderer<'_> {
    fn push(&mut self, item: &ThreadItem) {
        match item {
            ThreadItem::CommandExecution { .. } => {
                let start = self.push_command(item);
                self.item_cell_starts.push(start);
            }
            ThreadItem::FileChange {
                changes, status, ..
            } => {
                self.flush_exec();
                self.item_cell_starts.push(self.cells.len());
                self.cells.push(Arc::new(history_cell::new_patch_event(
                    file_update_changes_to_display(changes.clone()),
                    self.thread.cwd.as_path(),
                )));
                if matches!(status, PatchApplyStatus::Failed) {
                    self.cells
                        .push(Arc::new(history_cell::new_patch_apply_failure(
                            String::new(),
                        )));
                }
            }
            ThreadItem::McpToolCall { .. } => {
                self.flush_exec();
                self.item_cell_starts.push(self.cells.len());
                self.push_mcp(item);
            }
            ThreadItem::WebSearch(item) => {
                self.flush_exec();
                self.item_cell_starts.push(self.cells.len());
                self.cells.push(Arc::new(history_cell::new_web_search_call(
                    item.id.clone(),
                    item.query.clone(),
                    item.action
                        .clone()
                        .unwrap_or(codex_app_server_protocol::WebSearchAction::Other),
                )));
            }
            ThreadItem::ImageView { path, .. } => {
                self.flush_exec();
                self.item_cell_starts.push(self.cells.len());
                self.cells
                    .push(Arc::new(history_cell::new_view_image_tool_call(
                        path.clone(),
                        self.thread.cwd.as_path(),
                    )));
            }
            ThreadItem::ImageGeneration(item) => {
                self.flush_exec();
                self.item_cell_starts.push(self.cells.len());
                self.cells
                    .push(Arc::new(history_cell::new_image_generation_call(
                        item.id.clone(),
                        &item.status,
                        item.revised_prompt.clone(),
                        item.saved_path.clone(),
                    )));
            }
            item @ ThreadItem::CollabAgentToolCall { .. } => {
                self.flush_exec();
                self.item_cell_starts.push(self.cells.len());
                if let Some(cell) = crate::multi_agents::tool_call_history_cell(
                    item,
                    /*cached_spawn_request*/ None,
                    |_| AgentMetadata::default(),
                ) {
                    self.cells.push(Arc::new(cell));
                }
            }
            item @ ThreadItem::SubAgentActivity { .. } => {
                self.flush_exec();
                self.item_cell_starts.push(self.cells.len());
                if let Some(cell) = crate::multi_agents::sub_agent_activity_history_cell(item) {
                    self.cells.push(Arc::new(cell));
                }
            }
            item @ (ThreadItem::UserMessage { .. }
            | ThreadItem::HookPrompt { .. }
            | ThreadItem::AgentMessage { .. }
            | ThreadItem::FunctionCallOutput { .. }
            | ThreadItem::Plan { .. }
            | ThreadItem::Reasoning { .. }
            | ThreadItem::DynamicToolCall { .. }
            | ThreadItem::Sleep(_)
            | ThreadItem::EnteredReviewMode { .. }
            | ThreadItem::ExitedReviewMode { .. }
            | ThreadItem::ContextCompaction { .. }) => {
                self.flush_exec();
                self.item_cell_starts.push(self.cells.len());
                self.cells.extend(thread_items_to_transcript_cells(
                    ThreadId::from_string(&self.thread.id).ok(),
                    &self.thread.cwd,
                    std::iter::once(item.clone()),
                    raw_reasoning_visibility(self.config),
                    Some(self.config),
                ));
            }
        }
    }

    fn push_command(&mut self, item: &ThreadItem) -> usize {
        let ThreadItem::CommandExecution {
            id,
            command,
            source,
            status,
            command_actions,
            aggregated_output,
            exit_code,
            duration_ms,
            ..
        } = item
        else {
            return self.cells.len();
        };
        let command = split_command_string(command);
        let parsed = command_actions
            .iter()
            .cloned()
            .map(codex_app_server_protocol::CommandAction::into_core)
            .collect::<Vec<_>>();
        let added_to_group = self.pending_exec.as_mut().is_some_and(|cell| {
            cell.add_call(
                id.clone(),
                command.clone(),
                parsed.clone(),
                *source,
                /*interaction_input*/ None,
            )
        });
        let start = if added_to_group {
            self.pending_exec_start.unwrap_or(self.cells.len())
        } else {
            self.flush_exec();
            let start = self.cells.len();
            self.pending_exec = Some(new_active_exec_command(
                id.clone(),
                command,
                parsed,
                *source,
                /*interaction_input*/ None,
                self.config.animations,
            ));
            self.pending_exec_start = Some(start);
            start
        };

        if !matches!(status, CommandExecutionStatus::InProgress) {
            let exit_code = match status {
                CommandExecutionStatus::Completed => exit_code.unwrap_or_default(),
                CommandExecutionStatus::Failed | CommandExecutionStatus::Declined => {
                    exit_code.filter(|code| *code != 0).unwrap_or(1)
                }
                CommandExecutionStatus::InProgress => unreachable!(),
            };
            let output = if matches!(source, CommandExecutionSource::UnifiedExecInteraction) {
                String::new()
            } else {
                aggregated_output.clone().unwrap_or_default()
            };
            let duration = Duration::from_millis(duration_ms.unwrap_or_default().max(0) as u64);
            let completed = self.pending_exec.as_mut().is_some_and(|cell| {
                cell.complete_call(id, CommandOutput::new(exit_code, output), duration)
            });
            debug_assert!(completed, "newly rendered exec cell should contain {id}");
            if self
                .pending_exec
                .as_ref()
                .is_some_and(ExecCell::should_flush)
            {
                self.flush_exec();
            }
        }
        start
    }

    fn push_mcp(&mut self, item: &ThreadItem) {
        let ThreadItem::McpToolCall {
            id,
            server,
            tool,
            status,
            arguments,
            result,
            error,
            duration_ms,
            ..
        } = item
        else {
            return;
        };
        let mut cell = history_cell::new_active_mcp_tool_call(
            id.clone(),
            McpInvocation {
                server: server.clone(),
                tool: tool.clone(),
                arguments: Some(arguments.clone()),
            },
            self.config.animations,
        );
        let extra = if !matches!(status, McpToolCallStatus::InProgress) {
            let duration = Duration::from_millis(duration_ms.unwrap_or_default().max(0) as u64);
            let result = match (result.as_deref(), error.as_ref()) {
                (_, Some(error)) => Err(error.message.clone()),
                (Some(result), None) => Ok(codex_protocol::mcp::CallToolResult {
                    content: result.content.clone(),
                    structured_content: result.structured_content.clone(),
                    is_error: Some(false),
                    meta: None,
                }),
                (None, None) => Err("MCP tool call completed without a result".to_string()),
            };
            cell.complete(duration, result)
        } else {
            None
        };
        self.cells.push(Arc::new(cell));
        if let Some(extra) = extra {
            self.cells.push(Arc::<dyn HistoryCell>::from(extra));
        }
    }

    fn flush_exec(&mut self) {
        if let Some(cell) = self.pending_exec.take() {
            self.cells.push(Arc::new(cell));
        }
        self.pending_exec_start = None;
    }

    fn finish(mut self) -> RenderedTurn {
        self.flush_exec();
        RenderedTurn {
            cells: self.cells,
            item_cell_starts: self.item_cell_starts,
        }
    }
}

fn raw_reasoning_visibility(config: &Config) -> RawReasoningVisibility {
    if config.show_raw_agent_reasoning {
        RawReasoningVisibility::Visible
    } else {
        RawReasoningVisibility::Hidden
    }
}
