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

pub(super) fn turn_to_transcript_cells(
    thread: &Thread,
    turn: &Turn,
    config: &Config,
) -> TranscriptCells {
    let mut renderer = TurnTranscriptRenderer {
        thread,
        config,
        cells: Vec::new(),
        pending_exec: None,
    };
    for item in &turn.items {
        renderer.push(item);
    }
    renderer.finish()
}

struct TurnTranscriptRenderer<'a> {
    thread: &'a Thread,
    config: &'a Config,
    cells: TranscriptCells,
    pending_exec: Option<ExecCell>,
}

impl TurnTranscriptRenderer<'_> {
    fn push(&mut self, item: &ThreadItem) {
        match item {
            ThreadItem::CommandExecution { .. } => self.push_command(item),
            ThreadItem::FileChange {
                changes, status, ..
            } => {
                self.flush_exec();
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
                self.push_mcp(item);
            }
            ThreadItem::WebSearch(item) => {
                self.flush_exec();
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
                self.cells
                    .push(Arc::new(history_cell::new_view_image_tool_call(
                        path.clone(),
                        self.thread.cwd.as_path(),
                    )));
            }
            ThreadItem::ImageGeneration(item) => {
                self.flush_exec();
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
                if let Some(cell) = crate::multi_agents::sub_agent_activity_history_cell(item) {
                    self.cells.push(Arc::new(cell));
                }
            }
            item @ (ThreadItem::UserMessage { .. }
            | ThreadItem::HookPrompt { .. }
            | ThreadItem::AgentMessage { .. }
            | ThreadItem::Plan { .. }
            | ThreadItem::Reasoning { .. }
            | ThreadItem::DynamicToolCall { .. }
            | ThreadItem::Sleep(_)
            | ThreadItem::EnteredReviewMode { .. }
            | ThreadItem::ExitedReviewMode { .. }
            | ThreadItem::ContextCompaction { .. }) => {
                self.flush_exec();
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

    fn push_command(&mut self, item: &ThreadItem) {
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
            return;
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
        if !added_to_group {
            self.flush_exec();
            self.pending_exec = Some(new_active_exec_command(
                id.clone(),
                command,
                parsed,
                *source,
                /*interaction_input*/ None,
                self.config.animations,
            ));
        }

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
    }

    fn finish(mut self) -> TranscriptCells {
        self.flush_exec();
        self.cells
    }
}

fn raw_reasoning_visibility(config: &Config) -> RawReasoningVisibility {
    if config.show_raw_agent_reasoning {
        RawReasoningVisibility::Visible
    } else {
        RawReasoningVisibility::Hidden
    }
}
