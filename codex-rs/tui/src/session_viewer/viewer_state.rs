use std::time::Instant;

use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::Turn;
use color_eyre::eyre::Result;

use crate::keymap::RuntimeKeymap;
use crate::legacy_core::config::Config;
use crate::thread_transcript::RawReasoningVisibility;
use crate::thread_transcript::TranscriptCells;
use crate::thread_transcript::thread_to_transcript_cells;

use super::transcript::RenderedTurn;
use super::transcript::render_turn;
use super::transcript::rerender_turn_suffix;
use super::viewport::ConversationViewport;

pub(super) struct SessionViewer {
    pub(super) thread: Thread,
    pub(super) config: Config,
    rendered_turns: Vec<RenderedTurn>,
    pub(super) viewport: ConversationViewport,
    pub(super) rollout_offset: u64,
}

pub(super) struct PreparedTurnReplacement {
    turns: Vec<Turn>,
    rendered_turns: Vec<RenderedTurn>,
    cell_heights: Vec<Vec<Option<u16>>>,
    width: u16,
}

impl SessionViewer {
    pub(super) fn new(mut thread: Thread, config: Config, width: u16) -> Result<Self> {
        let keymap = RuntimeKeymap::from_config(&config.tui_keymap)
            .map_err(color_eyre::eyre::Report::msg)?
            .pager;
        let turns = std::mem::take(&mut thread.turns);
        let replacement = prepare_turn_replacement(&thread, &config, turns, width);
        thread.turns = replacement.turns;
        let rendered_turns = replacement.rendered_turns;
        let cells = flattened_transcript_cells(&thread, &config, &rendered_turns);
        let heights =
            flattened_prepared_heights(&cells, replacement.cell_heights, replacement.width);
        let mut viewport = ConversationViewport::new(Vec::new(), keymap);
        viewport.replace_cells_with_heights(cells, replacement.width, heights);
        Ok(Self {
            thread,
            config,
            rendered_turns,
            viewport,
            rollout_offset: 0,
        })
    }

    pub(super) fn install_prepared_turns(&mut self, replacement: PreparedTurnReplacement) -> bool {
        let visible_change =
            self.thread.turns.len() != replacement.turns.len()
                || self.thread.turns.iter().zip(&replacement.turns).any(
                    |(existing, replacement)| {
                        existing.id != replacement.id || existing.items != replacement.items
                    },
                );
        if !visible_change {
            self.thread.turns = replacement.turns;
            return false;
        }

        self.thread.turns = replacement.turns;
        self.rendered_turns = replacement.rendered_turns;
        let cells = flattened_transcript_cells(&self.thread, &self.config, &self.rendered_turns);
        let heights =
            flattened_prepared_heights(&cells, replacement.cell_heights, replacement.width);
        self.viewport
            .replace_cells_with_heights(cells, replacement.width, heights);
        true
    }

    pub(super) fn backfill_prepared_turns(
        &mut self,
        mut replacement: PreparedTurnReplacement,
        rollout_offset: u64,
    ) -> bool {
        if rollout_offset >= self.rollout_offset {
            self.rollout_offset = rollout_offset;
            return self.install_prepared_turns(replacement);
        }
        for (existing_index, existing_turn) in self.thread.turns.iter().enumerate() {
            if let Some(replacement_index) = replacement
                .turns
                .iter()
                .position(|turn| turn.id == existing_turn.id)
            {
                let previous_turn = replacement.turns[replacement_index].clone();
                let previous_render = replacement.rendered_turns[replacement_index].clone();
                let mut merged_turn = previous_turn.clone();
                let mut live_overlay = false;
                for item in &existing_turn.items {
                    if let Some(replacement_item) = merged_turn
                        .items
                        .iter_mut()
                        .find(|replacement| replacement.id() == item.id())
                    {
                        if replacement_item != item {
                            *replacement_item = item.clone();
                            live_overlay = true;
                        }
                    } else {
                        merged_turn.items.push(item.clone());
                        live_overlay = true;
                    }
                }
                if live_overlay {
                    merged_turn.status = existing_turn.status.clone();
                    merged_turn.error = existing_turn.error.clone();
                    merged_turn.started_at = existing_turn.started_at;
                    merged_turn.completed_at = existing_turn.completed_at;
                    merged_turn.duration_ms = existing_turn.duration_ms;
                    let merged_render = rerender_turn_suffix(
                        &self.thread,
                        &previous_turn,
                        &previous_render,
                        &merged_turn,
                        &self.config,
                    );
                    let reused_prefix = previous_render
                        .cells
                        .iter()
                        .zip(&merged_render.cells)
                        .take_while(|(previous, merged)| std::sync::Arc::ptr_eq(previous, merged))
                        .count();
                    replacement.cell_heights[replacement_index].truncate(reused_prefix);
                    replacement.cell_heights[replacement_index]
                        .resize(merged_render.cells.len(), None);
                    replacement.rendered_turns[replacement_index] = merged_render;
                    replacement.turns[replacement_index] = merged_turn;
                }
            } else {
                replacement.turns.push(existing_turn.clone());
                replacement
                    .rendered_turns
                    .push(self.rendered_turns[existing_index].clone());
                replacement
                    .cell_heights
                    .push(vec![None; self.rendered_turns[existing_index].cells.len()]);
            }
        }
        self.install_prepared_turns(replacement)
    }

    pub(super) fn merge_latest_turn(&mut self, turn: Turn) -> bool {
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
            self.rendered_turns[index] = rerender_turn_suffix(
                &self.thread,
                &previous_turn,
                &self.rendered_turns[index],
                &self.thread.turns[index],
                &self.config,
            );
        } else {
            let rendered = render_turn(&self.thread, &turn, &self.config);
            self.thread.turns.push(turn);
            self.rendered_turns.push(rendered);
        }
        self.viewport.replace_cells(flattened_transcript_cells(
            &self.thread,
            &self.config,
            &self.rendered_turns,
        ));
        true
    }

    pub(super) fn apply_history_changes(
        &mut self,
        changes: Vec<ThreadHistoryChangeSet>,
        rollout_offset: u64,
    ) -> bool {
        self.rollout_offset = rollout_offset;
        let mut visible_change = false;
        let mut previous_renders = Vec::new();
        for changes in changes {
            for turn_id in changes.removed_turn_ids {
                if let Some(index) = self.thread.turns.iter().position(|turn| turn.id == turn_id) {
                    self.thread.turns.remove(index);
                    self.rendered_turns.remove(index);
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
                    let index = self.thread.turns.len() - 1;
                    self.rendered_turns.push(render_turn(
                        &self.thread,
                        &self.thread.turns[index],
                        &self.config,
                    ));
                    index
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
                    let index = self.thread.turns.len() - 1;
                    self.rendered_turns.push(render_turn(
                        &self.thread,
                        &self.thread.turns[index],
                        &self.config,
                    ));
                    index
                };
                if !previous_renders
                    .iter()
                    .any(|(turn_id, _, _)| turn_id == &change.turn_id)
                {
                    previous_renders.push((
                        change.turn_id.clone(),
                        self.thread.turns[index].clone(),
                        self.rendered_turns[index].clone(),
                    ));
                }
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
                visible_change = true;
            }
        }

        for (turn_id, previous_turn, previous_render) in previous_renders {
            if let Some(index) = self.thread.turns.iter().position(|turn| turn.id == turn_id) {
                self.rendered_turns[index] = rerender_turn_suffix(
                    &self.thread,
                    &previous_turn,
                    &previous_render,
                    &self.thread.turns[index],
                    &self.config,
                );
            }
        }
        if visible_change {
            self.viewport.replace_cells(flattened_transcript_cells(
                &self.thread,
                &self.config,
                &self.rendered_turns,
            ));
        }
        visible_change
    }
}

pub(super) async fn prepare_initial_viewer(
    thread: Thread,
    config: Config,
    width: u16,
) -> Result<SessionViewer> {
    let started = Instant::now();
    let viewer =
        tokio::task::spawn_blocking(move || SessionViewer::new(thread, config, width)).await?;
    tracing::debug!(
        elapsed = ?started.elapsed(),
        "session viewer prepared initial frame"
    );
    viewer
}

pub(super) fn prepare_turn_replacement(
    thread: &Thread,
    config: &Config,
    turns: Vec<Turn>,
    width: u16,
) -> PreparedTurnReplacement {
    let rendered_turns = turns
        .iter()
        .map(|turn| render_turn(thread, turn, config))
        .collect::<Vec<_>>();
    let cell_heights = rendered_turns
        .iter()
        .map(|rendered| {
            rendered
                .cells
                .iter()
                .map(|cell| {
                    cell.has_stable_transcript_height()
                        .then(|| cell.desired_height(width))
                })
                .collect()
        })
        .collect();
    PreparedTurnReplacement {
        turns,
        rendered_turns,
        cell_heights,
        width,
    }
}

fn flattened_prepared_heights(
    cells: &TranscriptCells,
    turn_heights: Vec<Vec<Option<u16>>>,
    width: u16,
) -> Vec<Option<u16>> {
    let heights = turn_heights.into_iter().flatten().collect::<Vec<_>>();
    if heights.len() == cells.len() {
        return heights;
    }
    cells
        .iter()
        .map(|cell| {
            cell.has_stable_transcript_height()
                .then(|| cell.desired_height(width))
        })
        .collect()
}

fn flattened_transcript_cells(
    thread: &Thread,
    config: &Config,
    rendered_turns: &[RenderedTurn],
) -> TranscriptCells {
    let cells = rendered_turns
        .iter()
        .flat_map(|rendered| rendered.cells.iter().cloned())
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
