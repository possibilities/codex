use std::sync::Arc;

use crossterm::event::KeyEvent;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Text;
use ratatui::widgets::Clear;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Widget;
use ratatui::widgets::Wrap;

use crate::history_cell::HistoryCell;
use crate::history_cell::UserHistoryCell;
use crate::key_hint::KeyBindingListExt;
use crate::keymap::PagerKeymap;
use crate::pager_overlay::scrolling::render_offset_content;
use crate::render::renderable::Renderable;
use crate::style::user_message_style;
use crate::terminal_hyperlinks::HyperlinkLine;
use crate::terminal_hyperlinks::mark_buffer_hyperlinks;
use crate::terminal_hyperlinks::visible_lines_ref;
use crate::thread_transcript::TranscriptCells;

pub(super) struct ConversationViewport {
    pub(super) cells: TranscriptCells,
    cell_heights: Vec<Option<u16>>,
    cell_heights_width: Option<u16>,
    keymap: PagerKeymap,
    pub(super) scroll_offset: usize,
    pub(super) max_scroll: usize,
    pub(super) viewport_height: usize,
    pub(super) follow_tail: bool,
}

impl ConversationViewport {
    pub(super) fn new(cells: TranscriptCells, keymap: PagerKeymap) -> Self {
        let cell_heights = vec![None; cells.len()];
        Self {
            cells,
            cell_heights,
            cell_heights_width: None,
            keymap,
            scroll_offset: 0,
            max_scroll: 0,
            viewport_height: 0,
            follow_tail: true,
        }
    }

    pub(super) fn replace_cells(&mut self, cells: TranscriptCells) {
        let mut cell_heights = vec![None; cells.len()];
        let prefix_len = self
            .cells
            .iter()
            .zip(&cells)
            .take_while(|(existing, replacement)| Arc::ptr_eq(existing, replacement))
            .count();
        cell_heights[..prefix_len].copy_from_slice(&self.cell_heights[..prefix_len]);

        let max_suffix_len = self.cells.len().min(cells.len()).saturating_sub(prefix_len);
        let suffix_len = self
            .cells
            .iter()
            .rev()
            .zip(cells.iter().rev())
            .take(max_suffix_len)
            .take_while(|(existing, replacement)| Arc::ptr_eq(existing, replacement))
            .count();
        if suffix_len > 0 {
            let existing_start = self.cells.len() - suffix_len;
            let replacement_start = cells.len() - suffix_len;
            cell_heights[replacement_start..].copy_from_slice(&self.cell_heights[existing_start..]);
        }

        self.cells = cells;
        self.cell_heights = cell_heights;
    }

    pub(super) fn replace_cells_with_heights(
        &mut self,
        cells: TranscriptCells,
        width: u16,
        prepared_heights: Vec<Option<u16>>,
    ) {
        debug_assert_eq!(cells.len(), prepared_heights.len());
        self.replace_cells(cells);
        if self.cell_heights_width == Some(width) {
            for (height, prepared) in self.cell_heights.iter_mut().zip(prepared_heights) {
                if height.is_none() {
                    *height = prepared;
                }
            }
        } else {
            self.cell_heights = prepared_heights;
            self.cell_heights_width = Some(width);
        }
    }

    pub(super) fn is_following(&self) -> bool {
        self.follow_tail
    }

    pub(super) fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.keymap.close.is_pressed(key) {
            return true;
        }
        if self.keymap.scroll_up.is_pressed(key) {
            self.follow_tail = false;
            self.scroll_offset = self.scroll_offset.saturating_sub(1);
        } else if self.keymap.scroll_down.is_pressed(key) {
            self.scroll_offset = self.scroll_offset.saturating_add(1).min(self.max_scroll);
            self.follow_tail = self.scroll_offset == self.max_scroll;
        } else if self.keymap.page_up.is_pressed(key) {
            self.follow_tail = false;
            self.scroll_offset = self.scroll_offset.saturating_sub(self.viewport_height);
        } else if self.keymap.page_down.is_pressed(key) {
            self.scroll_offset = self
                .scroll_offset
                .saturating_add(self.viewport_height)
                .min(self.max_scroll);
            self.follow_tail = self.scroll_offset == self.max_scroll;
        } else if self.keymap.half_page_up.is_pressed(key) {
            self.follow_tail = false;
            self.scroll_offset = self
                .scroll_offset
                .saturating_sub(self.viewport_height.saturating_add(1) / 2);
        } else if self.keymap.half_page_down.is_pressed(key) {
            self.scroll_offset = self
                .scroll_offset
                .saturating_add(self.viewport_height.saturating_add(1) / 2)
                .min(self.max_scroll);
            self.follow_tail = self.scroll_offset == self.max_scroll;
        } else if self.keymap.jump_top.is_pressed(key) {
            self.follow_tail = false;
            self.scroll_offset = 0;
        } else if self.keymap.jump_bottom.is_pressed(key) {
            self.follow_tail = true;
        }
        false
    }

    pub(super) fn render(&mut self, area: Rect, buf: &mut Buffer) {
        Clear.render(area, buf);
        self.viewport_height = usize::from(area.height);
        self.prepare_height_cache(area.width);
        let content_height = self.content_height(area.width);
        self.max_scroll = content_height.saturating_sub(self.viewport_height);
        if self.follow_tail {
            self.scroll_offset = self.max_scroll;
        } else {
            self.scroll_offset = self.scroll_offset.min(self.max_scroll);
        }

        let viewport_bottom = self.scroll_offset.saturating_add(self.viewport_height);
        let mut cell_top = 0usize;
        for index in 0..self.cells.len() {
            if index > 0 && !self.cells[index].is_stream_continuation() {
                cell_top = cell_top.saturating_add(1);
            }
            let height = usize::from(self.cell_height(index, area.width));
            let cell_bottom = cell_top.saturating_add(height);
            if cell_bottom > self.scroll_offset && cell_top < viewport_bottom {
                let visible_top = cell_top.max(self.scroll_offset);
                let visible_bottom = cell_bottom.min(viewport_bottom);
                let draw_y = visible_top.saturating_sub(self.scroll_offset) as u16;
                let draw_height = visible_bottom.saturating_sub(visible_top) as u16;
                let draw_area = Rect::new(
                    area.x,
                    area.y.saturating_add(draw_y),
                    area.width,
                    draw_height,
                );
                render_offset_content(
                    draw_area,
                    buf,
                    &DisplayCellRenderable {
                        cell: self.cells[index].clone(),
                        highlighted: false,
                    },
                    visible_top.saturating_sub(cell_top) as u16,
                );
            }
            cell_top = cell_bottom;
            if cell_top >= viewport_bottom {
                break;
            }
        }
    }

    fn prepare_height_cache(&mut self, width: u16) {
        if self.cell_heights_width != Some(width) {
            self.cell_heights = vec![None; self.cells.len()];
            self.cell_heights_width = Some(width);
        } else if self.cell_heights.len() != self.cells.len() {
            self.cell_heights.resize(self.cells.len(), None);
        }
    }

    fn cell_height(&mut self, index: usize, width: u16) -> u16 {
        if let Some(height) = self.cell_heights[index] {
            return height;
        }
        let height = self.cells[index].desired_height(width);
        if self.cells[index].has_stable_transcript_height() {
            self.cell_heights[index] = Some(height);
        }
        height
    }

    pub(super) fn content_height(&mut self, width: u16) -> usize {
        self.prepare_height_cache(width);
        let mut height = 0usize;
        for index in 0..self.cells.len() {
            height = height
                .saturating_add(usize::from(
                    index > 0 && !self.cells[index].is_stream_continuation(),
                ))
                .saturating_add(usize::from(self.cell_height(index, width)));
        }
        height
    }
}

struct DisplayCellRenderable {
    cell: Arc<dyn HistoryCell>,
    highlighted: bool,
}

impl Renderable for DisplayCellRenderable {
    fn render(&self, area: Rect, buf: &mut Buffer) {
        self.render_scrolled(area, buf, /*scroll_offset*/ 0);
    }

    fn render_scrolled(&self, area: Rect, buf: &mut Buffer, scroll_offset: u16) -> bool {
        let hyperlink_lines = self.cell.display_hyperlink_lines(area.width);
        let style = if self.cell.as_any().is::<UserHistoryCell>() {
            if self.highlighted {
                user_message_style().reversed()
            } else {
                user_message_style()
            }
        } else {
            Style::default()
        };
        Paragraph::new(Text::from(visible_lines_ref(&hyperlink_lines)))
            .style(style)
            .wrap(Wrap { trim: false })
            .scroll((scroll_offset, 0))
            .render(area, buf);
        extend_line_styles(area, buf, &hyperlink_lines, scroll_offset);
        mark_buffer_hyperlinks(buf, area, &hyperlink_lines, usize::from(scroll_offset));
        true
    }

    fn desired_height(&self, width: u16) -> u16 {
        self.cell.desired_height(width)
    }
}

fn extend_line_styles(area: Rect, buf: &mut Buffer, lines: &[HyperlinkLine], scroll_offset: u16) {
    if area.is_empty() || lines.iter().all(|line| line.line.style == Style::default()) {
        return;
    }

    let visible_top = usize::from(scroll_offset);
    let visible_bottom = visible_top.saturating_add(usize::from(area.height));
    let mut line_top = 0usize;
    for line in lines {
        let line_height = Paragraph::new(line.line.clone())
            .wrap(Wrap { trim: false })
            .line_count(area.width)
            .max(1);
        let line_bottom = line_top.saturating_add(line_height);
        if line_bottom > visible_top && line_top < visible_bottom {
            let styled_top = line_top.max(visible_top).saturating_sub(visible_top);
            let styled_bottom = line_bottom.min(visible_bottom).saturating_sub(visible_top);
            buf.set_style(
                Rect::new(
                    area.x,
                    area.y.saturating_add(styled_top as u16),
                    area.width,
                    styled_bottom.saturating_sub(styled_top) as u16,
                ),
                line.line.style,
            );
        }
        line_top = line_bottom;
        if line_top >= visible_bottom {
            break;
        }
    }
}
