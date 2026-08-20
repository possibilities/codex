use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::io::Read as _;
use std::io::Seek as _;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::Instant;
use std::time::SystemTime;

use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;
use codex_rollout::ScanOutcome;
use codex_rollout::decode_rollout_line;
use codex_rollout::is_persisted_rollout_item;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

pub(super) const INITIAL_TURN_ITEM_LIMIT: usize = 24;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RolloutFileStamp {
    len: u64,
    modified: Option<SystemTime>,
}

pub(super) struct LocalRolloutWatcher {
    pub(super) path: PathBuf,
    history_mode: ThreadHistoryMode,
    last_stamp: Option<RolloutFileStamp>,
    pub(super) offset: u64,
    builder: Option<ThreadHistoryBuilder>,
    turns: Vec<Turn>,
}

#[derive(Debug, PartialEq)]
pub(super) enum LocalRolloutUpdate {
    Replace(Vec<Turn>),
    Changes(Vec<ThreadHistoryChangeSet>),
}

impl LocalRolloutWatcher {
    pub(super) fn new(path: PathBuf, history_mode: ThreadHistoryMode) -> Self {
        Self {
            path,
            history_mode,
            last_stamp: None,
            offset: 0,
            builder: None,
            turns: Vec::new(),
        }
    }

    pub(super) fn for_thread(thread: &Thread) -> Option<Self> {
        Some(Self::new(thread.path.clone()?, thread.history_mode.into()))
    }

    pub(super) fn fresh(&self) -> Self {
        Self::new(self.path.clone(), self.history_mode)
    }

    /// Seeds live following from the newest turn without replaying the full rollout.
    pub(super) async fn load_recent_turn(&mut self) -> io::Result<Vec<Turn>> {
        let started = Instant::now();
        let metadata = tokio::fs::metadata(&self.path).await?;
        let stamp = RolloutFileStamp {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        };
        if self.path.extension() == Some(OsStr::new("zst")) {
            self.builder = Some(ThreadHistoryBuilder::new());
            self.offset = stamp.len;
            self.last_stamp = Some(stamp);
            return Ok(Vec::new());
        }

        let path = self.path.clone();
        let history_mode = self.history_mode;
        let (turns, builder, parse_errors, consumed_len) = tokio::task::spawn_blocking(move || {
            let mut file = File::open(path)?;
            let consumed_len = complete_file_len(&mut file, stamp.len)?;
            let mut scanner = ReverseJsonlScanner::new_at(file, consumed_len)?;
            let mut items = Vec::new();
            let mut parse_errors = 0usize;
            while let Some(outcome) = scanner.scan_next::<Value>()? {
                let line = match outcome {
                    ScanOutcome::Parsed(value) => match decode_rollout_line(value) {
                        Ok(line) => line,
                        Err(_) => {
                            parse_errors = parse_errors.saturating_add(1);
                            continue;
                        }
                    },
                    ScanOutcome::Rejected(_) => {
                        parse_errors = parse_errors.saturating_add(1);
                        continue;
                    }
                };
                let is_turn_start =
                    matches!(&line.item, RolloutItem::EventMsg(EventMsg::TurnStarted(_)));
                items.push(line.item);
                if is_turn_start {
                    break;
                }
            }
            items.reverse();
            let (turns, builder) = build_turns(items, history_mode);
            Ok::<_, io::Error>((turns, builder, parse_errors, consumed_len))
        })
        .await
        .map_err(io::Error::other)??;
        log_parse_errors(&self.path, parse_errors);

        self.turns = turns;
        self.builder = Some(builder);
        self.offset = consumed_len;
        self.last_stamp = Some(RolloutFileStamp {
            len: self.offset,
            modified: stamp.modified,
        });
        tracing::debug!(
            path = %self.path.display(),
            turns = self.turns.len(),
            elapsed = ?started.elapsed(),
            "session viewer loaded recent rollout turn"
        );
        Ok(initial_turn_tail(self.turns.clone()))
    }

    pub(super) async fn load_turns_if_changed(&mut self) -> io::Result<Option<LocalRolloutUpdate>> {
        let metadata = tokio::fs::metadata(&self.path).await?;
        let stamp = RolloutFileStamp {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        };
        if self.last_stamp == Some(stamp) {
            return Ok(None);
        }

        let needs_full_reload = self.builder.is_none()
            || stamp.len < self.offset
            || stamp.len == self.offset
                && self
                    .last_stamp
                    .is_some_and(|previous| previous.modified != stamp.modified);
        if needs_full_reload || self.path.extension() == Some(OsStr::new("zst")) {
            return self.reload(stamp).await;
        }
        self.load_appended(stamp).await
    }

    async fn reload(&mut self, stamp: RolloutFileStamp) -> io::Result<Option<LocalRolloutUpdate>> {
        let (items, consumed_len, parse_errors) =
            if self.path.extension() == Some(OsStr::new("zst")) {
                let (items, _thread_id, parse_errors) =
                    RolloutRecorder::load_rollout_items(&self.path).await?;
                (items, stamp.len, parse_errors)
            } else {
                let bytes = read_prefix(&self.path, stamp.len).await?;
                let consumed_len = complete_prefix_len(&bytes);
                let decoded = tokio::task::spawn_blocking(move || {
                    let (items, parse_errors) = decode_items(&bytes[..consumed_len]);
                    (items, consumed_len, parse_errors)
                })
                .await
                .map_err(io::Error::other)?;
                let (items, consumed_len, parse_errors) = decoded;
                (
                    items,
                    u64::try_from(consumed_len).map_err(io::Error::other)?,
                    parse_errors,
                )
            };
        log_parse_errors(&self.path, parse_errors);
        let history_mode = self.history_mode;
        let (turns, builder) =
            tokio::task::spawn_blocking(move || build_turns(items, history_mode))
                .await
                .map_err(io::Error::other)?;
        self.turns = turns;
        self.builder = Some(builder);
        self.offset = consumed_len;
        self.last_stamp = Some(RolloutFileStamp {
            len: consumed_len,
            modified: stamp.modified,
        });
        Ok(Some(LocalRolloutUpdate::Replace(self.turns.clone())))
    }

    async fn load_appended(
        &mut self,
        stamp: RolloutFileStamp,
    ) -> io::Result<Option<LocalRolloutUpdate>> {
        let mut file = tokio::fs::File::open(&self.path).await?;
        file.seek(SeekFrom::Start(self.offset)).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        let consumed_len = complete_prefix_len(&bytes);
        if consumed_len == 0 {
            return Ok(None);
        }

        let (items, parse_errors) = decode_items(&bytes[..consumed_len]);
        log_parse_errors(&self.path, parse_errors);
        let persisted_items = items
            .into_iter()
            .filter(|item| is_persisted_rollout_item(item, self.history_mode))
            .collect::<Vec<_>>();
        let Some(builder) = self.builder.as_mut() else {
            return Err(io::Error::other(
                "rollout builder was not initialized before reading appended items",
            ));
        };
        let changes = match self.history_mode {
            ThreadHistoryMode::Legacy => {
                vec![builder.handle_rollout_items_with_changes(&persisted_items)]
            }
            ThreadHistoryMode::Paginated => persisted_items
                .iter()
                .flat_map(|item| paginated_change_sets(builder, item))
                .collect(),
        };
        let changes = changes
            .into_iter()
            .filter(|changes| !changes.is_empty())
            .collect::<Vec<_>>();
        for change in &changes {
            apply_changes(&mut self.turns, change.clone());
        }
        self.offset = self
            .offset
            .saturating_add(u64::try_from(consumed_len).map_err(io::Error::other)?);
        self.last_stamp = Some(RolloutFileStamp {
            len: self.offset,
            modified: stamp.modified,
        });
        Ok((!changes.is_empty()).then_some(LocalRolloutUpdate::Changes(changes)))
    }
}

fn initial_turn_tail(mut turns: Vec<Turn>) -> Vec<Turn> {
    for turn in &mut turns {
        let remove_count = turn.items.len().saturating_sub(INITIAL_TURN_ITEM_LIMIT);
        turn.items.drain(..remove_count);
    }
    turns
}

fn build_turns(
    items: Vec<RolloutItem>,
    history_mode: ThreadHistoryMode,
) -> (Vec<Turn>, ThreadHistoryBuilder) {
    let persisted_items = items
        .into_iter()
        .filter(|item| is_persisted_rollout_item(item, history_mode))
        .collect::<Vec<_>>();
    let mut live_builder = ThreadHistoryBuilder::new();
    let turns = match history_mode {
        ThreadHistoryMode::Legacy => {
            let mut snapshot_builder = ThreadHistoryBuilder::new();
            for item in &persisted_items {
                snapshot_builder.handle_rollout_item(item);
                live_builder.handle_rollout_item(item);
            }
            snapshot_builder.finish()
        }
        ThreadHistoryMode::Paginated => {
            let mut turns = Vec::new();
            for item in &persisted_items {
                for change in paginated_change_sets(&mut live_builder, item) {
                    apply_changes(&mut turns, change);
                }
            }
            turns
        }
    };
    (turns, live_builder)
}

fn paginated_change_sets(
    builder: &mut ThreadHistoryBuilder,
    item: &RolloutItem,
) -> Vec<ThreadHistoryChangeSet> {
    let builder_change = builder.handle_rollout_item_with_changes(item);
    let line = codex_rollout::RolloutLine {
        timestamp: String::new(),
        item: item.clone(),
        ordinal: None,
    };
    vec![builder_change, project_rollout_line(&line)]
}

async fn read_prefix(path: &std::path::Path, len: u64) -> io::Result<Vec<u8>> {
    let file = tokio::fs::File::open(path).await?;
    let capacity = usize::try_from(len).map_err(io::Error::other)?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(len).read_to_end(&mut bytes).await?;
    Ok(bytes)
}

fn complete_prefix_len(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1)
}

fn complete_file_len(file: &mut File, len: u64) -> io::Result<u64> {
    const READ_CHUNK_SIZE: usize = 64 * 1024;

    if len == 0 {
        return Ok(0);
    }
    file.seek(SeekFrom::Start(len - 1))?;
    let mut final_byte = [0];
    file.read_exact(&mut final_byte)?;
    if final_byte[0] == b'\n' {
        return Ok(len);
    }

    let mut chunk = vec![0; READ_CHUNK_SIZE];
    let mut chunk_end = len;
    while chunk_end > 0 {
        let read_len =
            usize::try_from(chunk_end.min(READ_CHUNK_SIZE as u64)).map_err(io::Error::other)?;
        let chunk_start = chunk_end - read_len as u64;
        file.seek(SeekFrom::Start(chunk_start))?;
        file.read_exact(&mut chunk[..read_len])?;
        if let Some(index) = chunk[..read_len].iter().rposition(|byte| *byte == b'\n') {
            return Ok(chunk_start + index as u64 + 1);
        }
        chunk_end = chunk_start;
    }
    Ok(0)
}

fn decode_items(bytes: &[u8]) -> (Vec<RolloutItem>, usize) {
    let mut items = Vec::new();
    let mut parse_errors = 0usize;
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let item = serde_json::from_slice::<Value>(line)
            .and_then(decode_rollout_line)
            .map(|line| line.item);
        match item {
            Ok(item) => items.push(item),
            Err(_) => parse_errors = parse_errors.saturating_add(1),
        }
    }
    (items, parse_errors)
}

fn log_parse_errors(path: &std::path::Path, parse_errors: usize) {
    if parse_errors > 0 {
        tracing::debug!(
            path = %path.display(),
            parse_errors,
            "session viewer skipped malformed rollout records"
        );
    }
}

pub(super) fn apply_changes(turns: &mut Vec<Turn>, changes: ThreadHistoryChangeSet) -> bool {
    let mut visible_change = !changes.removed_turn_ids.is_empty();
    turns.retain(|turn| !changes.removed_turn_ids.contains(&turn.id));

    for change in changes.changed_turns {
        let turn = find_or_insert_turn(turns, change.turn_id);
        turn.status = change.status;
        turn.error = change.error;
        turn.started_at = change.started_at;
        turn.completed_at = change.completed_at;
        turn.duration_ms = change.duration_ms;
    }
    for change in changes.changed_items {
        visible_change = true;
        let turn = find_or_insert_turn(turns, change.turn_id);
        if let Some(existing) = turn
            .items
            .iter_mut()
            .find(|item| item.id() == change.item.id())
        {
            *existing = change.item;
        } else {
            turn.items.push(change.item);
        }
    }
    visible_change
}

fn find_or_insert_turn(turns: &mut Vec<Turn>, turn_id: String) -> &mut Turn {
    let index = if let Some(index) = turns.iter().position(|turn| turn.id == turn_id) {
        index
    } else {
        turns.push(Turn {
            id: turn_id,
            items: Vec::<ThreadItem>::new(),
            items_view: TurnItemsView::Full,
            status: TurnStatus::InProgress,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
        });
        turns.len() - 1
    };
    &mut turns[index]
}
