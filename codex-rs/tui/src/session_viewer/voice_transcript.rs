//! Reducer for explicit AgentVoice observer recordings, never native rollout history.
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

use crate::history_cell::AgentMarkdownCell;
use crate::history_cell::new_user_prompt;
use crate::thread_transcript::TranscriptCells;

const MAX_LINE: usize = 1 << 20;
const MAX_TEXT: usize = 256 * 1024;
const MAX_TOTAL_TEXT: usize = 64 << 20;
const MAX_MESSAGES: usize = 100_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Status {
    Streaming,
    Complete,
    Incomplete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Message {
    pub role: Option<String>,
    pub text: String,
    pub status: Status,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VoiceData {
    instance_id: String,
    generation: u64,
    thread_id: String,
    item: Option<Item>,
    item_id: Option<String>,
    delta: Option<String>,
}

#[derive(Deserialize)]
struct Item {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    role: Option<String>,
    text: Option<String>,
}

#[derive(Default)]
pub(super) struct Transcript {
    pub workspace: PathBuf,
    thread: Option<String>,
    pub messages: Vec<Message>,
    indices: HashMap<(String, u64, String), usize>,
    cells: TranscriptCells,
    rendered: Vec<Message>,
    total_text: usize,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

impl Transcript {
    pub fn accept(&mut self, line: &[u8]) -> io::Result<()> {
        let record: Value = serde_json::from_slice(line)?;
        let kind = record["type"]
            .as_str()
            .ok_or_else(|| invalid("Missing record type"))?;
        if self.thread.is_none() {
            if kind != "voice_transcript" || record["format"] != "agentvoice" {
                return Err(invalid("Expected an AgentVoice recording header"));
            }
            self.thread = Some(
                record["threadId"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| invalid("Missing conversation identity"))?
                    .to_owned(),
            );
            self.workspace = PathBuf::from(
                record["workspace"]
                    .as_str()
                    .ok_or_else(|| invalid("Missing workspace"))?,
            );
            return Ok(());
        }
        match kind {
            "recording.started" | "recording.ended" | "recording.gap" => {
                self.interrupt();
            }
            "event" => {
                if record["v"] != 2 {
                    return Err(invalid("Unsupported AgentVoice event contract"));
                }
                let event = record["event"]
                    .as_str()
                    .ok_or_else(|| invalid("Missing voice event name"))?;
                if !matches!(
                    event,
                    "voice.item.started" | "voice.item.completed" | "voice.item.transcript.delta"
                ) {
                    return Err(invalid(format!("Unexpected voice event: {event}")));
                }
                let data: VoiceData = serde_json::from_value(record["data"].clone())?;
                if data.generation == 0
                    || data.instance_id.is_empty()
                    || data.instance_id.chars().count() > 256
                {
                    return Err(invalid("Invalid voice producer identity"));
                }
                if self.thread.as_ref() != Some(&data.thread_id) {
                    return Err(invalid("Conversation identity changed inside recording"));
                }
                let (id, role, text) = if event == "voice.item.transcript.delta" {
                    (
                        data.item_id
                            .ok_or_else(|| invalid("Missing delta item ID"))?,
                        None,
                        data.delta.ok_or_else(|| invalid("Missing delta"))?,
                    )
                } else {
                    let item = data.item.ok_or_else(|| invalid("Missing voice item"))?;
                    match item.kind.as_str() {
                        "realtimeSessionStarted" | "realtimeSessionClosed" => {
                            self.interrupt();
                            return Ok(());
                        }
                        "bemItemPromoted" => return Ok(()),
                        "transcriptSegment" => {}
                        _ => return Err(invalid("Unknown voice item type")),
                    }
                    if !matches!(item.role.as_deref(), Some("user" | "assistant")) {
                        return Err(invalid("Invalid voice speaker"));
                    }
                    (
                        item.id,
                        item.role,
                        item.text
                            .ok_or_else(|| invalid("Missing transcript text"))?,
                    )
                };
                if id.is_empty() || id.chars().count() > 256 {
                    return Err(invalid("Invalid voice item identity"));
                }
                let key = (data.instance_id, data.generation, id);
                let index = if let Some(index) = self.indices.get(&key) {
                    *index
                } else {
                    let index = self.messages.len();
                    self.push(Message {
                        role: None,
                        text: String::new(),
                        status: Status::Streaming,
                    })?;
                    self.indices.insert(key, index);
                    index
                };
                let message = &mut self.messages[index];
                let delta = event == "voice.item.transcript.delta";
                if delta && message.status != Status::Streaming {
                    return Ok(());
                }
                let new_len = if delta {
                    message.text.len() + text.len()
                } else {
                    text.len()
                };
                let total = self.total_text - message.text.len() + new_len;
                if new_len > MAX_TEXT || total > MAX_TOTAL_TEXT {
                    return Err(invalid("Voice transcript text limit exceeded"));
                }
                self.total_text = total;
                if delta {
                    message.text.push_str(&text);
                } else {
                    message.role = role;
                    message.text = text;
                    message.status = if event == "voice.item.completed" {
                        Status::Complete
                    } else {
                        Status::Streaming
                    };
                }
            }
            _ => return Err(invalid(format!("Unexpected recording entry: {kind}"))),
        }
        Ok(())
    }

    fn push(&mut self, message: Message) -> io::Result<()> {
        if self.messages.len() >= MAX_MESSAGES
            || self.total_text + message.text.len() > MAX_TOTAL_TEXT
        {
            return Err(invalid("Voice recording size limit exceeded"));
        }
        self.total_text += message.text.len();
        self.messages.push(message);
        Ok(())
    }

    pub fn interrupt(&mut self) {
        for message in &mut self.messages {
            if message.status == Status::Streaming {
                message.status = Status::Incomplete;
            }
        }
    }

    pub fn cells(&mut self) -> TranscriptCells {
        for (index, message) in self.messages.iter().enumerate() {
            if self.rendered.get(index) == Some(message) {
                continue;
            }
            let mut text = message.text.trim().to_owned();
            if message.role.is_none() {
                text.clear();
            } else if message.status != Status::Complete {
                text.push_str(if message.status == Status::Streaming {
                    "\n[Streaming…]"
                } else {
                    "\n[Incomplete recording]"
                });
            }
            let cell: Arc<dyn crate::history_cell::HistoryCell> =
                if message.role.as_deref() == Some("user") {
                    Arc::new(new_user_prompt(text, vec![], vec![], vec![]))
                } else {
                    Arc::new(AgentMarkdownCell::new_with_inline_visualizations(
                        text,
                        &self.workspace,
                        /*inline_visualization_context*/ None,
                    ))
                };
            if index == self.cells.len() {
                self.cells.push(cell);
                self.rendered.push(message.clone());
            } else {
                self.cells[index] = cell;
                self.rendered[index] = message.clone();
            }
        }
        self.cells
            .iter()
            .zip(&self.messages)
            .filter(|(_, message)| {
                message.role.is_some()
                    && (!message.text.trim().is_empty() || message.status != Status::Complete)
            })
            .map(|(cell, _)| cell.clone())
            .collect()
    }
}

pub(super) struct VoiceFile {
    file: File,
    path: PathBuf,
    metadata: std::fs::Metadata,
    offset: u64,
    pending: Vec<u8>,
    line: usize,
    pub transcript: Transcript,
}

impl VoiceFile {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(invalid("Voice input must be a regular JSONL file"));
        }
        Ok(Self {
            file,
            path: path.to_owned(),
            metadata,
            offset: 0,
            pending: Vec::new(),
            line: 0,
            transcript: Transcript::default(),
        })
    }

    pub fn refresh(&mut self) -> io::Result<bool> {
        let metadata = std::fs::metadata(&self.path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if (metadata.dev(), metadata.ino()) != (self.metadata.dev(), self.metadata.ino()) {
                return Err(invalid("Voice recording was replaced; reopen the viewer"));
            }
        }
        if metadata.len() < self.offset {
            return Err(invalid("Voice recording was truncated; reopen the viewer"));
        }
        let mut changed = false;
        let mut chunk = [0; 64 * 1024];
        // Limit each poll so a writer cannot starve keyboard processing.
        for _ in 0..64 {
            let count = self.file.read(&mut chunk)?;
            if count == 0 {
                break;
            }
            self.offset += count as u64;
            self.pending.extend_from_slice(&chunk[..count]);
            let mut consumed = 0;
            while let Some(end) = self.pending[consumed..]
                .iter()
                .position(|byte| *byte == b'\n')
            {
                let end = consumed + end;
                self.line += 1;
                if end - consumed > MAX_LINE {
                    return Err(invalid("Voice recording line exceeds 1 MiB"));
                }
                self.transcript
                    .accept(&self.pending[consumed..end])
                    .map_err(|error| {
                        invalid(format!("Voice recording line {}: {error}", self.line))
                    })?;
                consumed = end + 1;
                changed = true;
            }
            self.pending.drain(..consumed);
            if self.pending.len() > MAX_LINE {
                return Err(invalid("Voice recording line exceeds 1 MiB"));
            }
        }
        Ok(changed)
    }

    pub fn caught_up(&self) -> io::Result<bool> {
        Ok(self.offset >= self.file.metadata()?.len())
    }
}

#[cfg(test)]
#[path = "voice_transcript_tests.rs"]
mod tests;
