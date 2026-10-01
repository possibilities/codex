//! External output is isolated from host-agent output and fenced by native call incarnation.
use super::*;
use codex_protocol::external_realtime::ExternalRealtimeEvent;
use codex_protocol::external_realtime::ExternalRealtimeEventParams;
use codex_protocol::external_realtime::ExternalRealtimeHandoffEvent;
use codex_protocol::external_realtime::ExternalRealtimeHandoffSource;
use std::collections::HashSet;

const MAX_EVENTS: usize = 16_384;
const MAX_REPLAY_BYTES: usize = 8 * 1024 * 1024;
const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_EXECUTIONS: usize = 128;
const MAX_ITEMS: usize = 512;
const MAX_HANDOFFS: usize = 4096;
const V1_EXTERNAL_WORK_ENDED: &str =
    "Background agent work ended. Use the preceding status and output as the result.";

pub(super) struct ExternalRealtimeState {
    pub(super) incarnation_id: String,
    handoffs: HashSet<String>,
    events: Vec<ExternalRealtimeEventParams>,
    replay_bytes: usize,
    executions: HashMap<String, Execution>,
    failed: bool,
}

#[derive(Default)]
struct Execution {
    handoffs: HashSet<String>,
    items: HashMap<String, Item>,
    ended: HashSet<String>,
    settled: bool,
}

struct Item {
    handoff_id: Option<String>,
    text: String,
    phase: Option<MessagePhase>,
    stream: Option<RealtimeStreamedItem>,
}

fn invalid(message: &str) -> CodexErr {
    CodexErr::InvalidRequest(message.to_string())
}

impl ExternalRealtimeState {
    pub(super) fn new() -> Self {
        Self {
            incarnation_id: uuid::Uuid::new_v4().to_string(),
            handoffs: HashSet::new(),
            events: Vec::new(),
            replay_bytes: 0,
            executions: HashMap::new(),
            failed: false,
        }
    }

    pub(super) fn record_handoff(
        &mut self,
        handoff: &RealtimeHandoffRequested,
        realtime_session_id: Option<String>,
    ) -> CodexResult<ExternalRealtimeHandoffEvent> {
        if self.handoffs.len() >= MAX_HANDOFFS
            || handoff.handoff_id.is_empty()
            || !self.handoffs.insert(handoff.handoff_id.clone())
        {
            return Err(invalid("duplicate or excessive external handoff"));
        }
        Ok(ExternalRealtimeHandoffEvent {
            incarnation_id: self.incarnation_id.clone(),
            realtime_session_id,
            handoff_id: handoff.handoff_id.clone(),
            item_id: Some(handoff.item_id.clone()),
            source: ExternalRealtimeHandoffSource::Handoff,
            input_transcript: handoff.input_transcript.clone(),
            active_transcript: handoff.active_transcript.clone(),
            transcript_tail: None,
        })
    }

    pub(super) fn transcript_tail(
        &mut self,
        tail: Vec<RealtimeTranscriptEntry>,
        realtime_session_id: Option<String>,
    ) -> ExternalRealtimeHandoffEvent {
        let handoff_id = uuid::Uuid::new_v4().to_string();
        self.handoffs.insert(handoff_id.clone());
        ExternalRealtimeHandoffEvent {
            incarnation_id: self.incarnation_id.clone(),
            realtime_session_id,
            handoff_id,
            item_id: None,
            source: ExternalRealtimeHandoffSource::TranscriptTail,
            input_transcript: REALTIME_SESSION_ENDED_HANDOFF_INSTRUCTION.to_string(),
            active_transcript: Vec::new(),
            transcript_tail: Some(tail),
        }
    }

    // Replay comparison includes the whole event, execution, and handoff identity. Never silently
    // accept conflicting retries or sequence gaps. The journal is bounded for the lifetime of a call.
    fn validate(&self, params: &ExternalRealtimeEventParams) -> CodexResult<Option<usize>> {
        if self.incarnation_id != params.incarnation_id {
            return Err(invalid("stale realtime incarnation"));
        }
        if self.failed {
            return Err(invalid(
                "external feedback transport failed; restart realtime",
            ));
        }
        if params.sequence == 0 {
            return Err(invalid("external sequence starts at one"));
        }
        if let Some(previous) = usize::try_from(params.sequence - 1)
            .ok()
            .and_then(|index| self.events.get(index))
        {
            return if previous == params {
                Ok(None)
            } else {
                Err(invalid("conflicting external event replay"))
            };
        }
        if params.sequence != self.events.len() as u64 + 1 {
            return Err(invalid("external event sequence gap"));
        }
        let bytes = serde_json::to_vec(params)
            .map_err(|_| invalid("invalid external event"))?
            .len();
        if self.events.len() >= MAX_EVENTS
            || self.replay_bytes.saturating_add(bytes) > MAX_REPLAY_BYTES
        {
            return Err(invalid(
                "external replay capacity reached; restart realtime",
            ));
        }
        if params.execution_id.is_empty()
            || params.execution_id.len() > 256
            || params
                .handoff_id
                .as_ref()
                .is_some_and(|id| !self.handoffs.contains(id))
        {
            return Err(invalid(
                "unknown external handoff or invalid execution identity",
            ));
        }
        if let Some(id) = &params.handoff_id
            && self.executions.iter().any(|(execution, work)| {
                execution != &params.execution_id && work.handoffs.contains(id)
            })
        {
            return Err(invalid("external handoff belongs to another execution"));
        }
        let work = self.executions.get(&params.execution_id);
        if let Some(work) = work {
            if work.settled {
                return Err(invalid("external execution is already settled"));
            }
        } else if self.executions.len() >= MAX_EXECUTIONS {
            return Err(invalid("external execution capacity reached"));
        }
        if let ExternalRealtimeEvent::WorkSettled { handoff_ids }
        | ExternalRealtimeEvent::WorkFailed { handoff_ids, .. }
        | ExternalRealtimeEvent::WorkCancelled { handoff_ids } = &params.event
        {
            let unique: HashSet<_> = handoff_ids.iter().collect();
            if params
                .handoff_id
                .as_ref()
                .is_some_and(|id| !unique.contains(id))
                || handoff_ids.iter().any(|id| {
                    self.executions.iter().any(|(execution, work)| {
                        execution != &params.execution_id && work.handoffs.contains(id)
                    })
                })
                || unique.len() != handoff_ids.len()
                || handoff_ids.iter().any(|id| !self.handoffs.contains(id))
                || work.is_some_and(|work| work.handoffs.iter().any(|id| !unique.contains(id)))
            {
                return Err(invalid(
                    "terminal external event must contain every execution handoff exactly once",
                ));
            }
        }
        match &params.event {
            ExternalRealtimeEvent::ItemStarted { item_id, text, .. } => {
                if item_id.is_empty()
                    || item_id.len() > 256
                    || text
                        .as_ref()
                        .is_some_and(|text| text.len() > MAX_TEXT_BYTES)
                {
                    return Err(invalid("invalid external item identity or oversized text"));
                }
                if work.is_some_and(|work| {
                    work.items.contains_key(item_id)
                        || work.ended.contains(item_id)
                        || work.items.len() + work.ended.len() >= MAX_ITEMS
                }) {
                    return Err(invalid("duplicate or excessive external item"));
                }
            }
            ExternalRealtimeEvent::ItemDelta { item_id, delta } => {
                let item = work
                    .and_then(|work| work.items.get(item_id))
                    .ok_or_else(|| invalid("external delta requires an open item"))?;
                if item.handoff_id != params.handoff_id {
                    return Err(invalid("external item changed handoff"));
                }
                if item.text.len().saturating_add(delta.len()) > MAX_TEXT_BYTES {
                    return Err(invalid("external item text capacity reached"));
                }
            }
            ExternalRealtimeEvent::ItemEnded {
                item_id,
                text,
                phase,
            } => {
                let item = work
                    .and_then(|work| work.items.get(item_id))
                    .ok_or_else(|| invalid("external end requires an open item"))?;
                if item.handoff_id != params.handoff_id {
                    return Err(invalid("external item changed handoff"));
                }
                if text.len() > MAX_TEXT_BYTES {
                    return Err(invalid("external item text capacity reached"));
                }
                let emitted = item
                    .stream
                    .as_ref()
                    .is_some_and(|stream| stream.sent_bytes > 0);
                if emitted
                    && (!text.starts_with(&item.text) || (phase.is_some() && phase != &item.phase))
                {
                    return Err(invalid(
                        "external final text or phase contradicts already streamed output",
                    ));
                }
            }
            ExternalRealtimeEvent::WorkSettled { .. } => {
                if work.is_some_and(|work| !work.items.is_empty()) {
                    return Err(invalid("external work cannot settle with open items"));
                }
            }
            ExternalRealtimeEvent::WorkFailed { message, .. } => {
                if message.len() > MAX_TEXT_BYTES {
                    return Err(invalid("external failure text capacity reached"));
                }
            }
            ExternalRealtimeEvent::WorkCancelled { .. } => {}
        }
        Ok(Some(bytes))
    }

    fn output(
        &mut self,
        params: &ExternalRealtimeEventParams,
        handoff: &RealtimeHandoffState,
    ) -> Vec<RealtimeOutbound> {
        let work = self
            .executions
            .entry(params.execution_id.clone())
            .or_default();
        if let Some(id) = &params.handoff_id {
            work.handoffs.insert(id.clone());
        }
        let mut output = Vec::new();
        match &params.event {
            ExternalRealtimeEvent::ItemStarted {
                item_id,
                phase,
                text,
            } => {
                let text = text.clone().unwrap_or_default();
                let mut item = Item {
                    handoff_id: params.handoff_id.clone(),
                    text: text.clone(),
                    phase: phase.clone(),
                    stream: None,
                };
                if handoff.event_parser == RealtimeEventParser::FramelessBidi && phase.is_some() {
                    let mut stream = RealtimeStreamedItem {
                        handoff_id: params.handoff_id.clone().unwrap_or_default(),
                        phase: phase.clone(),
                        bem_channel_parser: None,
                        prefix_final_message: false,
                        sent_bytes: 0,
                        buffered_text: String::new(),
                        tail_text: String::new(),
                        truncated: false,
                        last_flush_at: Instant::now(),
                        flush_scheduled: false,
                    };
                    stream.push_text(&text);
                    if let Some(chunk) = stream.drain_stream_chunk() {
                        output.push(external_output(
                            params.handoff_id.clone(),
                            chunk,
                            phase.clone(),
                            handoff,
                        ));
                    }
                    item.stream = Some(stream);
                }
                work.items.insert(item_id.clone(), item);
            }
            ExternalRealtimeEvent::ItemDelta { item_id, delta } => {
                if let Some(item) = work.items.get_mut(item_id) {
                    item.text.push_str(delta);
                    if let Some(stream) = &mut item.stream {
                        stream.push_text(delta);
                        if let Some(chunk) = stream.drain_stream_chunk() {
                            output.push(external_output(
                                params.handoff_id.clone(),
                                chunk,
                                item.phase.clone(),
                                handoff,
                            ));
                        }
                    }
                }
            }
            ExternalRealtimeEvent::ItemEnded {
                item_id,
                text,
                phase,
            } => {
                if let Some(mut item) = work.items.remove(item_id) {
                    let phase = phase.clone().or(item.phase);
                    if let Some(stream) = &mut item.stream {
                        if stream.sent_bytes == 0 {
                            stream.buffered_text.clear();
                            stream.tail_text.clear();
                            stream.truncated = false;
                            stream.push_text(text);
                        } else {
                            stream.push_text(&text[item.text.len()..]);
                        }
                        if let Some(chunk) = stream.drain_final_chunk() {
                            output.push(external_output(
                                params.handoff_id.clone(),
                                chunk,
                                phase,
                                handoff,
                            ));
                        }
                    } else if !text.is_empty() {
                        output.push(external_output(
                            params.handoff_id.clone(),
                            truncate_realtime_text_to_token_budget(
                                text,
                                REALTIME_ASSISTANT_OUTPUT_TOKEN_BUDGET,
                            ),
                            phase,
                            handoff,
                        ));
                    }
                }
                work.ended.insert(item_id.clone());
            }
            ExternalRealtimeEvent::WorkSettled { handoff_ids }
            | ExternalRealtimeEvent::WorkFailed { handoff_ids, .. }
            | ExternalRealtimeEvent::WorkCancelled { handoff_ids } => {
                // Failures/cancellation retire partial items without presenting them as completed
                // answers. Work settlement is explicit and never inferred from an item boundary.
                work.items.clear();
                work.handoffs.extend(handoff_ids.iter().cloned());
                work.settled = true;
                let message = match &params.event {
                    ExternalRealtimeEvent::WorkFailed { message, .. } => {
                        Some(format!("External agent failed: {message}"))
                    }
                    ExternalRealtimeEvent::WorkCancelled { .. } => {
                        Some("External agent work was cancelled.".to_string())
                    }
                    _ => None,
                };
                if let Some(message) = message {
                    output.push(external_output(
                        params.handoff_id.clone(),
                        truncate_realtime_text_to_token_budget(
                            &message,
                            REALTIME_ASSISTANT_OUTPUT_TOKEN_BUDGET,
                        ),
                        Some(MessagePhase::Commentary),
                        handoff,
                    ));
                }
                if handoff.event_parser != RealtimeEventParser::FramelessBidi {
                    for handoff_id in handoff_ids {
                        output.push(RealtimeOutbound::CompletedHandoff {
                            handoff_id: handoff_id.clone(),
                            text: if handoff.event_parser == RealtimeEventParser::V1 {
                                V1_EXTERNAL_WORK_ENDED.to_string()
                            } else {
                                REALTIME_V2_HANDOFF_COMPLETE_ACKNOWLEDGEMENT.to_string()
                            },
                            phase: None,
                        });
                    }
                }
            }
        }
        output
    }
}

fn external_output(
    handoff_id: Option<String>,
    text: String,
    phase: Option<MessagePhase>,
    handoff: &RealtimeHandoffState,
) -> RealtimeOutbound {
    let text = realtime_backend_output(text, handoff.session_kind);
    match handoff_id {
        // Item boundaries are not work boundaries, including on the legacy V1 transport.
        Some(handoff_id) => RealtimeOutbound::HandoffAppend {
            handoff_id,
            text,
            phase,
        },
        None => RealtimeOutbound::StandaloneHandoff {
            text: if handoff.event_parser == RealtimeEventParser::V1
                && !matches!(phase, Some(MessagePhase::Commentary))
            {
                format!("{AGENT_FINAL_MESSAGE_PREFIX}{text}")
            } else {
                text
            },
            phase,
        },
    }
}

impl RealtimeConversationManager {
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "external event validation, delivery, and replay commit must stay serial; shutdown closes the channel without this lock"
    )]
    pub(crate) async fn external_event(
        &self,
        params: ExternalRealtimeEventParams,
    ) -> CodexResult<bool> {
        // Capture only this incarnation's channel. Never hold the manager lock while a full
        // output queue waits: stop must remain able to close it, and a replacement must never
        // receive old feedback. The external lock serializes validation, delivery, and replay.
        let (external, handoff, active) = {
            let guard = self.state.lock().await;
            let state = guard
                .as_ref()
                .filter(|state| state.realtime_active.load(Ordering::Relaxed))
                .ok_or_else(|| invalid("conversation is not running"))?;
            let external = state
                .external
                .as_ref()
                .ok_or_else(|| invalid("conversation has no external orchestrator"))?;
            (
                Arc::clone(external),
                state.handoff.clone(),
                Arc::clone(&state.realtime_active),
            )
        };
        let mut external = external.lock().await;
        if !active.load(Ordering::Relaxed) {
            return Err(invalid("conversation is not running"));
        }
        let Some(bytes) = external.validate(&params)? else {
            return Ok(false);
        };
        for output in external.output(&params, &handoff) {
            if handoff.output_tx.send(output).await.is_err() {
                external.failed = true;
                return Err(invalid("external feedback transport is closed"));
            }
        }
        external.replay_bytes += bytes;
        external.events.push(params);
        Ok(true)
    }
}

#[cfg(test)]
#[path = "external_tests.rs"]
mod tests;
