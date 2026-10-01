//! Ordered, incarnation-fenced feedback from an external realtime backing agent.
use crate::models::MessagePhase;
use crate::protocol::ConversationStartParams;
use crate::protocol::RealtimeTranscriptEntry;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalRealtimeStartParams {
    pub conversation: ConversationStartParams,
    pub startup_context: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct ExternalRealtimeHandoffEvent {
    pub incarnation_id: String,
    pub realtime_session_id: Option<String>,
    pub handoff_id: String,
    pub item_id: Option<String>,
    pub source: ExternalRealtimeHandoffSource,
    pub input_transcript: String,
    pub active_transcript: Vec<RealtimeTranscriptEntry>,
    pub transcript_tail: Option<Vec<RealtimeTranscriptEntry>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub enum ExternalRealtimeHandoffSource {
    Handoff,
    TranscriptTail,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct ExternalRealtimeEventParams {
    pub incarnation_id: String,
    pub handoff_id: Option<String>,
    pub execution_id: String,
    pub sequence: u64,
    pub event: ExternalRealtimeEvent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, JsonSchema, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[ts(tag = "type", rename_all = "camelCase")]
pub enum ExternalRealtimeEvent {
    ItemStarted {
        #[ts(rename = "itemId")]
        item_id: String,
        phase: Option<MessagePhase>,
        text: Option<String>,
    },
    ItemDelta {
        #[ts(rename = "itemId")]
        item_id: String,
        delta: String,
    },
    ItemEnded {
        #[ts(rename = "itemId")]
        item_id: String,
        text: String,
        phase: Option<MessagePhase>,
    },
    WorkSettled {
        #[ts(rename = "handoffIds")]
        handoff_ids: Vec<String>,
    },
    WorkFailed {
        message: String,
        #[ts(rename = "handoffIds")]
        handoff_ids: Vec<String>,
    },
    WorkCancelled {
        #[ts(rename = "handoffIds")]
        handoff_ids: Vec<String>,
    },
}
