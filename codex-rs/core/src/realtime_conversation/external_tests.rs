use super::*;
use pretty_assertions::assert_eq;

fn params(
    state: &ExternalRealtimeState,
    sequence: u64,
    event: ExternalRealtimeEvent,
) -> ExternalRealtimeEventParams {
    ExternalRealtimeEventParams {
        incarnation_id: state.incarnation_id.clone(),
        handoff_id: None,
        execution_id: "work".to_string(),
        sequence,
        event,
    }
}

fn handoff(parser: RealtimeEventParser) -> (RealtimeHandoffState, Receiver<RealtimeOutbound>) {
    let (output_tx, output_rx) = async_channel::bounded(64);
    (
        RealtimeHandoffState {
            external_orchestrator: true,
            output_tx,
            last_output: Arc::new(Mutex::new(None)),
            stream: Arc::new(Mutex::new(RealtimeHandoffStreamState::default())),
            client_managed_handoffs: false,
            codex_responses_as_items: false,
            codex_response_item_prefix: None,
            codex_response_handoff_mode: CodexResponseHandoffMode::Thinking,
            codex_response_handoff_channel_prefixes: Arc::new(BTreeMap::new()),
            session_kind: if parser == RealtimeEventParser::RealtimeV2 {
                RealtimeSessionKind::V2
            } else {
                RealtimeSessionKind::V1
            },
            event_parser: parser,
        },
        output_rx,
    )
}

fn apply(
    state: &mut ExternalRealtimeState,
    params: ExternalRealtimeEventParams,
    handoff: &RealtimeHandoffState,
) -> CodexResult<Vec<RealtimeOutbound>> {
    let Some(bytes) = state.validate(&params)? else {
        return Ok(Vec::new());
    };
    let output = state.output(&params, handoff);
    state.replay_bytes += bytes;
    state.events.push(params);
    Ok(output)
}

#[test]
fn replay_is_idempotent_but_conflicts_gaps_and_stale_incarnations_fail() {
    let mut state = ExternalRealtimeState::new();
    let (handoff, _) = handoff(RealtimeEventParser::V1);
    let p = params(
        &state,
        1,
        ExternalRealtimeEvent::ItemStarted {
            item_id: "item".into(),
            phase: None,
            text: None,
        },
    );
    apply(&mut state, p.clone(), &handoff).unwrap();
    assert_eq!(state.validate(&p).unwrap(), None);
    let mut changed = p.clone();
    changed.execution_id = "other".into();
    assert!(state.validate(&changed).is_err());
    changed = p.clone();
    changed.sequence = 3;
    assert!(state.validate(&changed).is_err());
    changed = p;
    changed.incarnation_id = "old".into();
    assert!(state.validate(&changed).is_err());
}

#[test]
fn phase_arriving_at_end_is_authoritative_and_does_not_settle_work() {
    let mut state = ExternalRealtimeState::new();
    let (handoff, _) = handoff(RealtimeEventParser::FramelessBidi);
    let start = params(
        &state,
        1,
        ExternalRealtimeEvent::ItemStarted {
            item_id: "item".into(),
            phase: None,
            text: Some("hel".into()),
        },
    );
    assert_eq!(apply(&mut state, start, &handoff).unwrap(), vec![]);
    let delta = params(
        &state,
        2,
        ExternalRealtimeEvent::ItemDelta {
            item_id: "item".into(),
            delta: "lo".into(),
        },
    );
    assert_eq!(apply(&mut state, delta, &handoff).unwrap(), vec![]);
    let end = params(
        &state,
        3,
        ExternalRealtimeEvent::ItemEnded {
            item_id: "item".into(),
            text: "hello!".into(),
            phase: Some(MessagePhase::Commentary),
        },
    );
    assert_eq!(
        apply(&mut state, end, &handoff).unwrap(),
        vec![RealtimeOutbound::StandaloneHandoff {
            text: "hello!".into(),
            phase: Some(MessagePhase::Commentary)
        }]
    );
    assert!(!state.executions["work"].settled);
    let next = params(
        &state,
        4,
        ExternalRealtimeEvent::ItemStarted {
            item_id: "second-step".into(),
            phase: None,
            text: None,
        },
    );
    apply(&mut state, next, &handoff).unwrap();
    let settle = params(
        &state,
        5,
        ExternalRealtimeEvent::WorkSettled {
            handoff_ids: vec![],
        },
    );
    assert!(state.validate(&settle).is_err());
}

#[test]
fn streamed_final_suffix_flushes_and_corrections_fail_after_emission() {
    let mut state = ExternalRealtimeState::new();
    let (handoff, _) = handoff(RealtimeEventParser::FramelessBidi);
    let start = params(
        &state,
        1,
        ExternalRealtimeEvent::ItemStarted {
            item_id: "item".into(),
            phase: Some(MessagePhase::FinalAnswer),
            text: Some("hello".into()),
        },
    );
    assert_eq!(
        apply(&mut state, start, &handoff).unwrap(),
        vec![RealtimeOutbound::StandaloneHandoff {
            text: "hello".into(),
            phase: Some(MessagePhase::FinalAnswer)
        }]
    );
    let bad = params(
        &state,
        2,
        ExternalRealtimeEvent::ItemEnded {
            item_id: "item".into(),
            text: "goodbye".into(),
            phase: None,
        },
    );
    assert!(state.validate(&bad).is_err());
    let end = params(
        &state,
        2,
        ExternalRealtimeEvent::ItemEnded {
            item_id: "item".into(),
            text: "hello world".into(),
            phase: None,
        },
    );
    assert_eq!(
        apply(&mut state, end, &handoff).unwrap(),
        vec![RealtimeOutbound::StandaloneHandoff {
            text: " world".into(),
            phase: Some(MessagePhase::FinalAnswer)
        }]
    );
}

#[test]
fn whole_work_closes_multiple_handoffs_including_no_text_work() {
    let mut state = ExternalRealtimeState::new();
    let (handoff, _) = handoff(RealtimeEventParser::RealtimeV2);
    state.handoffs.extend(["one".into(), "two".into()]);
    let settle = params(
        &state,
        1,
        ExternalRealtimeEvent::WorkSettled {
            handoff_ids: vec!["one".into(), "two".into()],
        },
    );
    assert_eq!(
        apply(&mut state, settle, &handoff).unwrap(),
        vec![
            RealtimeOutbound::CompletedHandoff {
                handoff_id: "one".into(),
                text: REALTIME_V2_HANDOFF_COMPLETE_ACKNOWLEDGEMENT.to_string(),
                phase: None
            },
            RealtimeOutbound::CompletedHandoff {
                handoff_id: "two".into(),
                text: REALTIME_V2_HANDOFF_COMPLETE_ACKNOWLEDGEMENT.to_string(),
                phase: None
            },
        ]
    );
    assert!(state.executions["work"].settled);
}

#[test]
fn raw_handoff_and_final_tail_preserve_identity_and_transcript() {
    let mut state = ExternalRealtimeState::new();
    let transcript = vec![RealtimeTranscriptEntry {
        role: "user".into(),
        text: "a <tag> and 🐈".into(),
    }];
    let event = state
        .record_handoff(
            &RealtimeHandoffRequested {
                handoff_id: "h".into(),
                item_id: "i".into(),
                input_transcript: "raw".into(),
                active_transcript: transcript.clone(),
            },
            Some("session".into()),
        )
        .unwrap();
    assert_eq!(
        event,
        ExternalRealtimeHandoffEvent {
            incarnation_id: state.incarnation_id.clone(),
            realtime_session_id: Some("session".into()),
            handoff_id: "h".into(),
            item_id: Some("i".into()),
            source: ExternalRealtimeHandoffSource::Handoff,
            input_transcript: "raw".into(),
            active_transcript: transcript.clone(),
            transcript_tail: None
        }
    );
    let tail = state.transcript_tail(transcript.clone(), None);
    assert_eq!(tail.transcript_tail, Some(transcript));
    assert_eq!(tail.item_id, None);
}

#[test]
fn no_text_settlement_closes_v1_and_v2_but_not_v3() {
    for parser in [
        RealtimeEventParser::V1,
        RealtimeEventParser::RealtimeV2,
        RealtimeEventParser::FramelessBidi,
    ] {
        let mut state = ExternalRealtimeState::new();
        let (handoff, _) = handoff(parser);
        state.handoffs.insert("voice".into());
        let settled = params(
            &state,
            1,
            ExternalRealtimeEvent::WorkSettled {
                handoff_ids: vec!["voice".into()],
            },
        );
        let expected = if parser == RealtimeEventParser::FramelessBidi {
            vec![]
        } else {
            vec![RealtimeOutbound::CompletedHandoff {
                handoff_id: "voice".into(),
                text: if parser == RealtimeEventParser::V1 {
                    V1_EXTERNAL_WORK_ENDED.to_string()
                } else {
                    REALTIME_V2_HANDOFF_COMPLETE_ACKNOWLEDGEMENT.to_string()
                },
                phase: None,
            }]
        };
        assert_eq!(apply(&mut state, settled, &handoff).unwrap(), expected);
    }
}

#[test]
fn final_text_correction_before_streaming_and_unicode_truncation_are_safe() {
    let mut state = ExternalRealtimeState::new();
    let (handoff, _) = handoff(RealtimeEventParser::FramelessBidi);
    let start = params(
        &state,
        1,
        ExternalRealtimeEvent::ItemStarted {
            item_id: "item".into(),
            phase: None,
            text: Some("discarded".into()),
        },
    );
    apply(&mut state, start, &handoff).unwrap();
    let text = format!("{}TAIL 🐈", "é🐈".repeat(4000));
    let end = params(
        &state,
        2,
        ExternalRealtimeEvent::ItemEnded {
            item_id: "item".into(),
            text,
            phase: Some(MessagePhase::FinalAnswer),
        },
    );
    let output = apply(&mut state, end, &handoff).unwrap();
    let [RealtimeOutbound::StandaloneHandoff { text, phase }] = output.as_slice() else {
        panic!("expected one bounded final item");
    };
    assert!(approx_token_count(text) <= REALTIME_ASSISTANT_OUTPUT_TOKEN_BUDGET);
    assert!(text.ends_with("TAIL 🐈"));
    assert_eq!(phase, &Some(MessagePhase::FinalAnswer));
    let late_delta = params(
        &state,
        3,
        ExternalRealtimeEvent::ItemDelta {
            item_id: "item".into(),
            delta: "late".into(),
        },
    );
    assert!(state.validate(&late_delta).is_err());
}

#[test]
fn cancellation_is_explicit_and_handoffs_cannot_move_between_executions() {
    let mut state = ExternalRealtimeState::new();
    let (handoff, _) = handoff(RealtimeEventParser::V1);
    state.handoffs.insert("voice".into());
    let mut start = params(
        &state,
        1,
        ExternalRealtimeEvent::ItemStarted {
            item_id: "item".into(),
            phase: None,
            text: None,
        },
    );
    start.handoff_id = Some("voice".into());
    apply(&mut state, start, &handoff).unwrap();
    let cancel = params(
        &state,
        2,
        ExternalRealtimeEvent::WorkCancelled {
            handoff_ids: vec!["voice".into()],
        },
    );
    let output = apply(&mut state, cancel.clone(), &handoff).unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(state.validate(&cancel).unwrap(), None);
    let mut theft = params(
        &state,
        3,
        ExternalRealtimeEvent::WorkSettled {
            handoff_ids: vec!["voice".into()],
        },
    );
    theft.execution_id = "different".into();
    assert!(state.validate(&theft).is_err());
}

#[tokio::test]
async fn stop_closes_a_full_external_output_queue_without_locking_out_shutdown() {
    let manager = Arc::new(RealtimeConversationManager::new());
    let external = Arc::new(Mutex::new(ExternalRealtimeState::new()));
    let incarnation = external.lock().await.incarnation_id.clone();
    let (handoff, output_rx) = handoff(RealtimeEventParser::V1);
    for _ in 0..64 {
        handoff
            .output_tx
            .try_send(RealtimeOutbound::StandaloneSpeech {
                text: String::new(),
            })
            .unwrap();
    }
    let stop_token = CancellationToken::new();
    let input_stop = stop_token.clone();
    let input_task = tokio::spawn(async move {
        input_stop.cancelled().await;
        drop(output_rx);
    });
    let (audio_tx, _) = async_channel::bounded(1);
    let (text_tx, _) = async_channel::bounded(1);
    *manager.state.lock().await = Some(ConversationState {
        audio_tx,
        text_tx,
        session_kind: RealtimeSessionKind::V1,
        handoff,
        input_task,
        fanout_task: None,
        realtime_active: Arc::new(AtomicBool::new(true)),
        stop_token,
        external: Some(Arc::clone(&external)),
    });
    let start = ExternalRealtimeEventParams {
        incarnation_id: incarnation.clone(),
        handoff_id: None,
        execution_id: "work".into(),
        sequence: 1,
        event: ExternalRealtimeEvent::ItemStarted {
            item_id: "item".into(),
            phase: None,
            text: None,
        },
    };
    assert!(manager.external_event(start).await.unwrap());
    let pending_manager = Arc::clone(&manager);
    let pending = tokio::spawn(async move {
        pending_manager
            .external_event(ExternalRealtimeEventParams {
                incarnation_id: incarnation,
                handoff_id: None,
                execution_id: "work".into(),
                sequence: 2,
                event: ExternalRealtimeEvent::ItemEnded {
                    item_id: "item".into(),
                    text: "result".into(),
                    phase: None,
                },
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while external.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(pending.await.unwrap().is_err());
    assert_eq!(manager.running_state().await, None);
}
