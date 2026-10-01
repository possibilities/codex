use super::*;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadRealtimeExternalCapabilitiesParams;
use codex_app_server_protocol::ThreadRealtimeExternalCapabilitiesResponse;
use codex_app_server_protocol::ThreadRealtimeExternalEventParams;
use codex_app_server_protocol::ThreadRealtimeExternalEventResponse;
use codex_app_server_protocol::ThreadRealtimeExternalHandoffNotification;
use pretty_assertions::assert_eq;

async fn external_event(
    harness: &mut RealtimeE2eHarness,
    incarnation: &str,
    sequence: u64,
    event: Value,
) -> Result<ThreadRealtimeExternalEventResponse> {
    let params: ThreadRealtimeExternalEventParams = serde_json::from_value(json!({
        "threadId": harness.thread_id, "incarnationId": incarnation, "sequence": sequence,
        "executionId": "work", "handoffId": "handoff_one", "event": event,
    }))?;
    let request_id = harness
        .mcp
        .send_thread_realtime_external_event_request(params)
        .await?;
    harness.mcp.read_response(request_id).await
}

async fn external_event_error(
    harness: &mut RealtimeE2eHarness,
    incarnation: &str,
    sequence: u64,
    event: Value,
) -> Result<JSONRPCError> {
    let params: ThreadRealtimeExternalEventParams = serde_json::from_value(json!({
        "threadId": harness.thread_id, "incarnationId": incarnation, "sequence": sequence,
        "executionId": "work", "handoffId": "handoff_one", "event": event,
    }))?;
    let request_id = harness
        .mcp
        .send_thread_realtime_external_event_request(params)
        .await?;
    harness
        .mcp
        .read_stream_until_error_message(RequestId::Integer(request_id))
        .await
}

async fn start_external(
    harness: &mut RealtimeE2eHarness,
) -> Result<ThreadRealtimeStartedNotification> {
    let params: ThreadRealtimeStartParams = serde_json::from_value(json!({
        "threadId": harness.thread_id, "externalOrchestrator": true, "externalStartupContext": "OpenCode history",
        "flushTranscriptTailOnSessionEnd": true, "outputModality": "audio", "realtimeSessionId": "same-session", "version": "v2",
    }))?;
    let _: ThreadRealtimeStartResponse = harness
        .mcp
        .request(|request_id| ClientRequest::ThreadRealtimeStart { request_id, params })
        .await?;
    harness.read_notification("thread/realtime/started").await
}

#[tokio::test]
async fn external_realtime_routes_exclusively_replays_and_fences_restarts() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let script = vec![vec![
        session_updated("same-session"),
        v2_background_agent_tool_call("handoff_one", "raw <request>"),
        v2_background_agent_tool_call("handoff_two", "steer"),
    ]];
    let mut harness = RealtimeE2eHarness::new(
        RealtimeTestVersion::V2,
        no_main_loop_responses(),
        realtime_sideband(vec![
            open_realtime_sideband_connection(script.clone()),
            open_realtime_sideband_connection(script),
        ]),
    )
    .await?;
    let capabilities: ThreadRealtimeExternalCapabilitiesResponse = harness
        .mcp
        .request(
            |request_id| ClientRequest::ThreadRealtimeExternalCapabilities {
                request_id,
                params: ThreadRealtimeExternalCapabilitiesParams {},
            },
        )
        .await?;
    assert_eq!(
        capabilities,
        ThreadRealtimeExternalCapabilitiesResponse {
            protocol_version: 1
        }
    );
    let started = start_external(&mut harness).await?;
    let incarnation = started.incarnation_id.context("external incarnation")?;
    let one: ThreadRealtimeExternalHandoffNotification = harness
        .read_notification("thread/realtime/externalHandoff")
        .await?;
    let two: ThreadRealtimeExternalHandoffNotification = harness
        .read_notification("thread/realtime/externalHandoff")
        .await?;
    assert_eq!(
        (
            one.incarnation_id,
            one.handoff_id,
            one.item_id,
            one.input_transcript
        ),
        (
            incarnation.clone(),
            "handoff_one".to_string(),
            Some("item_handoff_one".to_string()),
            "raw <request>".to_string()
        )
    );
    assert_eq!(two.handoff_id, "handoff_two");
    let initial = json!({"type":"itemStarted","itemId":"answer","phase":null,"text":"hel"});
    assert_eq!(
        external_event(&mut harness, &incarnation, 1, initial.clone()).await?,
        ThreadRealtimeExternalEventResponse { accepted: true }
    );
    assert_eq!(
        external_event(&mut harness, &incarnation, 1, initial).await?,
        ThreadRealtimeExternalEventResponse { accepted: false }
    );
    assert_eq!(
        external_event_error(
            &mut harness,
            &incarnation,
            1,
            json!({"type":"itemDelta","itemId":"answer","delta":"bad"})
        )
        .await?
        .error
        .code,
        -32600
    );
    external_event(
        &mut harness,
        &incarnation,
        2,
        json!({"type":"itemDelta","itemId":"answer","delta":"lo"}),
    )
    .await?;
    external_event(
        &mut harness,
        &incarnation,
        3,
        json!({"type":"itemEnded","itemId":"answer","text":"hello!","phase":"final_answer"}),
    )
    .await?;
    // Item end emits progress, but only the explicit terminal event closes either native call.
    let progress = harness.sideband_outbound_request(1).await;
    assert_eq!(progress["item"]["content"][0]["text"], "[BACKEND] hello!");
    external_event(
        &mut harness,
        &incarnation,
        4,
        json!({"type":"workSettled","handoffIds":["handoff_one","handoff_two"]}),
    )
    .await?;
    let mut completed = Vec::new();
    // The second response.create is queued until the first realtime response completes.
    for index in 2..5 {
        let request = harness.sideband_outbound_request(index).await;
        if request["item"]["type"] == "function_call_output" {
            completed.push(request["item"]["call_id"].clone());
        }
    }
    assert_eq!(completed, vec![json!("handoff_one"), json!("handoff_two")]);
    assert!(harness.main_loop_responses_requests().await?.is_empty());
    let params = ThreadRealtimeStopParams {
        thread_id: harness.thread_id.clone(),
    };
    let _: ThreadRealtimeStopResponse = harness
        .mcp
        .request(|request_id| ClientRequest::ThreadRealtimeStop { request_id, params })
        .await?;
    let closed: ThreadRealtimeClosedNotification =
        harness.read_notification("thread/realtime/closed").await?;
    assert_eq!(closed.incarnation_id.as_deref(), Some(incarnation.as_str()));
    let replacement = start_external(&mut harness).await?;
    assert_ne!(
        replacement.incarnation_id.as_deref(),
        Some(incarnation.as_str())
    );
    assert_eq!(
        external_event_error(
            &mut harness,
            &incarnation,
            5,
            json!({"type":"workCancelled","handoffIds":["handoff_one"]})
        )
        .await?
        .error
        .code,
        -32600
    );
    assert!(harness.main_loop_responses_requests().await?.is_empty());
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn external_realtime_requires_external_context_before_connecting() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let mut harness = RealtimeE2eHarness::new(
        RealtimeTestVersion::V2,
        no_main_loop_responses(),
        realtime_sideband(vec![]),
    )
    .await?;
    let params: ThreadRealtimeStartParams = serde_json::from_value(
        json!({"threadId":harness.thread_id,"externalOrchestrator":true,"outputModality":"audio"}),
    )?;
    let request_id = harness
        .mcp
        .send_thread_realtime_start_request(params)
        .await?;
    let error = harness
        .mcp
        .read_stream_until_error_message(RequestId::Integer(request_id))
        .await?;
    assert_eq!(error.error.code, -32600);
    assert!(harness.main_loop_responses_requests().await?.is_empty());
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn external_realtime_v3_preserves_provider_phase_and_flushes_final_suffix() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let mut harness = RealtimeE2eHarness::new(
        RealtimeTestVersion::V1, no_main_loop_responses(),
        realtime_sideband(vec![open_realtime_sideband_connection(vec![vec![
            session_started("v3-external"),
            json!({"type":"delegation.created","offset_ms":100,"item":{"id":"handoff_one","type":"delegation","target":"client","content":[{"type":"input_text","text":"help"}]}}),
        ]])]),
    ).await?;
    let params: ThreadRealtimeStartParams = serde_json::from_value(json!({
        "threadId": harness.thread_id, "externalOrchestrator": true, "includeStartupContext": false,
        "outputModality": "audio", "version":"v3",
    }))?;
    let _: ThreadRealtimeStartResponse = harness
        .mcp
        .request(|request_id| ClientRequest::ThreadRealtimeStart { request_id, params })
        .await?;
    let started: ThreadRealtimeStartedNotification =
        harness.read_notification("thread/realtime/started").await?;
    let incarnation = started.incarnation_id.context("external incarnation")?;
    let _: ThreadRealtimeExternalHandoffNotification = harness
        .read_notification("thread/realtime/externalHandoff")
        .await?;
    external_event(
        &mut harness,
        &incarnation,
        1,
        json!({"type":"itemStarted","itemId":"comment","text":null,"phase":null}),
    )
    .await?;
    external_event(
        &mut harness,
        &incarnation,
        2,
        json!({"type":"itemDelta","itemId":"comment","delta":"checking"}),
    )
    .await?;
    external_event(
        &mut harness,
        &incarnation,
        3,
        json!({"type":"itemEnded","itemId":"comment","text":"checking now","phase":"commentary"}),
    )
    .await?;
    external_event(
        &mut harness,
        &incarnation,
        4,
        json!({"type":"itemStarted","itemId":"answer","text":"done","phase":"final_answer"}),
    )
    .await?;
    external_event(
        &mut harness,
        &incarnation,
        5,
        json!({"type":"itemEnded","itemId":"answer","text":"done!","phase":"final_answer"}),
    )
    .await?;
    external_event(
        &mut harness,
        &incarnation,
        6,
        json!({"type":"workSettled","handoffIds":["handoff_one"]}),
    )
    .await?;
    for (index, (text, channel)) in [
        ("checking now", "commentary"),
        ("done", "speakable"),
        ("!", "speakable"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            harness.sideband_outbound_request(index + 1).await,
            json!({
                "type":"delegation.context.append", "delegation_item_id":"handoff_one", "channel":channel,
                "content":[{"type":"input_text","text":text}],
            })
        );
    }
    assert!(harness.main_loop_responses_requests().await?.is_empty());
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn external_realtime_rejects_unmapped_backing_instructions_and_routing() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let mut harness = RealtimeE2eHarness::new(
        RealtimeTestVersion::V2,
        no_main_loop_responses(),
        realtime_sideband(vec![]),
    )
    .await?;
    for (field, value) in [
        ("realtimeStartInstructions", json!("external entry")),
        ("realtimeEndInstructions", json!("external exit")),
        ("codexResponseHandoffMode", json!("commentary")),
        (
            "codexResponseHandoffChannelPrefixes",
            json!({"final":["DONE:"]}),
        ),
        ("codexResponseItemPrefix", json!("prefix")),
        ("clientManagedHandoffs", json!(true)),
        ("codexResponsesAsItems", json!(true)),
    ] {
        let mut params = json!({"threadId":harness.thread_id,"externalOrchestrator":true,"includeStartupContext":false,"outputModality":"audio"});
        params[field] = value;
        let request_id = harness
            .mcp
            .send_thread_realtime_start_request(serde_json::from_value(params)?)
            .await?;
        let error = harness
            .mcp
            .read_stream_until_error_message(RequestId::Integer(request_id))
            .await?;
        assert_eq!(error.error.code, -32600, "{field}");
    }
    assert!(harness.main_loop_responses_requests().await?.is_empty());
    harness.shutdown().await;
    Ok(())
}
