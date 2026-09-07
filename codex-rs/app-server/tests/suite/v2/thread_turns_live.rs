use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::AgentMessageDeltaNotification;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use core_test_support::responses;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::time::timeout;

#[cfg(windows)]
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
#[cfg(not(windows))]
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test]
async fn turns_list_includes_streaming_assistant_text_in_latest_page() -> Result<()> {
    let (release_stream, stream_gate) = oneshot::channel();
    let (server, _completions) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![
                responses::ev_response_created("resp-1"),
                responses::ev_assistant_message("msg-1", "first response"),
                responses::ev_completed("resp-1"),
            ]),
        }],
        vec![
            StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![
                    responses::ev_response_created("resp-2"),
                    responses::ev_message_item_added("msg-2", ""),
                    responses::ev_output_text_delta("partial response"),
                ]),
            },
            StreamingSseChunk {
                gate: Some(stream_gate),
                body: responses::sse(vec![
                    responses::ev_assistant_message("msg-2", "partial response"),
                    responses::ev_completed("resp-2"),
                ]),
            },
        ],
    ])
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(server.uri()).write(codex_home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let start_id = app
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            model: Some("mock-model".to_string()),
            history_mode: Some(ThreadHistoryMode::Paginated),
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(READ_TIMEOUT, app.read_response(start_id)).await??;

    let TurnStartResponse { turn: first_turn } = app
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                input: vec![UserInput::Text {
                    text: "first".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    timeout(
        READ_TIMEOUT,
        app.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let TurnStartResponse { turn: active_turn } = app
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                input: vec![UserInput::Text {
                    text: "second".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    let delta: AgentMessageDeltaNotification = timeout(
        READ_TIMEOUT,
        app.read_notification("item/agentMessage/delta"),
    )
    .await??;
    assert_eq!(
        delta,
        AgentMessageDeltaNotification {
            thread_id: thread.id.clone(),
            turn_id: active_turn.id.clone(),
            item_id: "msg-2".to_string(),
            delta: "partial response".to_string(),
        }
    );

    let latest_two: ThreadTurnsListResponse = app
        .request(|request_id| ClientRequest::ThreadTurnsList {
            request_id,
            params: ThreadTurnsListParams {
                thread_id: thread.id.clone(),
                cursor: None,
                limit: Some(2),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            },
        })
        .await?;
    assert_eq!(
        latest_two
            .data
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec![active_turn.id.as_str(), first_turn.id.as_str()]
    );

    let latest: ThreadTurnsListResponse = app
        .request(|request_id| ClientRequest::ThreadTurnsList {
            request_id,
            params: ThreadTurnsListParams {
                thread_id: thread.id.clone(),
                cursor: None,
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            },
        })
        .await?;
    assert_eq!(latest.data.len(), 1);
    assert_eq!(latest.data[0].id, active_turn.id);
    assert_eq!(latest.data[0].status, TurnStatus::InProgress);
    assert_eq!(latest.data[0].items_view, TurnItemsView::Full);
    assert_eq!(
        latest.data[0]
            .items
            .iter()
            .filter(|item| matches!(item, ThreadItem::AgentMessage { .. }))
            .cloned()
            .collect::<Vec<_>>(),
        vec![ThreadItem::AgentMessage {
            id: "msg-2".to_string(),
            text: "partial response".to_string(),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        }]
    );

    let next_cursor = latest
        .next_cursor
        .expect("latest page should link to the completed turn");
    let previous: ThreadTurnsListResponse = app
        .request(|request_id| ClientRequest::ThreadTurnsList {
            request_id,
            params: ThreadTurnsListParams {
                thread_id: thread.id.clone(),
                cursor: Some(next_cursor),
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            },
        })
        .await?;
    assert_eq!(previous.data.len(), 1);
    assert_eq!(previous.data[0].id, first_turn.id);

    release_stream
        .send(())
        .expect("streaming response should still be waiting");
    timeout(
        READ_TIMEOUT,
        app.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    server.shutdown().await;
    Ok(())
}
