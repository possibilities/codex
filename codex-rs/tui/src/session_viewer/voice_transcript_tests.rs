use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::io::Write;

fn header() -> Value {
    json!({"type":"voice_transcript","format":"agentvoice","workspace":"/tmp","threadId":"thread"})
}
fn event(name: &str, generation: u64, item: Value) -> Value {
    let mut data = json!({"instanceId":"controller","generation":generation,"threadId":"thread"});
    if name == "voice.item.transcript.delta" {
        data["itemId"] = item["id"].clone();
        data["delta"] = item["text"].clone();
    } else {
        data["item"] = item;
    }
    json!({"v":2,"type":"event","event":name,"data":data})
}
fn item(id: &str, role: &str, text: &str) -> Value {
    json!({"id":id,"realtimeSessionId":"call","type":"transcriptSegment","role":role,"text":text})
}
fn feed(transcript: &mut Transcript, value: Value) {
    transcript
        .accept(&serde_json::to_vec(&value).unwrap())
        .unwrap();
}
#[test]
fn interleaving_completion_correction_and_generation_identity() {
    let mut transcript = Transcript::default();
    feed(&mut transcript, header());
    for (name, generation, value) in [
        ("voice.item.started", 1, item("a", "user", "")),
        ("voice.item.started", 1, item("b", "assistant", "")),
        ("voice.item.transcript.delta", 1, item("a", "user", "Hi")),
        (
            "voice.item.transcript.delta",
            1,
            item("b", "assistant", "Hello"),
        ),
        ("voice.item.completed", 1, item("a", "user", "Hi 雪")),
        (
            "voice.item.completed",
            1,
            item("b", "assistant", "Hello back"),
        ),
        (
            "voice.item.completed",
            1,
            item("b", "assistant", "Hello corrected"),
        ),
        (
            "voice.item.transcript.delta",
            1,
            item("b", "assistant", "late duplicate"),
        ),
        ("voice.item.completed", 2, item("a", "user", "New runtime")),
    ] {
        feed(&mut transcript, event(name, generation, value));
    }
    assert_eq!(
        transcript.messages,
        vec![
            Message {
                role: Some("user".into()),
                text: "Hi 雪".into(),
                status: Status::Complete
            },
            Message {
                role: Some("assistant".into()),
                text: "Hello corrected".into(),
                status: Status::Complete
            },
            Message {
                role: Some("user".into()),
                text: "New runtime".into(),
                status: Status::Complete
            },
        ]
    );
    let original = transcript.cells();
    let refreshed = transcript.cells();
    assert!(
        original
            .iter()
            .zip(refreshed)
            .all(|(a, b)| Arc::ptr_eq(a, &b))
    );
}
#[test]
fn unknown_speaker_waits_for_completion_and_gap_marks_incomplete() {
    let mut transcript = Transcript::default();
    feed(&mut transcript, header());
    feed(
        &mut transcript,
        event(
            "voice.item.transcript.delta",
            1,
            item("a", "assistant", "unknown"),
        ),
    );
    assert!(transcript.cells().is_empty());
    feed(
        &mut transcript,
        event(
            "voice.item.completed",
            1,
            item("a", "assistant", "Canonical"),
        ),
    );
    feed(
        &mut transcript,
        event("voice.item.started", 1, item("b", "user", "Interrupted")),
    );
    feed(
        &mut transcript,
        json!({"type":"recording.gap","reason":"runtime_unavailable"}),
    );
    assert_eq!(transcript.messages[1].status, Status::Incomplete);
    assert_eq!(transcript.cells().len(), 2);
}
#[test]
fn follows_split_utf8_and_refuses_truncation_and_invalid_lines() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "{}", header()).unwrap();
    let mut source = VoiceFile::open(file.path()).unwrap();
    assert!(source.refresh().unwrap());
    let record = format!(
        "{}\n",
        event("voice.item.completed", 1, item("a", "user", "雪"))
    );
    let split = record.find('雪').unwrap() + 1;
    file.write_all(&record.as_bytes()[..split]).unwrap();
    assert!(!source.refresh().unwrap());
    file.write_all(&record.as_bytes()[split..]).unwrap();
    assert!(source.refresh().unwrap());
    assert_eq!(source.transcript.messages[0].text, "雪");
    file.as_file().set_len(0).unwrap();
    assert!(
        source
            .refresh()
            .unwrap_err()
            .to_string()
            .contains("truncated")
    );
    assert!(Transcript::default().accept(b"{}").is_err());
}
#[test]
fn voice_cells_snapshot() {
    let mut transcript = Transcript::default();
    feed(&mut transcript, header());
    feed(&mut transcript, json!({"type":"recording.started"}));
    feed(
        &mut transcript,
        event(
            "voice.item.completed",
            1,
            item("a", "user", "  Can we save this conversation?  "),
        ),
    );
    feed(
        &mut transcript,
        event(
            "voice.item.completed",
            1,
            item(
                "b",
                "assistant",
                "Yes. The **voice recording** stays in your JSONL file.",
            ),
        ),
    );
    feed(
        &mut transcript,
        event(
            "voice.item.started",
            1,
            item("c", "user", "And follow it live"),
        ),
    );
    feed(
        &mut transcript,
        json!({"type":"recording.gap","reason":"disconnected"}),
    );
    let keymap = crate::keymap::RuntimeKeymap::from_config(&Default::default())
        .unwrap()
        .pager;
    let mut viewport =
        super::super::viewport::ConversationViewport::new(transcript.cells(), keymap);
    let area = ratatui::layout::Rect::new(0, 0, 58, 13);
    let mut buffer = ratatui::buffer::Buffer::empty(area);
    viewport.render(area, &mut buffer);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .collect();
    insta::assert_snapshot!("voice_conversation", rows.join("\n"));
}

#[test]
fn rejects_wrong_conversation_and_oversized_text() {
    let mut transcript = Transcript::default();
    feed(&mut transcript, header());
    let mut wrong = event("voice.item.completed", 1, item("a", "user", "foreign"));
    wrong["data"]["threadId"] = json!("different");
    assert!(
        transcript
            .accept(&serde_json::to_vec(&wrong).unwrap())
            .unwrap_err()
            .to_string()
            .contains("identity")
    );
    let large = event(
        "voice.item.completed",
        1,
        item("a", "user", &"x".repeat(MAX_TEXT + 1)),
    );
    assert!(
        transcript
            .accept(&serde_json::to_vec(&large).unwrap())
            .unwrap_err()
            .to_string()
            .contains("limit")
    );
}

#[cfg(unix)]
#[test]
fn refuses_replaced_file_even_with_same_length() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("voice.jsonl");
    let replacement = directory.path().join("replacement.jsonl");
    let contents = format!("{}\n", header());
    std::fs::write(&path, &contents).unwrap();
    let mut source = VoiceFile::open(&path).unwrap();
    source.refresh().unwrap();
    std::fs::write(&replacement, contents).unwrap();
    std::fs::rename(replacement, path).unwrap();
    assert!(
        source
            .refresh()
            .unwrap_err()
            .to_string()
            .contains("replaced")
    );
}
