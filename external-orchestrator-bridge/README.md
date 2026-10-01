# Codex native voice / OpenCode V2 bridge

**Manual prerequisite:** this published branch needs the one-line test-fixture change described in [MANUAL-COMPATIBILITY.md](MANUAL-COMPATIBILITY.md) before the broader core integration-test build is complete. Automated publication of that existing fixture file was blocked; the included patch contains no credential material.

This experimental, dependency-free Node.js stdio proxy keeps Codex's native realtime transport while routing backing-agent work to a patched OpenCode V2 server. It requires **both fork patches**. It is not a replacement audio client and does not modify an installed desktop application.

## What is implemented

- Positive capability negotiation rejects unpatched Codex app-servers before starting voice
- Structured handoffs preserve the input transcript, active transcript, call-tail entries, native handoff ID and realtime incarnation
- OpenCode prompt IDs and host/session mappings are saved before network admission; exact retries reuse those identities
- Provider text start/delta/end boundaries, identity and known phase are preserved, with complete-text recovery after lost live deltas
- Durable per-session SSE owns replay cursors; live global SSE supplies ephemeral deltas and pending-request updates
- Only explicit whole-work settlement completes admissions; exact input ID sets prevent queued-input holes from completing unrelated work
- Per-incarnation sequence numbers, applied/replayed ACKs, exact retry receipts and native fences prevent duplicate or stale speech
- Voice stop does not interrupt OpenCode work; explicit turn interruption targets only its confirmed work ID
- Plain-text typed turns use the same OpenCode session, with native turn/item notifications, durable bridge history and server-side work-ID fences for steering/interruption
- Pending permissions and questions are recovered after reconnect and require explicit, session-scoped answers

## Important integration boundary

The public app-server API and bridge are testable here. Wiring the closed-source Codex desktop to this executable, native approval widgets, microphone/audio playback and authenticated end-to-end voice latency require a compatible client and separate runtime validation. **Do not treat these patches as verified desktop voice parity.**

Generic OpenCode permission actions cannot safely impersonate native shell/file approvals. This bridge exposes exact custom request/reply APIs and leaves work blocked if the client does not implement them. It never auto-approves. Multiple-selection questions retain all selections. Native shell execution, review, attachment inputs and model/security/environment overrides are rejected on externally bound threads rather than silently executed by a second agent.

The native thread transcript is not rewritten to pretend it owns OpenCode execution. Use `bridge/session/read` for external history. The bridge advertises its supported subset through `bridge/capabilities`. Guarded controls use `POST /api/session/:sessionID/work/:workID/prompt` and `/interrupt`, never a session-wide fallback. The server bounds pending guarded steers and rejects stale, closed or capacity-exhausted work.

## Run

Requirements:

- Node.js 22 or newer
- A Codex executable built with `thread/realtime/externalCapabilities` protocol version 1
- An OpenCode server built with `session.next.work.started` / `session.next.work.settled` and text correlation fields
- OpenCode's provider authentication already configured through its normal supported flow

Start the patched OpenCode server on loopback using its normal `serve` command. Set `OPENCODE_SERVER_PASSWORD` in both server and bridge environments if authentication is enabled. The server uses HTTP Basic authentication (`OPENCODE_SERVER_USERNAME`, default `opencode`), not a Bearer token.

Run the bridge as the app-server executable configured in your compatible client:

```sh
OPENCODE_DIRECTORY=/absolute/project/path \
OPENCODE_URL=http://127.0.0.1:4096 \
CODEX_COMMAND=/absolute/path/to/patched/codex \
node external-orchestrator-bridge/src/main.js
```

`CODEX_COMMAND` is one executable path/name, never a shell command. The bridge appends `app-server`. JSON-RPC uses newline-delimited JSON on stdin/stdout; diagnostics go to stderr.

The bridge forces its own upstream experimental API capability because these native realtime APIs are experimental. It verifies `thread/realtime/externalCapabilities` before forwarding the first realtime start and requires OpenCode `/api/health` to advertise `sessionWorkProtocolVersion:1`. Protected lifecycle notification opt-outs are removed upstream while the client’s downstream opt-outs remain respected.

### Configuration

| Variable                    | Default                    | Meaning                                                                                |
| --------------------------- | -------------------------- | -------------------------------------------------------------------------------------- |
| `OPENCODE_DIRECTORY`        | required                   | Absolute working directory for newly created OpenCode sessions                         |
| `OPENCODE_URL`              | `http://127.0.0.1:4096`    | Origin only; no path/query/embedded credentials                                        |
| `OPENCODE_SESSION_ID`       | generated per thread       | Adopt one existing session; cannot share it across host threads                        |
| `OPENCODE_SERVER_USERNAME`  | `opencode`                 | Basic auth username                                                                    |
| `OPENCODE_SERVER_PASSWORD`  | unset                      | Existing server credential; never saved in bridge state or passed to the child process |
| `OPENCODE_ALLOW_REMOTE`     | unset                      | `1` additionally permits remote HTTPS with a password                                  |
| `CODEX_COMMAND`             | `codex`                    | Patched native executable                                                              |
| `BRIDGE_STATE_DIRECTORY`    | `~/.codex-opencode-bridge` | Private bridge state directory                                                         |
| `BRIDGE_REQUEST_TIMEOUT_MS` | `15000`                    | Positive integer, at most 300000                                                       |
| `BRIDGE_MAX_FRAME_BYTES`    | `16777216`                 | Positive integer, at most 67108864                                                     |

Redirects are refused on every authenticated endpoint. Remote origins require explicit opt-in, HTTPS and authentication. Native Codex sandbox/approval settings do **not** configure the separate OpenCode server; configure that server's security policy explicitly. The bridge rejects unsupported per-turn policy overrides.

## Client extension contract

After ordinary app-server initialization, query `bridge/capabilities`. The proxy rewrites `thread/realtime/start` to enable external orchestration and supply a bounded recent OpenCode context (user and assistant text, excluding reasoning). Explicit `includeStartupContext:false` remains respected. Transcript-tail flushing defaults on unless explicitly disabled. Backing-agent start/end instruction overrides and alternate handoff routing overrides are unsupported and rejected rather than silently ignored.

### Typed work

`turn/start`, `turn/steer`, and `turn/interrupt` are intercepted once a thread is externally bound. Plain text input only is supported. Supply a stable `clientUserMessageId` to safely retry a request whose response was lost. `turn/steer.expectedTurnId` and `turn/interrupt.turnId` must match. Ordinary turn admission success means durably queued, not completed. Steering requires an observed authoritative work ID and `sessionWorkControlProtocolVersion:1`; it uses dedicated work-scoped endpoints that fail closed on older servers. A guarded steer waits for a safe provider/tool boundary and commits admission plus promotion atomically into that exact work. If the work closes first, the server rejects it without admitting it to a successor. Controls requested before promotion fail explicitly. Bun 1.3.14 may not report a wire disconnect to a waiting server handler, so disconnecting is not a cancellation guarantee. The server bounds a queued guard to 30 seconds; explicit work-scoped interruption is the cancellation action. A lost or timed-out response remains uncertain; retry the same `clientUserMessageId`, never mint a replacement just because a reply was lost. A turn containing multiple admitted inputs completes only after all of those inputs receive durable settlement.

### History and pending work

- `bridge/session/read {threadId, after?}` returns the OpenCode session ID, one durable history page, `hasMore`, and the bridge's typed-turn projection. Follow the last returned `durable.seq`; sequences may have gaps
- `bridge/requests/list {threadId}` refreshes pending permissions and questions
- `bridge/permission/requested` / `bridge/question/requested` notifications carry the complete original request and host thread ID
- `bridge/permission/reply {threadId, requestId, requestFingerprint, reply:"once"|"reject"}` answers one exact pending permission. Persistent `always` grants are deliberately unsupported
- `bridge/question/reply {threadId, requestId, requestFingerprint, answers:string[][]}` preserves ordered answers, including multiple selections
- `bridge/question/reject {threadId, requestId, requestFingerprint}` rejects the exact question
- `bridge/item` supplies authoritative text, item/work/provider IDs, phase and end state
- `bridge/event` exposes observational tool/work events; these are not instructions for the client to run the tools again
- `bridge/admission/retry {threadId, inputMessageId}` explicitly retries only a blocked, still-pending admission with the same exact ID and payload
- `bridge/status`, `bridge/work/blocked`, and `bridge/playback/error` expose outages and unresolved work

Request notifications and pending lists include a `requestFingerprint` that must be echoed with the decision; changed/replaced requests require a fresh decision. Permission replies also use an atomic server-side `expectedRequest` precondition to prevent an ID-reuse race between refresh and reply.

Permission responses cannot be inferred from voice transcript text. Clients must obtain the user's decision through their normal confirmation UI before invoking reply APIs.

## Recovery and limits

State includes conversation content and is stored in a mode-0700 directory with a mode-0600 file, atomic rename/fsync and one-writer locking. Credentials are excluded. State is bound to the exact OpenCode origin, project directory, and configured session to prevent accidental replay to a different destination. Use a local filesystem with POSIX permission support. A leftover `.writer-lock` after an unclean process exit intentionally blocks startup: verify the previous process has exited before manually removing that **empty lock directory**. Do not remove `state.json` to recover a lock; that loses admission correlation.

Restarting the bridge starts a new native app-server, so old realtime incarnations are retired and old speech is never replayed into a new call. Stable OpenCode sessions and unconfirmed admissions remain recoverable. Provider work interrupted by an OpenCode process crash is not automatically re-executed: missing settlement stays unresolved. A failed initialization may report selected but unpromoted input IDs as blocked while preserving their pending admission.

Native text feedback is capped at 64 KiB UTF-8 per item. An oversized or permanently rejected playback item retires voice with an explicit error while durable OpenCode work continues. Very large display text is marked truncated; complete content remains in OpenCode history. Startup context is capped at 21,200 UTF-8 bytes. State is capped at 32 MiB and native event/item journals are bounded: these are explicit fail-closed limits, not unlimited retention promises. Do not delete active state to work around them.

## Verification

```sh
cd external-orchestrator-bridge
npm run check
npm test
```

No npm install is needed. Tests cover HTTP authentication/redirects, malformed UTF-8, byte-exact framing, duplex RPC identity, subprocess lifecycle, durable admission/replay, uncertainty and stale-incarnation fences, missing live prefixes, exact multi-input settlement, startup context and fail-closed approval/question routing. The dedicated GitHub workflow exercises Node 22 and 24. Rust and OpenCode suites must also pass for the corresponding fork revisions. Mock protocol tests do not establish live authenticated audio parity.

The companion native-contract workflow runs the 598-test protocol/realtime library scope and the four synthetic app-server API integration cases on an ordinary hosted runner, without secrets. It explicitly fails if the runner would skip those integration bodies under its sandbox marker. It does not run the unrelated broad suite or clear sandbox markers.
