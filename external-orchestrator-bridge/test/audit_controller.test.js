import { test } from "node:test";
import assert from "node:assert/strict";
import { BridgeController, key } from "../src/controller.js";
import { RpcError } from "../src/rpc.js";
import { requestFingerprint } from "../src/requests.js";

function fixture(overrides = {}) {
  const binding = {
    threadId: "thread-a",
    sessionID: "ses_a",
    adopted: false,
    cursor: 0,
    admissions: {},
    items: {},
    works: {},
    pending: {},
    sequence: 0,
    incarnationId: null,
    outbox: null,
  };
  const sent = [],
    notifications = [],
    calls = [],
    snapshots = [];
  const store = {
    state: { version: 1, bindings: { a: binding } },
    async save() {
      snapshots.push(structuredClone(this.state));
    },
  };
  const api = {
    config: {},
    async capabilities() {
      return {
        healthy: true,
        sessionWorkProtocolVersion: 1,
        sessionWorkControlProtocolVersion: 1,
      };
    },
    async createSession(id) {
      calls.push(["create", id]);
      return { id };
    },
    async ensureSession(id) {
      return { id };
    },
    async context() {
      return [];
    },
    async pending() {
      return [];
    },
    async prompt(sessionID, id, text) {
      calls.push(["prompt", sessionID, id, text]);
      return { id, sessionID, admittedSeq: calls.length };
    },
    async guardedPrompt(sessionID, workID, id, text) {
      calls.push(["guardedPrompt", sessionID, workID, id, text]);
      return {
        id,
        sessionID,
        admittedSeq: calls.length,
        promotedSeq: calls.length,
      };
    },
    async interruptWork(sessionID, workID) {
      calls.push(["interruptWork", sessionID, workID]);
    },
    async request(...args) {
      calls.push(["request", ...args]);
    },
    async history() {
      return { data: [], hasMore: false };
    },
    ...overrides,
  };
  const peer = {
    async request(method, params) {
      sent.push({ method, params: structuredClone(params) });
      return { accepted: true };
    },
  };
  const controller = new BridgeController({
    api,
    peer,
    store,
    async notify(message) {
      notifications.push(structuredClone(message));
    },
  });
  controller.followLive = async () => {};
  controller.followSession = async () => {};
  binding.incarnationId = "inc-a";
  controller.liveReady = true;
  controller.liveEpoch = 1;
  return {
    controller,
    binding,
    sent,
    notifications,
    calls,
    snapshots,
    peer,
    store,
    api,
  };
}
function admission(f, id = "msg_a", extra = {}) {
  return (f.binding.admissions[id] = {
    id,
    text: "task",
    handoffId: "handoff-a",
    incarnationId: "inc-a",
    admitted: true,
    settled: false,
    ...extra,
  });
}
function event(type, id, data = {}) {
  return {
    id,
    type,
    data: {
      sessionID: "ses_a",
      timestamp: 0,
      workID: "work_a",
      inputMessageIDs: ["msg_a"],
      assistantMessageID: "assistant-a",
      textID: "text-a",
      ...data,
    },
  };
}
const plain = (extra = {}) => ({
  threadId: "thread-a",
  clientUserMessageId: "client-a",
  input: [{ type: "text", text: "hello" }],
  ...extra,
});

test("audit startup preserves actual user/assistant schema and excludes reasoning", async () => {
  const f = fixture({
    async context() {
      return [
        { type: "user", text: "user-visible task" },
        {
          type: "assistant",
          content: [
            { type: "text", id: "text-a", text: "assistant-visible result" },
            { type: "reasoning", text: "private-reasoning-marker" },
          ],
        },
      ];
    },
  });
  const result = await f.controller.start({ threadId: "thread-a" });
  assert.match(result.externalStartupContext, /user-visible task/);
  assert.match(result.externalStartupContext, /assistant-visible result/);
  assert.doesNotMatch(
    result.externalStartupContext,
    /private-reasoning-marker/,
  );
});

test("audit stale handoff cannot admit input and voice close never interrupts work", async () => {
  const f = fixture();
  admission(f);
  await f.controller.notification({
    method: "thread/realtime/externalHandoff",
    params: {
      threadId: "thread-a",
      incarnationId: "old-inc",
      handoffId: "old",
      inputTranscript: "task",
    },
  });
  assert.equal(f.calls.length, 0);
  await f.controller.notification({
    method: "thread/realtime/closed",
    params: { threadId: "thread-a", incarnationId: "inc-a" },
  });
  assert.equal(f.binding.incarnationId, null);
  assert.equal(f.calls.length, 0);
  assert.equal(f.binding.admissions.msg_a.settled, false);
});

test("audit repeated handoff uses stable admission identity and changed payload conflicts", async () => {
  const f = fixture();
  const message = {
    method: "thread/realtime/externalHandoff",
    params: {
      threadId: "thread-a",
      incarnationId: "inc-a",
      handoffId: "handoff-a",
      inputTranscript: "do task",
      activeTranscript: [],
    },
  };
  await f.controller.notification(message);
  await f.controller.notification(message);
  const prompts = f.calls.filter((call) => call[0] === "prompt");
  assert.equal(prompts.length, 1);
  assert.ok(prompts[0][2].startsWith("msg_"));
  assert.equal(Object.keys(f.binding.admissions).length, 1);
  await assert.rejects(
    f.controller.notification({
      ...message,
      params: { ...message.params, inputTranscript: "different task" },
    }),
    /Conflicting/,
  );
});

test("audit provider item IDs are scoped to assistant messages", async () => {
  const f = fixture();
  admission(f);
  for (const id of ["assistant-one", "assistant-two"]) {
    await f.controller.event(
      f.binding,
      event("session.next.text.started", `evt_${id}`, {
        assistantMessageID: id,
        providerMetadata: {
          openai: { itemId: "reused-provider-id", phase: "commentary" },
        },
      }),
      true,
    );
  }
  const starts = f.sent.filter(
    (call) => call.params.event.type === "itemStarted",
  );
  assert.equal(starts.length, 2);
  assert.notEqual(starts[0].params.event.itemId, starts[1].params.event.itemId);
});

test("audit replay after uncertain item ACK retries exact sequence without new duplicate start", async () => {
  const f = fixture();
  admission(f);
  let uncertain = true;
  f.peer.request = async (method, params) => {
    f.sent.push({ method, params: structuredClone(params) });
    if (uncertain) {
      uncertain = false;
      throw new Error("response lost after application");
    }
    return { accepted: false };
  };
  const started = event("session.next.text.started", "evt_start");
  await assert.rejects(
    f.controller.event(f.binding, started, true),
    /response lost/,
  );
  await f.controller.event(f.binding, started, true);
  assert.equal(f.sent.length, 2);
  assert.deepEqual(f.sent[0], f.sent[1]);
  assert.equal(f.binding.sequence, 1);
});

test("audit missed live prefix suppresses later deltas until authoritative final repair", async () => {
  const f = fixture();
  admission(f);
  await f.controller.event(
    f.binding,
    event("session.next.text.delta", "evt_early", { delta: "first" }),
    false,
  );
  await f.controller.event(
    f.binding,
    event("session.next.text.started", "evt_start"),
    true,
  );
  await f.controller.event(
    f.binding,
    event("session.next.text.delta", "evt_late", { delta: "second" }),
    false,
  );
  await f.controller.event(
    f.binding,
    event("session.next.text.ended", "evt_end", { text: "firstsecond" }),
    true,
  );
  assert.equal(
    f.sent.filter((call) => call.params.event.type === "itemDelta").length,
    0,
  );
  assert.equal(f.sent.at(-1).params.event.text, "firstsecond");
});

test("audit live starts permit ordered deltas; durable duplicate/end/late delta never repeat speech", async () => {
  const f = fixture();
  admission(f);
  const started = event("session.next.text.started", "evt_start");
  await f.controller.event(f.binding, started, false);
  await f.controller.event(f.binding, started, true);
  await f.controller.event(
    f.binding,
    event("session.next.text.delta", "evt_delta", { delta: "hello" }),
    false,
  );
  await f.controller.event(
    f.binding,
    event("session.next.text.ended", "evt_end", {
      text: "hello world",
      providerMetadata: { openai: { phase: "final_answer" } },
    }),
    true,
  );
  await f.controller.event(
    f.binding,
    event("session.next.text.delta", "evt_late", { delta: " world" }),
    false,
  );
  assert.deepEqual(
    f.sent.map((call) => call.params.event.type),
    ["itemStarted", "itemDelta", "itemEnded"],
  );
  assert.equal(f.sent.at(-1).params.event.phase, "final_answer");
});

test("audit exact work membership settles multiple native handoffs but leaves queued admission pending", async () => {
  const f = fixture();
  admission(f);
  admission(f, "msg_steer", { handoffId: "handoff-steer" });
  admission(f, "msg_queue", { handoffId: "handoff-queue" });
  await f.controller.event(
    f.binding,
    event("session.next.work.settled", "evt_settled", {
      outcome: "completed",
      inputMessageIDs: ["msg_a", "msg_steer"],
    }),
    true,
  );
  assert.deepEqual(f.sent[0].params.event.handoffIds.sort(), [
    "handoff-a",
    "handoff-steer",
  ]);
  assert.equal(f.binding.admissions.msg_queue.settled, false);
  assert.equal(f.binding.admissions.msg_a.settled, true);
});

test("audit start and steer share one terminal turn notification", async () => {
  const f = fixture();
  const first = await f.controller.typed("turn/start", plain());
  await f.controller.event(
    f.binding,
    {
      id: "prompted-first",
      type: "session.next.prompted",
      data: {
        sessionID: "ses_a",
        messageID: Object.keys(f.binding.admissions)[0],
        workID: "work_a",
      },
    },
    true,
  );
  await f.controller.typed(
    "turn/steer",
    plain({
      clientUserMessageId: "client-steer",
      expectedTurnId: first.turn.id,
    }),
  );
  await f.controller.event(
    f.binding,
    event("session.next.work.settled", "evt_settled", {
      outcome: "completed",
      inputMessageIDs: Object.keys(f.binding.admissions),
    }),
    true,
  );
  assert.equal(
    f.notifications.filter((message) => message.method === "turn/completed")
      .length,
    1,
  );
});

test("audit turn does not report completed while admitted steer remains unsettled", async () => {
  const f = fixture();
  const first = await f.controller.typed("turn/start", plain());
  await f.controller.event(
    f.binding,
    {
      id: "prompted-first",
      type: "session.next.prompted",
      data: {
        sessionID: "ses_a",
        messageID: Object.keys(f.binding.admissions)[0],
        workID: "work_a",
      },
    },
    true,
  );
  const firstID = Object.keys(f.binding.admissions)[0];
  await f.controller.typed(
    "turn/steer",
    plain({
      clientUserMessageId: "client-steer",
      expectedTurnId: first.turn.id,
    }),
  );
  await f.controller.event(
    f.binding,
    event("session.next.work.settled", "evt_settled", {
      outcome: "completed",
      inputMessageIDs: [firstID],
    }),
    true,
  );
  assert.equal(
    f.notifications.filter((message) => message.method === "turn/completed")
      .length,
    0,
  );
  assert.equal(first.turn.status, "inProgress");
});

test("audit typed requests reject security overrides, structured input, and wrong steer target", async () => {
  const f = fixture();
  for (const override of [
    { sandboxPolicy: { type: "dangerFullAccess" } },
    { approvalPolicy: "never" },
    { permissions: "wide" },
    { cwd: "/elsewhere" },
    { input: [{ type: "image", url: "https://example.test/x" }] },
  ]) {
    await assert.rejects(
      f.controller.typed("turn/start", plain(override)),
      /support|plain text/,
    );
  }
  await assert.rejects(
    f.controller.typed("turn/steer", plain({ expectedTurnId: "wrong" })),
    /identity/,
  );
  assert.equal(f.calls.length, 0);
});

test("audit exact completed steer retry returns the original turn without re-admission", async () => {
  const f = fixture();
  const first = await f.controller.typed("turn/start", plain());
  await f.controller.event(
    f.binding,
    {
      id: "prompted-first",
      type: "session.next.prompted",
      data: {
        sessionID: "ses_a",
        messageID: Object.keys(f.binding.admissions)[0],
        workID: "work_a",
      },
    },
    true,
  );
  const steer = plain({
    clientUserMessageId: "client-steer",
    expectedTurnId: first.turn.id,
  });
  const original = await f.controller.typed("turn/steer", steer);
  await f.controller.event(
    f.binding,
    event("session.next.work.settled", "evt_settled", {
      outcome: "completed",
      inputMessageIDs: Object.keys(f.binding.admissions),
    }),
    true,
  );
  const before = f.calls.length;
  assert.deepEqual(await f.controller.typed("turn/steer", steer), original);
  assert.equal(f.calls.length, before);
});

test("audit permission custom reply rejects persistent grant and cross-session pending request", async () => {
  const request = {
    id: "per_a",
    sessionID: "ses_a",
    action: "execute",
    resources: ["command"],
    save: ["*"],
  };
  const f = fixture({
    async pending(_session, kind) {
      return kind === "permission" ? [request] : [];
    },
  });
  await assert.rejects(
    f.controller.custom("bridge/permission/reply", {
      threadId: "thread-a",
      requestId: "per_a",
      requestFingerprint: requestFingerprint(request),
      reply: "always",
    }),
    /once or reject/,
  );
  request.sessionID = "ses_other";
  await assert.rejects(
    f.controller.custom("bridge/permission/reply", {
      threadId: "thread-a",
      requestId: "per_a",
      requestFingerprint: requestFingerprint(request),
      reply: "once",
    }),
    /does not belong/,
  );
  assert.equal(f.calls.length, 0);
});

test("audit custom question preserves multi-selection and rejects unknown or duplicate choices", async () => {
  const request = {
    id: "que_a",
    sessionID: "ses_a",
    questions: [
      {
        header: "Choose",
        question: "Pick some",
        custom: false,
        multiple: true,
        options: [{ label: "A" }, { label: "B" }],
      },
    ],
  };
  const f = fixture({
    async pending(_session, kind) {
      return kind === "question" ? [request] : [];
    },
  });
  for (const answers of [[["unknown"]], [["A", "A"]]]) {
    await assert.rejects(
      f.controller.custom("bridge/question/reply", {
        threadId: "thread-a",
        requestId: "que_a",
        requestFingerprint: requestFingerprint(request),
        answers,
      }),
      /Invalid question/,
    );
  }
  await f.controller.custom("bridge/question/reply", {
    threadId: "thread-a",
    requestId: "que_a",
    requestFingerprint: requestFingerprint(request),
    answers: [["A", "B"]],
  });
  assert.deepEqual(f.calls[0].at(-1), { answers: [["A", "B"]] });
});

test("audit startup enables supplied external context unless explicitly disabled", async () => {
  const f = fixture({
    async context() {
      return [{ type: "user", text: "context task" }];
    },
  });
  assert.equal(
    (await f.controller.start({ threadId: "thread-a" })).includeStartupContext,
    true,
  );
  assert.equal(
    (
      await f.controller.start({
        threadId: "thread-a",
        includeStartupContext: false,
      })
    ).includeStartupContext,
    false,
  );
});

test("audit external feedback requires typed accepted ACK before committing sequence", async () => {
  const f = fixture();
  admission(f);
  f.peer.request = async () => ({});
  await f.controller.event(
    f.binding,
    event("session.next.text.started", "evt_start"),
    true,
  );
  assert.equal(f.binding.sequence, 0);
  assert.equal(f.binding.incarnationId, null);
  assert.ok(
    f.notifications.some(
      (message) => message.method === "bridge/playback/error",
    ),
  );
});

test("audit per-session replay cursor tolerates public sequence gaps and ignores old duplicates", async () => {
  const f = fixture();
  const visited = [];
  f.api.sessionEvents = async function* (_session, after) {
    assert.equal(after, 0);
    for (const seq of [5, 12, 8, 12, 30])
      yield {
        id: `evt_${seq}`,
        type: "session.next.unknown",
        data: { sessionID: "ses_a" },
        durable: { aggregateID: "ses_a", seq, version: 1 },
      };
    f.controller.abort.abort();
  };
  f.controller.event = async (_binding, envelope) => {
    visited.push(envelope.durable.seq);
  };
  await BridgeController.prototype.followSession.call(f.controller, f.binding);
  assert.deepEqual(visited, [5, 12, 30]);
  assert.equal(f.binding.cursor, 30);
});

test("audit faster global durable events never advance the authoritative replay cursor", async () => {
  const f = fixture();
  admission(f);
  f.api.liveEvents = async function* () {
    yield { type: "server.connected", data: {} };
    yield {
      ...event("session.next.text.started", "evt_start"),
      durable: { aggregateID: "ses_a", seq: 999, version: 1 },
    };
    f.controller.abort.abort();
  };
  await BridgeController.prototype.followLive.call(f.controller);
  assert.equal(f.binding.cursor, 0);
});

test("audit duplicate same-incarnation started notification cannot rewind accepted sequence", async () => {
  const f = fixture();
  admission(f);
  await f.controller.event(
    f.binding,
    event("session.next.text.started", "evt_start"),
    true,
  );
  assert.equal(f.binding.sequence, 1);
  await f.controller.notification({
    method: "thread/realtime/started",
    params: { threadId: "thread-a", incarnationId: "inc-a" },
  });
  assert.equal(f.binding.sequence, 1);
});

test("audit transcript tail admission cannot emit audio after the call has ended", async () => {
  const f = fixture();
  await f.controller.notification({
    method: "thread/realtime/externalHandoff",
    params: {
      threadId: "thread-a",
      incarnationId: "inc-a",
      source: "transcriptTail",
      handoffId: "tail-a",
      itemId: null,
      inputTranscript: "The user ended this voice session.",
      activeTranscript: [],
      transcriptTail: [{ role: "user", text: "Please finish the task." }],
    },
  });
  const ids = Object.keys(f.binding.admissions);
  await f.controller.event(
    f.binding,
    event("session.next.text.started", "evt_tail_start", {
      inputMessageIDs: ids,
    }),
    true,
  );
  assert.equal(f.calls.filter((call) => call[0] === "prompt").length, 1);
  assert.equal(f.sent.length, 0);
});

test("audit permanent native playback rejection cannot prevent durable task settlement", async () => {
  const f = fixture();
  admission(f);
  f.peer.request = async (method) => {
    if (method === "thread/realtime/externalEvent")
      throw new RpcError(-32602, "external item text capacity reached");
    return {};
  };
  await f.controller.event(
    f.binding,
    event("session.next.text.started", "evt_start"),
    true,
  );
  await f.controller.event(
    f.binding,
    event("session.next.text.ended", "evt_end", { text: "completed task" }),
    true,
  );
  await f.controller.event(
    f.binding,
    event("session.next.work.settled", "evt_settled", { outcome: "completed" }),
    true,
  );
  assert.equal(f.binding.admissions.msg_a.settled, true);
  assert.equal(f.binding.incarnationId, null);
  assert.ok(
    f.notifications.some(
      (message) => message.method === "bridge/playback/error",
    ),
  );
  assert.equal(
    f.calls.filter((call) => call[0] === "request").length,
    0,
    "playback failure must not interrupt OpenCode",
  );
});

test("audit replacement call still admits retired incarnation tail exactly once without playback", async () => {
  const f = fixture();
  await f.controller.start({ threadId: "thread-a" });
  await f.controller.notification({
    method: "thread/realtime/started",
    params: { threadId: "thread-a", incarnationId: "inc-b" },
  });
  const tail = {
    method: "thread/realtime/externalHandoff",
    params: {
      threadId: "thread-a",
      incarnationId: "inc-a",
      source: "transcriptTail",
      handoffId: "tail-retired",
      itemId: null,
      inputTranscript: "The user ended the session.",
      activeTranscript: [],
      transcriptTail: [{ role: "user", text: "finish" }],
    },
  };
  await f.controller.notification(tail);
  await f.controller.notification(tail);
  assert.equal(f.calls.filter((call) => call[0] === "prompt").length, 1);
  const ids = Object.keys(f.binding.admissions);
  await f.controller.event(
    f.binding,
    event("session.next.text.started", "evt_tail_retired", {
      inputMessageIDs: ids,
    }),
    true,
  );
  assert.equal(f.sent.length, 0);
});

test("audit invalid OpenCode capabilities reject before session creation or native start", async () => {
  const f = fixture({
    async capabilities() {
      throw new Error("OpenCode session work protocol v1 is required");
    },
  });
  await assert.rejects(
    f.controller.start({ threadId: "thread-a" }),
    /protocol/,
  );
  assert.equal(f.calls.length, 0);
  assert.equal(f.sent.length, 0);
});

test("audit flushing permanently rejected outbox never constructs null-incarnation feedback", async () => {
  const f = fixture();
  admission(f);
  f.binding.outbox = {
    threadId: "thread-a",
    incarnationId: "inc-a",
    handoffId: "handoff-a",
    executionId: "old-work",
    sequence: 1,
    event: { type: "workCancelled", handoffIds: ["handoff-a"] },
  };
  f.peer.request = async (method, params) => {
    f.sent.push({ method, params: structuredClone(params) });
    if (method === "thread/realtime/externalEvent")
      throw new RpcError(-32602, "invalid prior feedback");
    return {};
  };
  await f.controller.event(
    f.binding,
    event("session.next.text.started", "evt_start"),
    true,
  );
  assert.equal(
    f.sent.filter((call) => call.method === "thread/realtime/externalEvent")
      .length,
    1,
  );
  assert.equal(f.binding.outbox, null);
});

test("audit delayed closed and started notifications from retired call cannot affect new call", async () => {
  const f = fixture();
  await f.controller.start({ threadId: "thread-a" });
  await f.controller.notification({
    method: "thread/realtime/started",
    params: { threadId: "thread-a", incarnationId: "inc-b" },
  });
  await f.controller.notification({
    method: "thread/realtime/closed",
    params: { threadId: "thread-a", incarnationId: "inc-a" },
  });
  assert.equal(f.binding.incarnationId, "inc-b");
  await f.controller.notification({
    method: "thread/realtime/started",
    params: { threadId: "thread-a", incarnationId: "inc-a" },
  });
  assert.equal(f.binding.incarnationId, "inc-b");
});

test("audit malformed settlement IDs cannot mutate inherited object prototypes", async () => {
  const f = fixture();
  const beforeSettled = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "settled",
  );
  const beforeOutcome = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "outcome",
  );
  try {
    await f.controller
      .event(
        f.binding,
        event("session.next.work.settled", "evt_poison", {
          outcome: "completed",
          inputMessageIDs: ["__proto__"],
        }),
        true,
      )
      .catch(() => {});
    assert.deepEqual(
      Object.getOwnPropertyDescriptor(Object.prototype, "settled"),
      beforeSettled,
    );
    assert.deepEqual(
      Object.getOwnPropertyDescriptor(Object.prototype, "outcome"),
      beforeOutcome,
    );
    assert.equal(f.sent.length, 0);
  } finally {
    if (beforeSettled)
      Object.defineProperty(Object.prototype, "settled", beforeSettled);
    else delete Object.prototype.settled;
    if (beforeOutcome)
      Object.defineProperty(Object.prototype, "outcome", beforeOutcome);
    else delete Object.prototype.outcome;
  }
});

test("audit typed turn projection retains shared identity after durable state reload", async () => {
  const f = fixture();
  const first = await f.controller.typed("turn/start", plain());
  await f.controller.event(
    f.binding,
    {
      id: "prompted-first",
      type: "session.next.prompted",
      data: {
        sessionID: "ses_a",
        messageID: Object.keys(f.binding.admissions)[0],
        workID: "work_a",
      },
    },
    true,
  );
  const firstID = Object.keys(f.binding.admissions)[0];
  await f.controller.typed(
    "turn/steer",
    plain({
      clientUserMessageId: "client-steer",
      expectedTurnId: first.turn.id,
    }),
  );
  await f.controller.event(
    f.binding,
    event("session.next.work.settled", "evt_before_restart", {
      outcome: "completed",
      inputMessageIDs: [firstID],
    }),
    true,
  );
  const reloadedStore = {
    state: JSON.parse(JSON.stringify(f.store.state)),
    async save() {},
  };
  const restarted = new BridgeController({
    api: f.api,
    peer: f.peer,
    store: reloadedStore,
    async notify() {},
  });
  restarted.followLive = async () => {};
  restarted.followSession = async () => {};
  const binding = restarted.bindings.get("thread-a");
  const ids = Object.keys(binding.admissions).filter((id) => id !== firstID);
  await restarted.event(
    binding,
    event("session.next.text.ended", "evt_after_restart", {
      workID: "work_next",
      inputMessageIDs: ids,
      text: "Result after restart",
    }),
    true,
  );
  const history = await restarted.custom("bridge/session/read", {
    threadId: "thread-a",
  });
  assert.equal(history.turns.length, 1);
  assert.equal(history.turns[0].items.length, 1);
  assert.equal(history.turns[0].items[0].text, "Result after restart");
});

test("permission decisions bind to displayed scope and include atomic server precondition", async () => {
  const original = {
    id: "per_a",
    sessionID: "ses_a",
    action: "execute",
    resources: ["safe"],
  };
  let current = original;
  const f = fixture({
    async pending(_session, kind) {
      return kind === "permission" ? [current] : [];
    },
  });
  const fingerprint = requestFingerprint(original);
  current = { ...original, resources: ["different"] };
  await assert.rejects(
    f.controller.custom("bridge/permission/reply", {
      threadId: "thread-a",
      requestId: "per_a",
      reply: "once",
      requestFingerprint: fingerprint,
    }),
    /changed/,
  );
  assert.equal(f.calls.length, 0);
  await f.controller.custom("bridge/permission/reply", {
    threadId: "thread-a",
    requestId: "per_a",
    reply: "once",
    requestFingerprint: requestFingerprint(current),
  });
  assert.deepEqual(f.calls[0].at(-1), {
    reply: "once",
    expectedRequest: current,
  });
  assert.equal(
    requestFingerprint({ b: 2, a: { y: 1, x: 3 } }),
    requestFingerprint({ a: { x: 3, y: 1 }, b: 2 }),
  );
});

test("blocked admission retry is explicit, scoped, and never creates a replacement input", async () => {
  const f = fixture();
  const pending = admission(f);
  await f.controller.event(
    f.binding,
    event("session.next.work.settled", "blocked-event", {
      inputMessageIDs: [],
      pendingInputMessageIDs: ["msg_a"],
      outcome: "failed",
      error: { message: "initialization failed" },
    }),
    true,
  );
  assert.equal(pending.settled, false);
  assert.equal(pending.blocked, true);
  assert.ok(
    f.notifications.some((message) => message.method === "bridge/work/blocked"),
  );
  await f.controller.custom("bridge/admission/retry", {
    threadId: "thread-a",
    inputMessageId: "msg_a",
  });
  assert.deepEqual(f.calls[0].slice(0, 4), [
    "prompt",
    "ses_a",
    "msg_a",
    "task",
  ]);
  assert.equal(pending.blocked, false);
  await assert.rejects(
    f.controller.custom("bridge/admission/retry", {
      threadId: "thread-a",
      inputMessageId: "msg_a",
    }),
    /blocked/,
  );
  await assert.rejects(
    f.controller.custom("bridge/admission/retry", {
      threadId: "thread-a",
      inputMessageId: "__proto__",
    }),
    /blocked/,
  );
});

test("unsupported backing instructions and alternate handoff routing fail before OpenCode start", async () => {
  const f = fixture();
  for (const params of [
    { realtimeStartInstructions: "special instructions" },
    { realtimeEndInstructions: "special instructions" },
    { codexResponseHandoffMode: "speech" },
    { codexResponseHandoffChannelPrefixes: {} },
    { clientManagedHandoffs: true },
    { codexResponsesAsItems: true },
    { externalOrchestrator: false },
  ])
    await assert.rejects(
      f.controller.start({ threadId: "thread-a", ...params }),
      /support|Conflicting/,
    );
  assert.equal(f.calls.length, 0);
});

test("state cannot be replayed to a different OpenCode destination", () => {
  const f = fixture();
  f.store.state.destination = {
    origin: "https://old.example",
    directory: "/old",
    sessionID: null,
  };
  f.api.config = { url: new URL("https://new.example"), directory: "/old" };
  assert.throws(
    () =>
      new BridgeController({
        api: f.api,
        peer: f.peer,
        store: f.store,
        notify: async () => {},
      }),
    /different OpenCode/,
  );
});

test("legacy history without work correlation cannot block new durable settlement", async () => {
  const f = fixture();
  admission(f);
  f.api.sessionEvents = async function* () {
    for (const [index, type] of [
      "session.next.text.started",
      "session.next.text.ended",
    ].entries()) {
      yield {
        id: `legacy-${index}`,
        type,
        data: {
          sessionID: "ses_a",
          assistantMessageID: "msg_old",
          textID: "old",
          text: "old history",
        },
        durable: { aggregateID: "ses_a", seq: index + 1 },
      };
    }
    yield {
      ...event("session.next.work.settled", "new-settlement", {
        outcome: "completed",
      }),
      durable: { aggregateID: "ses_a", seq: 3 },
    };
    f.controller.abort.abort();
  };
  await BridgeController.prototype.followSession.call(f.controller, f.binding);
  assert.equal(f.binding.cursor, 3);
  assert.equal(f.binding.admissions.msg_a.settled, true);
  assert.equal(
    f.sent.filter((message) => message.params?.event?.type === "itemStarted")
      .length,
    0,
  );
});

test("restored typed-only session starts replay and pending-request recovery without starting voice", async () => {
  const f = fixture();
  let live = 0,
    durable = 0;
  f.controller.followLive = async () => {
    live++;
  };
  f.controller.followSession = async (binding) => {
    assert.equal(binding.sessionID, "ses_a");
    durable++;
  };
  f.binding.incarnationId = null;
  const result = await f.controller.typed("turn/start", plain());
  assert.equal(result.turn.status, "inProgress");
  assert.equal(live, 1);
  assert.equal(durable, 1);
  assert.equal(f.binding.incarnationId, null);
  await f.controller.custom("bridge/requests/list", { threadId: "thread-a" });
  assert.equal(live, 1);
  assert.equal(durable, 1);
});

test("stale typed controls target the old work and never fall back to session-wide operations", async () => {
  const f = fixture();
  const first = await f.controller.typed("turn/start", plain());
  await f.controller.event(
    f.binding,
    {
      id: "promoted",
      type: "session.next.prompted",
      data: {
        sessionID: "ses_a",
        messageID: Object.keys(f.binding.admissions)[0],
        workID: "work_old",
      },
    },
    true,
  );
  const control = [];
  const conflict = () =>
    Object.assign(new Error("work changed"), { status: 409 });
  f.api.interruptWork = async (_session, workID) => {
    control.push(["interrupt", workID]);
    throw conflict();
  };
  f.api.guardedPrompt = async (_session, workID) => {
    control.push(["steer", workID]);
    throw conflict();
  };
  await assert.rejects(
    f.controller.typed("turn/interrupt", {
      threadId: "thread-a",
      turnId: first.turn.id,
    }),
    /nothing was interrupted/,
  );
  await assert.rejects(
    f.controller.typed(
      "turn/steer",
      plain({ clientUserMessageId: "stale", expectedTurnId: first.turn.id }),
    ),
    /not admitted/,
  );
  assert.deepEqual(control, [
    ["interrupt", "work_old"],
    ["steer", "work_old"],
  ]);
  assert.equal(Object.keys(f.binding.admissions).length, 1);
  assert.equal(f.calls.filter((entry) => entry[0] === "prompt").length, 1);
  assert.equal(f.calls.filter((entry) => entry[0] === "request").length, 0);
});

test(
  "guarded steer wait does not block exact-work interruption or replay",
  { timeout: 2000 },
  async () => {
    const f = fixture();
    const first = await f.controller.typed("turn/start", plain());
    const initialID = Object.keys(f.binding.admissions)[0];
    await f.controller.event(
      f.binding,
      {
        id: "promoted",
        type: "session.next.prompted",
        data: { sessionID: "ses_a", messageID: initialID, workID: "work_a" },
      },
      true,
    );
    let entered, rejectSteer;
    const ready = new Promise((resolve) => {
      entered = resolve;
    });
    f.api.guardedPrompt = async () => {
      entered();
      return new Promise((_resolve, reject) => {
        rejectSteer = reject;
      });
    };
    f.api.interruptWork = async (_session, workID) => {
      assert.equal(workID, "work_a");
      rejectSteer(Object.assign(new Error("work cancelled"), { status: 409 }));
    };
    const steering = f.controller.typed(
      "turn/steer",
      plain({ clientUserMessageId: "waiting", expectedTurnId: first.turn.id }),
    );
    const rejected = assert.rejects(steering, /not admitted/);
    await ready;
    await f.controller.typed("turn/interrupt", {
      threadId: "thread-a",
      turnId: first.turn.id,
    });
    await rejected;
    await f.controller.event(
      f.binding,
      event("session.next.work.settled", "cancelled", {
        outcome: "cancelled",
        inputMessageIDs: [initialID],
      }),
      true,
    );
    assert.equal(first.turn.status, "interrupted");
    assert.equal(Object.keys(f.binding.admissions).length, 1);
  },
);

test("control requires observed promotion and explicit backing work capability", async () => {
  const f = fixture();
  const first = await f.controller.typed("turn/start", plain());
  await assert.rejects(
    f.controller.typed("turn/interrupt", {
      threadId: "thread-a",
      turnId: first.turn.id,
    }),
    /confirmed active work/,
  );
  f.api.capabilities = async () => ({
    healthy: true,
    sessionWorkProtocolVersion: 1,
  });
  await assert.rejects(
    f.controller.typed(
      "turn/steer",
      plain({
        expectedTurnId: first.turn.id,
        clientUserMessageId: "unsupported",
      }),
    ),
    /work control/,
  );
  assert.equal(Object.keys(f.binding.admissions).length, 1);
});

test("initialization recovery restores unfinished work without voice or new admission", async () => {
  const f = fixture();
  admission(f);
  let followed = 0;
  f.controller.followSession = async () => {
    followed++;
  };
  await f.controller.restore();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(followed, 1);
  assert.equal(f.calls.filter((entry) => entry[0] === "prompt").length, 0);
  await f.controller.recoverThread("thread-a");
  assert.equal(followed, 1);
  await f.controller.close();
});
