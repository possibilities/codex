import { test } from "node:test";
import assert from "node:assert/strict";
import { PassThrough, Writable } from "node:stream";
import { BridgeHost } from "../src/host.js";
import { JsonLines, RpcError } from "../src/rpc.js";

function collector() {
  const messages = [];
  const waiters = [];
  const output = new Writable({
    write(chunk, _encoding, done) {
      const message = JSON.parse(chunk.toString());
      const index = waiters.findIndex((item) => item.matches(message));
      if (index < 0) messages.push(message);
      else {
        const [waiter] = waiters.splice(index, 1);
        clearTimeout(waiter.timer);
        waiter.resolve(message);
      }
      done();
    },
  });
  return {
    output,
    messages,
    next(matches = () => true) {
      const index = messages.findIndex(matches);
      if (index >= 0) return Promise.resolve(messages.splice(index, 1)[0]);
      return new Promise((resolve, reject) => {
        const waiter = {
          matches,
          resolve,
          timer: setTimeout(
            () => reject(new Error("Test message timed out")),
            2_000,
          ),
        };
        waiters.push(waiter);
      });
    },
  };
}

function fixture(t, overrides = {}) {
  const clientInput = new PassThrough();
  const serverInput = new PassThrough();
  const client = collector();
  const server = collector();
  const calls = [];
  const controller = {
    bindings: new Map(),
    async start(params) {
      calls.push(["start", params]);
      this.bindings.set(params.threadId, {});
      return {
        ...params,
        externalOrchestrator: true,
        externalStartupContext: "external history",
      };
    },
    async typed(method, params) {
      calls.push([method, params]);
      return { external: true };
    },
    async custom(method, params) {
      calls.push([method, params]);
      return { handled: true };
    },
    async notification(message) {
      calls.push(["notification", message]);
    },
    async close() {
      calls.push(["close"]);
    },
    ...overrides,
  };
  const host = new BridgeHost({
    client: new JsonLines(clientInput, client.output, 1024 * 1024),
    server: new JsonLines(serverInput, server.output, 1024 * 1024),
    controllerFactory: ({ peer, notify }) => {
      controller.peer = peer;
      controller.notify = notify;
      return controller;
    },
  });
  const running = host.run();
  running.catch(() => {});
  const writeClient = (message) =>
    clientInput.write(JSON.stringify(message) + "\n");
  const writeServer = (message) =>
    serverInput.write(JSON.stringify(message) + "\n");
  t.after(async () => {
    clientInput.end();
    await running.catch(() => {});
  });
  return {
    host,
    controller,
    calls,
    client,
    server,
    clientInput,
    serverInput,
    running,
    writeClient,
    writeServer,
    async initialize() {
      writeClient({
        id: "init",
        method: "initialize",
        params: { clientInfo: { name: "test", version: "1" } },
      });
      const request = await server.next();
      assert.deepEqual(request.params.capabilities, { experimentalApi: true });
      writeServer({ id: request.id, result: { userAgent: "test-app-server" } });
      assert.deepEqual(await client.next(), {
        id: "init",
        result: { userAgent: "test-app-server" },
      });
    },
  };
}

test("capability discovery works before initialize and declares unsupported desktop parity", async (t) => {
  const h = fixture(t);
  h.writeClient({ id: 0, method: "bridge/capabilities" });
  const reply = await h.client.next();
  assert.equal(reply.id, 0);
  assert.deepEqual(reply.result.nativeExternalOrchestrator, {
    verified: false,
    requiredProtocolVersion: 1,
  });
  assert.equal(reply.result.nativeDesktopParity, false);
  assert.equal(h.server.messages.length, 0);
});

test("ordinary forwarding preserves client IDs, null results, and full RPC errors", async (t) => {
  const h = fixture(t);
  await h.initialize();
  h.writeClient({
    jsonrpc: "2.0",
    id: 7,
    method: "model/list",
    params: { limit: 1 },
  });
  const request = await h.server.next();
  assert.notEqual(request.id, 7);
  assert.deepEqual(
    { ...request, id: 7 },
    { id: 7, method: "model/list", params: { limit: 1 } },
  );
  h.writeServer({ id: request.id, result: null });
  assert.deepEqual(await h.client.next(), {
    jsonrpc: "2.0",
    id: 7,
    result: null,
  });
  h.writeClient({ id: "7", method: "account/read", params: {} });
  const second = await h.server.next();
  const error = {
    code: -32009,
    message: "Native structured error",
    data: { retryAfter: 5 },
  };
  h.writeServer({ id: second.id, error });
  assert.deepEqual(await h.client.next(), { id: "7", error });
});

test("forwarding preserves omitted and null params for zero-argument native methods", async (t) => {
  const h = fixture(t);
  await h.initialize();
  for (const original of [
    { id: 1, method: "account/rateLimits/read" },
    { id: 2, method: "account/rateLimits/read", params: null },
  ]) {
    h.writeClient(original);
    const request = await h.server.next();
    assert.deepEqual({ ...request, id: original.id }, original);
    h.writeServer({ id: request.id, result: {} });
    await h.client.next();
  }
});

test("client, bridge, and server-initiated requests have independent ID ownership", async (t) => {
  const h = fixture(t);
  await h.initialize();
  h.writeClient({ id: "voice-bridge:1", method: "model/list", params: {} });
  const forwarded = await h.server.next();
  const background = h.controller.peer.request("server/diagnostics", {});
  const internal = await h.server.next();
  h.writeServer({
    id: forwarded.id,
    method: "item/tool/requestUserInput",
    params: { threadId: "native", questions: [] },
  });
  const toClient = await h.client.next();
  assert.notEqual(toClient.id, forwarded.id);
  assert.notEqual(internal.id, forwarded.id);
  h.writeClient({ id: toClient.id, result: { answers: {} } });
  assert.deepEqual(await h.server.next(), {
    id: forwarded.id,
    result: { answers: {} },
  });
  h.writeServer({
    method: "serverRequest/resolved",
    params: { threadId: "native", requestId: forwarded.id },
  });
  assert.deepEqual(await h.client.next(), {
    method: "serverRequest/resolved",
    params: { threadId: "native", requestId: toClient.id },
  });
  h.writeServer({ id: internal.id, result: { internal: true } });
  h.writeServer({ id: forwarded.id, result: { models: [] } });
  assert.deepEqual(await background, { internal: true });
  assert.deepEqual(await h.client.next(), {
    id: "voice-bridge:1",
    result: { models: [] },
  });
});

test("start requires a positive native capability handshake and rewrites only external fields", async (t) => {
  const h = fixture(t);
  await h.initialize();
  const params = {
    threadId: "external",
    transport: { type: "webrtc", sdp: "offer" },
    outputModality: "audio",
    voice: "test",
  };
  h.writeClient({ id: 2, method: "thread/realtime/start", params });
  const capability = await h.server.next();
  assert.equal(capability.method, "thread/realtime/externalCapabilities");
  h.writeServer({ id: capability.id, result: { protocolVersion: 1 } });
  const request = await h.server.next();
  assert.deepEqual(request.params, {
    ...params,
    externalOrchestrator: true,
    externalStartupContext: "external history",
  });
  h.writeServer({ id: request.id, result: {} });
  assert.deepEqual(await h.client.next(), { id: 2, result: {} });
  assert.equal(h.host.capabilities().nativeExternalOrchestrator.verified, true);
});

test("unpatched native server cannot start voice or receive a raced typed request", async (t) => {
  const h = fixture(t, {
    async typed() {
      throw new RpcError(-32602, "No external binding");
    },
  });
  await h.initialize();
  h.writeClient({
    id: 2,
    method: "thread/realtime/start",
    params: { threadId: "external" },
  });
  h.writeClient({
    id: 3,
    method: "turn/start",
    params: { threadId: "external", input: [] },
  });
  const capability = await h.server.next();
  h.writeServer({
    id: capability.id,
    error: { code: -32601, message: "Unknown method" },
  });
  assert.equal((await h.client.next()).error.code, -32003);
  assert.equal((await h.client.next()).error.code, -32602);
  assert.equal(h.server.messages.length, 0);
  assert.equal(
    h.calls.some(([method]) => method === "start"),
    false,
  );
});

test("bound typed requests and exact generic question answers only reach the controller", async (t) => {
  const h = fixture(t, { bindings: new Map([["external", {}]]) });
  await h.initialize();
  for (const method of [
    "turn/start",
    "turn/steer",
    "turn/interrupt",
    "bridge/permission/reply",
    "bridge/question/reply",
    "bridge/question/reject",
  ]) {
    const params = {
      threadId: "external",
      requestID: "request",
      answers: [["A", "B"], ["typed custom"]],
    };
    h.writeClient({ id: method, method, params });
    await h.client.next();
    assert.deepEqual(h.calls.at(-1), [method, params]);
  }
  assert.equal(h.server.messages.length, 0);
});

test("unsupported bound work, history, settings, and global native execution fail closed", async (t) => {
  const h = fixture(t, { bindings: new Map([["external", {}]]) });
  await h.initialize();
  for (const [method, params] of [
    ["thread/settings/update", { threadId: "external", model: "other" }],
    ["thread/shellCommand", { threadId: "external", command: "echo no" }],
    ["review/start", { threadId: "external" }],
    ["thread/compact/start", { threadId: "external" }],
    ["thread/read", { threadId: "external", includeTurns: true }],
    ["thread/resume", { threadId: "external" }],
    [
      "thread/resume",
      {
        threadId: "external",
        excludeTurns: true,
        sandbox: "danger-full-access",
      },
    ],
    ["thread/fork", { threadId: "external" }],
    ["thread/realtime/externalEvent", { threadId: "external" }],
    ["command/exec", { command: ["echo", "no"] }],
    ["process/spawn", { command: "echo no" }],
    ["config/value/write", { keyPath: "approval_policy", value: "never" }],
    ["mcpServer/tool/call", { server: "native", name: "native_tool" }],
    ["getConversationSummary", { conversationId: "external" }],
    [
      "thread/resume",
      {
        threadId: "unrelated",
        path: "/native/external/rollout.jsonl",
        model: "native",
      },
    ],
    [
      "thread/fork",
      { threadId: "unrelated", path: "/native/external/rollout.jsonl" },
    ],
  ]) {
    h.writeClient({ id: method, method, params });
    const response = await h.client.next();
    assert.ok(response.error, `${method} must fail closed`);
  }
  assert.equal(h.server.messages.length, 0);
});

test("bound metadata and resume without native history remain available", async (t) => {
  const h = fixture(t, { bindings: new Map([["external", {}]]) });
  await h.initialize();
  for (const [method, params] of [
    ["thread/read", { threadId: "external" }],
    ["thread/resume", { threadId: "external", excludeTurns: true }],
    [
      "thread/realtime/appendAudio",
      { threadId: "external", audio: { data: "AA==" } },
    ],
    ["thread/realtime/stop", { threadId: "external" }],
  ]) {
    h.writeClient({ id: method, method, params });
    const request = await h.server.next();
    assert.deepEqual(
      { ...request, id: method },
      { id: method, method, params },
    );
    h.writeServer({ id: request.id, result: {} });
    await h.client.next();
  }
});

test("native approvals for externally bound threads are denied instead of transformed", async (t) => {
  const h = fixture(t, { bindings: new Map([["external", {}]]) });
  for (const [method, identity] of [
    ["item/commandExecution/requestApproval", { threadId: "external" }],
    ["execCommandApproval", { conversationId: "external" }],
  ]) {
    h.writeServer({
      id: 100,
      method,
      params: { ...identity, command: "echo unsafe" },
    });
    assert.deepEqual(await h.server.next(), {
      id: 100,
      error: {
        code: -32003,
        message:
          "Native actions are disabled for externally orchestrated threads",
      },
    });
  }
  assert.equal(h.client.messages.length, 0);
});

test("duplicate threadless native request IDs cannot acquire two client owners", async (t) => {
  const h = fixture(t);
  h.writeServer({ id: 5, method: "currentTime/read", params: {} });
  await h.client.next();
  h.writeServer({ id: 5, method: "currentTime/read", params: {} });
  await assert.rejects(h.running, /Duplicate unresolved server request ID/);
  assert.equal(h.client.messages.length, 0);
});

test("native request timeouts reclaim capacity while retaining bounded resolution correlation", async (t) => {
  const h = fixture(t);
  h.host.timeoutMs = 5;
  h.host.maxPending = 1;
  for (let id = 1; id <= 3; id++) {
    h.writeServer({
      id,
      method: "item/tool/requestUserInput",
      params: { threadId: "native" },
    });
    const request = await h.client.next();
    assert.deepEqual(await h.server.next(), {
      id,
      error: { code: -32001, message: "Client response timed out" },
    });
    assert.equal(h.host.serverRequests.size, 0);
    assert.equal(h.host.serverRequestIds.size, 0);
    assert.ok(h.host.serverReplyHistory.size <= 1);
    h.writeServer({
      method: "serverRequest/resolved",
      params: { threadId: "native", requestId: id },
    });
    assert.deepEqual(await h.client.next(), {
      method: "serverRequest/resolved",
      params: { threadId: "native", requestId: request.id },
    });
  }
});

test("late expired client approval is discarded without native delivery or closing the bridge", async (t) => {
  const h = fixture(t);
  h.host.timeoutMs = 5;
  h.writeServer({
    id: 7,
    method: "item/tool/requestUserInput",
    params: { threadId: "native" },
  });
  const request = await h.client.next();
  await h.server.next();
  h.writeClient({ id: request.id, result: { answers: { approve: true } } });
  await h.initialize();
  assert.equal(h.server.messages.length, 0);
  assert.equal(h.host.closed, undefined);
});

test("late expired native response is discarded without being mistaken for a fresh response", async (t) => {
  const h = fixture(t);
  await h.initialize();
  h.host.peer.timeoutMs = 5;
  h.writeClient({ id: 7, method: "model/list", params: {} });
  const request = await h.server.next();
  assert.equal((await h.client.next()).error.code, -32001);
  h.writeServer({ id: request.id, result: { stale: true } });
  h.writeServer({ method: "server/alive", params: {} });
  assert.deepEqual(await h.client.next(), {
    method: "server/alive",
    params: {},
  });
  assert.equal(h.client.messages.length, 0);
  assert.equal(h.host.closed, undefined);
});

test("client notification opt-outs cannot suppress bridge-owned native lifecycle events", async (t) => {
  const h = fixture(t);
  h.writeClient({
    id: 1,
    method: "initialize",
    params: {
      clientInfo: { name: "test", version: "1" },
      capabilities: {
        experimentalApi: false,
        optOutNotificationMethods: [
          "thread/realtime/started",
          "thread/realtime/closed",
          "thread/realtime/externalHandoff",
          "thread/started",
        ],
      },
    },
  });
  const request = await h.server.next();
  assert.deepEqual(request.params.capabilities, {
    experimentalApi: true,
    optOutNotificationMethods: ["thread/started"],
  });
  h.writeServer({ id: request.id, result: {} });
  await h.client.next();
  const started = {
    method: "thread/realtime/started",
    params: { threadId: "external", incarnationId: "incarnation" },
  };
  h.writeServer(started);
  h.writeServer({ method: "server/alive", params: {} });
  assert.deepEqual(await h.client.next(), {
    method: "server/alive",
    params: {},
  });
  assert.deepEqual(h.calls[0], ["notification", started]);
  assert.equal(h.client.messages.length, 0);
});

test("controller can await native feedback without deadlocking the server input pump", async (t) => {
  const h = fixture(t, {
    async notification(message) {
      if (message.method === "thread/realtime/externalHandoff")
        await this.peer.request("thread/realtime/externalEvent", {
          feedback: true,
        });
    },
  });
  h.writeServer({
    method: "thread/realtime/externalHandoff",
    params: { threadId: "external" },
  });
  const feedback = await h.server.next();
  h.writeServer({
    method: "thread/realtime/outputAudio/delta",
    params: { threadId: "external", audio: "data" },
  });
  assert.equal(
    (await h.client.next()).method,
    "thread/realtime/outputAudio/delta",
  );
  h.writeServer({ id: feedback.id, result: { accepted: true } });
  await h.controller.notify({
    method: "bridge/item",
    params: { text: "done" },
  });
  assert.deepEqual(await h.client.next(), {
    method: "bridge/item",
    params: { text: "done" },
  });
  assert.equal(h.client.messages.length, 0);
});

test("request-shaped notifications cannot bypass external routing", async (t) => {
  const h = fixture(t, { bindings: new Map([["external", {}]]) });
  h.writeClient({
    method: "turn/start",
    params: { threadId: "external", input: [] },
  });
  await assert.rejects(h.running, /Unsupported client notification/);
  assert.equal(h.server.messages.length, 0);
});

test("invalid IDs and ambiguous response envelopes terminate without forwarding", async (t) => {
  for (const message of [
    { id: null, method: "model/list" },
    { id: 1.5, method: "model/list" },
    { id: [], method: "model/list" },
    { id: "unknown", result: {}, error: { code: -1, message: "ambiguous" } },
    { id: "unknown" },
    { id: "unknown", result: {} },
  ]) {
    await t.test(JSON.stringify(message), async (subtest) => {
      const h = fixture(subtest);
      h.writeClient(message);
      await assert.rejects(h.running, /Invalid|not owned/);
      assert.equal(h.server.messages.length, 0);
    });
  }
});

test("closing the client rejects outstanding bridge requests and closes the controller", async (t) => {
  const h = fixture(t);
  const pending = h.controller.peer.request("server/diagnostics", {});
  const rejection = assert.rejects(pending, /closed/);
  await h.server.next();
  h.clientInput.end();
  await h.running;
  await rejection;
  assert.deepEqual(h.calls, [["close"]]);
});

test(
  "client EOF aborts blocked output and completes shutdown",
  { timeout: 2000 },
  async () => {
    const clientInput = new PassThrough();
    const serverInput = new PassThrough();
    let wrote;
    const writing = new Promise((resolve) => {
      wrote = resolve;
    });
    const clientOutput = new Writable({
      write(_chunk, _encoding, _done) {
        wrote();
      },
    });
    const serverOutput = new PassThrough();
    const host = new BridgeHost({
      client: new JsonLines(clientInput, clientOutput, 1024),
      server: new JsonLines(serverInput, serverOutput, 1024),
      controllerFactory: () => ({ bindings: new Map(), async close() {} }),
    });
    const running = host.run();
    clientInput.write(
      JSON.stringify({ id: 1, method: "bridge/capabilities" }) + "\n",
    );
    await writing;
    clientInput.end();
    await running;
    assert.equal(host.closed, true);
    assert.equal(host.tasks.size, 0);
    clientOutput.destroy();
    serverOutput.destroy();
  },
);

test("incoming audio and interrupt bypass a waiting guarded steer", async (t) => {
  let release, entered;
  const waiting = new Promise((resolve) => {
    entered = resolve;
  });
  const h = fixture(t, {
    async typed(method) {
      if (method === "turn/steer") {
        entered();
        await new Promise((resolve) => {
          release = resolve;
        });
        return { turnId: "turn-a" };
      }
      return {};
    },
  });
  await h.initialize();
  h.controller.bindings.set("thread-a", {});
  h.writeClient({
    id: "steer",
    method: "turn/steer",
    params: { threadId: "thread-a", expectedTurnId: "turn-a" },
  });
  await waiting;
  h.writeClient({
    id: "audio",
    method: "thread/realtime/appendAudio",
    params: { threadId: "thread-a", audio: { data: "test" } },
  });
  const audio = await h.server.next();
  assert.equal(audio.method, "thread/realtime/appendAudio");
  h.writeServer({ id: audio.id, result: {} });
  assert.equal((await h.client.next()).id, "audio");
  h.writeClient({
    id: "interrupt",
    method: "turn/interrupt",
    params: { threadId: "thread-a", turnId: "turn-a" },
  });
  assert.equal((await h.client.next()).id, "interrupt");
  release();
  assert.equal((await h.client.next()).id, "steer");
});

test("initialize restores pending external work and metadata resume refreshes tracking", async (t) => {
  let restored = 0;
  const recovered = [];
  const h = fixture(t, {
    async restore() {
      restored++;
    },
    async recoverThread(id) {
      recovered.push(id);
    },
  });
  h.controller.bindings.set("thread-a", {});
  await h.initialize();
  assert.equal(restored, 1);
  h.writeClient({
    id: "resume",
    method: "thread/resume",
    params: { threadId: "thread-a", excludeTurns: true },
  });
  const request = await h.server.next();
  assert.deepEqual(recovered, ["thread-a"]);
  h.writeServer({ id: request.id, result: {} });
  assert.equal((await h.client.next()).id, "resume");
});

test("stop and input wait for an already requested realtime start", async (t) => {
  let release, entered;
  const entering = new Promise((resolve) => {
    entered = resolve;
  });
  const h = fixture(t, {
    async start(params) {
      this.bindings.set(params.threadId, {});
      entered();
      await new Promise((resolve) => {
        release = resolve;
      });
      return { ...params, externalOrchestrator: true };
    },
  });
  await h.initialize();
  h.writeClient({
    id: "start",
    method: "thread/realtime/start",
    params: { threadId: "thread-a" },
  });
  const capability = await h.server.next();
  h.writeServer({ id: capability.id, result: { protocolVersion: 1 } });
  await entering;
  h.writeClient({
    id: "stop",
    method: "thread/realtime/stop",
    params: { threadId: "thread-a" },
  });
  h.writeClient({
    id: "audio",
    method: "thread/realtime/appendAudio",
    params: { threadId: "thread-a", audio: { data: "test" } },
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(h.server.messages.length, 0);
  release();
  const start = await h.server.next();
  assert.equal(start.method, "thread/realtime/start");
  h.writeServer({ id: start.id, result: {} });
  assert.equal((await h.client.next()).id, "start");
  const stop = await h.server.next();
  const audio = await h.server.next();
  assert.equal(stop.method, "thread/realtime/stop");
  assert.equal(audio.method, "thread/realtime/appendAudio");
  h.writeServer({ id: stop.id, result: {} });
  h.writeServer({ id: audio.id, result: {} });
  await h.client.next();
  await h.client.next();
});

test("permission reply can release a work boundary while steering waits", async (t) => {
  let release, entered;
  const entering = new Promise((resolve) => {
    entered = resolve;
  });
  const h = fixture(t, {
    async typed() {
      entered();
      await new Promise((resolve) => {
        release = resolve;
      });
      return { turnId: "turn-a" };
    },
    async custom(method) {
      assert.equal(method, "bridge/permission/reply");
      release();
      return {};
    },
  });
  await h.initialize();
  h.controller.bindings.set("thread-a", {});
  h.writeClient({
    id: "steer",
    method: "turn/steer",
    params: { threadId: "thread-a", expectedTurnId: "turn-a" },
  });
  await entering;
  h.writeClient({
    id: "permission",
    method: "bridge/permission/reply",
    params: { threadId: "thread-a", requestId: "per_a", reply: "once" },
  });
  const replies = [await h.client.next(), await h.client.next()];
  assert.deepEqual(
    new Set(replies.map((reply) => reply.id)),
    new Set(["steer", "permission"]),
  );
});
