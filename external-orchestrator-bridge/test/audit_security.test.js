import { test } from "node:test";
import assert from "node:assert/strict";
import { Readable, PassThrough } from "node:stream";
import {
  mkdtemp,
  chmod,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { configFromEnv } from "../src/config.js";
import { parseSse } from "../src/sse.js";
import { JsonLines, ProxyPeer } from "../src/rpc.js";
import { OpenCodeHttp } from "../src/opencode.js";
import { StateStore } from "../src/state.js";

const config = (values = {}) =>
  configFromEnv({ OPENCODE_DIRECTORY: "/tmp/project", ...values });
const response = (value, status = 200) =>
  new Response(JSON.stringify(value), { status });
const consume = (stream) => Array.fromAsync(stream);

for (const url of [
  "http://example.test",
  "https://example.test",
  "http://127.0.0.1/path",
  "http://127.0.0.1/?token=x",
  "http://127.0.0.1/#x",
  "https://alice:secret@127.0.0.1/",
]) {
  test(`audit config rejects unsafe origin ${url.replace("secret", "redacted")}`, () => {
    assert.throws(() => config({ OPENCODE_URL: url }));
  });
}

test("audit config rejects invalid limits and Basic usernames", () => {
  for (const value of ["0", "-1", "Infinity", "1.5", "9007199254740993"]) {
    assert.throws(() => config({ BRIDGE_REQUEST_TIMEOUT_MS: value }));
    assert.throws(() => config({ BRIDGE_MAX_FRAME_BYTES: value }));
  }
  for (const username of ["name:other", "name\r\nInjected: x", "name\0"]) {
    assert.throws(() => config({ OPENCODE_SERVER_USERNAME: username }));
  }
});

test("audit credentials stay in headers and redirect rejection covers all transports", async () => {
  const seen = [];
  const secret = "test-only-audit-secret";
  const api = new OpenCodeHttp(
    config({ OPENCODE_SERVER_PASSWORD: secret }),
    async (url, options) => {
      seen.push({ url: String(url), options });
      if (String(url).includes("/event"))
        return new Response('data: {"type":"server.connected","data":{}}\n\n');
      return response({ data: [] });
    },
  );
  await api.context("ses_a");
  await consume(api.liveEvents());
  await consume(api.sessionEvents("ses_a", 0));
  assert.equal(seen.length, 3);
  for (const call of seen) {
    assert.equal(call.options.redirect, "error");
    assert.equal(
      call.options.headers.authorization,
      `Basic ${Buffer.from(`opencode:${secret}`).toString("base64")}`,
    );
    assert.equal(call.url.includes(secret), false);
  }
});

test("audit remote HTTP failure messages do not include credentials or response body", async () => {
  const api = new OpenCodeHttp(
    config({ OPENCODE_SERVER_PASSWORD: "audit-secret" }),
    async () =>
      new Response("server accidentally reflected audit-secret", {
        status: 403,
      }),
  );
  await assert.rejects(
    api.context("ses_a"),
    (error) =>
      error.message.includes("HTTP 403") &&
      !error.message.includes("audit-secret"),
  );
});

test("audit bounded HTTP body rejects oversized responses before parsing", async () => {
  const api = new OpenCodeHttp({ ...config(), maxFrameBytes: 10 }, async () =>
    response({ data: "x".repeat(100) }),
  );
  await assert.rejects(api.context("ses_a"), /exceeds/);
});

test("audit HTTP JSON must not silently repair malformed UTF-8", async () => {
  const api = new OpenCodeHttp(
    config(),
    async () =>
      new Response(
        Buffer.concat([
          Buffer.from('{"data":["'),
          Buffer.from([0xc3]),
          Buffer.from('"]}'),
        ]),
      ),
  );
  await assert.rejects(api.context("ses_a"), /UTF|encod|valid/i);
});

test("audit prompt admission validates nonnegative sequence and exact requested identity", async () => {
  for (const data of [
    { id: "msg_wrong", sessionID: "ses_a", admittedSeq: 1 },
    { id: "msg_a", sessionID: "ses_wrong", admittedSeq: 1 },
    { id: "msg_a", sessionID: "ses_a", admittedSeq: -1 },
    { id: "msg_a", sessionID: "ses_a", admittedSeq: 1.5 },
  ]) {
    const api = new OpenCodeHttp(config(), async () => response({ data }));
    await assert.rejects(api.prompt("ses_a", "msg_a", "hello"), /admission/);
  }
});

test("audit SSE accepts every possible byte split, multiline fields, and CRLF", async () => {
  const bytes = Buffer.from(
    ': keepalive\r\ndata: {"text":\r\ndata: "🧪café"}\r\n\r\n',
  );
  for (let split = 0; split <= bytes.length; split++) {
    assert.deepEqual(
      await consume(
        parseSse(
          Readable.from([bytes.subarray(0, split), bytes.subarray(split)]),
        ),
      ),
      [{ text: "🧪café" }],
    );
  }
});

test("audit SSE errors on invalid JSON and invalid UTF-8, drops incomplete EOF", async () => {
  await assert.rejects(
    consume(parseSse(Readable.from([Buffer.from("data: {invalid}\n\n")]))),
  );
  await assert.rejects(consume(parseSse(Readable.from([Buffer.from([0xff])]))));
  assert.deepEqual(
    await consume(
      parseSse(Readable.from([Buffer.from('data: {"ok":true}\n')])),
    ),
    [],
  );
});

test("audit SSE frame bound includes CRLF bytes rather than characters", async () => {
  const bytes = Buffer.from('data: {"ok":1}\r\n\r\n');
  await assert.rejects(
    consume(parseSse(Readable.from([bytes]), undefined, bytes.length - 1)),
    /exceeds/,
  );
});

test("audit JSON lines must not silently repair malformed UTF-8", async () => {
  const bytes = Buffer.concat([
    Buffer.from('{"id":"'),
    Buffer.from([0xc3]),
    Buffer.from('","result":1}\n'),
  ]);
  await assert.rejects(
    consume(
      new JsonLines(Readable.from([bytes]), new PassThrough(), 100).messages(),
    ),
    /UTF|encod|valid/i,
  );
});

test("audit JSON lines rejects oversized, truncated, primitive and array frames", async () => {
  for (const data of [
    "[1]\n",
    "null\n",
    '"string"\n',
    '{"x":"' + "z".repeat(200) + '"}\n',
    '{"id":1}',
  ]) {
    await assert.rejects(
      consume(
        new JsonLines(
          Readable.from([Buffer.from(data)]),
          new PassThrough(),
          80,
        ).messages(),
      ),
    );
  }
});

test("audit ProxyPeer cannot treat malformed response as successful admission", async () => {
  const sent = [];
  const peer = new ProxyPeer({
    async send(value) {
      sent.push(value);
    },
  });
  const request = peer.request("example", {});
  const settled = request.then(
    () => "resolved",
    () => "rejected",
  );
  const accepted = peer.accept({ id: sent[0].id });
  peer.close();
  assert.ok(
    accepted === false || (await settled) === "rejected",
    "response without result/error must not resolve successfully",
  );
  assert.equal(await settled, "rejected");
});

test("audit ProxyPeer rejects pending work immediately when output send fails", async () => {
  const peer = new ProxyPeer({
    async send() {
      throw new Error("writer closed");
    },
  });
  await assert.rejects(peer.request("example", {}), /writer closed/);
  assert.equal(peer.pending.size, 0);
  peer.close();
});

test("audit StateStore prevents second writer, persists bytes, and leaves no credentials by default", async () => {
  const directory = await mkdtemp(join(tmpdir(), "voice-audit-state-"));
  const first = new StateStore(directory);
  const second = new StateStore(directory);
  try {
    await first.load();
    await assert.rejects(second.load(), /EEXIST/);
    first.state.bindings.example = { threadId: "thread-a", sessionID: "ses_a" };
    await first.save();
    assert.deepEqual(
      JSON.parse(await readFile(first.path, "utf8")),
      first.state,
    );
    await first.close();
    await second.load();
    assert.deepEqual(second.state.bindings.example, {
      threadId: "thread-a",
      sessionID: "ses_a",
    });
  } finally {
    await first.close();
    await second.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("audit StateStore rejects readable-by-others directory and symlink state file", async () => {
  const directory = await mkdtemp(join(tmpdir(), "voice-audit-unsafe-"));
  try {
    await chmod(directory, 0o755);
    await assert.rejects(new StateStore(directory).load(), /private directory/);
    await chmod(directory, 0o700);
    await writeFile(join(directory, "target"), '{"version":1,"bindings":{}}', {
      mode: 0o600,
    });
    await symlink(join(directory, "target"), join(directory, "state.json"));
    await assert.rejects(new StateStore(directory).load());
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("audit OpenCode health capability rejects absent, unhealthy, or mismatched work protocol", async () => {
  for (const value of [
    { healthy: true },
    { healthy: false, sessionWorkProtocolVersion: 1 },
    { healthy: true, sessionWorkProtocolVersion: 2 },
    { healthy: true, sessionWorkProtocolVersion: "1" },
  ]) {
    await assert.rejects(
      new OpenCodeHttp(config(), async () => response(value)).capabilities(),
      /protocol version/,
    );
  }
  assert.equal(
    (
      await new OpenCodeHttp(config(), async () =>
        response({ healthy: true, sessionWorkProtocolVersion: 1 }),
      ).capabilities()
    ).sessionWorkProtocolVersion,
    1,
  );
});
