import { test } from "node:test";
import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import { configFromEnv } from "../src/config.js";
import { parseSse } from "../src/sse.js";
import { JsonLines, ProxyPeer } from "../src/rpc.js";

const base = { OPENCODE_DIRECTORY: process.cwd() };

test("credentials match OpenCode Basic auth and remote transport fails closed", () => {
  assert.equal(
    configFromEnv({ ...base, OPENCODE_SERVER_PASSWORD: "test-only" })
      .authorization,
    `Basic ${Buffer.from("opencode:test-only").toString("base64")}`,
  );
  for (const url of [
    "ftp://localhost",
    "http://remote.example",
    "https://user:pass@localhost",
    "http://localhost/x",
  ]) {
    assert.throws(() => configFromEnv({ ...base, OPENCODE_URL: url }));
  }
  assert.throws(() =>
    configFromEnv({ ...base, BRIDGE_REQUEST_TIMEOUT_MS: "NaN" }),
  );
  assert.throws(() =>
    configFromEnv({ ...base, BRIDGE_REQUEST_TIMEOUT_MS: "-1" }),
  );
  assert.equal(
    configFromEnv({
      ...base,
      OPENCODE_URL: "https://example.com",
      OPENCODE_ALLOW_REMOTE: "1",
      OPENCODE_SERVER_PASSWORD: "test",
    }).url.hostname,
    "example.com",
  );
});

test("SSE handles arbitrary CRLF and UTF-8 chunk boundaries", async () => {
  const bytes = Buffer.from(
    ': heartbeat\r\ndata: {"text":\r\ndata: "café"}\r\n\r\ndata: {"ok":true}\r\r',
  );
  async function* chunks() {
    for (const byte of bytes) yield Buffer.from([byte]);
  }
  assert.deepEqual(await Array.fromAsync(parseSse(chunks())), [
    { text: "café" },
    { ok: true },
  ]);
});

test("SSE bounds total frame bytes, including many short lines", async () => {
  async function* chunks() {
    for (let i = 0; i < 10; i++) yield Buffer.from(":x\n");
  }
  await assert.rejects(
    Array.fromAsync(parseSse(chunks(), undefined, 10)),
    /exceeds/,
  );
});

test("SSE drops incomplete frame and rejects invalid UTF-8", async () => {
  async function* incomplete() {
    yield Buffer.from('data: {"ok":true}');
  }
  assert.deepEqual(await Array.fromAsync(parseSse(incomplete())), []);
  async function* invalid() {
    yield Buffer.from([0xc3]);
  }
  await assert.rejects(Array.fromAsync(parseSse(invalid())));
});

test("JSON lines splits frames and rejects truncated/oversized frames", async () => {
  const input = new PassThrough();
  const transport = new JsonLines(input, new PassThrough(), 20);
  input.end('{"id":1}\n{"id":2}\n');
  assert.deepEqual(await Array.fromAsync(transport.messages()), [
    { id: 1 },
    { id: 2 },
  ]);
  const bad = new PassThrough();
  bad.end('{"id":1}');
  await assert.rejects(
    Array.fromAsync(new JsonLines(bad, new PassThrough(), 20).messages()),
    /Truncated/,
  );
});

test("RPC correlation cannot collide with client IDs and closes pending requests", async () => {
  const sent = [];
  const peer = new ProxyPeer({
    async send(value) {
      sent.push(value);
    },
  });
  const first = peer.request("a", {});
  assert.equal(peer.accept({ id: 1, result: false }), false);
  assert.equal(peer.accept({ id: sent[0].id, result: true }), true);
  assert.equal(await first, true);
  const second = peer.request("b", {});
  peer.close();
  await assert.rejects(second, /closed/);
});

test(
  "JSON output rejects closed backpressure and bounds queued bytes",
  { timeout: 2000 },
  async () => {
    const output = new PassThrough({ highWaterMark: 1 });
    output.on("error", () => {});
    const transport = new JsonLines(new PassThrough(), output, 100);
    const pending = transport.send({ value: "x".repeat(70) });
    const second = transport.send({ value: "x".repeat(70) });
    await assert.rejects(transport.send({ value: "x".repeat(70) }), /queue/);
    await new Promise((resolve) => setImmediate(resolve));
    output.destroy(new Error("test output closed"));
    await assert.rejects(pending, /closed/);
    await assert.rejects(second, /closed/);
    assert.equal(transport.queuedBytes, 0);
  },
);
