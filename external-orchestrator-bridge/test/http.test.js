import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { OpenCodeHttp } from "../src/opencode.js";
import { configFromEnv } from "../src/config.js";

async function server(t, handler) {
  const http = createServer(handler);
  http.listen(0, "127.0.0.1");
  await once(http, "listening");
  t.after(
    () =>
      new Promise((resolve) => {
        http.closeAllConnections();
        http.close(resolve);
      }),
  );
  return `http://127.0.0.1:${http.address().port}`;
}

test("real HTTP request uses Basic credentials, durable admission and bound session identity", async (t) => {
  const received = [];
  const url = await server(t, async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    received.push({
      path: request.url,
      auth: request.headers.authorization,
      body: JSON.parse(body),
    });
    response.setHeader("content-type", "application/json");
    response.end(
      JSON.stringify({
        data: { id: "msg_a", sessionID: "ses_a", admittedSeq: 12 },
      }),
    );
  });
  const api = new OpenCodeHttp(
    configFromEnv({
      OPENCODE_DIRECTORY: process.cwd(),
      OPENCODE_URL: url,
      OPENCODE_SERVER_PASSWORD: "test-only",
    }),
  );
  assert.equal((await api.prompt("ses_a", "msg_a", "hello")).admittedSeq, 12);
  assert.deepEqual(received, [
    {
      path: "/api/session/ses_a/prompt",
      auth: `Basic ${Buffer.from("opencode:test-only").toString("base64")}`,
      body: { id: "msg_a", prompt: { text: "hello" }, delivery: "steer" },
    },
  ]);
});

test("HTTP redirects never leak credentials or execute at another origin", async (t) => {
  let destinationRequests = 0;
  const destination = await server(t, (request, response) => {
    destinationRequests++;
    response.end("{}");
  });
  const source = await server(t, (request, response) => {
    response.writeHead(307, { location: destination });
    response.end();
  });
  const api = new OpenCodeHttp(
    configFromEnv({
      OPENCODE_DIRECTORY: process.cwd(),
      OPENCODE_URL: source,
      OPENCODE_SERVER_PASSWORD: "test-only",
    }),
  );
  await assert.rejects(
    api.request("POST", "/api/session", { private: "context" }),
  );
  assert.equal(destinationRequests, 0);
});

test("HTTP bodies bounded and wrong-session admission rejected", async (t) => {
  const url = await server(t, (request, response) =>
    response.end(
      JSON.stringify({
        data: { id: "msg_a", sessionID: "ses_other", admittedSeq: 1 },
      }),
    ),
  );
  const config = configFromEnv({
    OPENCODE_DIRECTORY: process.cwd(),
    OPENCODE_URL: url,
  });
  await assert.rejects(
    new OpenCodeHttp(config).prompt("ses_a", "msg_a", "hello"),
    /invalid prompt admission/,
  );
  await assert.rejects(
    new OpenCodeHttp({ ...config, maxFrameBytes: 5 }).request(
      "GET",
      "/api/session",
    ),
    /exceeds/,
  );
});

test("per-session SSE replays with exclusive cursor and Basic auth", async (t) => {
  const url = await server(t, (request, response) => {
    assert.equal(request.url, "/api/session/ses_a/event?after=9");
    assert.equal(request.headers.accept, "text/event-stream");
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end('data: {"id":"evt_12","durable":{"seq":12}}\n\n');
  });
  const api = new OpenCodeHttp(
    configFromEnv({ OPENCODE_DIRECTORY: process.cwd(), OPENCODE_URL: url }),
  );
  assert.deepEqual(await Array.fromAsync(api.sessionEvents("ses_a", 9)), [
    { id: "evt_12", durable: { seq: 12 } },
  ]);
});

test("guarded controls use only dedicated exact-work routes and preserve definitive conflicts", async (t) => {
  const paths = [];
  const url = await server(t, async (request, response) => {
    paths.push(request.url);
    if (request.url.endsWith("/interrupt")) {
      response.writeHead(409);
      response.end("private error body");
      return;
    }
    response.setHeader("content-type", "application/json");
    response.end(
      JSON.stringify({
        data: {
          id: "msg_a",
          sessionID: "ses_a",
          admittedSeq: 8,
          promotedSeq: 8,
        },
      }),
    );
  });
  const api = new OpenCodeHttp(
    configFromEnv({ OPENCODE_DIRECTORY: process.cwd(), OPENCODE_URL: url }),
  );
  assert.equal(
    (await api.guardedPrompt("ses_a", "work_a", "msg_a", "steer")).promotedSeq,
    8,
  );
  await assert.rejects(
    api.interruptWork("ses_a", "work_a"),
    (error) => error.status === 409 && !error.message.includes("private error"),
  );
  assert.deepEqual(paths, [
    "/api/session/ses_a/work/work_a/prompt",
    "/api/session/ses_a/work/work_a/interrupt",
  ]);
});
