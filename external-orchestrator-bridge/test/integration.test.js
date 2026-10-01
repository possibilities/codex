import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, writeFile, chmod, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { JsonLines } from "../src/rpc.js";

// Full process/HTTP/SSE exercise. The provider and native audio service are protocol fixtures;
// this deliberately makes no claim about authenticated real microphone/audio behavior.
test(
  "CLI handoff reaches durable OpenCode work and returns ordered native feedback; typed work stays external",
  { timeout: 20_000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "voice-integration-"));
    const command = join(directory, "native-fixture.mjs");
    await writeFile(
      command,
      `#!/usr/bin/env node
import { createInterface } from 'node:readline';
const send = (message) => process.stdout.write(JSON.stringify(message)+'\\n');
for await (const line of createInterface({input:process.stdin})) {
 const m=JSON.parse(line); if (!Object.hasOwn(m,'id')) continue;
 if(m.method==='initialize') send({id:m.id,result:{userAgent:'fixture'}});
 else if(m.method==='thread/realtime/externalCapabilities') send({id:m.id,result:{protocolVersion:1}});
 else if(m.method==='thread/realtime/start') {
  if(m.params.externalOrchestrator!==true || m.params.includeStartupContext!==true) throw Error('missing external startup');
  send({id:m.id,result:{}});
  send({method:'thread/realtime/started',params:{threadId:'thread-a',incarnationId:'inc-a',version:'v3'}});
  send({method:'thread/realtime/externalHandoff',params:{threadId:'thread-a',incarnationId:'inc-a',handoffId:'handoff-a',itemId:'voice-a',source:'handoff',inputTranscript:'say hello',activeTranscript:[{role:'user',text:'say hello'}],transcriptTail:null}});
 } else if(m.method==='thread/realtime/externalEvent') {
  send({id:m.id,result:{accepted:true}});
  send({method:'fixture/feedback',params:m.params});
 } else if(m.method==='thread/realtime/stop') {
  send({id:m.id,result:{}});
  send({method:'thread/realtime/closed',params:{threadId:'thread-a',incarnationId:'inc-a',reason:'requested'}});
 } else send({id:m.id,error:{code:-32601,message:'Unexpected native method '+m.method}});
}
`,
    );
    await chmod(command, 0o700);
    let sessionID;
    let globalStream;
    let sessionStream;
    const history = [];
    const prompts = [];
    const admissions = new Map();
    let interrupts = 0;
    const sse = (response, event) =>
      response?.write(`data: ${JSON.stringify(event)}\n\n`);
    const http = createServer(async (request, response) => {
      let body = "";
      for await (const chunk of request) body += chunk;
      const input = body ? JSON.parse(body) : {};
      const json = (data) => {
        response.setHeader("content-type", "application/json");
        response.end(JSON.stringify(data));
      };
      if (request.url === "/api/health")
        return json({ healthy: true, sessionWorkProtocolVersion: 1 });
      if (request.url === "/api/session" && request.method === "POST") {
        sessionID = input.id;
        return json({ data: { id: sessionID } });
      }
      if (request.url === "/api/event") {
        globalStream = response;
        response.writeHead(200, { "content-type": "text/event-stream" });
        return sse(response, {
          id: "connected",
          type: "server.connected",
          data: {},
        });
      }
      if (request.url === `/api/session/${sessionID}`)
        return json({ data: { id: sessionID } });
      if (request.url === `/api/session/${sessionID}/context`)
        return json({
          data: [
            {
              type: "assistant",
              content: [{ type: "text", text: "Prior context" }],
            },
          ],
        });
      if (
        [
          `/api/session/${sessionID}/permission`,
          `/api/session/${sessionID}/question`,
        ].includes(request.url)
      )
        return json({ data: [] });
      if (request.url?.startsWith(`/api/session/${sessionID}/event?`)) {
        sessionStream = response;
        response.writeHead(200, { "content-type": "text/event-stream" });
        response.flushHeaders();
        const after = Number(
          new URL(request.url, "http://fixture").searchParams.get("after"),
        );
        for (const event of history.filter(
          (event) => event.durable.seq > after,
        ))
          sse(response, event);
        return;
      }
      if (request.url === `/api/session/${sessionID}/prompt`) {
        prompts.push(input);
        let admission = admissions.get(input.id);
        if (!admission) {
          admission = {
            id: input.id,
            sessionID,
            admittedSeq: history.length + 1,
          };
          admissions.set(input.id, admission);
          const workID = `work-${admissions.size}`;
          const data = {
            sessionID,
            workID,
            inputMessageIDs: [input.id],
            assistantMessageID: `msg_assistant_${admissions.size}`,
            textID: "text-a",
            providerMetadata: {
              openai: { itemId: "provider-a", phase: "final_answer" },
            },
          };
          for (const [type, extra] of [
            ["text.started", {}],
            ["text.ended", { text: "Hello from OpenCode" }],
            ["work.settled", { outcome: "completed" }],
          ]) {
            const event = {
              id: `event-${history.length + 1}`,
              type: `session.next.${type}`,
              data: { ...data, ...extra },
              durable: {
                aggregateID: sessionID,
                seq: history.length + 1,
                version: 1,
              },
            };
            history.push(event);
            sse(sessionStream, event);
          }
        }
        return json({ data: admission });
      }
      if (request.url === `/api/session/${sessionID}/interrupt`) {
        interrupts++;
        response.writeHead(204);
        return response.end();
      }
      response.writeHead(404);
      response.end();
    });
    http.listen(0, "127.0.0.1");
    await once(http, "listening");
    const state = join(directory, "state");
    const child = spawn(
      process.execPath,
      [fileURLToPath(new URL("../src/main.js", import.meta.url))],
      {
        env: {
          ...process.env,
          CODEX_COMMAND: command,
          OPENCODE_DIRECTORY: directory,
          OPENCODE_URL: `http://127.0.0.1:${http.address().port}`,
          OPENCODE_SESSION_ID: "",
          BRIDGE_STATE_DIRECTORY: state,
        },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    const exited = once(child, "close");
    t.after(async () => {
      if (child.exitCode === null) child.kill("SIGTERM");
      await exited;
      globalStream?.end();
      sessionStream?.end();
      await new Promise((resolve) => {
        http.closeAllConnections();
        http.close(resolve);
      });
      await rm(directory, { recursive: true, force: true });
    });
    const transport = new JsonLines(child.stdout, child.stdin, 1024 * 1024);
    const messages = [];
    const waiting = new Set();
    const pump = (async () => {
      for await (const message of transport.messages()) {
        messages.push(message);
        for (const wake of waiting) wake();
      }
    })();
    async function waitFor(predicate) {
      const prior = messages.find(predicate);
      if (prior) return prior;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          waiting.delete(wake);
          reject(new Error(`Timed out waiting for message: ${stderr}`));
        }, 5000);
        const wake = () => {
          const value = messages.find(predicate);
          if (value) {
            clearTimeout(timer);
            waiting.delete(wake);
            resolve(value);
          }
        };
        waiting.add(wake);
      });
    }
    await transport.send({
      id: 1,
      method: "initialize",
      params: { clientInfo: { name: "integration", version: "1" } },
    });
    await waitFor((message) => message.id === 1);
    await transport.send({
      id: 2,
      method: "thread/realtime/start",
      params: { threadId: "thread-a", outputModality: "text", version: "v3" },
    });
    assert.deepEqual((await waitFor((message) => message.id === 2)).result, {});
    await waitFor(
      (message) =>
        message.method === "fixture/feedback" &&
        message.params.event.type === "workSettled",
    );
    const feedback = messages
      .filter((message) => message.method === "fixture/feedback")
      .map((message) => message.params);
    assert.deepEqual(
      feedback.map((event) => event.event.type),
      ["itemStarted", "itemEnded", "workSettled"],
    );
    assert.deepEqual(
      feedback.map((event) => event.sequence),
      [1, 2, 3],
    );
    assert.deepEqual(feedback[2].event.handoffIds, ["handoff-a"]);
    assert.match(prompts[0].prompt.text, /say hello/);
    await transport.send({
      id: 3,
      method: "thread/realtime/stop",
      params: { threadId: "thread-a" },
    });
    await waitFor((message) => message.id === 3);
    await transport.send({
      id: 4,
      method: "turn/start",
      params: {
        threadId: "thread-a",
        clientUserMessageId: "typed-a",
        input: [{ type: "text", text: "typed task" }],
      },
    });
    const turn = (await waitFor((message) => message.id === 4)).result.turn;
    await waitFor(
      (message) =>
        message.method === "turn/completed" &&
        message.params.turn.id === turn.id,
    );
    assert.equal(prompts.length, 2);
    assert.equal(prompts[1].prompt.text, "typed task");
    assert.equal(interrupts, 0);
    assert.ok(
      messages.some(
        (message) =>
          message.method === "item/completed" &&
          message.params.item.text === "Hello from OpenCode",
      ),
    );
    child.stdin.end();
    assert.deepEqual(await exited, [0, null]);
    await pump;
    const saved = JSON.parse(await readFile(join(state, "state.json"), "utf8"));
    assert.equal(
      Object.values(Object.values(saved.bindings)[0].admissions).every(
        (admission) => admission.settled,
      ),
      true,
    );
  },
);
