import { test } from "node:test";
import assert from "node:assert/strict";
import {
  mkdtemp,
  writeFile,
  chmod,
  access,
  readFile,
  rm,
} from "node:fs/promises";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { JsonLines } from "../src/rpc.js";

const cli = fileURLToPath(new URL("../src/main.js", import.meta.url));

async function launch(t) {
  const directory = await mkdtemp(join(tmpdir(), "voice-bridge-cli-"));
  const command = join(directory, "fake codex; no shell.mjs");
  await writeFile(
    command,
    `#!/usr/bin/env node
import { createInterface } from 'node:readline';
process.stderr.write('native diagnostic\\n');
for await (const line of createInterface({ input: process.stdin })) {
  const message = JSON.parse(line);
  if (message.method === 'test/crash') process.exit(9);
  if (!Object.hasOwn(message, 'id')) continue;
  const result = message.method === 'initialize'
    ? { argv: process.argv.slice(2), passwordPresent: Object.hasOwn(process.env, 'OPENCODE_SERVER_PASSWORD') }
    : {};
  process.stdout.write(JSON.stringify({ id: message.id, result }) + '\\n');
}
`,
  );
  await chmod(command, 0o700);
  const state = join(directory, "private-state");
  const child = spawn(process.execPath, [cli], {
    env: {
      ...process.env,
      OPENCODE_DIRECTORY: directory,
      BRIDGE_STATE_DIRECTORY: state,
      OPENCODE_SERVER_PASSWORD: "test-password-never-log",
      OPENCODE_URL: "http://127.0.0.1:4096",
      OPENCODE_SESSION_ID: "",
      CODEX_COMMAND: command,
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const exited = once(child, "close");
  const transport = new JsonLines(child.stdout, child.stdin, 1024 * 1024);
  const replies = transport.messages();
  t.after(async () => {
    child.stdout.resume();
    if (child.exitCode === null && child.signalCode === null)
      child.kill("SIGTERM");
    await exited;
    await rm(directory, { recursive: true, force: true });
  });
  return {
    child,
    state,
    directory,
    transport,
    replies,
    exited,
    stderr: () => stderr,
  };
}

test(
  "CLI spawns argv without a shell, separates diagnostics, and releases state lock on EOF",
  {
    timeout: 10_000,
    skip:
      process.platform === "win32" && "Fixture uses a Unix shebang executable",
  },
  async (t) => {
    const h = await launch(t);
    await h.transport.send({
      id: "original",
      method: "initialize",
      params: { clientInfo: { name: "cli-test", version: "1" } },
    });
    assert.deepEqual((await h.replies.next()).value, {
      id: "original",
      result: { argv: ["app-server"], passwordPresent: false },
    });
    await h.transport.send({ id: 2, method: "bridge/capabilities" });
    assert.equal((await h.replies.next()).value.result.protocolVersion, 1);
    h.child.stdin.end();
    assert.deepEqual(await h.exited, [0, null]);
    assert.match(h.stderr(), /native diagnostic/);
    assert.doesNotMatch(h.stderr(), /test-password-never-log/);
    await assert.rejects(access(join(h.state, ".writer-lock")), {
      code: "ENOENT",
    });
    assert.deepEqual(
      JSON.parse(await readFile(join(h.state, "state.json"), "utf8")),
      {
        version: 1,
        bindings: {},
        destination: {
          origin: "http://127.0.0.1:4096",
          directory: h.directory,
          sessionID: null,
        },
      },
    );
  },
);

test(
  "CLI reports unexpected native exit and still releases the state lock",
  {
    timeout: 10_000,
    skip:
      process.platform === "win32" && "Fixture uses a Unix shebang executable",
  },
  async (t) => {
    const h = await launch(t);
    await h.transport.send({
      id: 1,
      method: "initialize",
      params: { clientInfo: { name: "cli-test", version: "1" } },
    });
    await h.replies.next();
    await h.transport.send({ id: 2, method: "test/crash", params: {} });
    const remaining = Array.fromAsync(h.replies);
    assert.deepEqual(await h.exited, [1, null]);
    await remaining;
    assert.match(
      h.stderr(),
      /App-server output closed unexpectedly|App-server exited/,
    );
    await assert.rejects(access(join(h.state, ".writer-lock")), {
      code: "ENOENT",
    });
  },
);
