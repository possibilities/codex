#!/usr/bin/env node
import { spawn } from "node:child_process";
import { once } from "node:events";
import { pathToFileURL } from "node:url";
import { configFromEnv } from "./config.js";
import { BridgeController } from "./controller.js";
import { BridgeHost } from "./host.js";
import { OpenCodeHttp } from "./opencode.js";
import { JsonLines } from "./rpc.js";
import { StateStore } from "./state.js";

/** Run the stdio bridge. All diagnostics belong on stderr, never RPC stdout. */
export async function runBridge({
  env = process.env,
  input = process.stdin,
  output = process.stdout,
  diagnostics = process.stderr,
} = {}) {
  const config = configFromEnv(env);
  const store = await new StateStore(config.stateDirectory).load();
  let child;
  let host;
  let childExited;
  let killTimer;
  const stop = () => {
    host?.stop();
  };
  try {
    const childEnv = { ...env };
    // The native voice transport does not need the backing agent's password.
    delete childEnv.OPENCODE_SERVER_PASSWORD;
    child = spawn(config.codexCommand, config.codexArgs, {
      stdio: ["pipe", "pipe", "pipe"],
      env: childEnv,
      shell: false,
      windowsHide: true,
    });
    childExited = new Promise((resolve) =>
      child.once("close", (code, signal) => resolve({ code, signal })),
    );
    child.stderr.pipe(diagnostics, { end: false });
    await once(child, "spawn");
    host = new BridgeHost({
      client: new JsonLines(input, output, config.maxFrameBytes),
      server: new JsonLines(child.stdout, child.stdin, config.maxFrameBytes),
      timeoutMs: config.requestTimeoutMs,
      controllerFactory: ({ peer, notify }) =>
        new BridgeController({
          api: new OpenCodeHttp(config),
          peer,
          notify,
          store,
        }),
      report: (error) => diagnostics.write(`voice-bridge: ${error.message}\n`),
    });
    child.on("error", (error) => host.stop(error));
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
    await host.run();
    if (child.exitCode !== null && child.exitCode !== 0)
      throw new Error(`App-server exited with code ${child.exitCode}`);
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
    try {
      await host?.close();
    } finally {
      if (child && child.exitCode === null && child.signalCode === null) {
        child.stdin.end();
        child.kill("SIGTERM");
        killTimer = setTimeout(() => child.kill("SIGKILL"), 3_000);
        killTimer.unref();
      }
      if (childExited) await childExited;
      clearTimeout(killTimer);
      await store.close();
    }
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  if (Number(process.versions.node.split(".")[0]) < 22) {
    process.stderr.write("voice-bridge: Node.js 22 or newer is required\n");
    process.exitCode = 1;
  } else {
    runBridge().catch((error) => {
      process.stderr.write(`voice-bridge: ${error.message}\n`);
      process.exitCode = 1;
    });
  }
}
