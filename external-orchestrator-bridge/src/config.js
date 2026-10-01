import { isAbsolute, resolve } from "node:path";
import { homedir } from "node:os";

function positiveInteger(value, fallback, name, max) {
  const number = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(number) || number <= 0 || number > max) {
    throw new Error(
      `${name} must be a positive integer no greater than ${max}`,
    );
  }
  return number;
}

export function configFromEnv(env = process.env) {
  const url = new URL(env.OPENCODE_URL || "http://127.0.0.1:4096");
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/"
  ) {
    throw new Error(
      "OPENCODE_URL must be an origin without embedded credentials, path, query, or fragment",
    );
  }
  if (!["http:", "https:"].includes(url.protocol))
    throw new Error("OPENCODE_URL must use HTTP or HTTPS");
  const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (
    !loopback &&
    (env.OPENCODE_ALLOW_REMOTE !== "1" ||
      url.protocol !== "https:" ||
      !env.OPENCODE_SERVER_PASSWORD)
  ) {
    throw new Error(
      "Remote OpenCode requires HTTPS, OPENCODE_SERVER_PASSWORD, and OPENCODE_ALLOW_REMOTE=1",
    );
  }
  if (!env.OPENCODE_DIRECTORY || !isAbsolute(env.OPENCODE_DIRECTORY)) {
    throw new Error("OPENCODE_DIRECTORY must be an absolute path");
  }
  const username = env.OPENCODE_SERVER_USERNAME || "opencode";
  if (username.includes(":") || /[\r\n\0]/.test(username))
    throw new Error("Invalid OPENCODE_SERVER_USERNAME");
  const codexCommand = env.CODEX_COMMAND || "codex";
  if (!codexCommand || codexCommand.includes("\0"))
    throw new Error("Invalid CODEX_COMMAND");
  return {
    url,
    authorization: env.OPENCODE_SERVER_PASSWORD
      ? `Basic ${Buffer.from(`${username}:${env.OPENCODE_SERVER_PASSWORD}`).toString("base64")}`
      : undefined,
    directory: resolve(env.OPENCODE_DIRECTORY),
    stateDirectory: resolve(
      env.BRIDGE_STATE_DIRECTORY || `${homedir()}/.codex-opencode-bridge`,
    ),
    sessionID: env.OPENCODE_SESSION_ID || undefined,
    codexCommand,
    codexArgs: ["app-server"],
    requestTimeoutMs: positiveInteger(
      env.BRIDGE_REQUEST_TIMEOUT_MS,
      15_000,
      "BRIDGE_REQUEST_TIMEOUT_MS",
      300_000,
    ),
    maxFrameBytes: positiveInteger(
      env.BRIDGE_MAX_FRAME_BYTES,
      16 * 1024 * 1024,
      "BRIDGE_MAX_FRAME_BYTES",
      64 * 1024 * 1024,
    ),
  };
}
