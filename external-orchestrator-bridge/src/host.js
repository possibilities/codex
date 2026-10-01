import { ProxyPeer, RpcError } from "./rpc.js";

const own = (value, key) => Object.hasOwn(value, key);
const record = (value) =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const validId = (value) =>
  typeof value === "string" ||
  (typeof value === "number" && Number.isSafeInteger(value));
const typedMethods = new Set(["turn/start", "turn/steer", "turn/interrupt"]);
const bridgeNotifications = new Set([
  "thread/realtime/started",
  "thread/realtime/closed",
  "thread/realtime/externalHandoff",
]);
const externalTransportMethods = new Set([
  "thread/realtime/appendAudio",
  "thread/realtime/appendText",
  "thread/realtime/appendSpeech",
  "thread/realtime/stop",
  "thread/name/set",
  "thread/unsubscribe",
]);
const nativeMutations = new Set([
  "config/value/write",
  "config/batchWrite",
  "config/mcpServer/reload",
  "mcpServer/tool/call",
  "environment/add",
  "windowsSandbox/setupStart",
  "skills/extraRoots/set",
  "skills/config/write",
  "experimentalFeature/enablement/set",
  "externalAgentConfig/import",
  "externalAgentConfig/import/recordHistory",
  "fs/writeFile",
  "fs/createDirectory",
  "fs/remove",
  "fs/copy",
  "plugin/install",
  "plugin/uninstall",
  "marketplace/add",
  "marketplace/remove",
  "marketplace/upgrade",
]);

function kind(message) {
  if (
    !record(message) ||
    (own(message, "jsonrpc") && message.jsonrpc !== "2.0")
  ) {
    throw new RpcError(-32600, "Invalid JSON-RPC envelope");
  }
  if (own(message, "id") && !validId(message.id))
    throw new RpcError(-32600, "Invalid JSON-RPC ID");
  if (own(message, "method")) {
    if (
      typeof message.method !== "string" ||
      !message.method ||
      own(message, "result") ||
      own(message, "error")
    ) {
      throw new RpcError(-32600, "Invalid JSON-RPC request");
    }
    if (
      own(message, "params") &&
      message.params !== null &&
      !record(message.params)
    )
      throw new RpcError(-32602, "RPC params must be an object");
    return own(message, "id") ? "request" : "notification";
  }
  if (
    !own(message, "id") ||
    own(message, "result") === own(message, "error") ||
    (own(message, "error") &&
      (!record(message.error) ||
        !Number.isInteger(message.error.code) ||
        typeof message.error.message !== "string"))
  ) {
    throw new RpcError(-32600, "Invalid JSON-RPC response");
  }
  return "response";
}

function responseTo(request, body) {
  return {
    ...(own(request, "jsonrpc") ? { jsonrpc: request.jsonrpc } : {}),
    id: request.id,
    ...body,
  };
}

function rpcFailure(error) {
  return {
    code: Number.isInteger(error.code) ? error.code : -32000,
    message:
      error instanceof RpcError
        ? error.message
        : "Bridge request failed; see bridge stderr",
    ...(error instanceof RpcError && error.data !== undefined
      ? { data: error.data }
      : {}),
  };
}

/** Duplex routing with separate request ownership in both directions. */
export class BridgeHost {
  constructor({
    client,
    server,
    controllerFactory,
    timeoutMs = 15_000,
    maxPending = 1024,
    report = () => {},
  }) {
    this.client = client;
    this.server = server;
    this.peer = new ProxyPeer(server, timeoutMs);
    this.controller = controllerFactory({
      peer: this.peer,
      notify: (message) => client.send(message),
    });
    this.report = report;
    this.timeoutMs = timeoutMs;
    this.maxPending = maxPending;
    this.clientRequests = new Set();
    this.serverRequests = new Map();
    this.serverRequestIds = new Map();
    this.serverReplyHistory = new Map();
    this.expiredClientResponses = new Set();
    this.clientOptOut = new Set();
    this.threadQueues = new Map();
    this.startBarriers = new Map();
    this.notificationQueues = new Map();
    this.reservedThreads = new Set();
    this.tasks = new Set();
    this.serverSequence = 0;
    this.initialized = false;
    this.nativeVerified = false;
    this.stopped = new Promise((resolve) => {
      this.stop = resolve;
    });
  }

  isExternal(threadId) {
    return (
      typeof threadId === "string" &&
      (this.reservedThreads.has(threadId) ||
        this.controller.bindings.has(threadId))
    );
  }

  capabilities() {
    return {
      protocolVersion: 1,
      nativeExternalOrchestrator: {
        verified: this.nativeVerified,
        requiredProtocolVersion: 1,
      },
      typedInput: ["text"],
      backingWorkControl: {
        requiredProtocolVersion: 1,
        routing: "dedicatedWorkId",
      },
      typedMethods: [...typedMethods],
      permissionReply: "bridge/permission/reply",
      questionReply: "bridge/question/reply",
      questionReject: "bridge/question/reject",
      pendingRequests: "bridge/requests/list",
      retryBlockedAdmission: "bridge/admission/retry",
      requestFingerprintRequired: true,
      externalHistory: "bridge/session/read",
      nativeDesktopParity: false,
      unsupported: [
        "native shell execution",
        "native review",
        "attachments",
        "native model/security/environment overrides",
      ],
    };
  }

  async run() {
    const streams = [
      this.client.input,
      this.client.output,
      this.server.input,
      this.server.output,
    ].filter(Boolean);
    const fail = (error) => this.stop(error);
    for (const stream of streams) stream.on("error", fail);
    const pumps = [this.pumpClient(), this.pumpServer()];
    for (const pump of pumps) pump.then(() => this.stop(), fail);
    try {
      const error = await this.stopped;
      if (error) throw error;
    } finally {
      await this.close();
      await Promise.allSettled(pumps);
      for (const stream of streams) stream.off("error", fail);
    }
  }

  async pumpClient() {
    for await (const message of this.client.messages()) {
      const type = kind(message);
      if (type === "response") {
        const request = this.serverRequests.get(message.id);
        if (!request && this.expiredClientResponses.has(message.id)) continue;
        if (!request)
          throw new RpcError(
            -32600,
            "Response ID is not owned by an outstanding server request",
          );
        this.serverRequests.delete(message.id);
        this.serverRequestIds.delete(request.originalId);
        if (request.threadId !== undefined)
          this.rememberReply(request.originalId, message.id);
        clearTimeout(request.timer);
        await this.server.send({ ...message, id: request.originalId });
      } else if (type === "notification") {
        // Only the app-server initialization notification is defined client-side.
        // Never let a request-shaped notification bypass external-thread routing.
        if (message.method !== "initialized")
          throw new RpcError(-32600, "Unsupported client notification");
        await this.server.send(message);
      } else {
        if (this.clientRequests.has(message.id))
          throw new RpcError(-32600, "Duplicate in-flight client request ID");
        if (this.clientRequests.size >= this.maxPending)
          throw new RpcError(-32000, "Too many outstanding client requests");
        this.clientRequests.add(message.id);
        const threadId = message.params?.threadId;
        if (
          message.method === "thread/realtime/start" &&
          typeof threadId === "string" &&
          threadId.length > 0 &&
          threadId.length <= 512 &&
          !this.isExternal(threadId)
        ) {
          if (this.reservedThreads.size >= this.maxPending)
            throw new RpcError(
              -32000,
              "Too many unresolved external session starts",
            );
          this.reservedThreads.add(threadId);
        }
        const immediate = new Set([
          "turn/interrupt",
          "thread/realtime/appendAudio",
          "thread/realtime/appendText",
          "thread/realtime/appendSpeech",
          "thread/realtime/stop",
        ]);
        const key =
          typeof threadId === "string" &&
          !immediate.has(message.method) &&
          !message.method.startsWith("bridge/")
            ? threadId
            : message.method === "initialize"
              ? "initialize"
              : undefined;
        const previous =
          key !== undefined
            ? this.threadQueues.get(key)
            : message.method.startsWith("thread/realtime/")
              ? this.startBarriers.get(threadId)
              : undefined;
        const task = Promise.resolve(previous).then(async () => {
          if (this.closed) return;
          try {
            const result = await this.route(message.method, message.params);
            await this.client.send(
              responseTo(message, {
                result: result === undefined ? {} : result,
              }),
            );
          } catch (error) {
            if (!(error instanceof RpcError)) this.report(error);
            await this.client.send(
              responseTo(message, { error: rpcFailure(error) }),
            );
          } finally {
            this.clientRequests.delete(message.id);
          }
        });
        if (key !== undefined) this.threadQueues.set(key, task);
        if (
          message.method === "thread/realtime/start" &&
          typeof threadId === "string"
        )
          this.startBarriers.set(threadId, task);
        this.track(task, () => {
          if (this.startBarriers.get(threadId) === task)
            this.startBarriers.delete(threadId);
          if (key !== undefined && this.threadQueues.get(key) === task)
            this.threadQueues.delete(key);
        });
      }
    }
  }

  async pumpServer() {
    for await (const message of this.server.messages()) {
      const type = kind(message);
      if (type === "response") {
        if (!this.peer.accept(message))
          throw new RpcError(
            -32600,
            "Response ID is not owned by an outstanding app-server request",
          );
      } else if (type === "request") {
        if (
          this.isExternal(message.params?.threadId) ||
          this.isExternal(message.params?.conversationId)
        ) {
          await this.server.send(
            responseTo(message, {
              error: {
                code: -32003,
                message:
                  "Native actions are disabled for externally orchestrated threads",
              },
            }),
          );
          continue;
        }
        if (this.serverRequests.size >= this.maxPending) {
          throw new RpcError(-32000, "Too many outstanding server requests");
        }
        if (this.serverRequestIds.has(message.id))
          throw new RpcError(-32600, "Duplicate unresolved server request ID");
        const id = `voice-bridge:server:${++this.serverSequence}`;
        const timer = setTimeout(() => {
          this.serverRequests.delete(id);
          this.serverRequestIds.delete(message.id);
          this.rememberExpired(id);
          if (
            typeof (
              message.params?.threadId ?? message.params?.conversationId
            ) === "string"
          )
            this.rememberReply(message.id, id);
          this.track(
            this.server.send(
              responseTo(message, {
                error: { code: -32001, message: "Client response timed out" },
              }),
            ),
          );
        }, this.timeoutMs);
        this.serverReplyHistory.delete(message.id);
        this.serverRequests.set(id, {
          originalId: message.id,
          timer,
          threadId: message.params?.threadId ?? message.params?.conversationId,
        });
        this.serverRequestIds.set(message.id, id);
        await this.client.send({ ...message, id });
      } else {
        if (message.method === "serverRequest/resolved") {
          const id =
            this.serverRequestIds.get(message.params?.requestId) ??
            this.serverReplyHistory.get(message.params?.requestId);
          if (id !== undefined) {
            this.serverRequestIds.delete(message.params.requestId);
            this.serverReplyHistory.delete(message.params.requestId);
            const pending = this.serverRequests.get(id);
            if (pending) {
              clearTimeout(pending.timer);
              this.serverRequests.delete(id);
              this.rememberExpired(id);
            }
            await this.client.send({
              ...message,
              params: { ...message.params, requestId: id },
            });
          }
          continue;
        }
        if (!bridgeNotifications.has(message.method)) {
          await this.client.send(message);
          continue;
        }
        const key = message.params?.threadId ?? "";
        const previous = this.notificationQueues.get(key);
        const task = Promise.resolve(previous).then(async () => {
          if (this.closed) return;
          await this.controller.notification(message);
        });
        this.notificationQueues.set(key, task);
        this.track(task, () => {
          if (this.notificationQueues.get(key) === task)
            this.notificationQueues.delete(key);
        });
        if (
          message.method !== "thread/realtime/externalHandoff" &&
          !this.clientOptOut.has(message.method)
        )
          await this.client.send(message);
      }
    }
    if (!this.closed)
      throw new RpcError(-32002, "App-server output closed unexpectedly");
  }

  track(task, cleanup = () => {}) {
    this.tasks.add(task);
    task.then(
      () => {
        this.tasks.delete(task);
        cleanup();
      },
      (error) => {
        this.tasks.delete(task);
        cleanup();
        this.stop(error);
      },
    );
  }

  rememberReply(originalId, clientId) {
    this.serverReplyHistory.delete(originalId);
    this.serverReplyHistory.set(originalId, clientId);
    if (this.serverReplyHistory.size > this.maxPending)
      this.serverReplyHistory.delete(
        this.serverReplyHistory.keys().next().value,
      );
  }

  rememberExpired(clientId) {
    this.expiredClientResponses.add(clientId);
    if (this.expiredClientResponses.size > this.maxPending)
      this.expiredClientResponses.delete(
        this.expiredClientResponses.values().next().value,
      );
  }

  async route(method, rawParams) {
    const params = rawParams ?? {};
    if (method === "bridge/capabilities") return this.capabilities();
    if (method === "initialize") {
      if (this.initialized)
        throw new RpcError(-32600, "Bridge is already initialized");
      if (params.capabilities != null && !record(params.capabilities))
        throw new RpcError(-32602, "Invalid initialization capabilities");
      const capabilities = { ...params.capabilities, experimentalApi: true };
      const optOut = capabilities.optOutNotificationMethods;
      if (optOut != null) {
        if (
          !Array.isArray(optOut) ||
          optOut.some((item) => typeof item !== "string")
        )
          throw new RpcError(-32602, "Invalid notification opt-outs");
        capabilities.optOutNotificationMethods = optOut.filter(
          (item) => !bridgeNotifications.has(item),
        );
      }
      const result = await this.peer.request(method, {
        ...params,
        capabilities,
      });
      this.clientOptOut = new Set(optOut ?? []);
      this.initialized = true;
      if (this.controller.restore) this.track(this.controller.restore());
      return result;
    }
    if (!this.initialized)
      throw new RpcError(
        -32002,
        "Initialize the app-server before sending requests",
      );
    if (method.startsWith("bridge/"))
      return this.controller.custom(method, params);
    const externalMode =
      this.reservedThreads.size > 0 || this.controller.bindings.size > 0;
    if (
      externalMode &&
      (method.startsWith("command/") ||
        method.startsWith("process/") ||
        nativeMutations.has(method))
    ) {
      throw new RpcError(
        -32601,
        `Native execution or settings mutation ${method} is unavailable in external mode`,
      );
    }
    if (externalMode && method === "getConversationSummary") {
      throw new RpcError(
        -32601,
        "Use bridge/session/read for external session history",
      );
    }
    if (
      externalMode &&
      ["thread/resume", "thread/fork"].includes(method) &&
      (own(params, "path") || own(params, "history"))
    ) {
      throw new RpcError(
        -32602,
        "Path or history based thread loading is unavailable in external mode",
      );
    }
    if (method === "thread/realtime/externalEvent")
      throw new RpcError(
        -32601,
        "External event injection is owned by the bridge",
      );
    if (method === "thread/realtime/start") {
      if (
        typeof params.threadId !== "string" ||
        !params.threadId ||
        params.threadId.length > 512
      )
        throw new RpcError(-32602, "Invalid threadId");
      await this.verifyNative();
      const rewritten = await this.controller.start(params);
      if (!this.controller.bindings.has(params.threadId))
        throw new RpcError(
          -32003,
          "External session binding was not established",
        );
      this.reservedThreads.delete(params.threadId);
      return this.peer.request(method, rewritten);
    }
    if (this.isExternal(params.threadId)) {
      if (typedMethods.has(method))
        return this.controller.typed(method, params);
      if (method === "thread/read") {
        if (params.includeTurns === true)
          throw new RpcError(
            -32602,
            "Use bridge/session/read for external session history",
          );
        for (const key of Object.keys(params)) {
          if (!["threadId", "includeTurns"].includes(key))
            throw new RpcError(
              -32602,
              `Unsupported external thread read field: ${key}`,
            );
        }
      } else if (method === "thread/resume") {
        for (const key of Object.keys(params)) {
          if (!["threadId", "excludeTurns"].includes(key))
            throw new RpcError(
              -32602,
              `Unsupported external thread resume field: ${key}`,
            );
        }
        if (params.excludeTurns !== true)
          throw new RpcError(
            -32602,
            "External resume requires excludeTurns:true; use bridge/session/read for history",
          );
        await this.controller.recoverThread?.(params.threadId);
      } else if (!externalTransportMethods.has(method)) {
        throw new RpcError(
          -32601,
          `Native method ${method} is unavailable for externally orchestrated threads`,
        );
      }
    }
    return this.peer.request(method, rawParams);
  }

  async verifyNative() {
    if (!this.nativeCheck) {
      this.nativeCheck = this.peer
        .request("thread/realtime/externalCapabilities", {})
        .then((result) => {
          if (result?.protocolVersion !== 1) {
            throw new RpcError(
              -32003,
              "App-server does not advertise the required external orchestrator protocol",
            );
          }
          this.nativeVerified = true;
        })
        .catch(() => {
          throw new RpcError(
            -32003,
            "A patched app-server with external orchestrator protocol v1 is required",
          );
        });
    }
    return this.nativeCheck;
  }

  async close() {
    if (this.closing) return this.closing;
    this.closed = true;
    this.stop();
    this.peer.close();
    this.client.close?.();
    this.server.close?.();
    for (const request of this.serverRequests.values())
      clearTimeout(request.timer);
    this.serverRequests.clear();
    this.serverRequestIds.clear();
    this.serverReplyHistory.clear();
    this.expiredClientResponses.clear();
    this.startBarriers.clear();
    this.client.input?.destroy();
    this.server.input?.destroy();
    this.closing = Promise.resolve().then(async () => {
      try {
        await this.controller.close();
      } finally {
        await Promise.allSettled([...this.tasks]);
      }
    });
    return this.closing;
  }
}
