import { setTimeout as delay } from "node:timers/promises";
import { parseSse } from "./sse.js";
export { parseSse } from "./sse.js";

export class OpenCodeHttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

export class OpenCodeHttp {
  constructor(config, fetchImpl = fetch) {
    this.config = config;
    this.fetch = fetchImpl;
  }

  async request(method, path, body, signal = this.shutdownSignal) {
    const response = await this.fetch(new URL(path, this.config.url), {
      method,
      redirect: "error",
      headers: {
        ...(this.config.authorization
          ? { authorization: this.config.authorization }
          : {}),
        ...(body === undefined ? {} : { "content-type": "application/json" }),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: signal
        ? AbortSignal.any([
            signal,
            AbortSignal.timeout(this.config.requestTimeoutMs),
          ])
        : AbortSignal.timeout(this.config.requestTimeoutMs),
    });
    if (!response.ok)
      throw new OpenCodeHttpError(
        response.status,
        `OpenCode ${method} ${new URL(path, this.config.url).pathname} returned HTTP ${response.status}`,
      );
    if (response.status === 204) return undefined;
    const bytes = [];
    let size = 0;
    for await (const chunk of response.body) {
      size += chunk.byteLength;
      if (size > this.config.maxFrameBytes)
        throw new Error("OpenCode response exceeds configured limit");
      bytes.push(chunk);
    }
    return JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(bytes)),
    );
  }

  async capabilities() {
    const reply = await this.request("GET", "/api/health");
    if (reply.healthy !== true || reply.sessionWorkProtocolVersion !== 1) {
      throw new Error(
        "OpenCode server does not support durable voice work protocol version 1",
      );
    }
    return reply;
  }

  async ensureSession(sessionID = this.config.sessionID) {
    if (sessionID) {
      const reply = await this.request(
        "GET",
        `/api/session/${encodeURIComponent(sessionID)}`,
      );
      if (reply.data?.id !== sessionID)
        throw new Error("Configured OpenCode session identity mismatch");
      return reply.data;
    }
    const reply = await this.request("POST", "/api/session", {
      location: { directory: this.config.directory },
    });
    if (!reply.data?.id)
      throw new Error("OpenCode did not return a session ID");
    this.config.sessionID = reply.data.id;
    return reply.data;
  }

  async createSession(sessionID) {
    const reply = await this.request("POST", "/api/session", {
      id: sessionID,
      location: { directory: this.config.directory },
    });
    if (reply.data?.id !== sessionID)
      throw new Error("OpenCode session identity mismatch");
    return reply.data;
  }

  async prompt(sessionID, messageID, text) {
    const reply = await this.request(
      "POST",
      `/api/session/${encodeURIComponent(sessionID)}/prompt`,
      {
        id: messageID,
        prompt: { text },
        delivery: "steer",
      },
    );
    if (
      reply.data?.id !== messageID ||
      reply.data?.sessionID !== sessionID ||
      !Number.isSafeInteger(reply.data?.admittedSeq) ||
      reply.data.admittedSeq < 0
    ) {
      throw new Error("OpenCode returned an invalid prompt admission");
    }
    return reply.data;
  }

  async guardedPrompt(sessionID, workID, messageID, text) {
    const reply = await this.request(
      "POST",
      `/api/session/${encodeURIComponent(sessionID)}/work/${encodeURIComponent(workID)}/prompt`,
      { id: messageID, prompt: { text } },
    );
    if (
      reply.data?.id !== messageID ||
      reply.data?.sessionID !== sessionID ||
      !Number.isSafeInteger(reply.data.admittedSeq) ||
      reply.data.admittedSeq < 0 ||
      !Number.isSafeInteger(reply.data.promotedSeq) ||
      reply.data.promotedSeq < 0
    ) {
      throw new Error("OpenCode returned an invalid guarded prompt admission");
    }
    return reply.data;
  }

  async interruptWork(sessionID, workID) {
    await this.request(
      "POST",
      `/api/session/${encodeURIComponent(sessionID)}/work/${encodeURIComponent(workID)}/interrupt`,
    );
  }

  async history(sessionID, after) {
    const reply = await this.request(
      "GET",
      `/api/session/${encodeURIComponent(sessionID)}/history?after=${after}&limit=100`,
    );
    if (!Array.isArray(reply.data) || typeof reply.hasMore !== "boolean")
      throw new Error("OpenCode returned invalid session history");
    return reply;
  }

  async context(sessionID) {
    const reply = await this.request(
      "GET",
      `/api/session/${encodeURIComponent(sessionID)}/context`,
    );
    if (!Array.isArray(reply.data))
      throw new Error("OpenCode returned invalid session context");
    return reply.data;
  }

  async pending(sessionID, kind) {
    if (!["permission", "question"].includes(kind))
      throw new Error("Invalid pending request kind");
    const reply = await this.request(
      "GET",
      `/api/session/${encodeURIComponent(sessionID)}/${kind}`,
    );
    if (!Array.isArray(reply.data))
      throw new Error("Invalid pending request list");
    return reply.data;
  }

  async *sessionEvents(sessionID, after, signal) {
    const response = await this.fetch(
      new URL(
        `/api/session/${encodeURIComponent(sessionID)}/event?after=${after}`,
        this.config.url,
      ),
      {
        headers: {
          ...(this.config.authorization
            ? { authorization: this.config.authorization }
            : {}),
          accept: "text/event-stream",
        },
        signal,
        redirect: "error",
      },
    );
    if (!response.ok || !response.body)
      throw new Error(
        `OpenCode session subscription returned HTTP ${response.status}`,
      );
    yield* parseSse(response.body, signal, this.config.maxFrameBytes);
  }

  async *liveEvents(signal) {
    const response = await this.fetch(new URL("/api/event", this.config.url), {
      headers: {
        ...(this.config.authorization
          ? { authorization: this.config.authorization }
          : {}),
      },
      signal,
      redirect: "error",
    });
    if (!response.ok || !response.body)
      throw new Error(
        `OpenCode event subscription returned HTTP ${response.status}`,
      );
    yield* parseSse(response.body, signal, this.config.maxFrameBytes);
  }
}

export async function retryDelay(attempt, signal) {
  await delay(Math.min(5_000, 200 * 2 ** Math.min(attempt, 5)), undefined, {
    signal,
  });
}
