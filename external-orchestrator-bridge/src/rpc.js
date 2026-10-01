/** JSON-lines transport with bounded frames and backpressure, used by app-server stdio. */
export class JsonLines {
  constructor(input, output, maxFrameBytes) {
    this.input = input;
    this.output = output;
    this.maxFrameBytes = maxFrameBytes;
    this.writes = Promise.resolve();
    this.queuedBytes = 0;
    this.writeAbort = new AbortController();
  }

  async *messages() {
    let buffered = Buffer.alloc(0);
    for await (const chunk of this.input) {
      buffered = Buffer.concat([buffered, Buffer.from(chunk)]);
      let end;
      while ((end = buffered.indexOf(10)) !== -1) {
        if (end > this.maxFrameBytes)
          throw new Error("JSON-RPC frame exceeds configured limit");
        const line = new TextDecoder("utf-8", { fatal: true }).decode(
          buffered.subarray(0, end),
        );
        buffered = buffered.subarray(end + 1);
        if (!line.trim()) continue;
        const message = JSON.parse(line);
        if (!message || Array.isArray(message) || typeof message !== "object")
          throw new Error("Invalid JSON-RPC envelope");
        yield message;
      }
      if (buffered.length > this.maxFrameBytes)
        throw new Error("JSON-RPC frame exceeds configured limit");
    }
    if (buffered.toString("utf8").trim())
      throw new Error("Truncated JSON-RPC frame");
  }

  close() {
    this.writeAbort.abort();
  }

  send(message) {
    if (this.writeAbort.signal.aborted)
      return Promise.reject(new Error("JSON-RPC output closed"));
    const line = JSON.stringify(message) + "\n";
    if (Buffer.byteLength(line) > this.maxFrameBytes)
      return Promise.reject(
        new Error("JSON-RPC frame exceeds configured limit"),
      );
    const size = Buffer.byteLength(line);
    if (this.queuedBytes + size > this.maxFrameBytes * 2)
      return Promise.reject(
        new Error("JSON-RPC output queue exceeds configured limit"),
      );
    this.queuedBytes += size;
    const write = this.writes
      .then(
        () =>
          new Promise((resolve, reject) => {
            if (
              this.writeAbort.signal.aborted ||
              this.output.destroyed ||
              this.output.writableEnded
            ) {
              reject(new Error("JSON-RPC output closed"));
              return;
            }
            let finished = false;
            const done = (error) => {
              if (finished) return;
              finished = true;
              this.output.off("close", closed);
              this.output.off("error", done);
              this.writeAbort.signal.removeEventListener("abort", closed);
              if (error) reject(error);
              else resolve();
            };
            const closed = () => done(new Error("JSON-RPC output closed"));
            this.output.once("close", closed);
            this.output.once("error", done);
            this.writeAbort.signal.addEventListener("abort", closed, {
              once: true,
            });
            try {
              this.output.write(line, done);
            } catch (error) {
              done(error);
            }
          }),
      )
      .finally(() => {
        this.queuedBytes -= size;
      });
    this.writes = write.catch(() => {});
    return write;
  }
}

export class RpcError extends Error {
  constructor(code, message, data) {
    super(message);
    this.code = code;
    this.data = data;
  }
}

/** Remap EVERY outgoing ID; host IDs never share a namespace with bridge requests. */
export class ProxyPeer {
  constructor(transport, timeoutMs = 15_000) {
    this.transport = transport;
    this.timeoutMs = timeoutMs;
    this.next = 0;
    this.pending = new Map();
    this.expired = new Set();
  }

  request(method, params) {
    if (this.closed)
      return Promise.reject(
        new RpcError(-32002, "App-server connection closed"),
      );
    return new Promise((resolve, reject) => {
      const id = `voice-bridge:${++this.next}`;
      const timer = setTimeout(() => {
        this.pending.delete(id);
        this.expired.add(id);
        if (this.expired.size > 4096)
          this.expired.delete(this.expired.values().next().value);
        reject(new RpcError(-32001, "App-server request timed out"));
      }, this.timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.transport.send({ id, method, params }).catch((error) => {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(error);
      });
    });
  }

  accept(message) {
    if (message.method !== undefined) return false;
    if (this.expired.has(message.id)) return true;
    if (!this.pending.has(message.id)) return false;
    const pending = this.pending.get(message.id);
    this.pending.delete(message.id);
    clearTimeout(pending.timer);
    if (
      Object.hasOwn(message, "result") === Object.hasOwn(message, "error") ||
      (Object.hasOwn(message, "error") &&
        (!message.error ||
          !Number.isInteger(message.error.code) ||
          typeof message.error.message !== "string"))
    ) {
      pending.reject(new RpcError(-32600, "Invalid JSON-RPC response"));
      return true;
    }
    if (message.error)
      pending.reject(
        new RpcError(
          message.error.code,
          message.error.message,
          message.error.data,
        ),
      );
    else pending.resolve(message.result);
    return true;
  }

  close() {
    this.closed = true;
    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(new RpcError(-32002, "App-server connection closed"));
    }
    this.pending.clear();
  }
}
