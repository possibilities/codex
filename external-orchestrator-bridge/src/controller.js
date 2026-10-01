import { createHash, randomUUID } from "node:crypto";
import { RpcError } from "./rpc.js";
import { retryDelay } from "./opencode.js";
import { typedRequest, customRequest, requestFingerprint } from "./requests.js";

export const key = (...parts) =>
  createHash("sha256").update(JSON.stringify(parts)).digest("hex");
export function identifier(value, name) {
  if (typeof value !== "string" || !value || value.length > 512)
    throw new RpcError(-32602, `Invalid ${name}`);
  return value;
}
const MAX_TEXT = 64 * 1024;
function recentUtf8(text, maxBytes) {
  const bytes = Buffer.from(text);
  if (bytes.length <= maxBytes) return text;
  let start = bytes.length - maxBytes + 40;
  while ((bytes[start] & 0xc0) === 0x80) start++;
  return "[Earlier context omitted]\n" + bytes.subarray(start).toString("utf8");
}
const ownedAdmission = (binding, id) =>
  typeof id === "string" &&
  id.startsWith("msg_") &&
  Object.hasOwn(binding.admissions, id)
    ? binding.admissions[id]
    : undefined;
const phase = (data) =>
  ["commentary", "final_answer"].includes(data.providerMetadata?.openai?.phase)
    ? data.providerMetadata.openai.phase
    : null;

/** Owns durable admissions and serializes realtime feedback across two event transports. */
export class BridgeController {
  constructor({ api, peer, notify, store }) {
    this.api = api;
    this.peer = peer;
    this.notify = notify;
    this.store = store;
    const destination = {
      origin: api.config.url?.origin,
      directory: api.config.directory,
      sessionID: api.config.sessionID || null,
    };
    if (
      store.state.destination &&
      JSON.stringify(store.state.destination) !== JSON.stringify(destination)
    )
      throw new Error(
        "Bridge state belongs to a different OpenCode origin, directory, or configured session",
      );
    store.state.destination = destination;
    this.bindings = new Map(
      Object.values(store.state.bindings).map((binding) => [
        binding.threadId,
        binding,
      ]),
    );
    this.abort = new AbortController();
    this.api.shutdownSignal = this.abort.signal;
    this.tasks = new Set();
    this.queues = new Map();
    this.following = new Set();
    this.liveEpoch = 0;
    this.liveReady = false;
    this.closed = false;
    // JSON cannot preserve shared references: restore one canonical typed turn per ID.
    for (const binding of this.bindings.values()) {
      const turns = new Map();
      for (const admission of Object.values(binding.admissions)) {
        if (!admission.turn) continue;
        const prior = turns.get(admission.turn.id);
        if (!prior) turns.set(admission.turn.id, admission.turn);
        else {
          const items = new Map(
            [...prior.items, ...admission.turn.items].map((item) => [
              item.id,
              item,
            ]),
          );
          prior.items = [...items.values()];
          admission.turn = prior;
        }
      }
    }
    // A new app-server subprocess never shares the previous process's realtime incarnation.
    for (const binding of this.bindings.values()) {
      binding.incarnationId = null;
      binding.outbox = null;
      binding.sequence = 0;
    }
  }

  serial(binding, operation) {
    const prior = this.queues.get(binding.threadId) || Promise.resolve();
    const next = prior.then(operation);
    this.queues.set(
      binding.threadId,
      next.catch(() => {}),
    );
    return next;
  }

  task(operation) {
    const task = operation.catch(async () => {
      if (!this.closed)
        await this.notify({
          method: "bridge/error",
          params: {
            message: "Bridge event processing failed; work remains unresolved",
          },
        });
    });
    this.tasks.add(task);
    task.finally(() => this.tasks.delete(task));
  }

  async start(params) {
    for (const name of [
      "codexResponseHandoffChannelPrefixes",
      "codexResponseItemPrefix",
      "externalStartupContext",
    ]) {
      if (params[name] !== undefined && params[name] !== null)
        throw new RpcError(
          -32602,
          `OpenCode bridge does not support overriding ${name}`,
        );
    }
    for (const name of [
      "realtimeStartInstructions",
      "realtimeEndInstructions",
    ]) {
      if (params[name] != null && params[name] !== "")
        throw new RpcError(
          -32602,
          `OpenCode bridge does not support overriding ${name}`,
        );
    }
    if (
      params.codexResponseHandoffMode != null &&
      params.codexResponseHandoffMode !== "thinking"
    )
      throw new RpcError(
        -32602,
        "OpenCode bridge does not support alternate handoff mode",
      );
    if (
      params.clientManagedHandoffs === true ||
      params.codexResponsesAsItems === true ||
      params.externalOrchestrator === false
    ) {
      throw new RpcError(
        -32602,
        "Conflicting external handoff routing options",
      );
    }
    const threadId = identifier(params.threadId, "threadId");
    let binding = this.bindings.get(threadId);
    if (!binding) {
      if (this.api.config.sessionID && this.bindings.size)
        throw new RpcError(
          -32602,
          "Configured OpenCode session is already bound to another thread",
        );
      binding = {
        threadId,
        sessionID:
          this.api.config.sessionID || `ses_${key(threadId, randomUUID())}`,
        adopted: !!this.api.config.sessionID,
        cursor: 0,
        admissions: {},
        items: {},
        works: {},
        pending: {},
        sequence: 0,
        incarnationId: null,
        outbox: null,
      };
      this.bindings.set(threadId, binding);
      this.store.state.bindings[key(threadId)] = binding;
      await this.store.save(); // Session identity survives uncertain create responses.
    }
    return this.serial(binding, async () => {
      await this.api.capabilities();
      if (binding.adopted) await this.api.ensureSession(binding.sessionID);
      else await this.api.createSession(binding.sessionID); // Existing ID adopts the same Session.
      binding.retiredIncarnations ||= [];
      if (
        binding.incarnationId &&
        !binding.retiredIncarnations.includes(binding.incarnationId)
      )
        binding.retiredIncarnations.push(binding.incarnationId);
      binding.retiredIncarnations = binding.retiredIncarnations.slice(-8);
      binding.incarnationId = null;
      binding.outbox = null;
      binding.sequence = 0;
      await this.store.save();
      await this.ensureTracking(binding);
      const context = await this.api.context(binding.sessionID);
      const startup = context
        .filter((item) => ["user", "assistant"].includes(item.type))
        .slice(-20)
        .map(
          (item) =>
            `${item.type}: ${
              item.text ||
              item.content
                ?.filter((part) => part.type === "text")
                .map((part) => part.text)
                .join("\n") ||
              ""
            }`,
        )
        .join("\n");
      await this.recoverPending(binding);
      for (const admission of Object.values(binding.admissions)) {
        if (
          !admission.admitted &&
          !admission.settled &&
          !admission.targetWorkID
        )
          await this.admit(binding, admission);
      }
      return {
        ...params,
        externalOrchestrator: true,
        flushTranscriptTailOnSessionEnd:
          params.flushTranscriptTailOnSessionEnd ?? true,
        externalStartupContext: recentUtf8(startup, 21_200),
        includeStartupContext: params.includeStartupContext !== false,
      };
    });
  }

  async restore() {
    for (const binding of this.bindings.values()) {
      if (
        !Object.values(binding.admissions).some(
          (admission) => !admission.settled,
        )
      )
        continue;
      this.task(
        (async () => {
          let attempt = 0;
          while (!this.abort.signal.aborted) {
            try {
              await this.recoverThread(binding.threadId);
              return;
            } catch {
              if (this.abort.signal.aborted) return;
              if (!attempt)
                await this.notify({
                  method: "bridge/status",
                  params: {
                    threadId: binding.threadId,
                    transport: "restore",
                    connected: false,
                    message:
                      "Restoring pending OpenCode work; no provider work is being re-executed",
                  },
                });
            }
            await retryDelay(attempt++, this.abort.signal).catch(() => {});
          }
        })(),
      );
    }
  }

  async recoverThread(threadId) {
    const binding = this.bindings.get(threadId);
    if (binding) await this.serial(binding, () => this.ensureTracking(binding));
  }

  async ensureTracking(binding) {
    if (this.closed) throw new RpcError(-32002, "Bridge is closed");
    if (this.following.has(binding.threadId)) return;
    await this.api.capabilities();
    await this.api.ensureSession(binding.sessionID);
    if (!this.liveStarted) {
      this.liveStarted = true;
      this.task(this.followLive());
    }
    this.following.add(binding.threadId);
    this.task(this.followSession(binding));
    await this.recoverPending(binding);
  }

  async notification(message) {
    const params = message.params;
    const binding = params && this.bindings.get(params.threadId);
    if (!binding) return;
    await this.serial(binding, async () => {
      if (message.method === "thread/realtime/started") {
        if (
          params.incarnationId === binding.incarnationId ||
          binding.retiredIncarnations?.includes(params.incarnationId)
        )
          return;
        binding.incarnationId = identifier(
          params.incarnationId,
          "incarnationId",
        );
        binding.sequence = 0;
        binding.receipts = {};
        binding.playbackFailed = null;
        binding.outbox = null;
        // Old speech must never replay automatically into a new call.
        for (const item of Object.values(binding.items))
          item.forwardedIncarnation = null;
        await this.store.save();
      } else if (message.method === "thread/realtime/closed") {
        if (
          binding.incarnationId &&
          params.incarnationId !== binding.incarnationId
        )
          return;
        binding.retiredIncarnations ||= [];
        if (
          binding.incarnationId &&
          !binding.retiredIncarnations.includes(binding.incarnationId)
        )
          binding.retiredIncarnations.push(binding.incarnationId);
        binding.retiredIncarnations = binding.retiredIncarnations.slice(-8);
        binding.incarnationId = null;
        binding.outbox = null;
        await this.store.save(); // Closing voice is NOT an OpenCode interrupt.
      } else if (message.method === "thread/realtime/externalHandoff") {
        if (
          params.incarnationId !== binding.incarnationId &&
          !(
            params.source === "transcriptTail" &&
            (params.incarnationId === binding.playbackFailed?.incarnationId ||
              binding.retiredIncarnations?.includes(params.incarnationId))
          )
        )
          return;
        const handoffId = identifier(params.handoffId, "handoffId");
        if (
          typeof params.inputTranscript !== "string" ||
          Buffer.byteLength(params.inputTranscript) > MAX_TEXT
        )
          throw new Error("Invalid handoff transcript");
        const id = `msg_${key(binding.sessionID, params.incarnationId, handoffId)}`;
        const text = JSON.stringify({
          type: "codexVoiceHandoff",
          inputTranscript: params.inputTranscript,
          activeTranscript: params.activeTranscript,
          transcriptTail: params.transcriptTail ?? null,
        });
        if (Buffer.byteLength(text) > MAX_TEXT)
          throw new Error("Handoff exceeds bridge context limit");
        const existing = binding.admissions[id];
        if (existing && existing.text !== text)
          throw new Error("Conflicting handoff retry");
        const admission = existing || {
          id,
          text,
          handoffId,
          source: params.source,
          incarnationId:
            params.source === "transcriptTail" ? null : params.incarnationId,
          admitted: false,
          settled: false,
        };
        binding.admissions[id] = admission;
        await this.store.save();
        await this.admit(binding, admission);
      }
    });
  }

  async admit(binding, admission) {
    if (admission.admitted) return;
    const response = await this.api.prompt(
      binding.sessionID,
      admission.id,
      admission.text,
    );
    admission.admitted = true;
    admission.admittedSeq = response.admittedSeq;
    await this.store.save();
  }

  async feedback(binding, executionId, event, handoffId = null, token = null) {
    if (!binding.incarnationId) return;
    if (binding.outbox) await this.flush(binding);
    if (!binding.incarnationId) return;
    binding.receipts ||= {};
    if (token && binding.receipts[key(token)] === binding.incarnationId) return;
    binding.outboxToken = token;
    binding.outbox = {
      threadId: binding.threadId,
      incarnationId: binding.incarnationId,
      handoffId,
      executionId,
      sequence: binding.sequence + 1,
      event,
    };
    await this.store.save();
    await this.flush(binding);
  }

  async flush(binding) {
    if (!binding.outbox) return;
    const payload = binding.outbox;
    if (payload.incarnationId !== binding.incarnationId) {
      binding.outbox = null;
      await this.store.save();
      return;
    }
    // Applied ACK required; uncertain requests retry this exact payload and sequence.
    try {
      const result = await this.peer.request(
        "thread/realtime/externalEvent",
        payload,
      );
      if (typeof result?.accepted !== "boolean")
        throw new RpcError(
          -32600,
          "Invalid native external-event acknowledgement",
        );
    } catch (error) {
      if ([-32600, -32601, -32602, -32003].includes(error.code)) {
        await this.failPlayback(
          binding,
          "Native voice rejected external feedback; durable OpenCode work is still tracked",
        );
        return;
      }
      throw error;
    }
    binding.sequence = payload.sequence;
    if (binding.outboxToken) {
      binding.receipts ||= {};
      binding.receipts[key(binding.outboxToken)] = binding.incarnationId;
    }
    binding.outboxToken = null;
    binding.outbox = null;
    await this.store.save();
  }

  async failPlayback(binding, message) {
    if (!binding.incarnationId) return;
    binding.playbackFailed = { incarnationId: binding.incarnationId, message };
    binding.incarnationId = null;
    binding.outbox = null;
    await this.store.save();
    await this.notify({
      method: "bridge/playback/error",
      params: { threadId: binding.threadId, message },
    });
    // Retire the broken voice session only. The backing work and its durable replay continue.
    await this.peer
      .request("thread/realtime/stop", { threadId: binding.threadId })
      .catch(() => {});
  }

  async followLive() {
    let attempt = 0;
    while (!this.abort.signal.aborted) {
      try {
        for await (const event of this.api.liveEvents(this.abort.signal)) {
          if (event.type === "server.connected") {
            this.liveReady = true;
            this.liveEpoch++;
            attempt = 0;
            await this.notify({
              method: "bridge/status",
              params: { transport: "live", connected: true },
            });
            for (const binding of this.bindings.values())
              await this.serial(binding, () => this.recoverPending(binding));
            continue;
          }
          const binding = [...this.bindings.values()].find(
            (item) => item.sessionID === event.data?.sessionID,
          );
          if (!binding) continue;
          // Only the ordered per-session stream owns durable events and its cursor.
          if (event.durable && event.type !== "session.next.text.started")
            continue;
          await this.serial(binding, () => this.event(binding, event, false));
        }
      } catch {
        if (this.abort.signal.aborted) break;
      }
      if (this.liveReady)
        await this.notify({
          method: "bridge/status",
          params: {
            transport: "live",
            connected: false,
            message:
              "Live stream disconnected; complete text will recover from durable history",
          },
        });
      this.liveReady = false;
      for (const binding of this.bindings.values()) {
        for (const item of Object.values(binding.items))
          if (!item.ended) item.repairOnly = true;
      }
      if (!this.abort.signal.aborted)
        await retryDelay(attempt++, this.abort.signal).catch(() => {});
    }
  }

  async followSession(binding) {
    let attempt = 0;
    while (!this.abort.signal.aborted) {
      try {
        for await (const event of this.api.sessionEvents(
          binding.sessionID,
          binding.cursor,
          this.abort.signal,
        )) {
          await this.serial(binding, async () => {
            if (
              event.durable?.aggregateID !== binding.sessionID ||
              !Number.isSafeInteger(event.durable.seq)
            )
              throw new Error("Invalid session event envelope");
            if (event.durable.seq <= binding.cursor) return;
            await this.event(binding, event, true);
            binding.cursor = event.durable.seq;
            await this.store.save();
          });
          attempt = 0;
        }
      } catch {
        if (this.abort.signal.aborted) break;
        if (!attempt)
          await this.notify({
            method: "bridge/status",
            params: {
              threadId: binding.threadId,
              transport: "durable",
              connected: false,
              message:
                "Replay is reconnecting; work remains unresolved until a durable settlement arrives",
            },
          });
      }
      if (!this.abort.signal.aborted)
        await retryDelay(attempt++, this.abort.signal).catch(() => {});
    }
  }

  async event(binding, envelope, durable) {
    const data = envelope.data;
    if (
      !data ||
      Array.isArray(data) ||
      typeof data !== "object" ||
      data.sessionID !== binding.sessionID
    )
      return;
    identifier(envelope.id, "event id");
    if (typeof envelope.type !== "string")
      throw new Error("Invalid event type");
    // Existing sessions may contain pre-contract text events. They are replayable
    // history, but cannot be attributed to new external work or speech.
    if (
      envelope.type.startsWith("session.next.text.") &&
      data.workID === undefined &&
      data.inputMessageIDs === undefined
    )
      return;
    if (
      envelope.type === "session.next.work.settled" ||
      envelope.type.startsWith("session.next.text.")
    ) {
      identifier(data.workID, "workID");
      if (
        !Array.isArray(data.inputMessageIDs) ||
        data.inputMessageIDs.length > 4096 ||
        data.inputMessageIDs.some(
          (id) =>
            typeof id !== "string" || !id.startsWith("msg_") || id.length > 512,
        )
      )
        throw new Error("Invalid work admission IDs");
      if (
        data.pendingInputMessageIDs !== undefined &&
        (!Array.isArray(data.pendingInputMessageIDs) ||
          data.pendingInputMessageIDs.some(
            (id) =>
              typeof id !== "string" ||
              !id.startsWith("msg_") ||
              id.length > 512,
          ))
      )
        throw new Error("Invalid pending admission IDs");
    }
    if (
      envelope.type.startsWith("permission.v2.") ||
      envelope.type.startsWith("question.v2.")
    ) {
      await this.recoverPending(binding);
      return;
    }
    if (envelope.type === "session.next.prompted") {
      const admission = ownedAdmission(binding, data.messageID);
      if (admission && data.workID !== undefined) {
        identifier(data.workID, "workID");
        if (admission.targetWorkID && admission.targetWorkID !== data.workID)
          throw new Error("Guarded admission was promoted into another work");
        admission.workID = data.workID;
        admission.admitted = true;
        await this.store.save();
      }
      return;
    }
    if (envelope.type === "session.next.work.settled") {
      if (
        !durable ||
        !Array.isArray(data.inputMessageIDs) ||
        !["completed", "failed", "cancelled"].includes(data.outcome)
      )
        throw new Error("Invalid work settlement");
      const blocked = (data.pendingInputMessageIDs || []).filter((id) =>
        ownedAdmission(binding, id),
      );
      for (const id of blocked) binding.admissions[id].blocked = true;
      if (blocked.length)
        await this.notify({
          method: "bridge/work/blocked",
          params: {
            threadId: binding.threadId,
            sessionID: binding.sessionID,
            executionId: data.workID,
            inputMessageIDs: blocked,
            message: String(
              data.error?.message ||
                "Input remains pending after failed work initialization",
            ).slice(0, 4096),
          },
        });
      const admissions = data.inputMessageIDs
        .map((id) => ownedAdmission(binding, id))
        .filter(Boolean);
      const active = admissions.filter(
        (admission) =>
          binding.incarnationId &&
          admission.incarnationId === binding.incarnationId &&
          !admission.settled,
      );
      if (active.length) {
        const handoffIds = [
          ...new Set(active.map((item) => item.handoffId).filter(Boolean)),
        ];
        const terminal =
          data.outcome === "completed"
            ? { type: "workSettled", handoffIds }
            : data.outcome === "cancelled"
              ? { type: "workCancelled", handoffIds }
              : {
                  type: "workFailed",
                  handoffIds,
                  message: String(
                    data.error?.message || "OpenCode work failed",
                  ).slice(0, 4096),
                };
        await this.feedback(binding, data.workID, terminal, null, envelope.id);
      }
      for (const admission of admissions) {
        admission.admitted = true;
        admission.settled = true;
        admission.outcome = data.outcome;
        admission.error = data.error?.message || null;
      }
      const rejectedTurns = [];
      for (const admission of Object.values(binding.admissions)) {
        if (
          !admission.admitted &&
          admission.targetWorkID === data.workID &&
          !data.inputMessageIDs.includes(admission.id)
        ) {
          if (admission.turn) rejectedTurns.push(admission.turn);
          delete binding.admissions[admission.id];
        }
      }
      await this.settleTurns(binding, [
        ...admissions.map((admission) => admission.turn).filter(Boolean),
        ...rejectedTurns,
      ]);
      binding.works[key(data.workID)] = {
        settled: true,
        inputMessageIDs: data.inputMessageIDs,
        outcome: data.outcome,
      };
      await this.store.save();
      return;
    }
    if (!envelope.type.startsWith("session.next.text.")) {
      if (
        envelope.type.startsWith("session.next.tool.") ||
        envelope.type === "session.next.work.started"
      ) {
        await this.notify({
          method: "bridge/event",
          params: { threadId: binding.threadId, event: envelope },
        });
      }
      return;
    }
    if (!data.workID || !Array.isArray(data.inputMessageIDs)) return; // Historical/unpatched events cannot settle new work.
    const admissions = data.inputMessageIDs
      .map((id) => ownedAdmission(binding, id))
      .filter(Boolean);
    if (!admissions.length) return;
    identifier(data.assistantMessageID, "assistantMessageID");
    identifier(data.textID, "textID");
    const id = key(data.assistantMessageID, data.textID);
    let item = binding.items[id];
    if (!item) {
      if (!durable && envelope.type !== "session.next.text.started") return; // A delta cannot establish a prefix.
      item = {
        itemId: `oc_${key(data.assistantMessageID, data.textID)}`,
        providerItemId: data.providerMetadata?.openai?.itemId ?? null,
        workID: data.workID,
        text: "",
        ended: false,
        phase: phase(data),
        seen: [],
        handoffId: null,
        repairOnly: true,
        liveEpoch: this.liveEpoch,
        forwardedIncarnation: null,
      };
      binding.items[id] = item;
    }
    if (item.ended) return;
    // Only observing the live start proves we have the beginning of the delta stream.
    if (
      !durable &&
      envelope.type === "session.next.text.started" &&
      this.liveReady &&
      !item.text
    ) {
      item.repairOnly = false;
      item.liveEpoch = this.liveEpoch;
    }
    if (typeof envelope.id !== "string" || item.seen.includes(envelope.id))
      return;
    const active = admissions.find(
      (admission) =>
        binding.incarnationId &&
        admission.incarnationId === binding.incarnationId &&
        !admission.settled,
    );
    const forward = async (event, token = envelope.id) => {
      if (active && binding.incarnationId)
        await this.feedback(binding, data.workID, event, item.handoffId, token);
    };
    if (
      item.forwardedIncarnation !== binding.incarnationId &&
      active &&
      binding.incarnationId
    ) {
      item.handoffId = active.handoffId || null;
      await forward(
        {
          type: "itemStarted",
          itemId: item.itemId,
          phase: item.phase,
          text: null,
        },
        `${id}:start`,
      );
      item.forwardedIncarnation = binding.incarnationId;
    }
    if (envelope.type === "session.next.text.delta") {
      if (
        item.repairOnly ||
        item.liveEpoch !== this.liveEpoch ||
        !this.liveReady
      )
        return;
      if (typeof data.delta !== "string") throw new Error("Invalid text delta");
      if (
        Buffer.byteLength(item.text) + Buffer.byteLength(data.delta) >
        MAX_TEXT
      ) {
        item.repairOnly = true;
        await this.failPlayback(
          binding,
          "Assistant text exceeded the native voice limit; complete output remains in OpenCode history",
        );
        return;
      }
      await forward({
        type: "itemDelta",
        itemId: item.itemId,
        delta: data.delta,
      });
      item.text += data.delta;
    } else if (envelope.type === "session.next.text.ended") {
      if (typeof data.text !== "string") throw new Error("Invalid final text");
      if (Buffer.byteLength(data.text) > MAX_TEXT) {
        await this.failPlayback(
          binding,
          "Assistant text exceeded the native voice limit; complete output remains in OpenCode history",
        );
      }
      await forward({
        type: "itemEnded",
        itemId: item.itemId,
        text: data.text,
        phase: phase(data),
      });
      item.text = recentUtf8(data.text, 256 * 1024);
      item.displayTruncated = item.text !== data.text;
      item.phase = phase(data);
      item.providerItemId =
        data.providerMetadata?.openai?.itemId ?? item.providerItemId;
      item.ended = true;
    }
    const turns = new Map(
      admissions
        .filter((admission) => admission.turn)
        .map((admission) => [admission.turn.id, admission.turn]),
    );
    for (const turn of turns.values()) {
      let projected = turn.items.find((value) => value.id === item.itemId);
      if (!projected) {
        projected = {
          type: "agentMessage",
          id: item.itemId,
          text: "",
          phase: item.phase,
          memoryCitation: null,
        };
        turn.items.push(projected);
        await this.notify({
          method: "item/started",
          params: {
            threadId: binding.threadId,
            turnId: turn.id,
            item: { ...projected },
          },
        });
      }
      if (envelope.type === "session.next.text.delta") {
        await this.notify({
          method: "item/agentMessage/delta",
          params: {
            threadId: binding.threadId,
            turnId: turn.id,
            itemId: item.itemId,
            delta: data.delta,
          },
        });
      }
      projected.text = item.text;
      projected.phase = item.phase;
      if (item.ended)
        await this.notify({
          method: "item/completed",
          params: {
            threadId: binding.threadId,
            turnId: turn.id,
            item: { ...projected },
          },
        });
      for (const admission of Object.values(binding.admissions))
        if (admission.turn?.id === turn.id) admission.turn = turn;
    }
    item.seen.push(envelope.id);
    if (item.seen.length > 10_000) {
      item.repairOnly = true;
      item.seen = item.seen.slice(-100);
    }
    await this.notify({
      method: "bridge/item",
      params: {
        threadId: binding.threadId,
        sessionID: binding.sessionID,
        executionId: data.workID,
        itemId: item.itemId,
        providerItemId: item.providerItemId,
        phase: item.phase,
        text: item.text,
        ended: item.ended,
        displayTruncated: item.displayTruncated || false,
      },
    });
    await this.store.save();
  }

  async settleTurns(binding, values) {
    const turns = new Map(values.map((turn) => [turn.id, turn]));
    for (const turn of turns.values()) {
      const related = Object.values(binding.admissions).filter(
        (admission) => admission.turn?.id === turn.id,
      );
      if (
        related.some((admission) => !admission.settled) ||
        turn.status !== "inProgress"
      )
        continue;
      const failed = related.find(
        (admission) => admission.outcome === "failed",
      );
      turn.status = failed
        ? "failed"
        : related.some((admission) => admission.outcome === "cancelled")
          ? "interrupted"
          : "completed";
      turn.completedAt = Math.floor(Date.now() / 1000);
      turn.durationMs = Math.max(0, (turn.completedAt - turn.startedAt) * 1000);
      turn.error = failed
        ? {
            message: failed.error || "OpenCode work failed",
            codexErrorInfo: null,
            additionalDetails: null,
          }
        : null;
      for (const admission of related) admission.turn = turn;
      await this.notify({
        method: "turn/completed",
        params: { threadId: binding.threadId, turn },
      });
    }
  }

  async recoverPending(binding) {
    for (const kind of ["permission", "question"]) {
      const requests = await this.api.pending(binding.sessionID, kind);
      binding.pending[kind] = requests;
      for (const request of requests) {
        if (request.sessionID !== binding.sessionID)
          throw new Error("Pending request session mismatch");
        await this.notify({
          method: `bridge/${kind}/requested`,
          params: {
            threadId: binding.threadId,
            request,
            requestFingerprint: requestFingerprint(request),
          },
        });
      }
    }
    await this.store.save();
  }

  typed(method, params) {
    return typedRequest(this, method, params);
  }
  custom(method, params) {
    return customRequest(this, method, params);
  }
  async close() {
    this.closed = true;
    this.abort.abort();
    await Promise.allSettled([...this.tasks]);
    await this.store.save();
  }
}
