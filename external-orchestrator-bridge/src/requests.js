import { randomUUID } from "node:crypto";
import { RpcError } from "./rpc.js";
import { key, identifier } from "./controller.js";

export function requestFingerprint(request) {
  function canonical(value) {
    if (Array.isArray(value)) return value.map(canonical);
    if (value && typeof value === "object")
      return Object.fromEntries(
        Object.keys(value)
          .sort()
          .map((name) => [name, canonical(value[name])]),
      );
    return value;
  }
  return key(canonical(request));
}

function textInput(params, allowed) {
  for (const [name, value] of Object.entries(params)) {
    if (!allowed.includes(name) && value !== undefined && value !== null) {
      throw new RpcError(
        -32602,
        `OpenCode bridge does not support ${name}; no native fallback is allowed`,
      );
    }
  }
  if (!Array.isArray(params.input) || !params.input.length)
    throw new RpcError(-32602, "Input is required");
  const text = params.input
    .map((input) => {
      if (
        input.type !== "text" ||
        typeof input.text !== "string" ||
        (input.textElements?.length ?? 0) !== 0 ||
        Object.keys(input).some(
          (name) => !["type", "text", "textElements"].includes(name),
        )
      ) {
        throw new RpcError(
          -32602,
          "OpenCode bridge supports plain text input only",
        );
      }
      return input.text;
    })
    .join("\n");
  if (text.length > 128 * 1024)
    throw new RpcError(-32602, "Input exceeds bridge limit");
  return text;
}

function activeWork(binding, turnID) {
  const pending = Object.values(binding.admissions).filter(
    (admission) => admission.turn?.id === turnID && !admission.settled,
  );
  const workIDs = new Set(
    pending.map((admission) => admission.workID || admission.targetWorkID),
  );
  if (!pending.length || workIDs.has(undefined) || workIDs.size !== 1)
    throw new RpcError(
      -32602,
      "The external turn has no single confirmed active work; wait for promotion or refresh history",
    );
  return [...workIDs][0];
}

async function requireWorkControl(controller) {
  const capabilities = await controller.api.capabilities();
  if (capabilities.sessionWorkControlProtocolVersion !== 1)
    throw new RpcError(
      -32601,
      "OpenCode guarded work control protocol version 1 is required",
    );
}

export async function typedRequest(controller, method, params) {
  const binding = controller.bindings.get(
    identifier(params.threadId, "threadId"),
  );
  if (!binding) throw new RpcError(-32602, "Thread is not bound to OpenCode");
  const plan = await controller.serial(binding, async () => {
    await controller.ensureTracking(binding);
    const active = Object.values(binding.admissions).find(
      (item) => item.turn && !item.settled,
    );
    if (method === "turn/interrupt") {
      if (!active || params.turnId !== active.turn.id)
        throw new RpcError(-32602, "External turn identity mismatch");
      await requireWorkControl(controller);
      const workID = activeWork(binding, active.turn.id);
      try {
        await controller.api.interruptWork(binding.sessionID, workID);
      } catch (error) {
        if (error.status === 409)
          throw new RpcError(
            -32602,
            "The target work has ended or changed; nothing was interrupted",
          );
        throw error;
      }
      return { result: {} };
    }
    const text = textInput(params, [
      "threadId",
      "input",
      "clientUserMessageId",
      ...(method === "turn/steer" ? ["expectedTurnId"] : []),
    ]);
    if (!["turn/start", "turn/steer"].includes(method))
      throw new RpcError(-32601, "Unsupported external turn method");
    const clientId =
      params.clientUserMessageId == null
        ? randomUUID()
        : identifier(params.clientUserMessageId, "clientUserMessageId");
    const id = `msg_${key(binding.sessionID, clientId)}`;
    const existing = binding.admissions[id];
    if (existing && (existing.text !== text || existing.kind !== method))
      throw new RpcError(-32602, "Conflicting clientUserMessageId retry");
    if (
      method === "turn/steer" &&
      (existing
        ? existing.turn.id !== params.expectedTurnId
        : !active || active.turn.id !== params.expectedTurnId)
    )
      throw new RpcError(-32602, "External turn identity mismatch");
    if (method === "turn/start" && active && !existing)
      throw new RpcError(
        -32602,
        "An external turn is already active; use turn/steer",
      );
    if (method === "turn/steer" && existing?.admitted)
      return { result: { turnId: existing.turn.id } };
    const turn =
      existing?.turn ||
      (method === "turn/steer"
        ? active.turn
        : {
            id: randomUUID(),
            items: [],
            itemsView: "full",
            status: "inProgress",
            error: null,
            startedAt: Math.floor(Date.now() / 1000),
            completedAt: null,
            durationMs: null,
          });
    let targetWorkID;
    if (method === "turn/steer") {
      await requireWorkControl(controller);
      targetWorkID = existing?.targetWorkID || activeWork(binding, turn.id);
    }
    const admission = existing || {
      id,
      text,
      clientId,
      kind: method,
      turn,
      admitted: false,
      settled: false,
      handoffId: null,
      incarnationId: binding.incarnationId,
      ...(targetWorkID ? { targetWorkID } : {}),
    };
    binding.admissions[id] = admission;
    await controller.store.save();
    if (method === "turn/steer") return { admission };
    await controller.admit(binding, admission);
    if (!existing)
      await controller.notify({
        method: "turn/started",
        params: { threadId: binding.threadId, turn },
      });
    return { result: { turn } };
  });
  if (plan.result) return plan.result;
  const admission = plan.admission;
  // The HTTP call waits for a safe promotion boundary. Do not hold the controller
  // lane: replay and explicit interruption must keep working while it waits.
  try {
    const response = await controller.api.guardedPrompt(
      binding.sessionID,
      admission.targetWorkID,
      admission.id,
      admission.text,
    );
    await controller.serial(binding, async () => {
      admission.admitted = true;
      admission.admittedSeq = response.admittedSeq;
      admission.workID = admission.targetWorkID;
      await controller.store.save();
    });
    return { turnId: admission.turn.id };
  } catch (error) {
    if (error.status === 409) {
      await controller.serial(binding, async () => {
        if (
          binding.admissions[admission.id] === admission &&
          !admission.admitted
        )
          delete binding.admissions[admission.id];
        await controller.settleTurns(binding, [admission.turn]);
        await controller.store.save();
      });
      throw new RpcError(
        -32602,
        "The target work ended or changed; the steer was not admitted",
      );
    }
    // A lost response is uncertain, not a new admission. Retain exact ID/target for retry.
    throw error;
  }
}

export async function customRequest(controller, method, params) {
  const binding = controller.bindings.get(
    identifier(params.threadId, "threadId"),
  );
  if (!binding) throw new RpcError(-32602, "Thread is not bound to OpenCode");
  return controller.serial(binding, async () => {
    await controller.ensureTracking(binding);
    if (method === "bridge/session/read") {
      const after = params.after ?? 0;
      if (!Number.isSafeInteger(after) || after < 0)
        throw new RpcError(-32602, "Invalid history cursor");
      return {
        sessionID: binding.sessionID,
        ...(await controller.api.history(binding.sessionID, after)),
        turns: Object.values(binding.admissions)
          .filter((item) => item.turn && item.kind === "turn/start")
          .map((item) => item.turn),
      };
    }
    if (method === "bridge/requests/list") {
      await controller.recoverPending(binding);
      return {
        sessionID: binding.sessionID,
        ...Object.fromEntries(
          Object.entries(binding.pending).map(([kind, requests]) => [
            kind,
            requests.map((request) => ({
              ...request,
              requestFingerprint: requestFingerprint(request),
            })),
          ]),
        ),
      };
    }
    if (method === "bridge/admission/retry") {
      const id = identifier(params.inputMessageId, "inputMessageId");
      const admission =
        id.startsWith("msg_") && Object.hasOwn(binding.admissions, id)
          ? binding.admissions[id]
          : null;
      if (!admission?.blocked || admission.settled)
        throw new RpcError(
          -32602,
          "Only an explicitly blocked pending admission can be retried",
        );
      const result = await controller.api.prompt(
        binding.sessionID,
        id,
        admission.text,
      );
      admission.blocked = false;
      await controller.store.save();
      return { inputMessageId: id, admittedSeq: result.admittedSeq };
    }
    const match = /^bridge\/(permission|question)\/(reply|reject)$/.exec(
      method,
    );
    if (!match) throw new RpcError(-32601, "Unknown bridge method");
    const [, kind, action] = match;
    const requestId = identifier(params.requestId, "requestId");
    const requests = await controller.api.pending(binding.sessionID, kind);
    const request = requests.find(
      (item) => item.id === requestId && item.sessionID === binding.sessionID,
    );
    if (!request)
      throw new RpcError(
        -32602,
        "Pending request does not belong to this session or is no longer active",
      );
    if (params.requestFingerprint !== requestFingerprint(request))
      throw new RpcError(
        -32602,
        "Pending request changed; refresh and obtain a new decision",
      );
    const path = `/api/session/${encodeURIComponent(binding.sessionID)}/${kind}/${encodeURIComponent(requestId)}`;
    if (kind === "permission") {
      // Persistent permission changes intentionally have no bridge shortcut.
      if (action !== "reply" || !["once", "reject"].includes(params.reply))
        throw new RpcError(-32602, "Permission reply must be once or reject");
      await controller.api.request("POST", `${path}/reply`, {
        reply: params.reply,
        expectedRequest: request,
      });
    } else if (action === "reject")
      await controller.api.request("POST", `${path}/reject`);
    else {
      if (
        !Array.isArray(params.answers) ||
        params.answers.length !== request.questions.length
      )
        throw new RpcError(
          -32602,
          "Answer count does not match pending questions",
        );
      params.answers.forEach((answer, index) => {
        const question = request.questions[index];
        if (
          !Array.isArray(answer) ||
          answer.some(
            (value) => typeof value !== "string" || value.length > 16_384,
          ) ||
          (!question.multiple && answer.length > 1) ||
          new Set(answer).size !== answer.length ||
          (question.custom === false &&
            answer.some(
              (value) =>
                !question.options.some((option) => option.label === value),
            ))
        ) {
          throw new RpcError(-32602, "Invalid question answer");
        }
      });
      await controller.api.request("POST", `${path}/reply`, {
        answers: params.answers,
      });
    }
    await controller.recoverPending(binding);
    return {};
  });
}
