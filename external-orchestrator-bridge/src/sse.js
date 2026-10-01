/** Parse bounded SSE events, including CR, LF and CRLF split across chunks. */
export async function* parseSse(body, signal, maxBytes = 16 * 1024 * 1024) {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let line = "";
  let data = [];
  let eventBytes = 0;
  let cr = false;
  function finishLine() {
    const current = line;
    line = "";
    if (!current) {
      const event = data.length ? JSON.parse(data.join("\n")) : undefined;
      data = [];
      eventBytes = 0;
      return event;
    }
    if (current === "data") data.push("");
    else if (current.startsWith("data:"))
      data.push(current.slice(5).replace(/^ /, ""));
  }
  for await (const chunk of body) {
    if (signal?.aborted) return;
    for (const character of decoder.decode(chunk, { stream: true })) {
      if (cr && character !== "\n") {
        cr = false;
        const event = finishLine();
        if (event !== undefined) yield event;
      }
      eventBytes += Buffer.byteLength(character);
      if (eventBytes > maxBytes)
        throw new Error("OpenCode SSE frame exceeds configured limit");
      if (character === "\r") cr = true;
      else if (character === "\n") {
        cr = false;
        const event = finishLine();
        if (event !== undefined) yield event;
      } else line += character;
    }
  }
  decoder.decode(); // Reject truncated UTF-8; incomplete SSE events are deliberately discarded.
  if (cr) {
    const event = finishLine();
    if (event !== undefined) yield event;
  }
}
