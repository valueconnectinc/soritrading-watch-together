// Server-Sent Events parser — pure, incremental. Vectors: ../../conformance/sse/*.json
// `id` on an emitted event is the id field of that block (null when absent); `lastEventId` keeps the last one seen.

export function createSseParser(onEvent) {
  let buf = '', data = [], event = null, id = null, hasData = false;
  const st = { lastEventId: null };
  function line(l) {
    if (l === '') {
      if (hasData) onEvent({ event: event || 'message', id, data: data.join('\n') });
      data = []; event = null; id = null; hasData = false;
      return;
    }
    if (l[0] === ':') return;
    const i = l.indexOf(':');
    const field = i < 0 ? l : l.slice(0, i);
    let value = i < 0 ? '' : l.slice(i + 1);
    if (value[0] === ' ') value = value.slice(1);
    if (field === 'data') { data.push(value); hasData = true; }
    else if (field === 'event') event = value;
    else if (field === 'id') { id = value; st.lastEventId = value; }
  }
  return {
    feed(chunk) {
      buf += chunk;
      for (;;) {
        const m = /\r\n|\r|\n/.exec(buf);
        if (!m) break;
        // a lone \r at the very end may be the first half of \r\n — wait for more
        if (m[0] === '\r' && m.index === buf.length - 1) break;
        line(buf.slice(0, m.index));
        buf = buf.slice(m.index + m[0].length);
      }
    },
    get lastEventId() { return st.lastEventId; },
  };
}
