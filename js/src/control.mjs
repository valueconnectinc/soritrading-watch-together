// Control-plane tracker — pure. Turns `control` states into a per-exchange level on the caller's monotonic clock.
// Spec: ../../docs/control-plane.md · vectors: ../../conformance/control/*.json

export const LEVELS = Object.freeze(['free', 'soft', 'hard']);
const RANK = Object.freeze({ free: 0, soft: 1, hard: 2 });
const ANY = '*';
const norm = (ex) => String(ex == null ? '' : ex).trim().toLowerCase() || ANY;

/**
 * @param {{ uses?: string[] }} [o]  exchanges this program uses — only these are acknowledged
 */
export function createControlTracker({ uses = [] } = {}) {
  let known = false, unsupported = false, boot = null, epoch = -1;
  let table = {}, frozen = {}, notice = null;
  const prev = new Map(uses.map((ex) => [norm(ex), 'free']));
  let acks = [];

  function level(ex, at) {
    if (!known) return 'unknown';
    let best = 'free';
    for (const k of [norm(ex), ANY]) {
      const e = table[k];
      if (e && at < e.until && RANK[e.level] > RANK[best]) best = e.level;
    }
    return best;
  }
  function advance(at) {
    if (!known) return;
    for (const [ex, p] of prev) {
      const l = level(ex, at);
      if (l !== p) { prev.set(ex, l); acks.push({ epoch, ex, level: l }); }
    }
  }
  /** Apply a full control state received at `at`. Returns false when ignored (older epoch in the same boot). */
  function apply(c, at) {
    if (!c || typeof c !== 'object') return false;
    const b = c.boot == null ? null : String(c.boot);
    const e = Number(c.epoch);
    if (!Number.isFinite(e)) return false;
    if (b === boot && known && e < epoch) return false;
    boot = b; epoch = e; known = true; unsupported = false;
    table = {};
    for (const [ex, y] of Object.entries(c.yield || {})) {
      if (!y || !(y.level in RANK) || y.level === 'free') continue;
      table[norm(ex)] = { level: y.level, until: at + Math.max(0, Number(y.ttlMs) || 0), why: Array.isArray(y.why) ? y.why.slice() : [] };
    }
    frozen = {};
    for (const [ex, f] of Object.entries(c.frozen || {})) {
      if (f && Number(f.untilMs) > 0) frozen[norm(ex)] = { until: at + Number(f.untilMs), why: f.why == null ? null : String(f.why) };
    }
    notice = c.notice && c.notice.kind ? { kind: String(c.notice.kind), inMs: Number(c.notice.inMs) || 0, why: c.notice.why == null ? null : String(c.notice.why) } : null;
    advance(at);
    return true;
  }
  return {
    apply,
    advance,
    level,
    /** The agent has no control plane (control.state answered 501/404). Levels stay `unknown` — never `free`. */
    setUnsupported() { unsupported = true; known = false; },
    isFrozen(ex, at) { const f = frozen[norm(ex)]; return !!(f && at < f.until); },
    why(ex, at) { const out = []; for (const k of [norm(ex), ANY]) { const e = table[k]; if (e && at < e.until) for (const w of e.why) if (!out.includes(w)) out.push(w); } return out; },
    /** Earliest time after `at` when some level or frozen flag can change without a new message (for a timer). */
    nextChangeAt(at) {
      let n = Infinity;
      for (const e of Object.values(table)) if (e.until > at && e.until < n) n = e.until;
      for (const f of Object.values(frozen)) if (f.until > at && f.until < n) n = f.until;
      return n === Infinity ? null : n;
    },
    drainAcks() { const a = acks; acks = []; return a; },
    get known() { return known; },
    get unsupported() { return unsupported; },
    get notice() { return notice; },
    get epoch() { return known ? epoch : null; },
    get boot() { return boot; },
  };
}
