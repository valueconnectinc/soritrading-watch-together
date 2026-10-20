// WATCH TOGETHER connector — JavaScript (ESM). Node.js 22+ (global WebSocket and fetch). No dependencies.
// One client, two transports: WebSocket (P2P daemon) and REST + Server-Sent Events (agent).
// Read-only: there is no order, key or balance operation in this API.

import { createControlTracker } from './control.mjs';
import { createSseParser } from './sse.mjs';

export const PROTOCOL_VERSION = 1;
export const OPS = Object.freeze(['status', 'conds.list', 'conds.add', 'conds.del', 'signals.since', 'peers', 'usage',
  'control.state', 'control.ack', 'client.register', 'usage.report']);

/** An error reply. `code` is machine-readable (`body.error`); read codes, not message text. */
export class ApiError extends Error {
  constructor(status, code, body) {
    super(code + (body && body.reason ? ' (' + body.reason + ')' : '') + ' [' + status + ']');
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.reason = (body && body.reason) || null;
    this.body = body || null;
  }
}

const RE_TOKEN = /^[0-9a-f]{64}$/;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const LEVEL_RANK = { free: 0, soft: 1, hard: 2 };

// ── REST mapping: op → method + path. The agent serves the same operations as the daemon's WebSocket. ──
function restRoute(op, args) {
  const a = args || {};
  switch (op) {
    case 'status': return ['GET', '/v1/status'];
    case 'conds.list': return ['GET', '/v1/conds'];
    case 'conds.add': return ['POST', '/v1/conds', { cond: a.cond, ex: a.ex }];
    case 'conds.del': return ['DELETE', '/v1/conds/' + encodeURIComponent(String(a.slot || ''))];
    case 'signals.since': return ['GET', '/v1/signals?since=' + encodeURIComponent(a.since ?? 0) + '&limit=' + encodeURIComponent(a.limit ?? 100)];
    case 'peers': return ['GET', '/v1/peers'];
    case 'usage': return ['GET', '/v1/usage'];
    case 'control.state': return ['GET', '/v1/control'];
    case 'control.ack': return ['POST', '/v1/control/ack', a];
    case 'client.register': return ['POST', '/v1/clients', a];
    case 'usage.report': return ['POST', '/v1/usage/report', a];
    default: return null;
  }
}

/** REST transport — request/response over HTTP, events over SSE (`/v1/events`). */
function restTransport({ url, token, fetchImpl, timeoutMs, onEvent, onClose }) {
  const base = String(url).replace(/\/+$/, '');
  const auth = { authorization: 'Bearer ' + token };
  let abort = null, closed = false;
  async function request(op, args) {
    const r = restRoute(op, args);
    if (!r) throw new ApiError(404, 'unknown-op', null);
    const [method, path, body] = r;
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), timeoutMs);
    let res;
    try {
      res = await fetchImpl(base + path, { method, signal: ctl.signal,
        headers: { ...auth, ...(body ? { 'content-type': 'application/json' } : {}) }, ...(body ? { body: JSON.stringify(body) } : {}) });
    } catch (e) { throw new ApiError(0, ctl.signal.aborted ? 'client-timeout' : 'unreachable', { detail: String(e && e.message) }); }
    finally { clearTimeout(timer); }
    let j = null; try { j = await res.json(); } catch { j = null; }
    if (!res.ok) throw new ApiError(res.status, (j && j.error) || 'http-' + res.status, j);
    return j;
  }
  async function stream(lastSeq) {
    abort = new AbortController();
    const res = await fetchImpl(base + '/v1/events', { signal: abort.signal, headers: { ...auth, accept: 'text/event-stream', ...(lastSeq ? { 'last-event-id': String(lastSeq) } : {}) } });
    if (!res.ok) { let j = null; try { j = await res.json(); } catch {} throw new ApiError(res.status, (j && j.error) || 'http-' + res.status, j); }
    const parser = createSseParser((e) => { let d = null; try { d = JSON.parse(e.data); } catch { return; } onEvent(e.event, d); });
    (async () => {
      const dec = new TextDecoder();
      const rd = res.body.getReader();
      try { for (;;) { const { value, done } = await rd.read(); if (done) break; parser.feed(dec.decode(value, { stream: true })); } }
      catch { /* aborted or dropped */ }
      if (!closed) onClose(1006);
    })();
  }
  return { kind: 'rest', request, stream, close() { closed = true; if (abort) abort.abort(); } };
}

/** WebSocket transport — the daemon's /v1 endpoint. First message authenticates; events arrive on the same socket. */
async function wsTransport({ url, token, WebSocketImpl, timeoutMs, onEvent, onClose }) {
  if (typeof WebSocketImpl !== 'function') throw new ApiError(0, 'no-websocket', { detail: 'use Node.js 22+ or pass WebSocketImpl' });
  const ws = new WebSocketImpl(url);
  const wait = new Map();
  let id = 0, closedCode = null;
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = () => reject(new ApiError(0, 'unreachable', { detail: 'cannot connect to ' + url }));
    ws.onclose = (e) => reject(new ApiError(0, 'closed', { code: e.code }));
  });
  ws.onmessage = (e) => {
    let m; try { m = JSON.parse(typeof e.data === 'string' ? e.data : String(e.data)); } catch { return; }
    if (m && m.ev) { onEvent(m.ev, m.ev === 'signal' || m.ev === 'control' ? m.item : m); return; }
    const w = wait.get(m && m.id);
    if (!w) return;
    wait.delete(m.id); clearTimeout(w.timer);
    if (m.ok) w.resolve(m.body); else w.reject(new ApiError(m.status || 0, (m.body && m.body.error) || m.error || 'error', m.body || null));
  };
  ws.onclose = (e) => {
    closedCode = e.code;
    for (const [, w] of wait) { clearTimeout(w.timer); w.reject(new ApiError(0, 'closed', { code: e.code })); }
    wait.clear();
    onClose(e.code);
  };
  ws.onerror = () => {};
  function send(o) {
    if (closedCode != null) return Promise.reject(new ApiError(0, 'closed', { code: closedCode }));
    const rid = ++id;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { wait.delete(rid); reject(new ApiError(0, 'client-timeout', null)); }, timeoutMs);
      wait.set(rid, { resolve, reject, timer });
      ws.send(JSON.stringify({ id: rid, ...o }));
    });
  }
  const hello = await send({ op: 'auth', token });
  return { kind: 'ws', hello, request: (op, args) => send({ op, args: args || {} }), stream: async () => {}, close() { try { ws.close(1000); } catch {} } };
}

/**
 * Connect.
 * @param {object} o
 * @param {'auto'|'ws'|'rest'} [o.transport='auto']  auto = WebSocket when `ws` is given and reachable, else REST
 * @param {{url:string, token:string}} [o.ws]         daemon WebSocket, e.g. ws://127.0.0.1:27720/v1 + ws-api.token
 * @param {{url:string, token:string}} [o.rest]       agent REST, e.g. http://127.0.0.1:27719 + local-api.token
 * @param {string} [o.name]                           register under this name (control plane) — omit to stay anonymous
 * @param {Array<{ex:string, sameKey?:boolean|null, sameHost?:boolean|null}>} [o.uses]  exchanges you call yourself
 * @param {boolean} [o.reconnect=true]
 */
export async function connect(o = {}) {
  const c = new Client(o);
  await c._open();
  return c;
}

class Client {
  constructor({ transport = 'auto', ws = null, rest = null, name = null, uses = [], reconnect = true, timeoutMs = 20_000,
    now = () => performance.now(), WebSocketImpl = globalThis.WebSocket, fetchImpl = globalThis.fetch } = {}) {
    for (const [k, v] of [['ws', ws], ['rest', rest]]) if (v && !RE_TOKEN.test(String(v.token || ''))) throw new Error(k + '.token must be 64 hex characters');
    if (transport === 'ws' && !ws) throw new Error('transport "ws" needs ws: {url, token}');
    if (transport === 'rest' && !rest) throw new Error('transport "rest" needs rest: {url, token}');
    if (transport === 'auto' && !ws && !rest) throw new Error('give ws: {url, token} and/or rest: {url, token}');
    this.o = { transport, ws, rest, name, uses: Array.isArray(uses) ? uses : [], reconnect, timeoutMs, now, WebSocketImpl, fetchImpl };
    this.tracker = createControlTracker({ uses: this.o.uses.map((u) => u.ex) });
    this.transport = null;   // 'ws' | 'rest' — what is actually in use (never guessed)
    this.clientId = null;
    this.gate = null;
    this.lastSeq = 0;
    this.boot = null;
    this._t = null; this._closed = false; this._ls = {}; this._timers = new Set(); this._leaseTimer = null; this._regTimer = null;
    this._lastLevels = new Map();
  }

  // ── events ──
  /** on('signal'|'control'|'gate'|'hello'|'level'|'notice'|'gap'|'transport'|'close'|'error', fn) → unsubscribe */
  on(ev, fn) { (this._ls[ev] = this._ls[ev] || []).push(fn); return () => { this._ls[ev] = this._ls[ev].filter((x) => x !== fn); }; }
  _emit(ev, v) { for (const f of this._ls[ev] || []) { try { f(v); } catch (err) { queueMicrotask(() => { throw err; }); } } }

  async _open() {
    const tryWs = this.o.transport === 'ws' || (this.o.transport === 'auto' && this.o.ws);
    const common = { timeoutMs: this.o.timeoutMs, onEvent: (ev, d) => this._onEvent(ev, d), onClose: (code) => this._onClose(code) };
    let t = null, wsErr = null;
    if (tryWs) {
      try { t = await wsTransport({ ...this.o.ws, WebSocketImpl: this.o.WebSocketImpl, ...common }); }
      catch (e) { wsErr = e; if (this.o.transport === 'ws' || !this.o.rest) throw e; }
    }
    if (!t) t = restTransport({ ...this.o.rest, fetchImpl: this.o.fetchImpl, ...common });
    this._t = t;
    if (this.transport !== t.kind) { this.transport = t.kind; this._emit('transport', { kind: t.kind, wsError: wsErr ? wsErr.code : null }); }
    if (t.hello) this.gate = t.hello.gate || null;
    await this._sync();
    await t.stream(this.lastSeq);
  }

  /** (Re)connect order: register → control state → missed signals. */
  async _sync() {
    if (this.o.name) await this._register().catch((e) => this._emit('error', e));
    await this.refreshControl().catch((e) => this._emit('error', e));
    //  WebSocket has no Last-Event-ID — fetch what was missed while disconnected (REST resumes through the SSE header).
    if (this._t.kind === 'ws' && this.lastSeq > 0) {
      try {
        const r = await this.request('signals.since', { since: this.lastSeq, limit: 200 });
        if (r.boot && this.boot && r.boot !== this.boot) this.lastSeq = 0;
        if (r.boot) this.boot = r.boot;
        if (r.gap) this._emit('gap', { since: this.lastSeq });
        for (const it of r.items || []) this._onEvent('signal', it);
      } catch (e) { this._emit('error', e); }
    }
  }
  async _register() {
    const r = await this.request('client.register', { name: this.o.name, uses: this.o.uses, ...(this.clientId ? { clientId: this.clientId } : {}) });
    this.clientId = r.clientId;
    clearTimeout(this._regTimer);
    this._regTimer = setTimeout(() => { this._register().catch((e) => this._emit('error', e)); }, Math.max(5000, Math.floor((r.ttlMs || 120000) / 2)));
    if (this._regTimer.unref) this._regTimer.unref();
    return r;
  }
  /** Pull the control state (done on every connect). 501/404 = this agent has no control plane → levels stay `unknown`. */
  async refreshControl() {
    try { const b = await this.request('control.state', {}); this._applyControl(b.control); }
    catch (e) { if (e.status === 501 || e.status === 404) { this.tracker.setUnsupported(); this._levelsChanged(); } else throw e; }
  }

  _onEvent(ev, d) {
    if (ev === 'signal') { if (d && d.seq > this.lastSeq) this.lastSeq = d.seq; this._emit('signal', d); }
    else if (ev === 'control') this._applyControl(d);
    else if (ev === 'gate') { this.gate = { ok: !!d.ok, reason: d.reason || null }; this._emit('gate', this.gate); }
    else if (ev === 'hello') {
      if (d && d.gate) { this.gate = d.gate; this._emit('gate', this.gate); }
      if (d && d.boot && this.boot && d.boot !== this.boot) this.lastSeq = 0;   // agent restarted — seq starts again
      if (d && d.boot) this.boot = d.boot;
      this._emit('hello', d);
    }
  }
  _applyControl(c) {
    if (!c) return;
    const prevNotice = this.tracker.notice;
    if (!this.tracker.apply(c, this.o.now())) return;
    this._emit('control', c);
    if (this.tracker.notice && JSON.stringify(this.tracker.notice) !== JSON.stringify(prevNotice)) this._emit('notice', this.tracker.notice);
    this._levelsChanged();
  }
  /** After any change: send acks, notify guard listeners, arm the lease timer. */
  _levelsChanged() {
    const now = this.o.now();
    this.tracker.advance(now);
    for (const a of this.tracker.drainAcks()) {
      if (this.clientId) this.request('control.ack', { clientId: this.clientId, epoch: a.epoch, ex: a.ex, paused: a.level !== 'free' }).catch(() => {});
    }
    const seen = new Set([...this._lastLevels.keys(), ...this.o.uses.map((u) => String(u.ex).toLowerCase())]);
    for (const ex of seen) {
      const l = this.tracker.level(ex, now);
      if (this._lastLevels.get(ex) !== l) { this._lastLevels.set(ex, l); this._emit('level', { ex, level: l, why: this.tracker.why(ex, now), frozen: this.tracker.isFrozen(ex, now) }); }
    }
    clearTimeout(this._leaseTimer);
    const next = this.tracker.nextChangeAt(now);
    if (next != null) { this._leaseTimer = setTimeout(() => this._levelsChanged(), Math.max(1, Math.ceil(next - now))); if (this._leaseTimer.unref) this._leaseTimer.unref(); }
  }

  async _onClose(code) {
    this._emit('close', code);
    if (this._closed || !this.o.reconnect) return;
    //  A planned restart says when; otherwise back off 1 s → 30 s.
    const n = this.tracker.notice;
    let wait = n ? Math.max(1000, n.inMs) : 1000;
    while (!this._closed) {
      await sleep(wait);
      if (this._closed) return;
      try { await this._open(); return; } catch (e) { this._emit('error', e); wait = Math.min(30_000, wait * 2); }
    }
  }

  /** Low-level request on the current transport. Resolves to the reply body; rejects with ApiError. */
  request(op, args) {
    if (!this._t) return Promise.reject(new ApiError(0, 'not-connected', null));
    if (!OPS.includes(op)) return Promise.reject(new ApiError(404, 'unknown-op', null));
    return this._t.request(op, args || {});
  }

  status() { return this.request('status', {}); }
  condsList() { return this.request('conds.list', {}).then((b) => b.items); }
  condsAdd(cond, ex) { return this.request('conds.add', { cond, ex }); }
  condsDel(slot) { return this.request('conds.del', { slot }); }
  signalsSince(since = 0, limit = 100) { return this.request('signals.since', { since, limit }); }
  peers() { return this.request('peers', {}); }
  usage() { return this.request('usage', {}); }
  /** Report the exchange weight you spent since your last report (shown to the agent as `external`). Needs `name`. */
  reportUsage(ex, w, bucket = 'rest') {
    if (!this.clientId) return Promise.reject(new ApiError(0, 'not-registered', { detail: 'connect with a name to report usage' }));
    return this.request('usage.report', { clientId: this.clientId, ex, bucket, w });
  }

  // ── guard: wrap your own background exchange calls ──
  get guard() {
    const self = this;
    return {
      /** 'free' | 'soft' | 'hard' | 'unknown' */
      level(ex) { return self.tracker.level(ex, self.o.now()); },
      frozen(ex) { return self.tracker.isFrozen(ex, self.o.now()); },
      why(ex) { return self.tracker.why(ex, self.o.now()); },
      /** Resolves when `ex` is below `below` (default 'hard') or after `timeoutMs`. `unknown` never blocks. */
      clear(ex, { below = 'hard', timeoutMs = 60_000 } = {}) {
        const ok = () => { const l = self.tracker.level(ex, self.o.now()); return l === 'unknown' || LEVEL_RANK[l] < LEVEL_RANK[below]; };
        if (ok()) return Promise.resolve(true);
        return new Promise((resolve) => {
          const off = self.on('level', (e) => { if (e.ex === String(ex).toLowerCase() && ok()) { off(); clearTimeout(t); resolve(true); } });
          const t = setTimeout(() => { off(); resolve(false); }, timeoutMs);
        });
      },
      /** fn({ex, level, why, frozen}) on every change for `ex` — returns unsubscribe */
      on(ex, fn) { const k = String(ex).toLowerCase(); return self.on('level', (e) => { if (e.ex === k) fn(e); }); },
    };
  }

  close() {
    this._closed = true;
    clearTimeout(this._leaseTimer); clearTimeout(this._regTimer);
    if (this._t) this._t.close();
  }
}

export { createControlTracker } from './control.mjs';
export { createSseParser } from './sse.mjs';
