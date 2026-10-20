"""WATCH TOGETHER connector - Python (asyncio, standard library only).

One client, two transports: WebSocket (P2P daemon) and REST + Server-Sent Events (agent).
Read-only: there is no order, key or balance operation in this API.
"""

import asyncio
import base64
import hashlib
import json
import os
import re
import ssl as _ssl
import struct
import time
from urllib.parse import quote, urlsplit

from .control import ControlTracker
from .sse import SseParser

PROTOCOL_VERSION = 1
OPS = ("status", "conds.list", "conds.add", "conds.del", "signals.since", "peers", "usage",
       "control.state", "control.ack", "client.register", "usage.report")

_RE_TOKEN = re.compile(r"[0-9a-f]{64}")
_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_MAX_MSG = 1 << 20
_MAX_HEAD = 64 * 1024
_RANK = {"free": 0, "soft": 1, "hard": 2}


class ApiError(Exception):
    """An error reply. `code` is machine-readable (`body.error`); read codes, not message text."""

    def __init__(self, status, code, body=None):
        self.status = status
        self.code = code
        self.body = body if isinstance(body, dict) else None
        self.reason = self.body.get("reason") if self.body else None
        super().__init__("%s%s [%s]" % (code, " (%s)" % self.reason if self.reason else "", status))


def _monotonic_ms():
    return time.monotonic() * 1000.0


# -- REST mapping: op -> method + path. The agent serves the same operations as the daemon's WebSocket. --
def _rest_route(op, a):
    a = a or {}
    if op == "status":
        return "GET", "/v1/status", None
    if op == "conds.list":
        return "GET", "/v1/conds", None
    if op == "conds.add":
        return "POST", "/v1/conds", {"cond": a.get("cond"), "ex": a.get("ex")}
    if op == "conds.del":
        return "DELETE", "/v1/conds/" + quote(str(a.get("slot") or ""), safe=""), None
    if op == "signals.since":
        return "GET", "/v1/signals?since=%s&limit=%s" % (quote(str(a.get("since", 0))), quote(str(a.get("limit", 100)))), None
    if op == "peers":
        return "GET", "/v1/peers", None
    if op == "usage":
        return "GET", "/v1/usage", None
    if op == "control.state":
        return "GET", "/v1/control", None
    if op == "control.ack":
        return "POST", "/v1/control/ack", a
    if op == "client.register":
        return "POST", "/v1/clients", a
    if op == "usage.report":
        return "POST", "/v1/usage/report", a
    return None


# -- minimal HTTP/1.1 over asyncio streams (no third-party packages) --
async def _open_http(url):
    u = urlsplit(url)
    if u.scheme not in ("http", "https"):
        raise ValueError("rest url must start with http:// or https://")
    port = u.port or (443 if u.scheme == "https" else 80)
    ctx = _ssl.create_default_context() if u.scheme == "https" else None
    r, w = await asyncio.open_connection(u.hostname, port, ssl=ctx)
    host = u.hostname if ":" not in u.hostname else "[%s]" % u.hostname
    if u.port is not None:
        host += ":%d" % u.port
    return r, w, host, (u.path or "").rstrip("/")


async def _read_head(r):
    head = await r.readuntil(b"\r\n\r\n")
    if len(head) > _MAX_HEAD:
        raise ConnectionError("response head too big")
    lines = head.decode("latin-1").split("\r\n")
    parts = lines[0].split(" ", 2)
    status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    hdr = {}
    for ln in lines[1:]:
        k, sep, v = ln.partition(":")
        if sep:
            hdr[k.strip().lower()] = v.strip()
    return status, hdr


async def _body_chunks(r, hdr):
    """Yield the response body as it arrives (chunked, sized or until EOF)."""
    if "chunked" in hdr.get("transfer-encoding", "").lower():
        while True:
            line = await r.readuntil(b"\r\n")
            n = int(line.split(b";")[0].strip() or b"0", 16)
            if n == 0:
                return
            if n > _MAX_MSG:
                raise ConnectionError("chunk too big")
            data = await r.readexactly(n)
            await r.readexactly(2)
            yield data
    elif "content-length" in hdr:
        n = int(hdr["content-length"])
        if n > _MAX_MSG:
            raise ConnectionError("body too big")
        if n:
            yield await r.readexactly(n)
    else:
        while True:
            data = await r.read(65536)
            if not data:
                return
            yield data


def _req_bytes(method, path, host, headers, body):
    h = ["%s %s HTTP/1.1" % (method, path), "Host: " + host]
    h += ["%s: %s" % kv for kv in headers.items()]
    if body is not None:
        h += ["Content-Type: application/json", "Content-Length: %d" % len(body)]
    return ("\r\n".join(h) + "\r\n\r\n").encode("latin-1") + (body or b"")


class _RestTransport:
    kind = "rest"
    hello = None

    def __init__(self, url, token, timeout, on_event, on_close):
        self._url = str(url).rstrip("/")
        self._auth = "Bearer " + token
        self._timeout = timeout
        self._on_event = on_event
        self._on_close = on_close
        self._stream_task = None
        self._stream_w = None
        self._closed = False

    async def _call(self, method, path, body):
        r, w, host, prefix = await _open_http(self._url)
        try:
            # No Origin header: the agent refuses requests that carry one.
            w.write(_req_bytes(method, prefix + path, host, {"Authorization": self._auth, "Connection": "close"}, body))
            await w.drain()
            status, hdr = await _read_head(r)
            data = b"".join([c async for c in _body_chunks(r, hdr)])
        finally:
            w.close()
        try:
            j = json.loads(data.decode("utf-8")) if data else None
        except ValueError:
            j = None
        if not 200 <= status < 300:
            raise ApiError(status, (j or {}).get("error") if isinstance(j, dict) and j.get("error") else "http-%d" % status, j)
        return j

    async def request(self, op, args):
        route = _rest_route(op, args)
        if not route:
            raise ApiError(404, "unknown-op")
        method, path, body = route
        raw = None if body is None else json.dumps(body).encode("utf-8")
        try:
            return await asyncio.wait_for(self._call(method, path, raw), self._timeout)
        except asyncio.TimeoutError:
            raise ApiError(0, "client-timeout")
        except (OSError, ConnectionError, asyncio.IncompleteReadError) as e:
            raise ApiError(0, "unreachable", {"detail": str(e)})

    async def stream(self, last_seq):
        r, w, host, prefix = await asyncio.wait_for(_open_http(self._url), self._timeout)
        hd = {"Authorization": self._auth, "Accept": "text/event-stream"}
        if last_seq:
            hd["Last-Event-ID"] = str(last_seq)
        w.write(_req_bytes("GET", prefix + "/v1/events", host, hd, None))
        await w.drain()
        status, hdr = await asyncio.wait_for(_read_head(r), self._timeout)
        if status != 200:
            data = b"".join([c async for c in _body_chunks(r, hdr)])
            w.close()
            try:
                j = json.loads(data.decode("utf-8"))
            except ValueError:
                j = None
            raise ApiError(status, (j or {}).get("error") if isinstance(j, dict) and j.get("error") else "http-%d" % status, j)
        self._stream_w = w

        def on_sse(e):
            try:
                d = json.loads(e["data"])
            except ValueError:
                return
            self._on_event(e["event"], d)

        parser = SseParser(on_sse)

        async def pump():
            try:
                async for chunk in _body_chunks(r, hdr):
                    parser.feed(chunk.decode("utf-8", "replace"))
            except (OSError, ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError):
                pass
            finally:
                w.close()
            if not self._closed:
                self._on_close(1006)

        self._stream_task = asyncio.ensure_future(pump())

    def close(self):
        self._closed = True
        if self._stream_task:
            self._stream_task.cancel()
        if self._stream_w:
            self._stream_w.close()


def _frame(opcode, payload):
    """Client -> server frame (always masked, RFC 6455 section 5.3)."""
    mask = os.urandom(4)
    n = len(payload)
    if n < 126:
        head = struct.pack("!BB", 0x80 | opcode, 0x80 | n)
    elif n < 65536:
        head = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, n)
    else:
        head = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, n)
    return head + mask + bytes(b ^ mask[i & 3] for i, b in enumerate(payload))


class _WsTransport:
    kind = "ws"

    def __init__(self, timeout, on_event, on_close):
        self._timeout = timeout
        self._on_event = on_event
        self._on_close = on_close
        self._r = self._w = None
        self._wait = {}
        self._id = 0
        self._closed_code = None
        self._task = None
        self.hello = None

    async def open(self, url, token):
        u = urlsplit(url)
        if u.scheme not in ("ws", "wss"):
            raise ValueError("ws url must start with ws:// or wss://")
        port = u.port or (443 if u.scheme == "wss" else 80)
        ctx = _ssl.create_default_context() if u.scheme == "wss" else None
        try:
            self._r, self._w = await asyncio.wait_for(asyncio.open_connection(u.hostname, port, ssl=ctx), self._timeout)
        except (OSError, asyncio.TimeoutError) as e:
            raise ApiError(0, "unreachable", {"detail": "cannot connect to %s (%s)" % (url, e)})
        key = base64.b64encode(os.urandom(16)).decode()
        host = u.hostname if ":" not in u.hostname else "[%s]" % u.hostname
        if u.port is not None:
            host += ":%d" % u.port
        # No Origin header: the daemon refuses requests that carry one.
        self._w.write(("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                       "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n" % (u.path or "/v1", host, key)).encode("ascii"))
        await self._w.drain()
        status, hdr = await asyncio.wait_for(_read_head(self._r), self._timeout)
        want = base64.b64encode(hashlib.sha1((key + _GUID).encode()).digest()).decode()
        if status != 101 or hdr.get("sec-websocket-accept") != want:
            self._w.close()
            raise ApiError(0, "unreachable", {"detail": "upgrade refused (%d)" % status})
        self._task = asyncio.ensure_future(self._read_loop())
        self.hello = await self._send({"op": "auth", "token": token})
        return self

    async def _read_frame(self):
        b0, b1 = await self._r.readexactly(2)
        if b1 & 0x80:
            raise ConnectionError("server frame is masked")
        n = b1 & 0x7F
        if n == 126:
            (n,) = struct.unpack("!H", await self._r.readexactly(2))
        elif n == 127:
            (n,) = struct.unpack("!Q", await self._r.readexactly(8))
        if n > _MAX_MSG:
            raise ConnectionError("frame too big")
        return b0 & 0x80, b0 & 0x0F, await self._r.readexactly(n)

    async def _read_loop(self):
        code, parts = 1006, []
        try:
            while True:
                fin, op, data = await self._read_frame()
                if op == 8:
                    code = struct.unpack("!H", data[:2])[0] if len(data) >= 2 else 1005
                    break
                if op == 9:
                    self._w.write(_frame(10, data))
                    continue
                if op in (0, 1):
                    parts.append(data)
                    if fin:
                        self._on_text(b"".join(parts).decode("utf-8"))
                        parts = []
        except (asyncio.IncompleteReadError, ConnectionError, OSError, asyncio.CancelledError):
            pass
        finally:
            self._finish(code)

    def _on_text(self, text):
        try:
            m = json.loads(text)
        except ValueError:
            return
        if not isinstance(m, dict):
            return
        if m.get("ev"):
            self._on_event(m["ev"], m.get("item") if m["ev"] in ("signal", "control") else m)
            return
        fut = self._wait.pop(m.get("id"), None)
        if fut is None or fut.done():
            return
        if m.get("ok"):
            fut.set_result(m.get("body"))
        else:
            body = m.get("body") if isinstance(m.get("body"), dict) else None
            fut.set_exception(ApiError(m.get("status") or 0, (body or {}).get("error") or m.get("error") or "error", body))

    def _finish(self, code):
        if self._closed_code is not None:
            return
        self._closed_code = code
        for fut in self._wait.values():
            if not fut.done():
                fut.set_exception(ApiError(0, "closed", {"code": code}))
        self._wait.clear()
        try:
            self._w.close()
        except Exception:
            pass
        self._on_close(code)

    async def _send(self, msg):
        if self._closed_code is not None:
            raise ApiError(0, "closed", {"code": self._closed_code})
        self._id += 1
        rid = self._id
        fut = asyncio.get_running_loop().create_future()
        self._wait[rid] = fut
        self._w.write(_frame(1, json.dumps(dict(msg, id=rid)).encode("utf-8")))
        await self._w.drain()
        try:
            return await asyncio.wait_for(fut, self._timeout)
        except asyncio.TimeoutError:
            self._wait.pop(rid, None)
            raise ApiError(0, "client-timeout")

    async def request(self, op, args):
        return await self._send({"op": op, "args": args or {}})

    async def stream(self, last_seq):
        return None

    def close(self):
        if self._closed_code is None and self._w is not None:
            try:
                self._w.write(_frame(8, struct.pack("!H", 1000)))
            except Exception:
                pass
        if self._task:
            self._task.cancel()


class Guard:
    """Wrap your own background exchange calls with this."""

    def __init__(self, client):
        self._c = client

    def level(self, ex):
        """'free' | 'soft' | 'hard' | 'unknown'"""
        return self._c.tracker.level(ex, self._c._now())

    def frozen(self, ex):
        return self._c.tracker.is_frozen(ex, self._c._now())

    def why(self, ex):
        return self._c.tracker.why(ex, self._c._now())

    async def clear(self, ex, below="hard", timeout=60.0):
        """True when `ex` is below `below` (default 'hard'), False after `timeout` seconds. 'unknown' never blocks."""
        def ok():
            lv = self.level(ex)
            return lv == "unknown" or _RANK[lv] < _RANK[below]

        if ok():
            return True
        fut = asyncio.get_running_loop().create_future()
        k = str(ex).lower()

        def on_level(e):
            if e["ex"] == k and ok() and not fut.done():
                fut.set_result(True)

        off = self._c.on("level", on_level)
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return False
        finally:
            off()

    def on(self, ex, fn):
        """fn({ex, level, why, frozen}) on every change for `ex` - returns unsubscribe"""
        k = str(ex).lower()
        return self._c.on("level", lambda e: fn(e) if e["ex"] == k else None)


_EVENTS = ("signal", "control", "gate", "hello", "level", "notice", "gap", "transport", "close", "error")


class Client:
    def __init__(self, transport="auto", ws=None, rest=None, name=None, uses=None, reconnect=True, timeout=20.0, now=None):
        for k, v in (("ws", ws), ("rest", rest)):
            if v is not None and not _RE_TOKEN.fullmatch(str(v.get("token") or "")):
                raise ValueError(k + ".token must be 64 hex characters")
        if transport == "ws" and not ws:
            raise ValueError('transport "ws" needs ws={"url", "token"}')
        if transport == "rest" and not rest:
            raise ValueError('transport "rest" needs rest={"url", "token"}')
        if transport == "auto" and not ws and not rest:
            raise ValueError('give ws={"url", "token"} and/or rest={"url", "token"}')
        if transport not in ("auto", "ws", "rest"):
            raise ValueError("transport must be 'auto', 'ws' or 'rest'")
        self._o = {"transport": transport, "ws": ws, "rest": rest, "name": name, "uses": list(uses or []),
                   "reconnect": reconnect, "timeout": timeout}
        self._now = now or _monotonic_ms
        self.tracker = ControlTracker([u["ex"] for u in self._o["uses"]])
        self.transport = None   # 'ws' | 'rest' - what is actually in use (never guessed)
        self.client_id = None
        self.gate = None
        self.last_seq = 0
        self.boot = None
        self._t = None
        self._closed = False
        self._ls = {ev: [] for ev in _EVENTS}
        self._lease = None
        self._reg = None
        self._last_levels = {}
        self._tasks = set()
        self.guard = Guard(self)

    # -- events ---------------------------------------------------------
    def on(self, ev, fn):
        """on('signal'|'control'|'gate'|'hello'|'level'|'notice'|'gap'|'transport'|'close'|'error', fn) -> unsubscribe"""
        if ev not in self._ls:
            raise ValueError("unknown event " + ev)
        self._ls[ev].append(fn)
        return lambda: fn in self._ls[ev] and self._ls[ev].remove(fn)

    def _emit(self, ev, v):
        for f in list(self._ls[ev]):
            try:
                r = f(v)
                if asyncio.iscoroutine(r):
                    self._spawn(r)
            except Exception:  # a listener error must not break the client
                pass

    def _spawn(self, coro):
        t = asyncio.ensure_future(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)
        return t

    # -- connection -----------------------------------------------------
    async def _open(self):
        o = self._o
        try_ws = o["transport"] == "ws" or (o["transport"] == "auto" and o["ws"])
        t, ws_err = None, None
        if try_ws:
            try:
                t = await _WsTransport(o["timeout"], self._on_event, self._on_close).open(o["ws"]["url"], o["ws"]["token"])
            except ApiError as e:
                ws_err = e
                if o["transport"] == "ws" or not o["rest"]:
                    raise
        if t is None:
            t = _RestTransport(o["rest"]["url"], o["rest"]["token"], o["timeout"], self._on_event, self._on_close)
        self._t = t
        if self.transport != t.kind:
            self.transport = t.kind
            self._emit("transport", {"kind": t.kind, "wsError": ws_err.code if ws_err else None})
        if t.hello:
            self.gate = t.hello.get("gate")
        await self._sync()
        await t.stream(self.last_seq)

    async def _sync(self):
        """(Re)connect order: register -> control state -> missed signals."""
        if self._o["name"]:
            try:
                await self._register()
            except ApiError as e:
                self._emit("error", e)
        try:
            await self.refresh_control()
        except ApiError as e:
            self._emit("error", e)
        # WebSocket has no Last-Event-ID - fetch what was missed while disconnected (REST resumes through the SSE header).
        if self._t.kind == "ws" and self.last_seq > 0:
            try:
                r = await self.request("signals.since", {"since": self.last_seq, "limit": 200})
                if r.get("boot") and self.boot and r["boot"] != self.boot:
                    self.last_seq = 0
                if r.get("boot"):
                    self.boot = r["boot"]
                if r.get("gap"):
                    self._emit("gap", {"since": self.last_seq})
                for it in r.get("items") or []:
                    self._on_event("signal", it)
            except ApiError as e:
                self._emit("error", e)

    async def _register(self):
        args = {"name": self._o["name"], "uses": self._o["uses"]}
        if self.client_id:
            args["clientId"] = self.client_id
        r = await self.request("client.register", args)
        self.client_id = r["clientId"]
        if self._reg:
            self._reg.cancel()
        delay = max(5.0, (r.get("ttlMs") or 120000) / 2000.0)
        self._reg = asyncio.get_running_loop().call_later(delay, lambda: self._spawn(self._renew()))
        return r

    async def _renew(self):
        try:
            await self._register()
        except ApiError as e:
            self._emit("error", e)

    async def refresh_control(self):
        """Pull the control state (done on every connect). 501/404 = this agent has no control plane -> levels stay 'unknown'."""
        try:
            b = await self.request("control.state", {})
            self._apply_control(b.get("control"))
        except ApiError as e:
            if e.status in (501, 404):
                self.tracker.set_unsupported()
                self._levels_changed()
            else:
                raise

    def _on_event(self, ev, d):
        if not isinstance(d, dict):
            return
        if ev == "signal":
            seq = d.get("seq")
            if isinstance(seq, int) and seq > self.last_seq:
                self.last_seq = seq
            self._emit("signal", d)
        elif ev == "control":
            self._apply_control(d)
        elif ev == "gate":
            self.gate = {"ok": bool(d.get("ok")), "reason": d.get("reason")}
            self._emit("gate", self.gate)
        elif ev == "hello":
            if d.get("gate"):
                self.gate = d["gate"]
                self._emit("gate", self.gate)
            if d.get("boot") and self.boot and d["boot"] != self.boot:
                self.last_seq = 0   # agent restarted - seq starts again
            if d.get("boot"):
                self.boot = d["boot"]
            self._emit("hello", d)

    def _apply_control(self, c):
        if not c:
            return
        prev = self.tracker.notice
        if not self.tracker.apply(c, self._now()):
            return
        self._emit("control", c)
        if self.tracker.notice and self.tracker.notice != prev:
            self._emit("notice", self.tracker.notice)
        self._levels_changed()

    def _levels_changed(self):
        """After any change: send acks, notify guard listeners, arm the lease timer."""
        now = self._now()
        self.tracker.advance(now)
        for a in self.tracker.drain_acks():
            if self.client_id and self._t:
                args = {"clientId": self.client_id, "epoch": a["epoch"], "ex": a["ex"], "paused": a["level"] != "free"}
                self._spawn(self._quiet(self.request("control.ack", args)))
        seen = list(self._last_levels) + [str(u["ex"]).lower() for u in self._o["uses"]]
        for ex in dict.fromkeys(seen):
            lv = self.tracker.level(ex, now)
            if self._last_levels.get(ex) != lv:
                self._last_levels[ex] = lv
                self._emit("level", {"ex": ex, "level": lv, "why": self.tracker.why(ex, now), "frozen": self.tracker.is_frozen(ex, now)})
        if self._lease:
            self._lease.cancel()
            self._lease = None
        nxt = self.tracker.next_change_at(now)
        if nxt is not None and not self._closed:
            self._lease = asyncio.get_running_loop().call_later(max(0.001, (nxt - now) / 1000.0), self._levels_changed)

    @staticmethod
    async def _quiet(coro):
        try:
            await coro
        except ApiError:
            pass

    def _on_close(self, code):
        self._emit("close", code)
        if self._closed or not self._o["reconnect"]:
            return
        self._spawn(self._reconnect())

    async def _reconnect(self):
        # A planned restart says when; otherwise back off 1 s -> 30 s.
        n = self.tracker.notice
        wait = max(1.0, n["inMs"] / 1000.0) if n else 1.0
        while not self._closed:
            await asyncio.sleep(wait)
            if self._closed:
                return
            try:
                await self._open()
                return
            except (ApiError, OSError, ValueError) as e:
                self._emit("error", e)
                wait = min(30.0, wait * 2)

    # -- requests -------------------------------------------------------
    async def request(self, op, args=None):
        """Low-level request on the current transport. Returns the reply body; raises ApiError."""
        if not self._t:
            raise ApiError(0, "not-connected")
        if op not in OPS:
            raise ApiError(404, "unknown-op")
        return await self._t.request(op, args or {})

    async def status(self):
        return await self.request("status")

    async def conds_list(self):
        return (await self.request("conds.list"))["items"]

    async def conds_add(self, cond, ex):
        return await self.request("conds.add", {"cond": cond, "ex": ex})

    async def conds_del(self, slot):
        return await self.request("conds.del", {"slot": slot})

    async def signals_since(self, since=0, limit=100):
        return await self.request("signals.since", {"since": since, "limit": limit})

    async def peers(self):
        return await self.request("peers")

    async def usage(self):
        return await self.request("usage")

    async def report_usage(self, ex, w, bucket="rest"):
        """Report the exchange weight you spent since your last report (shown to the agent as `external`). Needs `name`."""
        if not self.client_id:
            raise ApiError(0, "not-registered", {"detail": "connect with a name to report usage"})
        return await self.request("usage.report", {"clientId": self.client_id, "ex": ex, "bucket": bucket, "w": w})

    def close(self):
        self._closed = True
        for h in (self._lease, self._reg):
            if h:
                h.cancel()
        if self._t:
            self._t.close()
        for t in list(self._tasks):
            t.cancel()


async def connect(transport="auto", ws=None, rest=None, name=None, uses=None, reconnect=True, timeout=20.0, now=None):
    """Connect.

    transport: 'auto' (WebSocket when `ws` is given and reachable, else REST) | 'ws' | 'rest'
    ws:   {"url": "ws://127.0.0.1:27720/v1", "token": <ws-api.token>}       (P2P daemon)
    rest: {"url": "http://127.0.0.1:27719", "token": <local-api.token>}     (agent)
    name: register under this name (control plane) - omit to stay anonymous
    uses: [{"ex": "binance", "sameKey": True, "sameHost": None}, ...] exchanges you call yourself
    """
    c = Client(transport, ws, rest, name, uses, reconnect, timeout, now)
    await c._open()
    return c
