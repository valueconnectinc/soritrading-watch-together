//! Blocking client — two transports, one client:
//!  · REST + Server-Sent Events (the agent, `http://127.0.0.1:27719`) over plain HTTP/1.1
//!  · WebSocket (the P2P daemon, `ws://127.0.0.1:27720/v1`) via `tungstenite`
//! Both listeners are local, so this crate speaks `http://` and `ws://` only; reach a remote machine through an SSH tunnel.

use crate::control::{norm, ControlTracker};
use crate::sse::SseParser;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use tungstenite::{Message, WebSocket};
use std::thread;
use std::time::{Duration, Instant};

pub const PROTOCOL_VERSION: u32 = 1;
pub const OPS: [&str; 11] = [
    "status", "conds.list", "conds.add", "conds.del", "signals.since", "peers", "usage",
    "control.state", "control.ack", "client.register", "usage.report",
];
const MAX_BODY: usize = 1 << 20;

/// An error reply. `code` is machine-readable (body.error); read codes, not message text.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub reason: Option<String>,
    pub body: Option<Value>,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.reason {
            Some(r) => write!(f, "{} ({}) [{}]", self.code, r, self.status),
            None => write!(f, "{} [{}]", self.code, self.status),
        }
    }
}

impl std::error::Error for ApiError {}

fn err(status: u16, code: &str, body: Option<Value>) -> ApiError {
    let reason = body.as_ref().and_then(|b| b.get("reason")).and_then(|r| r.as_str()).map(String::from);
    ApiError { status, code: code.to_string(), reason, body }
}

/// An exchange this program calls itself. `None` = unknown.
#[derive(Debug, Clone)]
pub struct Uses {
    pub ex: String,
    pub same_key: Option<bool>,
    pub same_host: Option<bool>,
}

/// A listener address and the contents of its token file (64 hex characters).
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub url: String,
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct Options {
    /// "auto" (WebSocket when `ws` is set and reachable, else REST) | "ws" | "rest"
    pub transport: String,
    /// agent REST, e.g. http://127.0.0.1:27719 + local-api.token
    pub rest: Option<Endpoint>,
    /// daemon WebSocket, e.g. ws://127.0.0.1:27720/v1 + ws-api.token
    pub ws: Option<Endpoint>,
    /// register under this name (control plane); None = anonymous
    pub name: Option<String>,
    pub uses: Vec<Uses>,
    pub reconnect: bool,
    pub timeout: Duration,
}

impl Options {
    /// REST only (the agent). Add `.with_ws(...)` for the daemon WebSocket.
    pub fn new(rest_url: &str, rest_token: &str) -> Self {
        Options {
            transport: "auto".to_string(),
            rest: Some(Endpoint { url: rest_url.to_string(), token: rest_token.to_string() }),
            ws: None,
            name: None,
            uses: Vec::new(),
            reconnect: true,
            timeout: Duration::from_secs(20),
        }
    }
    /// WebSocket only (the daemon).
    pub fn ws_only(ws_url: &str, ws_token: &str) -> Self {
        let mut o = Options::new("", "");
        o.rest = None;
        o.ws = Some(Endpoint { url: ws_url.to_string(), token: ws_token.to_string() });
        o
    }
    pub fn with_ws(mut self, ws_url: &str, ws_token: &str) -> Self {
        self.ws = Some(Endpoint { url: ws_url.to_string(), token: ws_token.to_string() });
        self
    }
}

#[derive(Debug, Clone)]
pub struct LevelEvent {
    pub ex: String,
    pub level: &'static str,
    pub why: Vec<String>,
    pub frozen: bool,
}

type LevelFn = Box<dyn Fn(&LevelEvent) + Send + 'static>;
type SignalFn = Box<dyn Fn(&Value) + Send + 'static>;

struct State {
    tracker: ControlTracker,
    client_id: Option<String>,
    last_seq: i64,
    boot: Option<String>,
    gate: Option<Value>,
    last_levels: Vec<(String, &'static str)>,
    closed: bool,
}

struct Listeners {
    level: Vec<LevelFn>,
    signal: Vec<SignalFn>,
}

struct Ep {
    host: String,
    port: u16,
    path: String,
    token: String,
}

struct WsOut {
    id: u64,
    text: String,
    reply: mpsc::Sender<Result<Value, ApiError>>,
}

struct Inner {
    o: Options,
    rest: Option<Ep>,
    ws: Option<Ep>,
    /// "ws" | "rest" — what is actually in use (never guessed)
    kind: Mutex<&'static str>,
    ws_tx: Mutex<Option<mpsc::Sender<WsOut>>>,
    next_id: AtomicU64,
    start: Instant,
    st: Mutex<State>,
    cv: Condvar,
    ls: Mutex<Listeners>,
}

/// A connection to the agent's local API. Listeners are called on the stream thread; do not call `on_*` from inside one.
pub struct Client {
    inner: Arc<Inner>,
}

fn parse_url(url: &str, scheme: &str) -> Result<(String, u16, String), ApiError> {
    let rest = url.strip_prefix(scheme).ok_or_else(|| err(0, "bad-url", Some(json!({"detail": format!("url must start with {}", scheme)}))))?;
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].trim_end_matches('/').to_string()),
        None => (rest, String::new()),
    };
    let (host, port) = if let Some(h) = hostport.strip_prefix('[') {
        let end = h.find(']').ok_or_else(|| err(0, "bad-url", None))?;
        let port = h[end + 1..].strip_prefix(':').map(|p| p.parse::<u16>()).transpose().map_err(|_| err(0, "bad-url", None))?;
        (h[..end].to_string(), port.unwrap_or(80))
    } else {
        match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse::<u16>().map_err(|_| err(0, "bad-url", None))?),
            None => (hostport.to_string(), 80),
        }
    };
    Ok((host, port, path))
}

fn host_fmt(h: &str) -> String {
    if h.contains(':') {
        format!("[{}]", h)
    } else {
        h.to_string()
    }
}

fn find_head_end(b: &[u8]) -> Option<usize> {
    b.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_head(head: &str) -> (u16, Vec<(String, String)>) {
    let mut lines = head.split("\r\n");
    let status = lines.next().and_then(|l| l.split(' ').nth(1)).and_then(|s| s.parse().ok()).unwrap_or(0);
    let hdr = lines.filter_map(|l| l.split_once(':')).map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string())).collect();
    (status, hdr)
}

fn header<'a>(h: &'a [(String, String)], k: &str) -> Option<&'a str> {
    h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

/// Incremental `Transfer-Encoding: chunked` decoder.
struct Dechunk {
    pending: Vec<u8>,
    remaining: usize,
    crlf: usize,
    done: bool,
}

impl Dechunk {
    fn new() -> Self {
        Dechunk { pending: Vec::new(), remaining: 0, crlf: 0, done: false }
    }
    fn feed(&mut self, data: &[u8]) -> Result<Vec<u8>, ApiError> {
        self.pending.extend_from_slice(data);
        let mut out = Vec::new();
        loop {
            if self.done {
                return Ok(out);
            }
            if self.crlf > 0 {
                let n = self.crlf.min(self.pending.len());
                self.pending.drain(..n);
                self.crlf -= n;
                if self.crlf > 0 {
                    return Ok(out);
                }
            }
            if self.remaining > 0 {
                let n = self.remaining.min(self.pending.len());
                out.extend(self.pending.drain(..n));
                self.remaining -= n;
                if self.remaining > 0 {
                    return Ok(out);
                }
                self.crlf = 2;
                continue;
            }
            let i = match self.pending.windows(2).position(|w| w == b"\r\n") {
                Some(i) => i,
                None => return Ok(out),
            };
            let line = String::from_utf8_lossy(&self.pending[..i]).to_string();
            self.pending.drain(..i + 2);
            let size = usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16).map_err(|_| err(0, "bad-chunk", None))?;
            if size > MAX_BODY {
                return Err(err(0, "body-too-big", None));
            }
            if size == 0 {
                self.done = true;
            } else {
                self.remaining = size;
            }
        }
    }
}

impl Inner {
    fn now(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    fn connect_tcp(&self, ep: &Ep, read_timeout: Duration) -> Result<TcpStream, ApiError> {
        let addr = (ep.host.as_str(), ep.port).to_socket_addrs().ok().and_then(|mut a| a.next()).ok_or_else(|| err(0, "unreachable", None))?;
        let s = TcpStream::connect_timeout(&addr, self.o.timeout).map_err(|e| err(0, "unreachable", Some(json!({"detail": e.to_string()}))))?;
        let _ = s.set_read_timeout(Some(read_timeout));
        let _ = s.set_write_timeout(Some(self.o.timeout));
        Ok(s)
    }

    fn rest_ep(&self) -> Result<&Ep, ApiError> {
        self.rest.as_ref().ok_or_else(|| err(0, "no-rest", Some(json!({"detail": "no REST endpoint configured"}))))
    }

    fn write_req(&self, s: &mut TcpStream, method: &str, path: &str, extra: &[(&str, String)], body: Option<&[u8]>) -> Result<(), ApiError> {
        let ep = self.rest_ep()?;
        // No Origin header: the agent refuses requests that carry one.
        let mut h = format!("{} {}{} HTTP/1.1\r\nHost: {}:{}\r\nAuthorization: Bearer {}\r\n", method, ep.path, path, host_fmt(&ep.host), ep.port, ep.token);
        for (k, v) in extra {
            h.push_str(&format!("{}: {}\r\n", k, v));
        }
        if let Some(b) = body {
            h.push_str(&format!("Content-Type: application/json\r\nContent-Length: {}\r\n", b.len()));
        }
        h.push_str("\r\n");
        let mut out = h.into_bytes();
        if let Some(b) = body {
            out.extend_from_slice(b);
        }
        s.write_all(&out).map_err(|e| err(0, "unreachable", Some(json!({"detail": e.to_string()}))))
    }

    fn http(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, ApiError> {
        let mut s = self.connect_tcp(self.rest_ep()?, self.o.timeout)?;
        let raw = body.map(|b| b.to_string().into_bytes());
        self.write_req(&mut s, method, path, &[("Connection", "close".to_string())], raw.as_deref())?;
        let mut buf = Vec::new();
        let mut tmp = [0u8; 8192];
        loop {
            match s.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                    if buf.len() > MAX_BODY + 65536 {
                        return Err(err(0, "body-too-big", None));
                    }
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => return Err(err(0, "client-timeout", None)),
                Err(e) => return Err(err(0, "unreachable", Some(json!({"detail": e.to_string()})))),
            }
        }
        let he = find_head_end(&buf).ok_or_else(|| err(0, "bad-response", None))?;
        let (status, hdr) = parse_head(&String::from_utf8_lossy(&buf[..he]));
        let rest = &buf[he + 4..];
        let data = if header(&hdr, "transfer-encoding").map_or(false, |v| v.to_ascii_lowercase().contains("chunked")) {
            Dechunk::new().feed(rest)?
        } else {
            rest.to_vec()
        };
        let j: Option<Value> = serde_json::from_slice(&data).ok();
        if !(200..300).contains(&status) {
            let code = j.as_ref().and_then(|v| v.get("error")).and_then(|e| e.as_str()).map(String::from).unwrap_or_else(|| format!("http-{}", status));
            return Err(err(status, &code, j));
        }
        Ok(j.unwrap_or_else(|| json!({})))
    }

    /// WebSocket request: handed to the socket thread, answered by id.
    fn ws_request(&self, op: &str, a: &Value) -> Result<Value, ApiError> {
        let tx = self.ws_tx.lock().unwrap().clone().ok_or_else(|| err(0, "closed", None))?;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        let (rtx, rrx) = mpsc::channel();
        let text = json!({"id": id, "op": op, "args": a}).to_string();
        tx.send(WsOut { id, text, reply: rtx }).map_err(|_| err(0, "closed", None))?;
        match rrx.recv_timeout(self.o.timeout) {
            Ok(r) => r,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(err(0, "client-timeout", None)),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(err(0, "closed", None)),
        }
    }

    fn request(&self, op: &str, a: &Value) -> Result<Value, ApiError> {
        if *self.kind.lock().unwrap() == "ws" {
            return self.ws_request(op, a);
        }
        let s = |k: &str, d: &str| -> String {
            match a.get(k) {
                Some(Value::String(x)) => x.clone(),
                Some(v) if !v.is_null() => v.to_string(),
                _ => d.to_string(),
            }
        };
        let enc = |x: String| -> String {
            x.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) { (b as char).to_string() } else { format!("%{:02X}", b) }).collect()
        };
        match op {
            "status" => self.http("GET", "/v1/status", None),
            "conds.list" => self.http("GET", "/v1/conds", None),
            "conds.add" => self.http("POST", "/v1/conds", Some(&json!({"cond": a.get("cond"), "ex": a.get("ex")}))),
            "conds.del" => self.http("DELETE", &format!("/v1/conds/{}", enc(s("slot", ""))), None),
            "signals.since" => self.http("GET", &format!("/v1/signals?since={}&limit={}", enc(s("since", "0")), enc(s("limit", "100"))), None),
            "peers" => self.http("GET", "/v1/peers", None),
            "usage" => self.http("GET", "/v1/usage", None),
            "control.state" => self.http("GET", "/v1/control", None),
            "control.ack" => self.http("POST", "/v1/control/ack", Some(a)),
            "client.register" => self.http("POST", "/v1/clients", Some(a)),
            "usage.report" => self.http("POST", "/v1/usage/report", Some(a)),
            _ => Err(err(404, "unknown-op", None)),
        }
    }
}

fn register(inner: &Arc<Inner>) -> Result<(), ApiError> {
    let name = match &inner.o.name {
        Some(n) => n.clone(),
        None => return Ok(()),
    };
    let uses: Vec<Value> = inner.o.uses.iter().map(|u| json!({"ex": u.ex, "sameKey": u.same_key, "sameHost": u.same_host})).collect();
    let mut args = json!({"name": name, "uses": uses});
    if let Some(id) = inner.st.lock().unwrap().client_id.clone() {
        args["clientId"] = json!(id);
    }
    let r = inner.request("client.register", &args)?;
    inner.st.lock().unwrap().client_id = r.get("clientId").and_then(|v| v.as_str()).map(String::from);
    Ok(())
}

fn refresh_control(inner: &Arc<Inner>) -> Result<(), ApiError> {
    match inner.request("control.state", &json!({})) {
        Ok(b) => {
            let at = inner.now();
            let c = b.get("control").cloned().unwrap_or(Value::Null);
            let changed = inner.st.lock().unwrap().tracker.apply(&c, at);
            if changed {
                levels_changed(inner);
            }
            Ok(())
        }
        Err(e) if e.status == 501 || e.status == 404 => {
            inner.st.lock().unwrap().tracker.set_unsupported();
            levels_changed(inner);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// After any change: send acks, notify level listeners, wake waiters.
fn levels_changed(inner: &Arc<Inner>) {
    let at = inner.now();
    let (acks, events, id) = {
        let mut st = inner.st.lock().unwrap();
        st.tracker.advance(at);
        let acks = st.tracker.drain_acks();
        let mut keys: Vec<String> = st.last_levels.iter().map(|(k, _)| k.clone()).collect();
        for u in &inner.o.uses {
            let k = norm(&u.ex);
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
        let mut events = Vec::new();
        for ex in keys {
            let l = st.tracker.level(&ex, at);
            let pos = st.last_levels.iter().position(|(k, _)| *k == ex);
            if pos.map(|p| st.last_levels[p].1) != Some(l) {
                match pos {
                    Some(p) => st.last_levels[p].1 = l,
                    None => st.last_levels.push((ex.clone(), l)),
                }
                events.push(LevelEvent { why: st.tracker.why(&ex, at), frozen: st.tracker.is_frozen(&ex, at), ex, level: l });
            }
        }
        (acks, events, st.client_id.clone())
    };
    inner.cv.notify_all();
    if let Some(id) = id {
        for a in acks {
            let inn = Arc::clone(inner);
            let args = json!({"clientId": id, "epoch": a.epoch, "ex": a.ex, "paused": a.level != "free"});
            thread::spawn(move || {
                let _ = inn.request("control.ack", &args);
            });
        }
    }
    let ls = inner.ls.lock().unwrap();
    for e in &events {
        for f in &ls.level {
            f(e);
        }
    }
}

fn on_event(inner: &Arc<Inner>, ev: &str, d: Value) {
    match ev {
        "signal" => {
            if let Some(seq) = d.get("seq").and_then(|s| s.as_i64()) {
                let mut st = inner.st.lock().unwrap();
                if seq > st.last_seq {
                    st.last_seq = seq;
                }
            }
            let ls = inner.ls.lock().unwrap();
            for f in &ls.signal {
                f(&d);
            }
        }
        "control" => {
            let at = inner.now();
            let changed = inner.st.lock().unwrap().tracker.apply(&d, at);
            if changed {
                levels_changed(inner);
            }
        }
        "gate" => {
            inner.st.lock().unwrap().gate = Some(json!({"ok": d.get("ok").and_then(|v| v.as_bool()).unwrap_or(false), "reason": d.get("reason")}));
        }
        "hello" => {
            let mut st = inner.st.lock().unwrap();
            if let Some(g) = d.get("gate") {
                st.gate = Some(g.clone());
            }
            if let Some(b) = d.get("boot").and_then(|b| b.as_str()) {
                if st.boot.as_deref().map_or(false, |x| x != b) {
                    st.last_seq = 0; // agent restarted — seq starts again
                }
                st.boot = Some(b.to_string());
            }
        }
        _ => {}
    }
}

/// One SSE connection: returns when it drops. Lease expiry is checked on every read timeout.
fn stream_rest_once(inner: &Arc<Inner>) -> Result<(), ApiError> {
    let mut s = inner.connect_tcp(inner.rest_ep()?, Duration::from_millis(250))?;
    let seq = inner.st.lock().unwrap().last_seq;
    let mut extra = vec![("Accept", "text/event-stream".to_string())];
    if seq > 0 {
        extra.push(("Last-Event-ID", seq.to_string()));
    }
    inner.write_req(&mut s, "GET", "/v1/events", &extra, None)?;
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let mut chunked: Option<Option<Dechunk>> = None; // None = head not read yet; Some(None) = identity body
    let mut parser = SseParser::new();
    let mut utf8_pending: Vec<u8> = Vec::new();
    loop {
        if inner.st.lock().unwrap().closed {
            return Ok(());
        }
        match s.read(&mut tmp) {
            Ok(0) => return Ok(()),
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted) => {
                // leases expire without a message — re-evaluate (emits only on a real change)
                levels_changed(inner);
                continue;
            }
            Err(e) => return Err(err(0, "unreachable", Some(json!({"detail": e.to_string()})))),
        }
        if chunked.is_none() {
            let he = match find_head_end(&buf) {
                Some(i) => i,
                None => continue,
            };
            let (status, hdr) = parse_head(&String::from_utf8_lossy(&buf[..he]));
            if status != 200 {
                return Err(err(status, &format!("http-{}", status), None));
            }
            let is_chunked = header(&hdr, "transfer-encoding").map_or(false, |v| v.to_ascii_lowercase().contains("chunked"));
            chunked = Some(if is_chunked { Some(Dechunk::new()) } else { None });
            buf.drain(..he + 4);
        }
        let data = match chunked.as_mut() {
            Some(Some(d)) => d.feed(&buf)?,
            _ => buf.clone(),
        };
        buf.clear();
        utf8_pending.extend_from_slice(&data);
        let valid = match std::str::from_utf8(&utf8_pending) {
            Ok(_) => utf8_pending.len(),
            Err(e) => e.valid_up_to(),
        };
        let text = String::from_utf8_lossy(&utf8_pending[..valid]).to_string();
        utf8_pending.drain(..valid);
        for e in parser.feed(&text) {
            if let Ok(d) = serde_json::from_str::<Value>(&e.data) {
                on_event(inner, &e.event, d);
            }
        }
    }
}

fn reply_err(m: &Value) -> ApiError {
    let body = m.get("body").filter(|b| b.is_object()).cloned();
    let code = body
        .as_ref()
        .and_then(|b| b.get("error"))
        .and_then(|e| e.as_str())
        .or_else(|| m.get("error").and_then(|e| e.as_str()))
        .unwrap_or("error")
        .to_string();
    let status = m.get("status").and_then(|s| s.as_u64()).unwrap_or(0) as u16;
    err(status, &code, body)
}

/// Open the daemon WebSocket and authenticate. Returns the socket (read timeout short) and the hello body.
fn ws_open(inner: &Arc<Inner>) -> Result<(WebSocket<TcpStream>, Value), ApiError> {
    let ep = inner.ws.as_ref().ok_or_else(|| err(0, "no-ws", None))?;
    let tcp = inner.connect_tcp(ep, inner.o.timeout)?;
    let uri = format!("ws://{}:{}{}", host_fmt(&ep.host), ep.port, ep.path);
    // tungstenite sends no Origin header — the daemon refuses requests that carry one.
    let (mut ws, _resp) = tungstenite::client(uri, tcp).map_err(|_| err(0, "unreachable", Some(json!({"detail": "websocket handshake failed"}))))?;
    ws.send(Message::text(json!({"id": 0, "op": "auth", "token": ep.token}).to_string())).map_err(|_| err(0, "closed", None))?;
    let deadline = Instant::now() + inner.o.timeout;
    let hello = loop {
        if Instant::now() >= deadline {
            return Err(err(0, "client-timeout", None));
        }
        let msg = ws.read().map_err(|_| err(0, "closed", None))?;
        if msg.is_close() {
            return Err(err(0, "closed", None));
        }
        if !msg.is_text() {
            continue;
        }
        let m: Value = serde_json::from_str(msg.to_text().unwrap_or("")).unwrap_or(Value::Null);
        if m.get("id").and_then(|v| v.as_u64()) == Some(0) {
            if m.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                break m.get("body").cloned().unwrap_or(Value::Null);
            }
            return Err(reply_err(&m));
        }
    };
    let _ = ws.get_mut().set_read_timeout(Some(Duration::from_millis(50)));
    Ok((ws, hello))
}

/// The socket thread: sends queued requests, routes replies by id, dispatches events, re-checks leases.
fn ws_loop(inner: Arc<Inner>, mut ws: WebSocket<TcpStream>, rx: mpsc::Receiver<WsOut>) {
    let mut pending: HashMap<u64, mpsc::Sender<Result<Value, ApiError>>> = HashMap::new();
    let mut last_tick = Instant::now();
    loop {
        if inner.st.lock().unwrap().closed {
            let _ = ws.close(None);
            break;
        }
        let mut broken = false;
        while let Ok(out) = rx.try_recv() {
            pending.insert(out.id, out.reply);
            if ws.send(Message::text(out.text)).is_err() {
                broken = true;
                break;
            }
        }
        if broken {
            break;
        }
        match ws.read() {
            Ok(msg) => {
                if msg.is_close() {
                    break;
                }
                if msg.is_text() {
                    let m: Value = serde_json::from_str(msg.to_text().unwrap_or("")).unwrap_or(Value::Null);
                    if let Some(ev) = m.get("ev").and_then(|e| e.as_str()) {
                        let d = if ev == "signal" || ev == "control" { m.get("item").cloned().unwrap_or(Value::Null) } else { m.clone() };
                        on_event(&inner, ev, d);
                    } else if let Some(id) = m.get("id").and_then(|v| v.as_u64()) {
                        if let Some(tx) = pending.remove(&id) {
                            let r = if m.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                                Ok(m.get("body").cloned().unwrap_or_else(|| json!({})))
                            } else {
                                Err(reply_err(&m))
                            };
                            let _ = tx.send(r);
                        }
                    }
                }
            }
            Err(tungstenite::Error::Io(e)) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted) => {}
            Err(_) => break,
        }
        if last_tick.elapsed() >= Duration::from_millis(250) {
            // leases expire without a message — re-evaluate (emits only on a real change)
            levels_changed(&inner);
            last_tick = Instant::now();
        }
    }
    // pending replies are dropped here → their callers get "closed"
    *inner.ws_tx.lock().unwrap() = None;
}

/// (Re)connect: pick the transport, then register → control state → missed signals. Returns the thread that ends when the connection drops.
fn open(inner: &Arc<Inner>) -> Result<thread::JoinHandle<()>, ApiError> {
    let t = inner.o.transport.as_str();
    let try_ws = t == "ws" || (t == "auto" && inner.ws.is_some());
    let mut handle = None;
    if try_ws {
        match ws_open(inner) {
            Ok((ws, hello)) => {
                if let Some(g) = hello.get("gate") {
                    inner.st.lock().unwrap().gate = Some(g.clone());
                }
                let (tx, rx) = mpsc::channel();
                *inner.ws_tx.lock().unwrap() = Some(tx);
                *inner.kind.lock().unwrap() = "ws";
                let inn = Arc::clone(inner);
                handle = Some(thread::spawn(move || ws_loop(inn, ws, rx)));
            }
            Err(e) => {
                if t == "ws" || inner.rest.is_none() {
                    return Err(e);
                }
            }
        }
    }
    if handle.is_none() {
        *inner.kind.lock().unwrap() = "rest";
    }
    register(inner)?;
    refresh_control(inner)?;
    match handle {
        Some(h) => {
            // WebSocket has no Last-Event-ID — fetch what was missed while disconnected.
            let since = inner.st.lock().unwrap().last_seq;
            if since > 0 {
                if let Ok(r) = inner.request("signals.since", &json!({"since": since, "limit": 200})) {
                    if let Some(b) = r.get("boot").and_then(|b| b.as_str()) {
                        let mut st = inner.st.lock().unwrap();
                        if st.boot.as_deref().map_or(false, |x| x != b) {
                            st.last_seq = 0;
                        }
                        st.boot = Some(b.to_string());
                    }
                    for it in r.get("items").and_then(|i| i.as_array()).cloned().unwrap_or_default() {
                        on_event(inner, "signal", it);
                    }
                }
            }
            Ok(h)
        }
        None => {
            let inn = Arc::clone(inner);
            Ok(thread::spawn(move || {
                let _ = stream_rest_once(&inn);
            }))
        }
    }
}

impl Client {
    /// Connect and sync (register → control state), then follow events on a background thread.
    pub fn connect(o: Options) -> Result<Client, ApiError> {
        let tok_ok = |t: &str| t.len() == 64 && t.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !["auto", "ws", "rest"].contains(&o.transport.as_str()) {
            return Err(err(0, "bad-transport", Some(json!({"detail": "transport must be auto, ws or rest"}))));
        }
        let rest = match &o.rest {
            Some(e) => {
                if !tok_ok(&e.token) {
                    return Err(err(0, "bad-token", Some(json!({"detail": "rest.token must be 64 hex characters"}))));
                }
                let (host, port, path) = parse_url(&e.url, "http://")?;
                Some(Ep { host, port, path, token: e.token.clone() })
            }
            None => None,
        };
        let ws = match &o.ws {
            Some(e) => {
                if !tok_ok(&e.token) {
                    return Err(err(0, "bad-token", Some(json!({"detail": "ws.token must be 64 hex characters"}))));
                }
                let (host, port, path) = parse_url(&e.url, "ws://")?;
                Some(Ep { host, port, path: if path.is_empty() { "/v1".to_string() } else { path }, token: e.token.clone() })
            }
            None => None,
        };
        if (o.transport == "ws" && ws.is_none()) || (o.transport == "rest" && rest.is_none()) || (rest.is_none() && ws.is_none()) {
            return Err(err(0, "bad-args", Some(json!({"detail": "missing endpoint for the chosen transport"}))));
        }
        let uses: Vec<String> = o.uses.iter().map(|u| u.ex.clone()).collect();
        let inner = Arc::new(Inner {
            o,
            rest,
            ws,
            kind: Mutex::new("rest"),
            ws_tx: Mutex::new(None),
            next_id: AtomicU64::new(0),
            start: Instant::now(),
            st: Mutex::new(State { tracker: ControlTracker::new(uses), client_id: None, last_seq: 0, boot: None, gate: None, last_levels: Vec::new(), closed: false }),
            cv: Condvar::new(),
            ls: Mutex::new(Listeners { level: Vec::new(), signal: Vec::new() }),
        });
        let first = open(&inner)?;
        let inn = Arc::clone(&inner);
        thread::spawn(move || {
            let mut current = Some(first);
            let mut wait = Duration::from_secs(1);
            loop {
                if let Some(h) = current.take() {
                    let _ = h.join();
                }
                if inn.st.lock().unwrap().closed || !inn.o.reconnect {
                    return;
                }
                // A planned restart says when; otherwise back off 1 s → 30 s.
                let n = inn.st.lock().unwrap().tracker.notice().map(|n| n.in_ms);
                let w = n.map_or(wait, |ms| Duration::from_millis(ms.max(1000) as u64));
                thread::sleep(w);
                if inn.st.lock().unwrap().closed {
                    return;
                }
                match open(&inn) {
                    Ok(h) => {
                        current = Some(h);
                        wait = Duration::from_secs(1);
                    }
                    Err(_) => wait = (wait * 2).min(Duration::from_secs(30)),
                }
            }
        });
        // Re-register before the 120 s registration expires.
        let inn = Arc::clone(&inner);
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(60));
            if inn.st.lock().unwrap().closed {
                return;
            }
            let _ = register(&inn);
        });
        Ok(Client { inner })
    }

    pub fn request(&self, op: &str, args: Value) -> Result<Value, ApiError> {
        if !OPS.contains(&op) {
            return Err(err(404, "unknown-op", None));
        }
        self.inner.request(op, &args)
    }

    pub fn status(&self) -> Result<Value, ApiError> {
        self.request("status", json!({}))
    }
    pub fn usage(&self) -> Result<Value, ApiError> {
        self.request("usage", json!({}))
    }
    pub fn conds_list(&self) -> Result<Value, ApiError> {
        self.request("conds.list", json!({})).map(|b| b.get("items").cloned().unwrap_or(Value::Null))
    }
    pub fn conds_add(&self, cond: Value, ex: &str) -> Result<Value, ApiError> {
        self.request("conds.add", json!({"cond": cond, "ex": ex}))
    }
    pub fn conds_del(&self, slot: &str) -> Result<Value, ApiError> {
        self.request("conds.del", json!({"slot": slot}))
    }
    pub fn signals_since(&self, since: i64, limit: u32) -> Result<Value, ApiError> {
        self.request("signals.since", json!({"since": since, "limit": limit}))
    }

    /// Report the exchange weight spent since your last report (shown to the agent as "external"). Needs a name.
    pub fn report_usage(&self, ex: &str, w: f64, bucket: &str) -> Result<Value, ApiError> {
        let id = self.client_id().ok_or_else(|| err(0, "not-registered", Some(json!({"detail": "connect with a name to report usage"}))))?;
        self.request("usage.report", json!({"clientId": id, "ex": ex, "bucket": bucket, "w": w}))
    }

    /// "ws" or "rest" — what is actually in use (never guessed).
    pub fn transport(&self) -> &'static str {
        *self.inner.kind.lock().unwrap()
    }
    pub fn client_id(&self) -> Option<String> {
        self.inner.st.lock().unwrap().client_id.clone()
    }
    pub fn gate(&self) -> Option<Value> {
        self.inner.st.lock().unwrap().gate.clone()
    }
    /// The agent answered control.state with 501/404 (no control plane).
    pub fn unsupported(&self) -> bool {
        self.inner.st.lock().unwrap().tracker.unsupported()
    }

    /// "free" | "soft" | "hard" | "unknown"
    pub fn level(&self, ex: &str) -> &'static str {
        self.inner.st.lock().unwrap().tracker.level(ex, self.inner.now())
    }
    pub fn frozen(&self, ex: &str) -> bool {
        self.inner.st.lock().unwrap().tracker.is_frozen(ex, self.inner.now())
    }
    pub fn why(&self, ex: &str) -> Vec<String> {
        self.inner.st.lock().unwrap().tracker.why(ex, self.inner.now())
    }

    /// Blocks until `ex` is below `below` ("hard"), or the timeout. "unknown" never blocks.
    pub fn clear(&self, ex: &str, below: &str, timeout: Duration) -> bool {
        let rk = |l: &str| match l {
            "soft" => 1,
            "hard" => 2,
            _ => 0,
        };
        let deadline = Instant::now() + timeout;
        let mut st = self.inner.st.lock().unwrap();
        loop {
            let at = self.inner.now();
            let l = st.tracker.level(ex, at);
            if l == "unknown" || rk(l) < rk(below) {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            // wake for messages (notify) or when a lease can expire on its own
            let mut d = deadline - now;
            if let Some(t) = st.tracker.next_change_at(at) {
                d = d.min(Duration::from_millis((t - at).max(1.0).ceil() as u64));
            }
            st = self.inner.cv.wait_timeout(st, d).unwrap().0;
        }
    }

    pub fn on_level<F: Fn(&LevelEvent) + Send + 'static>(&self, f: F) {
        self.inner.ls.lock().unwrap().level.push(Box::new(f));
    }
    pub fn on_signal<F: Fn(&Value) + Send + 'static>(&self, f: F) {
        self.inner.ls.lock().unwrap().signal.push(Box::new(f));
    }

    pub fn close(&self) {
        self.inner.st.lock().unwrap().closed = true;
        self.inner.cv.notify_all();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.close();
    }
}
