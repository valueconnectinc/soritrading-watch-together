package watchtogether;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * WATCH TOGETHER connector — Java 11+ (java.net.http), no dependencies. Usable from Kotlin as is.
 * One client, two transports: WebSocket (P2P daemon) and REST + Server-Sent Events (agent).
 * Read-only: there is no order, key or balance operation in this API.
 *
 * <p>Threads: events, timers and state changes run on one internal thread; listeners are called there —
 * do not block inside a listener. Request methods block the calling thread.
 */
public final class Client implements AutoCloseable {
    public static final int PROTOCOL_VERSION = 1;
    public static final List<String> OPS = Collections.unmodifiableList(Arrays.asList(
            "status", "conds.list", "conds.add", "conds.del", "signals.since", "peers", "usage",
            "control.state", "control.ack", "client.register", "usage.report"));
    private static final Pattern RE_TOKEN = Pattern.compile("[0-9a-f]{64}");
    private static final Map<String, Integer> RANK = Map.of("free", 0, "soft", 1, "hard", 2);

    /** Endpoint: base URL + token file contents. */
    public static final class Endpoint {
        final String url, token;

        public Endpoint(String url, String token) {
            if (token == null || !RE_TOKEN.matcher(token).matches()) throw new IllegalArgumentException("token must be 64 hex characters");
            this.url = Objects.requireNonNull(url);
            this.token = token;
        }
    }

    /** Exchange this program calls itself. sameKey / sameHost: true, false or null (unknown). */
    public static final class Uses {
        final String ex;
        final Boolean sameKey, sameHost;

        public Uses(String ex, Boolean sameKey, Boolean sameHost) { this.ex = ex; this.sameKey = sameKey; this.sameHost = sameHost; }
    }

    public static final class Options {
        public String transport = "auto";   // auto | ws | rest
        public Endpoint ws, rest;
        public String name;
        public List<Uses> uses = new ArrayList<>();
        public boolean reconnect = true;
        public Duration timeout = Duration.ofSeconds(20);
    }

    /** Level change for one exchange. */
    public static final class LevelEvent {
        public final String ex, level;
        public final List<String> why;
        public final boolean frozen;

        LevelEvent(String ex, String level, List<String> why, boolean frozen) { this.ex = ex; this.level = level; this.why = why; this.frozen = frozen; }
    }

    private interface Transport {
        String kind();
        Map<String, Object> hello();
        Map<String, Object> request(String op, Map<String, Object> args);
        void stream(long lastSeq) throws Exception;
        void close();
    }

    private final Options o;
    public final ControlTracker tracker;
    private final HttpClient http;
    private final ScheduledExecutorService loop;
    private final ExecutorService io = Executors.newCachedThreadPool(daemon("wt-io"));
    private final Map<String, List<Consumer<Object>>> ls = new ConcurrentHashMap<>();
    private final Map<String, String> lastLevels = new LinkedHashMap<>();
    private final long t0 = System.nanoTime();
    private volatile Transport t;
    private volatile String transport;    // what is actually in use (never guessed)
    private volatile String clientId;
    private volatile Map<String, Object> gate;
    private volatile long lastSeq;
    private volatile String boot;
    private volatile boolean closed;
    private ScheduledFuture<?> leaseTimer, regTimer;
    public final Guard guard = new Guard();

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> { Thread th = new Thread(r, name); th.setDaemon(true); return th; };
    }

    private Client(Options o) {
        if ("ws".equals(o.transport) && o.ws == null) throw new IllegalArgumentException("transport \"ws\" needs ws");
        if ("rest".equals(o.transport) && o.rest == null) throw new IllegalArgumentException("transport \"rest\" needs rest");
        if ("auto".equals(o.transport) && o.ws == null && o.rest == null) throw new IllegalArgumentException("give ws and/or rest");
        if (!Arrays.asList("auto", "ws", "rest").contains(o.transport)) throw new IllegalArgumentException("transport must be auto, ws or rest");
        this.o = o;
        List<String> uses = new ArrayList<>();
        for (Uses u : o.uses) uses.add(u.ex);
        this.tracker = new ControlTracker(uses);
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(o.timeout).build();
        this.loop = Executors.newSingleThreadScheduledExecutor(daemon("wt-loop"));
    }

    /** Connect and sync (register → control state → missed signals). */
    public static Client connect(Options o) {
        Client c = new Client(o);
        c.open();
        return c;
    }

    double now() { return (System.nanoTime() - t0) / 1e6; }

    public String transport() { return transport; }
    public String clientId() { return clientId; }
    public Map<String, Object> gate() { return gate; }
    public long lastSeq() { return lastSeq; }

    // ── events ──
    /** on("signal"|"control"|"gate"|"hello"|"level"|"notice"|"gap"|"transport"|"close"|"error", fn) → unsubscribe */
    public Runnable on(String ev, Consumer<Object> fn) {
        List<Consumer<Object>> l = ls.computeIfAbsent(ev, k -> new CopyOnWriteArrayList<>());
        l.add(fn);
        return () -> l.remove(fn);
    }

    private void emit(String ev, Object v) {
        for (Consumer<Object> f : ls.getOrDefault(ev, List.of())) {
            try { f.accept(v); } catch (RuntimeException ignored) { /* a listener error must not break the client */ }
        }
    }

    private void onLoop(Runnable r) {
        if (!loop.isShutdown()) try { loop.execute(r); } catch (java.util.concurrent.RejectedExecutionException ignored) { }
    }

    // ── connection ──
    private void open() {
        boolean tryWs = "ws".equals(o.transport) || ("auto".equals(o.transport) && o.ws != null);
        Transport nt = null;
        ApiError wsErr = null;
        if (tryWs) {
            try { nt = new WsTransport(o.ws); } catch (ApiError e) {
                wsErr = e;
                if ("ws".equals(o.transport) || o.rest == null) throw e;
            }
        }
        if (nt == null) nt = new RestTransport(o.rest);
        t = nt;
        if (!nt.kind().equals(transport)) {
            transport = nt.kind();
            Map<String, Object> ev = new LinkedHashMap<>();
            ev.put("kind", nt.kind());
            ev.put("wsError", wsErr == null ? null : wsErr.code);
            onLoop(() -> emit("transport", ev));
        }
        if (nt.hello() != null) gate = Json.obj(nt.hello().get("gate"));
        sync();
        try { nt.stream(lastSeq); } catch (ApiError e) { throw e; } catch (Exception e) { throw new ApiError(0, "unreachable", Map.of("detail", String.valueOf(e))); }
    }

    private void sync() {
        if (o.name != null) {
            try { register(); } catch (ApiError e) { onLoop(() -> emit("error", e)); }
        }
        try { refreshControl(); } catch (ApiError e) { onLoop(() -> emit("error", e)); }
        // WebSocket has no Last-Event-ID — fetch what was missed while disconnected (REST resumes through the SSE header).
        if ("ws".equals(t.kind()) && lastSeq > 0) {
            try {
                Map<String, Object> r = request("signals.since", map("since", lastSeq, "limit", 200));
                Object b = r.get("boot");
                if (b != null && boot != null && !b.equals(boot)) lastSeq = 0;
                if (b != null) boot = String.valueOf(b);
                if (Boolean.TRUE.equals(r.get("gap"))) { long s = lastSeq; onLoop(() -> emit("gap", map("since", s))); }
                List<Object> items = Json.arr(r.get("items"));
                if (items != null) for (Object it : items) onLoop(() -> onEvent("signal", Json.obj(it)));
            } catch (ApiError e) { onLoop(() -> emit("error", e)); }
        }
    }

    private void register() {
        List<Object> uses = new ArrayList<>();
        for (Uses u : o.uses) uses.add(map("ex", u.ex, "sameKey", u.sameKey, "sameHost", u.sameHost));
        Map<String, Object> args = map("name", o.name, "uses", uses);
        if (clientId != null) args.put("clientId", clientId);
        Map<String, Object> r = request("client.register", args);
        clientId = String.valueOf(r.get("clientId"));
        long ttl = r.get("ttlMs") instanceof Number ? ((Number) r.get("ttlMs")).longValue() : 120_000;
        synchronized (this) {
            if (regTimer != null) regTimer.cancel(false);
            if (!closed) regTimer = loop.schedule(() -> io.execute(() -> {
                try { register(); } catch (ApiError e) { onLoop(() -> emit("error", e)); }
            }), Math.max(5000, ttl / 2), TimeUnit.MILLISECONDS);
        }
    }

    /** Pull the control state (done on every connect). 501/404 = this agent has no control plane → levels stay "unknown". */
    public void refreshControl() {
        try {
            Map<String, Object> b = request("control.state", Map.of());
            Map<String, Object> c = Json.obj(b.get("control"));
            double at = now();
            onLoop(() -> applyControl(c, at));
        } catch (ApiError e) {
            if (e.status == 501 || e.status == 404) { tracker.setUnsupported(); onLoop(this::levelsChanged); } else throw e;
        }
    }

    private void onEvent(String ev, Map<String, Object> d) {
        if (d == null) return;
        switch (ev) {
            case "signal": {
                Object s = d.get("seq");
                if (s instanceof Number && ((Number) s).longValue() > lastSeq) lastSeq = ((Number) s).longValue();
                emit("signal", d);
                break;
            }
            case "control": applyControl(d, now()); break;
            case "gate": gate = map("ok", Boolean.TRUE.equals(d.get("ok")), "reason", d.get("reason")); emit("gate", gate); break;
            case "hello": {
                if (d.get("gate") != null) { gate = Json.obj(d.get("gate")); emit("gate", gate); }
                Object b = d.get("boot");
                if (b != null && boot != null && !b.equals(boot)) lastSeq = 0;   // agent restarted — seq starts again
                if (b != null) boot = String.valueOf(b);
                emit("hello", d);
                break;
            }
            default: break;
        }
    }

    private void applyControl(Map<String, Object> c, double at) {
        if (c == null) return;
        Map<String, Object> prev = tracker.notice();
        if (!tracker.apply(c, at)) return;
        emit("control", c);
        Map<String, Object> n = tracker.notice();
        if (n != null && !n.equals(prev)) emit("notice", n);
        levelsChanged();
    }

    /** After any change: send acks, notify guard listeners, arm the lease timer. Runs on the loop thread. */
    private void levelsChanged() {
        double at = now();
        tracker.advance(at);
        for (Map<String, Object> a : tracker.drainAcks()) {
            String id = clientId;
            if (id == null || t == null) continue;
            Map<String, Object> args = map("clientId", id, "epoch", a.get("epoch"), "ex", a.get("ex"), "paused", !"free".equals(a.get("level")));
            io.execute(() -> { try { request("control.ack", args); } catch (ApiError ignored) { } });
        }
        Set<String> seen = new LinkedHashSet<>(lastLevels.keySet());
        for (Uses u : o.uses) seen.add(ControlTracker.norm(u.ex));
        for (String ex : seen) {
            String l = tracker.level(ex, at);
            if (!l.equals(lastLevels.get(ex))) {
                lastLevels.put(ex, l);
                emit("level", new LevelEvent(ex, l, tracker.why(ex, at), tracker.isFrozen(ex, at)));
            }
        }
        synchronized (this) {
            if (leaseTimer != null) leaseTimer.cancel(false);
            leaseTimer = null;
            Double next = tracker.nextChangeAt(at);
            if (next != null && !closed) leaseTimer = loop.schedule(this::levelsChanged, Math.max(1, (long) Math.ceil(next - at)), TimeUnit.MILLISECONDS);
        }
    }

    private void onClose(int code) {
        onLoop(() -> emit("close", code));
        if (closed || !o.reconnect) return;
        io.execute(() -> {
            Map<String, Object> n = tracker.notice();
            long wait = n != null ? Math.max(1000, ((Number) n.get("inMs")).longValue()) : 1000;
            while (!closed) {
                try { Thread.sleep(wait); } catch (InterruptedException e) { return; }
                if (closed) return;
                try { open(); return; } catch (RuntimeException e) { onLoop(() -> emit("error", e)); wait = Math.min(30_000, wait * 2); }
            }
        });
    }

    // ── requests ──
    /** Low-level request on the current transport. Returns the reply body; throws ApiError. */
    public Map<String, Object> request(String op, Map<String, Object> args) {
        Transport tt = t;
        if (tt == null) throw new ApiError(0, "not-connected", null);
        if (!OPS.contains(op)) throw new ApiError(404, "unknown-op", null);
        return tt.request(op, args == null ? Map.of() : args);
    }

    public Map<String, Object> status() { return request("status", null); }
    public List<Object> condsList() { return Json.arr(request("conds.list", null).get("items")); }
    public Map<String, Object> condsAdd(Map<String, Object> cond, String ex) { return request("conds.add", map("cond", cond, "ex", ex)); }
    public Map<String, Object> condsDel(String slot) { return request("conds.del", map("slot", slot)); }
    public Map<String, Object> signalsSince(long since, int limit) { return request("signals.since", map("since", since, "limit", limit)); }
    public Map<String, Object> peers() { return request("peers", null); }
    public Map<String, Object> usage() { return request("usage", null); }

    /** Report the exchange weight you spent since your last report (shown to the agent as {@code external}). Needs a name. */
    public Map<String, Object> reportUsage(String ex, double w, String bucket) {
        if (clientId == null) throw new ApiError(0, "not-registered", Map.of("detail", "connect with a name to report usage"));
        return request("usage.report", map("clientId", clientId, "ex", ex, "bucket", bucket == null ? "rest" : bucket, "w", w));
    }

    @Override
    public void close() {
        closed = true;
        synchronized (this) {
            if (leaseTimer != null) leaseTimer.cancel(false);
            if (regTimer != null) regTimer.cancel(false);
        }
        if (t != null) t.close();
        loop.shutdownNow();
        io.shutdownNow();
    }

    // ── guard: wrap your own background exchange calls ──
    public final class Guard {
        /** "free" | "soft" | "hard" | "unknown" */
        public String level(String ex) { return tracker.level(ex, now()); }
        public boolean frozen(String ex) { return tracker.isFrozen(ex, now()); }
        public List<String> why(String ex) { return tracker.why(ex, now()); }

        /** True when {@code ex} is below {@code below} ("hard" by default), false after the timeout. "unknown" never blocks. */
        public boolean clear(String ex, String below, Duration timeout) throws InterruptedException {
            String b = below == null ? "hard" : below;
            CountDownLatch done = new CountDownLatch(1);
            String k = ControlTracker.norm(ex);
            Runnable off = Client.this.on("level", e -> { if (k.equals(((LevelEvent) e).ex) && ok(ex, b)) done.countDown(); });
            try {
                if (ok(ex, b)) return true;
                return done.await(timeout.toMillis(), TimeUnit.MILLISECONDS) || ok(ex, b);
            } finally { off.run(); }
        }

        public boolean clear(String ex) throws InterruptedException { return clear(ex, "hard", Duration.ofSeconds(60)); }

        private boolean ok(String ex, String below) {
            String l = level(ex);
            return "unknown".equals(l) || RANK.get(l) < RANK.get(below);
        }

        /** fn on every change for {@code ex} — returns unsubscribe */
        public Runnable on(String ex, Consumer<LevelEvent> fn) {
            String k = ControlTracker.norm(ex);
            return Client.this.on("level", e -> { if (k.equals(((LevelEvent) e).ex)) fn.accept((LevelEvent) e); });
        }
    }

    static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    private static ApiError errOf(int status, String text) {
        Map<String, Object> j = null;
        try { j = Json.obj(Json.parse(text)); } catch (RuntimeException ignored) { }
        String code = j != null && j.get("error") != null ? String.valueOf(j.get("error")) : "http-" + status;
        return new ApiError(status, code, j);
    }

    // ── REST transport — request/response over HTTP, events over SSE (/v1/events) ──
    private final class RestTransport implements Transport {
        final String base, auth;
        volatile InputStream sse;
        volatile boolean tClosed;

        RestTransport(Endpoint e) {
            this.base = e.url.replaceAll("/+$", "");
            this.auth = "Bearer " + e.token;
        }

        public String kind() { return "rest"; }
        public Map<String, Object> hello() { return null; }

        public Map<String, Object> request(String op, Map<String, Object> a) {
            String method, path;
            Map<String, Object> body = null;
            switch (op) {
                case "status": method = "GET"; path = "/v1/status"; break;
                case "conds.list": method = "GET"; path = "/v1/conds"; break;
                case "conds.add": method = "POST"; path = "/v1/conds"; body = map("cond", a.get("cond"), "ex", a.get("ex")); break;
                case "conds.del": method = "DELETE"; path = "/v1/conds/" + enc(String.valueOf(a.getOrDefault("slot", ""))); break;
                case "signals.since":
                    method = "GET";
                    path = "/v1/signals?since=" + enc(String.valueOf(a.getOrDefault("since", 0))) + "&limit=" + enc(String.valueOf(a.getOrDefault("limit", 100)));
                    break;
                case "peers": method = "GET"; path = "/v1/peers"; break;
                case "usage": method = "GET"; path = "/v1/usage"; break;
                case "control.state": method = "GET"; path = "/v1/control"; break;
                case "control.ack": method = "POST"; path = "/v1/control/ack"; body = a; break;
                case "client.register": method = "POST"; path = "/v1/clients"; body = a; break;
                case "usage.report": method = "POST"; path = "/v1/usage/report"; body = a; break;
                default: throw new ApiError(404, "unknown-op", null);
            }
            HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(base + path)).timeout(o.timeout).header("Authorization", auth);
            if (body != null) rb.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(Json.write(body)));
            else rb.method(method, HttpRequest.BodyPublishers.noBody());
            HttpResponse<String> res;
            try { res = http.send(rb.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)); }
            catch (java.net.http.HttpTimeoutException e) { throw new ApiError(0, "client-timeout", null); }
            catch (java.io.IOException e) { throw new ApiError(0, "unreachable", map("detail", String.valueOf(e.getMessage()))); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new ApiError(0, "interrupted", null); }
            if (res.statusCode() < 200 || res.statusCode() >= 300) throw errOf(res.statusCode(), res.body());
            Map<String, Object> j = Json.obj(Json.parse(res.body()));
            return j == null ? new LinkedHashMap<>() : j;
        }

        public void stream(long since) throws Exception {
            HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(base + "/v1/events")).header("Authorization", auth).header("Accept", "text/event-stream");
            if (since > 0) rb.header("Last-Event-ID", String.valueOf(since));
            HttpResponse<InputStream> res = http.send(rb.GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            if (res.statusCode() != 200) {
                String text;
                try (InputStream in = res.body()) { text = new String(in.readAllBytes(), StandardCharsets.UTF_8); }
                throw errOf(res.statusCode(), text);
            }
            sse = res.body();
            SseParser parser = new SseParser(e -> {
                Map<String, Object> d;
                try { d = Json.obj(Json.parse(e.data)); } catch (RuntimeException ex) { return; }
                onLoop(() -> onEvent(e.event, d));
            });
            io.execute(() -> {
                char[] buf = new char[8192];
                try (Reader r = new InputStreamReader(sse, StandardCharsets.UTF_8)) {
                    int n;
                    while ((n = r.read(buf)) >= 0) parser.feed(new String(buf, 0, n));
                } catch (java.io.IOException ignored) { /* dropped or closed */ }
                if (!tClosed) onClose(1006);
            });
        }

        public void close() {
            tClosed = true;
            InputStream s = sse;
            if (s != null) try { s.close(); } catch (java.io.IOException ignored) { }
        }
    }

    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }

    // ── WebSocket transport — the daemon's /v1 endpoint. First message authenticates; events arrive on the same socket. ──
    private final class WsTransport implements Transport, WebSocket.Listener {
        final Map<Long, CompletableFuture<Map<String, Object>>> wait = new ConcurrentHashMap<>();
        final AtomicLong id = new AtomicLong();
        final StringBuilder part = new StringBuilder();
        final WebSocket ws;
        final Map<String, Object> hello;
        volatile boolean tClosed;
        CompletableFuture<WebSocket> sending = CompletableFuture.completedFuture(null);

        WsTransport(Endpoint e) {
            try {
                // No Origin header is sent: the daemon refuses requests that carry one.
                ws = http.newWebSocketBuilder().connectTimeout(o.timeout).buildAsync(URI.create(e.url), this)
                        .get(o.timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                throw new ApiError(0, "unreachable", map("detail", "cannot connect to " + e.url + " (" + ex + ")"));
            }
            hello = send(map("op", "auth", "token", e.token));
        }

        public String kind() { return "ws"; }
        public Map<String, Object> hello() { return hello; }

        Map<String, Object> send(Map<String, Object> msg) {
            if (tClosed) throw new ApiError(0, "closed", null);
            long rid = id.incrementAndGet();
            CompletableFuture<Map<String, Object>> f = new CompletableFuture<>();
            wait.put(rid, f);
            msg.put("id", rid);
            String text = Json.write(msg);
            synchronized (this) { sending = sending.thenCompose(w -> ws.sendText(text, true)); }   // one send at a time
            try { return f.get(o.timeout.toMillis(), TimeUnit.MILLISECONDS); }
            catch (TimeoutException ex) { wait.remove(rid); throw new ApiError(0, "client-timeout", null); }
            catch (java.util.concurrent.ExecutionException ex) {
                if (ex.getCause() instanceof ApiError) throw (ApiError) ex.getCause();
                throw new ApiError(0, "closed", null);
            } catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new ApiError(0, "interrupted", null); }
        }

        public Map<String, Object> request(String op, Map<String, Object> args) { return send(map("op", op, "args", args)); }
        public void stream(long since) { }

        @Override public void onOpen(WebSocket w) { w.request(1); }

        @Override
        public java.util.concurrent.CompletionStage<?> onText(WebSocket w, CharSequence data, boolean last) {
            part.append(data);
            if (last) {
                String s = part.toString();
                part.setLength(0);
                onText(s);
            }
            w.request(1);
            return null;
        }

        void onText(String s) {
            Map<String, Object> m;
            try { m = Json.obj(Json.parse(s)); } catch (RuntimeException e) { return; }
            if (m == null) return;
            Object ev = m.get("ev");
            if (ev != null) {
                String k = String.valueOf(ev);
                Map<String, Object> d = "signal".equals(k) || "control".equals(k) ? Json.obj(m.get("item")) : m;
                onLoop(() -> onEvent(k, d));
                return;
            }
            Object rid = m.get("id");
            CompletableFuture<Map<String, Object>> f = rid instanceof Number ? wait.remove(((Number) rid).longValue()) : null;
            if (f == null) return;
            if (Boolean.TRUE.equals(m.get("ok"))) {
                Map<String, Object> b = Json.obj(m.get("body"));
                f.complete(b == null ? new LinkedHashMap<>() : b);
            } else {
                Map<String, Object> b = Json.obj(m.get("body"));
                String code = b != null && b.get("error") != null ? String.valueOf(b.get("error")) : m.get("error") != null ? String.valueOf(m.get("error")) : "error";
                int st = m.get("status") instanceof Number ? ((Number) m.get("status")).intValue() : 0;
                f.completeExceptionally(new ApiError(st, code, b));
            }
        }

        @Override
        public java.util.concurrent.CompletionStage<?> onClose(WebSocket w, int code, String reason) { finish(code); return null; }

        @Override public void onError(WebSocket w, Throwable err) { finish(1006); }

        void finish(int code) {
            boolean was = tClosed;
            tClosed = true;
            for (CompletableFuture<Map<String, Object>> f : wait.values()) f.completeExceptionally(new ApiError(0, "closed", map("code", code)));
            wait.clear();
            if (!was) Client.this.onClose(code);
        }

        public void close() {
            if (tClosed) return;
            tClosed = true;
            try { ws.sendClose(WebSocket.NORMAL_CLOSURE, "").orTimeout(2, TimeUnit.SECONDS); } catch (RuntimeException ignored) { }
            ws.abort();
        }
    }
}
