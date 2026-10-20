using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace WatchTogether;

/// <summary>An error reply. <c>Code</c> is machine-readable (body.error); read codes, not message text.</summary>
public sealed class ApiError : Exception
{
    public int Status { get; }
    public string Code { get; }
    public string? Reason { get; }
    public JsonObject? Body { get; }

    public ApiError(int status, string code, JsonObject? body = null)
        : base(code + (body?["reason"] is JsonNode r ? $" ({r})" : "") + $" [{status}]")
    {
        Status = status; Code = code; Body = body;
        Reason = body?["reason"] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;
    }
}

/// <summary>Base URL + the token from the token file.</summary>
public sealed record Endpoint(string Url, string Token);

/// <summary>An exchange this program calls itself. SameKey / SameHost: null = unknown.</summary>
public sealed record Uses(string Ex, bool? SameKey = null, bool? SameHost = null);

public sealed class ClientOptions
{
    /// <summary>"auto" | "ws" | "rest"</summary>
    public string Transport { get; set; } = "auto";
    /// <summary>daemon WebSocket, e.g. ws://127.0.0.1:27720/v1</summary>
    public Endpoint? Ws { get; set; }
    /// <summary>agent REST, e.g. http://127.0.0.1:27719</summary>
    public Endpoint? Rest { get; set; }
    /// <summary>register under this name (control plane); null = anonymous</summary>
    public string? Name { get; set; }
    public List<Uses> Uses { get; set; } = new();
    public bool Reconnect { get; set; } = true;
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(20);
}

public sealed record LevelEvent(string Ex, string Level, List<string> Why, bool Frozen);

/// <summary>
/// WATCH TOGETHER connector — .NET 8, no packages. One client, two transports: WebSocket (P2P daemon) and REST + Server-Sent Events (agent).
/// Read-only: there is no order, key or balance operation in this API. Listeners run on one internal task; do not block in them.
/// </summary>
public sealed class Client : IAsyncDisposable
{
    public const int ProtocolVersion = 1;
    public static readonly IReadOnlyList<string> Ops = new[] { "status", "conds.list", "conds.add", "conds.del", "signals.since", "peers", "usage",
        "control.state", "control.ack", "client.register", "usage.report" };
    static readonly Regex ReToken = new("^[0-9a-f]{64}$");
    static readonly Dictionary<string, int> Rank = new() { ["free"] = 0, ["soft"] = 1, ["hard"] = 2 };

    interface ITransport
    {
        string Kind { get; }
        JsonObject? Hello { get; }
        Task<JsonObject> RequestAsync(string op, JsonObject args);
        Task StreamAsync(long lastSeq);
        void Close();
    }

    readonly ClientOptions _o;
    readonly HttpClient _http;
    readonly Stopwatch _clock = Stopwatch.StartNew();
    readonly Channel<Action> _events = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions { SingleReader = true });
    readonly ConcurrentDictionary<string, List<Action<object?>>> _ls = new();
    readonly Dictionary<string, string> _lastLevels = new();
    readonly List<string> _levelOrder = new();
    readonly CancellationTokenSource _cts = new();
    readonly object _lock = new();
    ITransport? _t;
    Timer? _lease, _reg;
    volatile bool _closed;

    public ControlTracker Tracker { get; }
    /// <summary>"ws" or "rest" — what is actually in use (never guessed).</summary>
    public string? Transport { get; private set; }
    public string? ClientId { get; private set; }
    public JsonObject? Gate { get; private set; }
    public long LastSeq { get; private set; }
    public string? Boot { get; private set; }

    Client(ClientOptions o)
    {
        foreach (var e in new[] { o.Ws, o.Rest })
            if (e is not null && !ReToken.IsMatch(e.Token)) throw new ArgumentException("token must be 64 hex characters");
        if (o.Transport is not ("auto" or "ws" or "rest")) throw new ArgumentException("transport must be auto, ws or rest");
        if ((o.Transport == "ws" && o.Ws is null) || (o.Transport == "rest" && o.Rest is null) || (o.Transport == "auto" && o.Ws is null && o.Rest is null))
            throw new ArgumentException("missing endpoint for transport " + o.Transport);
        _o = o;
        _http = new HttpClient { Timeout = System.Threading.Timeout.InfiniteTimeSpan };
        Tracker = new ControlTracker(o.Uses.Select(u => u.Ex));
        _ = Task.Run(PumpAsync);
    }

    /// <summary>Connect and sync (register → control state → missed signals).</summary>
    public static async Task<Client> ConnectAsync(ClientOptions o)
    {
        var c = new Client(o);
        try { await c.OpenAsync(); }
        catch { await c.DisposeAsync(); throw; }
        return c;
    }

    double Now() => _clock.Elapsed.TotalMilliseconds;

    async Task PumpAsync()
    {
        try { await foreach (var f in _events.Reader.ReadAllAsync(_cts.Token)) f(); }
        catch (OperationCanceledException) { }
    }

    void Post(Action f) { if (!_closed) _events.Writer.TryWrite(f); }

    // ── events ──
    /// <summary>On("signal"|"control"|"gate"|"hello"|"level"|"notice"|"gap"|"transport"|"close"|"error", fn) → unsubscribe</summary>
    public Action On(string ev, Action<object?> fn)
    {
        var l = _ls.GetOrAdd(ev, _ => new List<Action<object?>>());
        lock (l) l.Add(fn);
        return () => { lock (l) l.Remove(fn); };
    }

    void Emit(string ev, object? v)
    {
        if (!_ls.TryGetValue(ev, out var l)) return;
        Action<object?>[] arr;
        lock (l) arr = l.ToArray();
        foreach (var f in arr) { try { f(v); } catch { /* a listener error must not break the client */ } }
    }

    // ── connection ──
    async Task OpenAsync()
    {
        var tryWs = _o.Transport == "ws" || (_o.Transport == "auto" && _o.Ws is not null);
        ITransport? t = null;
        ApiError? wsErr = null;
        if (tryWs)
        {
            try { t = await WsTransport.OpenAsync(this, _o.Ws!); }
            catch (ApiError e) { wsErr = e; if (_o.Transport == "ws" || _o.Rest is null) throw; }
        }
        t ??= new RestTransport(this, _o.Rest!);
        _t = t;
        if (Transport != t.Kind)
        {
            Transport = t.Kind;
            var ev = new JsonObject { ["kind"] = t.Kind, ["wsError"] = wsErr?.Code };
            Post(() => Emit("transport", ev));
        }
        if (t.Hello?["gate"] is JsonObject g) Gate = g;
        await SyncAsync();
        await t.StreamAsync(LastSeq);
    }

    async Task SyncAsync()
    {
        if (_o.Name is not null)
        {
            try { await RegisterAsync(); } catch (ApiError e) { Post(() => Emit("error", e)); }
        }
        try { await RefreshControlAsync(); } catch (ApiError e) { Post(() => Emit("error", e)); }
        // WebSocket has no Last-Event-ID — fetch what was missed while disconnected (REST resumes through the SSE header).
        if (_t!.Kind == "ws" && LastSeq > 0)
        {
            try
            {
                var r = await RequestAsync("signals.since", new JsonObject { ["since"] = LastSeq, ["limit"] = 200 });
                var b = r["boot"]?.GetValue<string>();
                if (b is not null && Boot is not null && b != Boot) LastSeq = 0;
                if (b is not null) Boot = b;
                if (r["gap"] is JsonValue gv && gv.TryGetValue<bool>(out var gap) && gap) { var s = LastSeq; Post(() => Emit("gap", s)); }
                if (r["items"] is JsonArray items)
                    foreach (var it in items) if (it is JsonObject m) { var copy = (JsonObject)m.DeepClone(); Post(() => OnEvent("signal", copy)); }
            }
            catch (ApiError e) { Post(() => Emit("error", e)); }
        }
    }

    async Task RegisterAsync()
    {
        var uses = new JsonArray();
        foreach (var u in _o.Uses) uses.Add(new JsonObject { ["ex"] = u.Ex, ["sameKey"] = u.SameKey, ["sameHost"] = u.SameHost });
        var args = new JsonObject { ["name"] = _o.Name, ["uses"] = uses };
        if (ClientId is not null) args["clientId"] = ClientId;
        var r = await RequestAsync("client.register", args);
        ClientId = r["clientId"]?.GetValue<string>();
        var ttl = r["ttlMs"] is JsonValue tv && tv.TryGetValue<double>(out var d) ? d : 120000;
        lock (_lock)
        {
            _reg?.Dispose();
            if (!_closed) _reg = new Timer(_ => _ = Task.Run(async () =>
            {
                try { await RegisterAsync(); } catch (ApiError e) { Post(() => Emit("error", e)); }
            }), null, TimeSpan.FromMilliseconds(Math.Max(5000, ttl / 2)), System.Threading.Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>Pull the control state (done on every connect). 501/404 = no control plane → levels stay "unknown".</summary>
    public async Task RefreshControlAsync()
    {
        try
        {
            var b = await RequestAsync("control.state", new JsonObject());
            var c = b["control"] as JsonObject;
            var at = Now();
            Post(() => ApplyControl(c, at));
        }
        catch (ApiError e) when (e.Status is 501 or 404)
        {
            Tracker.SetUnsupported();
            Post(LevelsChanged);
        }
    }

    void OnEvent(string ev, JsonObject? d)
    {
        if (d is null) return;
        switch (ev)
        {
            case "signal":
                if (d["seq"] is JsonValue sv && sv.TryGetValue<long>(out var seq) && seq > LastSeq) LastSeq = seq;
                Emit("signal", d);
                break;
            case "control": ApplyControl(d, Now()); break;
            case "gate":
                Gate = new JsonObject { ["ok"] = d["ok"] is JsonValue ov && ov.TryGetValue<bool>(out var ok) && ok, ["reason"] = d["reason"]?.DeepClone() };
                Emit("gate", Gate);
                break;
            case "hello":
                if (d["gate"] is JsonObject g) { Gate = (JsonObject)g.DeepClone(); Emit("gate", Gate); }
                var b = d["boot"] is JsonValue bv && bv.TryGetValue<string>(out var bs) ? bs : null;
                if (b is not null && Boot is not null && b != Boot) LastSeq = 0; // agent restarted — seq starts again
                if (b is not null) Boot = b;
                Emit("hello", d);
                break;
        }
    }

    void ApplyControl(JsonObject? c, double at)
    {
        if (c is null) return;
        var prev = Tracker.Notice;
        if (!Tracker.Apply(c, at)) return;
        Emit("control", c);
        var n = Tracker.Notice;
        if (n is not null && n != prev) Emit("notice", n);
        LevelsChanged();
    }

    /// <summary>After any change: send acks, notify guard listeners, arm the lease timer. Runs on the event task.</summary>
    void LevelsChanged()
    {
        var at = Now();
        Tracker.Advance(at);
        foreach (var a in Tracker.DrainAcks())
        {
            if (ClientId is null) continue;
            var args = new JsonObject { ["clientId"] = ClientId, ["epoch"] = a.Epoch, ["ex"] = a.Ex, ["paused"] = a.Level != "free" };
            _ = Task.Run(async () => { try { await RequestAsync("control.ack", args); } catch (ApiError) { } });
        }
        foreach (var u in _o.Uses)
        {
            var k = ControlTracker.Norm(u.Ex);
            if (!_lastLevels.ContainsKey(k)) { _levelOrder.Add(k); _lastLevels[k] = ""; }
        }
        foreach (var ex in _levelOrder)
        {
            var l = Tracker.Level(ex, at);
            if (_lastLevels[ex] != l) { _lastLevels[ex] = l; Emit("level", new LevelEvent(ex, l, Tracker.Why(ex, at), Tracker.IsFrozen(ex, at))); }
        }
        lock (_lock)
        {
            _lease?.Dispose();
            _lease = null;
            var next = Tracker.NextChangeAt(at);
            if (next is double nx && !_closed)
                _lease = new Timer(_ => Post(LevelsChanged), null, TimeSpan.FromMilliseconds(Math.Max(1, Math.Ceiling(nx - at))), System.Threading.Timeout.InfiniteTimeSpan);
        }
    }

    void OnClose(int code)
    {
        Post(() => Emit("close", code));
        if (_closed || !_o.Reconnect) return;
        _ = Task.Run(async () =>
        {
            var n = Tracker.Notice;
            var wait = n is not null ? Math.Max(1000, n.InMs) : 1000;
            while (!_closed)
            {
                try { await Task.Delay((int)wait, _cts.Token); } catch (OperationCanceledException) { return; }
                try { await OpenAsync(); return; }
                catch (Exception e) { Post(() => Emit("error", e)); wait = Math.Min(30000, wait * 2); }
            }
        });
    }

    // ── requests ──
    /// <summary>Low-level request on the current transport. Returns the reply body; throws ApiError.</summary>
    public Task<JsonObject> RequestAsync(string op, JsonObject? args = null)
    {
        var t = _t;
        if (t is null) throw new ApiError(0, "not-connected");
        if (!Ops.Contains(op)) throw new ApiError(404, "unknown-op");
        return t.RequestAsync(op, args ?? new JsonObject());
    }

    public Task<JsonObject> StatusAsync() => RequestAsync("status");
    public Task<JsonObject> UsageAsync() => RequestAsync("usage");
    public Task<JsonObject> PeersAsync() => RequestAsync("peers");
    public async Task<JsonArray?> CondsListAsync() => (await RequestAsync("conds.list"))["items"] as JsonArray;
    public Task<JsonObject> CondsAddAsync(JsonObject cond, string ex) => RequestAsync("conds.add", new JsonObject { ["cond"] = cond, ["ex"] = ex });
    public Task<JsonObject> CondsDelAsync(string slot) => RequestAsync("conds.del", new JsonObject { ["slot"] = slot });
    public Task<JsonObject> SignalsSinceAsync(long since = 0, int limit = 100) => RequestAsync("signals.since", new JsonObject { ["since"] = since, ["limit"] = limit });

    /// <summary>Report the exchange weight spent since your last report (shown to the agent as "external"). Needs a name.</summary>
    public Task<JsonObject> ReportUsageAsync(string ex, double w, string bucket = "rest")
    {
        if (ClientId is null) throw new ApiError(0, "not-registered", new JsonObject { ["detail"] = "connect with a name to report usage" });
        return RequestAsync("usage.report", new JsonObject { ["clientId"] = ClientId, ["ex"] = ex, ["bucket"] = bucket, ["w"] = w });
    }

    // ── guard: wrap your own background exchange calls ──
    /// <summary>"free" | "soft" | "hard" | "unknown"</summary>
    public string Level(string ex) => Tracker.Level(ex, Now());
    public bool Frozen(string ex) => Tracker.IsFrozen(ex, Now());
    public List<string> Why(string ex) => Tracker.Why(ex, Now());

    /// <summary>True when <paramref name="ex"/> is below <paramref name="below"/>, false on timeout. "unknown" never blocks.</summary>
    public async Task<bool> ClearAsync(string ex, string below = "hard", TimeSpan? timeout = null)
    {
        bool Ok() { var l = Level(ex); return l == "unknown" || Rank[l] < Rank[below]; }
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var k = ControlTracker.Norm(ex);
        var off = On("level", e => { if (e is LevelEvent le && le.Ex == k && Ok()) tcs.TrySetResult(true); });
        try
        {
            if (Ok()) return true;
            var done = await Task.WhenAny(tcs.Task, Task.Delay(timeout ?? TimeSpan.FromSeconds(60)));
            return done == tcs.Task || Ok();
        }
        finally { off(); }
    }

    /// <summary>fn on every change for <paramref name="ex"/> — returns unsubscribe</summary>
    public Action OnLevel(string ex, Action<LevelEvent> fn)
    {
        var k = ControlTracker.Norm(ex);
        return On("level", e => { if (e is LevelEvent le && le.Ex == k) fn(le); });
    }

    public ValueTask DisposeAsync()
    {
        _closed = true;
        lock (_lock) { _lease?.Dispose(); _reg?.Dispose(); }
        _t?.Close();
        _cts.Cancel();
        _events.Writer.TryComplete();
        _http.Dispose();
        return ValueTask.CompletedTask;
    }

    static ApiError ErrOf(int status, string text)
    {
        JsonObject? j = null;
        try { j = JsonNode.Parse(text) as JsonObject; } catch { }
        var code = j?["error"] is JsonValue v && v.TryGetValue<string>(out var s) ? s : "http-" + status;
        return new ApiError(status, code, j);
    }

    // ── REST transport — request/response over HTTP, events over SSE (/v1/events) ──
    sealed class RestTransport : ITransport
    {
        readonly Client _c;
        readonly string _base, _auth;
        CancellationTokenSource? _stream;
        volatile bool _closed;

        public RestTransport(Client c, Endpoint e) { _c = c; _base = e.Url.TrimEnd('/'); _auth = "Bearer " + e.Token; }
        public string Kind => "rest";
        public JsonObject? Hello => null;

        public async Task<JsonObject> RequestAsync(string op, JsonObject a)
        {
            static string Enc(JsonNode? n, string d) => Uri.EscapeDataString(n is null ? d : n is JsonValue v && v.TryGetValue<string>(out var s) ? s : n.ToJsonString());
            HttpMethod m;
            string path;
            JsonNode? body = null;
            switch (op)
            {
                case "status": m = HttpMethod.Get; path = "/v1/status"; break;
                case "conds.list": m = HttpMethod.Get; path = "/v1/conds"; break;
                case "conds.add": m = HttpMethod.Post; path = "/v1/conds"; body = new JsonObject { ["cond"] = a["cond"]?.DeepClone(), ["ex"] = a["ex"]?.DeepClone() }; break;
                case "conds.del": m = HttpMethod.Delete; path = "/v1/conds/" + Enc(a["slot"], ""); break;
                case "signals.since": m = HttpMethod.Get; path = "/v1/signals?since=" + Enc(a["since"], "0") + "&limit=" + Enc(a["limit"], "100"); break;
                case "peers": m = HttpMethod.Get; path = "/v1/peers"; break;
                case "usage": m = HttpMethod.Get; path = "/v1/usage"; break;
                case "control.state": m = HttpMethod.Get; path = "/v1/control"; break;
                case "control.ack": m = HttpMethod.Post; path = "/v1/control/ack"; body = a; break;
                case "client.register": m = HttpMethod.Post; path = "/v1/clients"; body = a; break;
                case "usage.report": m = HttpMethod.Post; path = "/v1/usage/report"; body = a; break;
                default: throw new ApiError(404, "unknown-op");
            }
            using var req = new HttpRequestMessage(m, _base + path);
            req.Headers.TryAddWithoutValidation("Authorization", _auth);
            if (body is not null) req.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
            using var cts = new CancellationTokenSource(_c._o.Timeout);
            HttpResponseMessage res;
            try { res = await _c._http.SendAsync(req, cts.Token); }
            catch (OperationCanceledException) { throw new ApiError(0, "client-timeout"); }
            catch (HttpRequestException e) { throw new ApiError(0, "unreachable", new JsonObject { ["detail"] = e.Message }); }
            using (res)
            {
                var text = await res.Content.ReadAsStringAsync();
                if (!res.IsSuccessStatusCode) throw ErrOf((int)res.StatusCode, text);
                return JsonNode.Parse(text) as JsonObject ?? new JsonObject();
            }
        }

        public async Task StreamAsync(long since)
        {
            var cts = new CancellationTokenSource();
            var req = new HttpRequestMessage(HttpMethod.Get, _base + "/v1/events");
            req.Headers.TryAddWithoutValidation("Authorization", _auth);
            req.Headers.TryAddWithoutValidation("Accept", "text/event-stream");
            if (since > 0) req.Headers.TryAddWithoutValidation("Last-Event-ID", since.ToString());
            HttpResponseMessage res;
            try { res = await _c._http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token); }
            catch (HttpRequestException e) { throw new ApiError(0, "unreachable", new JsonObject { ["detail"] = e.Message }); }
            if (!res.IsSuccessStatusCode) { var text = await res.Content.ReadAsStringAsync(); res.Dispose(); throw ErrOf((int)res.StatusCode, text); }
            _stream = cts;
            var parser = new SseParser(e =>
            {
                JsonObject? d;
                try { d = JsonNode.Parse(e.Data) as JsonObject; } catch { return; }
                _c.Post(() => _c.OnEvent(e.Event, d));
            });
            _ = Task.Run(async () =>
            {
                try
                {
                    using (res)
                    using (var s = await res.Content.ReadAsStreamAsync(cts.Token))
                    using (var r = new StreamReader(s, Encoding.UTF8))
                    {
                        var buf = new char[8192];
                        int n;
                        while ((n = await r.ReadAsync(buf.AsMemory(), cts.Token)) > 0) parser.Feed(new string(buf, 0, n));
                    }
                }
                catch (Exception) { /* dropped or closed */ }
                if (!_closed) _c.OnClose(1006);
            });
        }

        public void Close() { _closed = true; _stream?.Cancel(); }
    }

    // ── WebSocket transport — the daemon's /v1 endpoint. First message authenticates; events arrive on the same socket. ──
    sealed class WsTransport : ITransport
    {
        readonly Client _c;
        readonly ClientWebSocket _ws = new();
        readonly ConcurrentDictionary<long, TaskCompletionSource<JsonObject>> _wait = new();
        readonly SemaphoreSlim _send = new(1, 1);
        long _id;
        volatile bool _closed;

        public string Kind => "ws";
        public JsonObject? Hello { get; private set; }

        WsTransport(Client c) => _c = c;

        public static async Task<WsTransport> OpenAsync(Client c, Endpoint e)
        {
            var t = new WsTransport(c);
            try
            {
                using var cts = new CancellationTokenSource(c._o.Timeout);
                // ClientWebSocket sends no Origin header unless one is set — the daemon refuses requests that carry one.
                await t._ws.ConnectAsync(new Uri(e.Url), cts.Token);
            }
            catch (Exception ex) when (ex is WebSocketException or OperationCanceledException or UriFormatException)
            {
                throw new ApiError(0, "unreachable", new JsonObject { ["detail"] = $"cannot connect to {e.Url} ({ex.Message})" });
            }
            _ = Task.Run(t.ReadLoopAsync);
            t.Hello = await t.SendAsync(new JsonObject { ["op"] = "auth", ["token"] = e.Token });
            return t;
        }

        async Task ReadLoopAsync()
        {
            var code = 1006;
            var buf = new byte[65536];
            var ms = new MemoryStream();
            try
            {
                while (_ws.State == WebSocketState.Open)
                {
                    var r = await _ws.ReceiveAsync(new ArraySegment<byte>(buf), CancellationToken.None);
                    if (r.MessageType == WebSocketMessageType.Close) { code = (int)(r.CloseStatus ?? (WebSocketCloseStatus)1005); break; }
                    ms.Write(buf, 0, r.Count);
                    if (ms.Length > 1 << 20) throw new IOException("message too big");
                    if (!r.EndOfMessage) continue;
                    var text = Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
                    ms.SetLength(0);
                    OnText(text);
                }
            }
            catch (Exception) { }
            Finish(code);
        }

        void OnText(string text)
        {
            JsonObject? m;
            try { m = JsonNode.Parse(text) as JsonObject; } catch { return; }
            if (m is null) return;
            if (m["ev"] is JsonValue evv && evv.TryGetValue<string>(out var ev))
            {
                var d = ev is "signal" or "control" ? m["item"] as JsonObject : m;
                _c.Post(() => _c.OnEvent(ev, d));
                return;
            }
            if (m["id"] is not JsonValue idv || !idv.TryGetValue<long>(out var id) || !_wait.TryRemove(id, out var tcs)) return;
            var body = m["body"] as JsonObject;
            if (m["ok"] is JsonValue okv && okv.TryGetValue<bool>(out var ok) && ok) { tcs.TrySetResult(body ?? new JsonObject()); return; }
            var code = body?["error"] is JsonValue ev2 && ev2.TryGetValue<string>(out var c1) ? c1 : m["error"] is JsonValue ev3 && ev3.TryGetValue<string>(out var c2) ? c2 : "error";
            var st = m["status"] is JsonValue sv && sv.TryGetValue<int>(out var s) ? s : 0;
            tcs.TrySetException(new ApiError(st, code, body));
        }

        void Finish(int code)
        {
            var was = _closed;
            _closed = true;
            foreach (var kv in _wait) kv.Value.TrySetException(new ApiError(0, "closed", new JsonObject { ["code"] = code }));
            _wait.Clear();
            if (!was) _c.OnClose(code);
        }

        async Task<JsonObject> SendAsync(JsonObject msg)
        {
            if (_closed) throw new ApiError(0, "closed");
            var id = Interlocked.Increment(ref _id);
            var tcs = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
            _wait[id] = tcs;
            msg["id"] = id;
            var bytes = Encoding.UTF8.GetBytes(msg.ToJsonString());
            await _send.WaitAsync();   // one send at a time
            try { await _ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None); }
            catch (Exception e) when (e is WebSocketException or ObjectDisposedException) { _wait.TryRemove(id, out _); throw new ApiError(0, "closed"); }
            finally { _send.Release(); }
            var done = await Task.WhenAny(tcs.Task, Task.Delay(_c._o.Timeout));
            if (done != tcs.Task) { _wait.TryRemove(id, out _); throw new ApiError(0, "client-timeout"); }
            return await tcs.Task;
        }

        public Task<JsonObject> RequestAsync(string op, JsonObject args) => SendAsync(new JsonObject { ["op"] = op, ["args"] = args.DeepClone() });
        public Task StreamAsync(long since) => Task.CompletedTask;

        public void Close()
        {
            if (_closed) return;
            _closed = true;
            try { _ = _ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None); } catch { }
            _ws.Abort();
        }
    }
}
