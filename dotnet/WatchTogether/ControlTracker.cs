using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace WatchTogether;

/// <summary>An acknowledgement the tracker produced (a change of level for a used exchange).</summary>
public sealed record Ack(long Epoch, string Ex, string Level);

/// <summary>A planned restart or stop.</summary>
public sealed record Notice(string Kind, long InMs, string? Why);

/// <summary>
/// Control-plane tracker — pure. Turns control states into a per-exchange level on the caller's monotonic clock (ms). Thread-safe.
/// Spec: ../../docs/control-plane.md · vectors: ../../conformance/control/*.json
/// </summary>
public sealed class ControlTracker
{
    public const string Any = "*";
    static readonly Dictionary<string, int> Rank = new() { ["free"] = 0, ["soft"] = 1, ["hard"] = 2 };

    sealed record Entry(string Level, double Until, List<string> Why);
    sealed record Frozen(double Until, string? Why);

    readonly object _lock = new();
    bool _known, _unsupported;
    string? _boot;
    long _epoch = -1;
    Dictionary<string, Entry> _table = new();
    Dictionary<string, Frozen> _frozen = new();
    Notice? _notice;
    readonly List<string> _order = new();
    readonly Dictionary<string, string> _prev = new();
    List<Ack> _acks = new();

    /// <param name="uses">exchanges this program calls itself — only these are acknowledged</param>
    public ControlTracker(IEnumerable<string>? uses = null)
    {
        foreach (var ex in uses ?? Array.Empty<string>())
        {
            var k = Norm(ex);
            if (_prev.ContainsKey(k)) continue;
            _order.Add(k);
            _prev[k] = "free";
        }
    }

    internal static string Norm(string? ex)
    {
        var s = (ex ?? "").Trim().ToLowerInvariant();
        return s.Length == 0 ? Any : s;
    }

    static double Num(JsonNode? n)
    {
        if (n is JsonValue v && v.TryGetValue<double>(out var d) && !double.IsNaN(d) && !double.IsInfinity(d)) return d;
        return 0;
    }

    static string? Str(JsonNode? n) => n is null ? null : n is JsonValue v && v.TryGetValue<string>(out var s) ? s : n.ToJsonString();

    string LevelLocked(string ex, double at)
    {
        if (!_known) return "unknown";
        var best = "free";
        foreach (var k in new[] { Norm(ex), Any })
            if (_table.TryGetValue(k, out var e) && at < e.Until && Rank[e.Level] > Rank[best]) best = e.Level;
        return best;
    }

    /// <summary>"free" | "soft" | "hard" | "unknown"</summary>
    public string Level(string ex, double at) { lock (_lock) return LevelLocked(ex, at); }

    public bool IsFrozen(string ex, double at) { lock (_lock) return _frozen.TryGetValue(Norm(ex), out var f) && at < f.Until; }

    public List<string> Why(string ex, double at)
    {
        lock (_lock)
        {
            var o = new List<string>();
            foreach (var k in new[] { Norm(ex), Any })
                if (_table.TryGetValue(k, out var e) && at < e.Until)
                    foreach (var w in e.Why) if (!o.Contains(w)) o.Add(w);
            return o;
        }
    }

    /// <summary>Earliest time after <paramref name="at"/> when a level or frozen flag can change without a new message, or null.</summary>
    public double? NextChangeAt(double at)
    {
        lock (_lock)
        {
            var ts = _table.Values.Select(e => e.Until).Concat(_frozen.Values.Select(f => f.Until)).Where(u => u > at).ToList();
            return ts.Count == 0 ? null : ts.Min();
        }
    }

    void AdvanceLocked(double at)
    {
        if (!_known) return;
        foreach (var ex in _order)
        {
            var l = LevelLocked(ex, at);
            if (l != _prev[ex]) { _prev[ex] = l; _acks.Add(new Ack(_epoch, ex, l)); }
        }
    }

    public void Advance(double at) { lock (_lock) AdvanceLocked(at); }

    /// <summary>Apply a full control state received at <paramref name="at"/>. False when ignored (older epoch in the same boot).</summary>
    public bool Apply(JsonObject? c, double at)
    {
        if (c is null) return false;
        if (c["epoch"] is not JsonValue ev || !ev.TryGetValue<double>(out var ed) || double.IsNaN(ed) || double.IsInfinity(ed)) return false;
        var e = (long)ed;
        var b = Str(c["boot"]);
        lock (_lock)
        {
            if (_known && b == _boot && e < _epoch) return false;
            _boot = b; _epoch = e; _known = true; _unsupported = false;
            _table = new();
            if (c["yield"] is JsonObject y)
                foreach (var (ex, raw) in y)
                {
                    if (raw is not JsonObject v) continue;
                    var lv = Str(v["level"]);
                    if (lv != "soft" && lv != "hard") continue;
                    var why = v["why"] is JsonArray wa ? wa.Select(x => Str(x) ?? "").ToList() : new List<string>();
                    _table[Norm(ex)] = new Entry(lv, at + Math.Max(0, Num(v["ttlMs"])), why);
                }
            _frozen = new();
            if (c["frozen"] is JsonObject fz)
                foreach (var (ex, raw) in fz)
                    if (raw is JsonObject v && Num(v["untilMs"]) > 0) _frozen[Norm(ex)] = new Frozen(at + Num(v["untilMs"]), Str(v["why"]));
            _notice = c["notice"] is JsonObject n && Str(n["kind"]) is { Length: > 0 } kind ? new Notice(kind, (long)Num(n["inMs"]), Str(n["why"])) : null;
            AdvanceLocked(at);
            return true;
        }
    }

    /// <summary>The agent has no control plane (control.state answered 501/404). Levels stay "unknown" — never "free".</summary>
    public void SetUnsupported() { lock (_lock) { _unsupported = true; _known = false; } }

    public List<Ack> DrainAcks() { lock (_lock) { var a = _acks; _acks = new(); return a; } }

    public bool Known { get { lock (_lock) return _known; } }
    public bool Unsupported { get { lock (_lock) return _unsupported; } }
    public Notice? Notice { get { lock (_lock) return _notice; } }
    public long? Epoch { get { lock (_lock) return _known ? _epoch : null; } }
    public string? Boot { get { lock (_lock) return _boot; } }
}
