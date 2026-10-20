// Runs the language-neutral vectors in ../../conformance. Exit code 0 = pass.
//   dotnet run --project dotnet/Conformance -- conformance
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using WatchTogether;

var root = args.Length > 0 ? args[0] : Path.Combine("..", "..", "conformance");
int fails = 0, checks = 0;

void Eq(JsonNode? got, JsonNode? want, string where)
{
    checks++;
    if (!JsonNode.DeepEquals(got, want)) { fails++; Console.WriteLine($"FAIL {where}: got {got?.ToJsonString() ?? "null"} want {want?.ToJsonString() ?? "null"}"); }
}

JsonObject Read(string p) => (JsonObject)JsonNode.Parse(File.ReadAllText(p))!;

var controlFiles = Directory.GetFiles(Path.Combine(root, "control"), "*.json").OrderBy(f => f, StringComparer.Ordinal).ToList();
foreach (var f in controlFiles)
{
    var v = Read(f);
    var name = Path.GetFileName(f);
    var uses = (v["uses"] as JsonArray)?.Select(x => x!.GetValue<string>()).ToList() ?? new List<string>();
    var t = new ControlTracker(uses);
    var last = double.NegativeInfinity;
    var steps = (JsonArray)v["steps"]!;
    for (var i = 0; i < steps.Count; i++)
    {
        var s = (JsonObject)steps[i]!;
        var at = s["at"]!.GetValue<double>();
        var where = $"{name} step {i}";
        if (at < last) { fails++; Console.WriteLine($"FAIL {where}: steps out of order"); }
        last = at;
        if (s["control"] is JsonObject c) t.Apply(c, at);
        if (s["unsupported"] is JsonValue u && u.GetValue<bool>()) t.SetUnsupported();
        if (s["expect"] is not JsonObject e) continue;
        t.Advance(at);
        if (e.ContainsKey("known")) Eq(JsonValue.Create(t.Known), e["known"], where + " known");
        if (e["level"] is JsonObject lv) foreach (var (ex, want) in lv) Eq(JsonValue.Create(t.Level(ex, at)), want, $"{where} level {ex}");
        if (e["frozen"] is JsonObject fz) foreach (var (ex, want) in fz) Eq(JsonValue.Create(t.IsFrozen(ex, at)), want, $"{where} frozen {ex}");
        if (e.ContainsKey("notice"))
        {
            var n = t.Notice;
            Eq(n is null ? null : new JsonObject { ["kind"] = n.Kind, ["inMs"] = n.InMs, ["why"] = n.Why }, e["notice"], where + " notice");
        }
        if (e.ContainsKey("acks"))
        {
            var arr = new JsonArray();
            foreach (var a in t.DrainAcks()) arr.Add(new JsonObject { ["epoch"] = a.Epoch, ["ex"] = a.Ex, ["level"] = a.Level });
            Eq(arr, e["acks"], where + " acks");
        }
    }
}
if (controlFiles.Count < 7) { fails++; Console.WriteLine($"FAIL only {controlFiles.Count} control vectors"); }

foreach (var f in Directory.GetFiles(Path.Combine(root, "sse"), "*.json").OrderBy(f => f, StringComparer.Ordinal))
{
    var v = Read(f);
    var got = new JsonArray();
    var p = new SseParser(e => got.Add(new JsonObject { ["event"] = e.Event, ["id"] = e.Id, ["data"] = e.Data }));
    foreach (var c in (JsonArray)v["chunks"]!) p.Feed(c!.GetValue<string>());
    Eq(got, v["events"], Path.GetFileName(f));
}

var ops = new JsonArray();
foreach (var o in Client.Ops) ops.Add(o);
Eq(ops, Read(Path.Combine(root, "ops.json"))["ops"], "ops.json");

Console.WriteLine($"{(fails == 0 ? "PASS" : "FAIL")} {checks - fails}/{checks} checks, {controlFiles.Count} control vectors");
return fails == 0 ? 0 : 1;
