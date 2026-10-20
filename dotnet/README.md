# SoriTrading.WatchTogether (C# / .NET)

```sh
dotnet add package SoriTrading.WatchTogether
```

.NET 8, no packages (`HttpClient`, `ClientWebSocket`, `System.Text.Json`).

```csharp
await using var api = await Client.ConnectAsync(new ClientOptions
{
    Rest = new Endpoint("http://127.0.0.1:27719", File.ReadAllText("local-api.token").Trim()),
    Ws = new Endpoint("ws://127.0.0.1:27720/v1", File.ReadAllText("ws-api.token").Trim()),
    Name = "my-bot",
    Uses = { new Uses("binance", SameKey: true, SameHost: true) },
});
api.OnLevel("binance", e => bot.SetPolling(e.Level));
await api.ClearAsync("binance");              // waits while "hard"; "unknown" never blocks
await api.ReportUsageAsync("binance", 5);     // optional: shown to the agent as `external`
```

- `Level(ex)` is `free`, `soft`, `hard` or `unknown`; `api.Tracker.Unsupported` tells "older agent" from "no state yet".
- Errors are `ApiError` with a machine-readable `Code`.
- Listeners run on one internal task — do not block inside them.

Tests: `dotnet run --project dotnet/Conformance -- conformance` (from the repository root) runs the shared vectors.
