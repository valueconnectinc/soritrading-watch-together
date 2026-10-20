# soritrading-watch-together (Go)

```sh
go get github.com/valueconnectinc/soritrading-watch-together/go
```

```go
import watchtogether "github.com/valueconnectinc/soritrading-watch-together/go"
```

Go 1.21+, standard library only (the WebSocket client is built in).

```go
api, err := watchtogether.Connect(watchtogether.Options{
	REST:      &watchtogether.Endpoint{URL: "http://127.0.0.1:27719", Token: restToken},
	WS:        &watchtogether.Endpoint{URL: "ws://127.0.0.1:27720/v1", Token: wsToken},
	Name:      "my-bot",
	Uses:      []watchtogether.Uses{{Ex: "binance", SameKey: &yes, SameHost: &yes}},
	Reconnect: true,
})
if err != nil { log.Fatal(err) }
defer api.Close()

api.OnLevel("binance", func(e watchtogether.LevelEvent) { bot.SetPolling(e.Level) })
ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
api.Clear(ctx, "binance", "hard")   // waits while "hard"; "unknown" never blocks
cancel()
```

- `Level(ex)` is `free`, `soft`, `hard` or `unknown`; `api.Tracker.Unsupported()` tells "older agent" from "no state yet".
- Errors are `*ApiError` with a machine-readable `Code`.
- Listeners run on one internal goroutine — do not block inside them.

Tests: `go test ./...` runs the shared vectors in `../conformance`.
