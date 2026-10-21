# SoriTrading WATCH TOGETHER — connectors

Client libraries for the **SoriTrading agent's local API**: let your own program (a bot, a script, a terminal) watch conditions on the WATCH TOGETHER network, receive confirmed signals, and **coordinate exchange rate limits** with the agent running on the same machine or key.

> **Status: not released yet.** Protocol v1 (signals, conditions and the [control plane](docs/control-plane.md)) is implemented in the agent; field names may still change before the first release.

## What is WATCH TOGETHER?

Every exchange limits how many requests one API key may send. That is enough to follow a handful of symbols, not to check a condition across every market every minute.

WATCH TOGETHER splits that work across many SoriTrading agents:

1. **You describe a condition, not a price feed** — e.g. "RSI(14) on 1h below 30 and volume above its 20-bar average". Conditions are declarative (data, not code).
2. **Agents scan their share of the market** with the spare part of their own API limit. Your own trading and data always come first; the network only gets what is left.
3. **Only the result travels**: "symbol X met condition C on bar t". No prices, candles or order books are shared.
4. **You never trust a stranger's claim.** Before a signal reaches you, *your* agent re-checks that one symbol with *your* exchange data. Only confirmed signals are delivered; refuted or unverifiable ones are dropped.

The network is peer-to-peer (libp2p). The SoriTrading server only introduces peers and assigns scan shards; it does not see your keys and does not relay prices.

**Contribution comes first.** Signals are served only while your agent is taking part in the network (connected to peers, sharing spare quota). The control plane and usage numbers are answered regardless.

## What the API is — and is not

- **Read-only.** No order, key, balance or strategy operation exists. Your program places its own orders with its own keys.
- **Confirmed signals only.** Every hit is re-checked by the agent with its own exchange data. No prices are sent — only which symbol and which bar.
- **Contribution required.** Signals are served only while the agent is contributing to the network. The control plane is answered regardless.
- **Local.** The listeners bind to `127.0.0.1`. Reach them from elsewhere through an SSH tunnel.

## Install the agent

The connectors talk to a SoriTrading agent on the same machine. Install it once (Linux or macOS, Node.js 22+ — the script installs Node if needed):

**With a SoriTrading account** — get a connect code on the website (*Agents* page), then:

```sh
curl -fsSL https://soritrading.com/dl/install.sh | bash -s -- <connect-code>
```

**Without an account** — WATCH TOGETHER and the local admin UI only (requires a recent install script):

```sh
curl -fsSL https://soritrading.com/dl/install.sh | bash -s -- --standalone
```

Both install the agent (`soritrading-agent`) and the P2P daemon (`soritrading-p2p`, its own system user, no keys) as services. The script verifies the release signature before installing. Update later with the command the script prints (`node ~/soritrading-agent/agent/agent.js https://soritrading.com --update`).

### Turn on a listener for your program

Both listeners are off by default and bind to `127.0.0.1` only. You need one of them; REST is enough.

**REST (agent, port 27719)** — Linux (systemd):

```sh
sudo systemctl edit soritrading-agent
#   add these two lines, save:
#   [Service]
#   Environment=SORITRADING_LOCAL_API=1
sudo systemctl restart soritrading-agent
cat ~/.soritrading-agent/local-api.token     # the token for your program
```

The override survives `--update`. On macOS add `SORITRADING_LOCAL_API` = `1` to `EnvironmentVariables` in `~/Library/LaunchAgents/com.soritrading.agent.plist` and reload it (`launchctl unload` / `launchctl load`); `--update` rewrites that file, so add it again after an update.

**WebSocket (P2P daemon, port 27720)** — re-run the install script with `--update --p2p-ws`; it prints the address and the command that shows `ws-api.token`.

### Check it

```sh
curl -s -H "Authorization: Bearer $(cat ~/.soritrading-agent/local-api.token)" http://127.0.0.1:27719/v1/status
```

`gate.ok: false` with a `reason` means the agent is not contributing yet (for example `no-peers`, `share-off`) — status, usage and the control plane still answer; signals start once it joins.

To reach the listener from another machine, use an SSH tunnel (`ssh -L 27719:127.0.0.1:27719 user@host`) — never expose it directly.

## Two transports, one client

| | WebSocket | REST |
|---|---|---|
| Served by | the P2P daemon (default `127.0.0.1:27720`, token `ws-api.token`) | the agent (default `127.0.0.1:27719`, token `local-api.token`) |
| Push | `signal`, `gate`, `control` | Server-Sent Events (`/v1/events`) |
| Best for | long-running bots | scripts, cron jobs, setups without the daemon listener |

`transport: "auto"` tries WebSocket and falls back to REST; the client reports which one it actually uses.

**One token is enough.** REST alone carries every operation and event, so the simplest setup is the agent's `local-api.token`. The daemon's `ws-api.token` lives in the daemon's own state folder (it runs as a separate system user) — read it only if you want the WebSocket transport.

## Yielding the rate limit

Exchange limits count per key and per IP, not per process. When the agent is about to trade it asks programs that share the key or host to pause background calls:

```js
api.guard.on('binance', (e) => bot.setPolling(e.level));   // free · soft · hard · unknown
await api.guard.clear('binance');                           // waits while 'hard' (bounded by the lease)
```

`unknown` (an older agent, or no state yet) is never reported as `free`, and `clear()` never blocks on it. The agent never waits for you — see [docs/control-plane.md](docs/control-plane.md).

## Install

| | |
|---|---|
| JavaScript | `npm install soritrading-watch-together` |
| Python | `pip install soritrading-watch-together` (import `watch_together`) |
| Go | `go get github.com/valueconnectinc/soritrading-watch-together/go` |
| C# | `dotnet add package SoriTrading.WatchTogether` |
| Rust | `cargo add soritrading-watch-together` (library `watch_together`) |
| Java / Kotlin | Maven `io.github.valueconnectinc:soritrading-watch-together` |

## Languages

| dir | language | transports | verified |
|---|---|---|---|
| [`js/`](js) | JavaScript / TypeScript (Node 22+) | WebSocket, REST + SSE | vectors + live round trip against the agent |
| [`python/`](python) | Python 3.8+ (asyncio, standard library) | WebSocket, REST + SSE | vectors + live round trip against the agent |
| [`jvm/`](jvm) | Java 11+ / Kotlin (java.net.http) | WebSocket, REST + SSE | vectors + live round trip against the agent |
| [`go/`](go) | Go 1.21+ (standard library) | WebSocket, REST + SSE | CI only |
| [`dotnet/`](dotnet) | C# / .NET 8 | WebSocket, REST + SSE | CI only |
| [`rust/`](rust) | Rust 2021 (serde_json + tungstenite) | WebSocket, REST + SSE | CI only |

Every connector must pass the same language-neutral vectors in [`conformance/`](conformance) (see `.github/workflows/conformance.yml`). Expected values in the vectors are written by hand, never generated by an implementation.

## Security

Tokens are created by the agent and the daemon on first start (files with mode `0600`). Never commit them — see `.gitignore`. Report security issues privately; do not open a public issue.

## License

[MIT](LICENSE) © 2026 Value Connect Global Inc.
