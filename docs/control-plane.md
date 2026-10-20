# Control plane — yielding the exchange rate limit

> Protocol v1 (`control.v1`). Not released yet — field names may still change before the first release.

## Why

Exchange rate limits are counted **per API key and per IP address**, not per process. The agent pauses its own network work when a trade is imminent ("focus"), but a bot on the same key or the same server keeps spending the same budget — and the agent's order can be refused.

The control plane tells your program **when to back off, before the agent needs the budget**.

## Rules

1. **The agent never waits for you.** Orders must not depend on an external program being alive. Your acknowledgement is recorded for visibility only.
2. **State, not events.** Every `control` message carries the full current state. If you miss one, the next one corrects you. On every (re)connect, fetch `control.state`.
3. **Leases expire.** Every yield has `ttlMs` (relative milliseconds). The agent renews it while it still applies. If renewals stop (disconnect, agent restart), resume after `ttlMs` on your own monotonic clock.
4. **Background calls only.** Yielding means pausing market-data polling and scans. Whether your own orders wait is your decision.

## Message

```json
{"ev": "control", "item": {
  "v": 1, "boot": "a1b2c3", "epoch": 17,
  "yield": {
    "binance": {"level": "hard", "why": ["focus"],      "ttlMs": 30000},
    "upbit":   {"level": "soft", "why": ["focus-soon"], "ttlMs": 30000}
  },
  "frozen": {"binance": {"untilMs": 4200, "why": "429"}},
  "notice": {"kind": "restart", "inMs": 10000, "why": "update"}
}}
```

| field | meaning |
|---|---|
| `yield.<ex>.level` | absent = free · `soft` = a trade is getting close, reduce · `hard` = a trade is imminent, pause background calls |
| `yield.<ex>.why` | list of causes (`focus`, `frozen`, `focus-soon`) — several can apply at once |
| `yield["*"]` | the agent does not know which exchange (e.g. a run without one) — applies to **every** exchange; the effective level is the higher of `<ex>` and `"*"` |
| `frozen.<ex>` | the exchange refused requests; the agent is backing off for `untilMs` — you should too if you share the key or IP |
| `notice` | planned restart or update; reconnect after `inMs` |
| `epoch` | increases on every change; acknowledgements carry it |
| `boot` | changes when the agent restarts — signal `seq` starts again from 1 |

## Operations

These are answered even while the contribution gate is closed (they carry no signals).

| op (WebSocket) | REST | args | reply |
|---|---|---|---|
| `client.register` | `POST /v1/clients` | `{name, uses: [{ex, sameKey, sameHost}], clientId?}` | `{clientId, ttlMs, renewed}` — re-register before `ttlMs` (120 s) or the entry expires. `sameKey` / `sameHost` are `true`, `false` or `null` (unknown). An unknown `clientId` gets a new one. |
| `control.state` | `GET /v1/control` | — | `{control: {...}}` (the message above) |
| `control.ack` | `POST /v1/control/ack` | `{clientId, epoch, ex, paused}` | `{recorded, stale, epoch}` — visibility only; an ack for an old epoch is not recorded |
| `usage.report` | `POST /v1/usage/report` | `{clientId, ex, bucket, w}` | `{ok}` — `w` is the weight spent since your last report; shown in `usage.external` (reported, not measured) |

Push: WebSocket `{"ev": "control", "item": {...}}`; REST `GET /v1/events` (Server-Sent Events) sends `hello` and the current `control` on connect, then `control` and `signal` events (signals carry `id: <seq>`; reconnect with `Last-Event-ID`).

Capabilities: `status.caps` lists `control.v1`, `client.v1`, `usage.report.v1`. An older agent answers these operations with **501** (`control-unavailable`); connectors then report every level as **`unknown`** — never as `free` — and `tracker.unsupported` is true. With no agent behind the daemon they answer **503** (`no-agent`).

## Client helper

```js
await api.guard.clear('binance');                 // waits while `hard` (bounded by the lease)
api.guard.on('binance', (s) => bot.setPolling(s.level));
```

## Failure cases

| case | behaviour |
|---|---|
| your program dies while yielding | nothing waits on you |
| the agent dies while you yield | the lease expires; you resume |
| a `control` message is lost | the next full state corrects it; reconnects re-fetch |
| WebSocket listener down, agent up | `auto` falls back to REST; `client.transport` says which one is in use |
| clock skew | `ttlMs` and `inMs` are relative; measure them with your own monotonic clock |
