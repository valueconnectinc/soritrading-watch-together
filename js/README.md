# soritrading-watch-together (JavaScript / TypeScript)

```sh
npm install soritrading-watch-together
```

ESM, no dependencies. Node.js 22+ (global `WebSocket` and `fetch`). Types in `index.d.ts`.

```js
import fs from 'node:fs';
import { connect } from 'soritrading-watch-together';

const api = await connect({
  rest: { url: 'http://127.0.0.1:27719', token: fs.readFileSync('local-api.token', 'utf8').trim() },
  ws:   { url: 'ws://127.0.0.1:27720/v1', token: fs.readFileSync('ws-api.token', 'utf8').trim() },
  name: 'my-bot',
  uses: [{ ex: 'binance', sameKey: true, sameHost: true }],
});
console.log('transport:', api.transport);       // 'ws' or 'rest' — what is actually in use

api.on('signal', (s) => console.log('signal', s.seq, s.signal.sym));
api.guard.on('binance', (e) => bot.setPolling(e.level));

await api.guard.clear('binance');                // waits while 'hard' (bounded by the lease)
await api.reportUsage('binance', 5);             // optional: shown to the agent as `external`
```

- `guard.level(ex)` is `free`, `soft`, `hard` or `unknown`. `unknown` means the agent has no control plane (older agent) or no state has been received yet — never reported as `free`. `api.tracker.unsupported` tells the two apart.
- `guard.clear()` never blocks on `unknown`.
- Errors are `ApiError` with a machine-readable `code` (and `reason` for `not-contributing`).

Tests: `node --test test/conformance.test.mjs` runs the shared vectors in `../conformance`.
