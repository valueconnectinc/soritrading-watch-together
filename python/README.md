# soritrading-watch-together (Python)

```sh
pip install soritrading-watch-together
```

asyncio, standard library only. Python 3.8+. The import name is `watch_together`.

```python
import asyncio
from watch_together import connect

async def main():
    api = await connect(
        rest={"url": "http://127.0.0.1:27719", "token": open("local-api.token").read().strip()},
        ws={"url": "ws://127.0.0.1:27720/v1", "token": open("ws-api.token").read().strip()},
        name="my-bot",
        uses=[{"ex": "binance", "sameKey": True, "sameHost": True}],
    )
    print("transport:", api.transport)          # 'ws' or 'rest' - what is actually in use

    api.on("signal", lambda s: print("signal", s["seq"], s["signal"]["sym"]))
    api.guard.on("binance", lambda e: print("binance is now", e["level"], e["why"]))

    while True:
        await api.guard.clear("binance")          # waits while 'hard' (bounded by the lease)
        ...                                       # your own background calls
        await api.report_usage("binance", 5)      # optional: shown to the agent as `external`
        await asyncio.sleep(1)

asyncio.run(main())
```

- `guard.level(ex)` is `free`, `soft`, `hard` or `unknown`. `unknown` means the agent has no control plane (older agent) or no state has been received yet — it is never reported as `free`. `api.tracker.unsupported` tells the two apart.
- `guard.clear()` never blocks on `unknown`.
- Errors are `ApiError` with a machine-readable `code` (and `reason` for `not-contributing`). Read codes, not message text.

Tests: `python3 -m unittest discover -s tests` runs the shared vectors in `../conformance`.
