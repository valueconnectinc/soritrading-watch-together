# soritrading-watch-together (Rust)

```sh
cargo add soritrading-watch-together
```

Rust 2021, blocking (threads, no async runtime). Dependencies: `serde_json`, `tungstenite` (WebSocket, no TLS).
The library name is `watch_together`. Both transports: REST + Server-Sent Events (the agent) and WebSocket (the P2P daemon).

```rust
use watch_together::{Client, Options, Uses};

let rest_token = std::fs::read_to_string("local-api.token")?;
let ws_token = std::fs::read_to_string("ws-api.token")?;
let mut o = Options::new("http://127.0.0.1:27719", rest_token.trim())      // REST
    .with_ws("ws://127.0.0.1:27720/v1", ws_token.trim());                  // + WebSocket ("auto" prefers it)
o.name = Some("my-bot".into());
o.uses = vec![Uses { ex: "binance".into(), same_key: Some(true), same_host: Some(true) }];
let api = Client::connect(o)?;
println!("transport: {}", api.transport());                                // "ws" or "rest" — what is actually in use
api.on_level(|e| println!("{} is now {}", e.ex, e.level));
api.clear("binance", "hard", std::time::Duration::from_secs(60));          // "unknown" never blocks
api.report_usage("binance", 5.0, "rest")?;
```

`Options::ws_only(url, token)` connects to the daemon alone; `o.transport = "rest".into()` forces REST.

- `level(ex)` is `free`, `soft`, `hard` or `unknown`; `unsupported()` tells "older agent" from "no state yet".
- Errors are `ApiError` with a machine-readable `code`.
- Listeners run on the connection thread — do not register new listeners from inside one.

Tests: `cargo test` runs the shared vectors in `../conformance`.
