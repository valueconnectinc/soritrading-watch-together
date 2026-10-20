# soritrading-watch-together (Java / Kotlin)

Not on Maven Central yet — build from source with `java tools/Build.java` (JDK 11+) and put `build/classes` on your classpath.

Java 11+, no dependencies (`java.net.http` for HTTP and WebSocket). Package `watchtogether`.

```java
Client.Options o = new Client.Options();
o.rest = new Client.Endpoint("http://127.0.0.1:27719", Files.readString(Path.of("local-api.token")).trim());
o.ws   = new Client.Endpoint("ws://127.0.0.1:27720/v1", Files.readString(Path.of("ws-api.token")).trim());
o.name = "my-bot";
o.uses = List.of(new Client.Uses("binance", true, true));
try (Client api = Client.connect(o)) {
    api.on("signal", s -> System.out.println("signal " + s));
    api.guard.on("binance", e -> bot.setPolling(e.level));
    api.guard.clear("binance");               // waits while "hard" (bounded by the lease)
    api.reportUsage("binance", 5, null);      // optional: shown to the agent as `external`
}
```

Kotlin uses the same classes:

```kotlin
val api = Client.connect(Client.Options().apply { rest = Client.Endpoint(url, token); name = "my-bot" })
api.guard.on("binance") { e -> bot.setPolling(e.level) }
```

- `guard.level(ex)` is `free`, `soft`, `hard` or `unknown`; `api.tracker.unsupported()` tells "older agent" from "no state yet".
- Listeners run on one internal thread — do not block inside them. Request methods block the caller.
- Errors are `ApiError` with a machine-readable `code` (and `reason` for `not-contributing`).

Build and run the shared vectors without Gradle or Maven: `java tools/Build.java` (needs a JDK).
