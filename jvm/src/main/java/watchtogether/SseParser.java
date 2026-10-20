package watchtogether;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Server-Sent Events parser — pure, incremental. Vectors: ../../conformance/sse/*.json */
public final class SseParser {
    /** One event. {@code id} is the id field of that block (null when absent). */
    public static final class Event {
        public final String event, id, data;

        Event(String event, String id, String data) { this.event = event; this.id = id; this.data = data; }
    }

    private final Consumer<Event> on;
    private final StringBuilder buf = new StringBuilder();
    private List<String> data = new ArrayList<>();
    private String event, id;
    private boolean has;
    private String lastEventId;

    public SseParser(Consumer<Event> on) { this.on = on; }

    public String lastEventId() { return lastEventId; }

    private void line(String l) {
        if (l.isEmpty()) {
            if (has) on.accept(new Event(event == null ? "message" : event, id, String.join("\n", data)));
            data = new ArrayList<>();
            event = null;
            id = null;
            has = false;
            return;
        }
        if (l.charAt(0) == ':') return;
        int i = l.indexOf(':');
        String field = i < 0 ? l : l.substring(0, i);
        String value = i < 0 ? "" : l.substring(i + 1);
        if (value.startsWith(" ")) value = value.substring(1);
        switch (field) {
            case "data": data.add(value); has = true; break;
            case "event": event = value; break;
            case "id": id = value; lastEventId = value; break;
            default: break;
        }
    }

    public void feed(String chunk) {
        buf.append(chunk);
        while (true) {
            int n = -1, len = 0;
            for (int k = 0; k < buf.length(); k++) {
                char c = buf.charAt(k);
                if (c == '\n') { n = k; len = 1; break; }
                if (c == '\r') {
                    if (k == buf.length() - 1) return;  // may be the first half of \r\n — wait for more
                    n = k;
                    len = buf.charAt(k + 1) == '\n' ? 2 : 1;
                    break;
                }
            }
            if (n < 0) return;
            String l = buf.substring(0, n);
            buf.delete(0, n + len);
            line(l);
        }
    }
}
