package watchtogether;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Control-plane tracker — pure. Turns {@code control} states into a per-exchange level on the caller's monotonic clock (ms).
 * Spec: ../../docs/control-plane.md · vectors: ../../conformance/control/*.json
 */
public final class ControlTracker {
    public static final String ANY = "*";
    private static final Map<String, Integer> RANK = Map.of("free", 0, "soft", 1, "hard", 2);

    private static final class Entry {
        final String level;
        final double until;
        final List<String> why;

        Entry(String level, double until, List<String> why) { this.level = level; this.until = until; this.why = why; }
    }

    private static final class Frozen {
        final double until;
        final String why;

        Frozen(double until, String why) { this.until = until; this.why = why; }
    }

    private boolean known, unsupported;
    private String boot;
    private long epoch = -1;
    private Map<String, Entry> table = new HashMap<>();
    private Map<String, Frozen> frozen = new HashMap<>();
    private Map<String, Object> notice;
    private final LinkedHashMap<String, String> prev = new LinkedHashMap<>();
    private List<Map<String, Object>> acks = new ArrayList<>();

    /** @param uses exchanges this program calls itself — only these are acknowledged */
    public ControlTracker(Collection<String> uses) {
        if (uses != null) for (String ex : uses) prev.put(norm(ex), "free");
    }

    static String norm(String ex) {
        String s = ex == null ? "" : ex.trim().toLowerCase(Locale.ROOT);
        return s.isEmpty() ? ANY : s;
    }

    private static double num(Object v) {
        if (!(v instanceof Number)) return 0;
        double d = ((Number) v).doubleValue();
        return Double.isNaN(d) || Double.isInfinite(d) ? 0 : d;
    }

    /** "free" | "soft" | "hard" | "unknown" */
    public synchronized String level(String ex, double at) {
        if (!known) return "unknown";
        String best = "free";
        for (String k : new String[] {norm(ex), ANY}) {
            Entry e = table.get(k);
            if (e != null && at < e.until && RANK.get(e.level) > RANK.get(best)) best = e.level;
        }
        return best;
    }

    public synchronized boolean isFrozen(String ex, double at) {
        Frozen f = frozen.get(norm(ex));
        return f != null && at < f.until;
    }

    public synchronized List<String> why(String ex, double at) {
        List<String> out = new ArrayList<>();
        for (String k : new String[] {norm(ex), ANY}) {
            Entry e = table.get(k);
            if (e != null && at < e.until) for (String w : e.why) if (!out.contains(w)) out.add(w);
        }
        return out;
    }

    /** Earliest time after {@code at} when a level or frozen flag can change without a new message, or null. */
    public synchronized Double nextChangeAt(double at) {
        double n = Double.POSITIVE_INFINITY;
        for (Entry e : table.values()) if (e.until > at && e.until < n) n = e.until;
        for (Frozen f : frozen.values()) if (f.until > at && f.until < n) n = f.until;
        return n == Double.POSITIVE_INFINITY ? null : n;
    }

    public synchronized void advance(double at) {
        if (!known) return;
        for (Map.Entry<String, String> p : prev.entrySet()) {
            String l = level(p.getKey(), at);
            if (!l.equals(p.getValue())) {
                p.setValue(l);
                Map<String, Object> a = new LinkedHashMap<>();
                a.put("epoch", epoch);
                a.put("ex", p.getKey());
                a.put("level", l);
                acks.add(a);
            }
        }
    }

    /** Apply a full control state received at {@code at}. Returns false when ignored (older epoch in the same boot). */
    public synchronized boolean apply(Map<String, Object> c, double at) {
        if (c == null) return false;
        Object eo = c.get("epoch");
        if (!(eo instanceof Number)) return false;
        double ed = ((Number) eo).doubleValue();
        if (Double.isNaN(ed) || Double.isInfinite(ed)) return false;
        long e = (long) ed;
        String b = c.get("boot") == null ? null : String.valueOf(c.get("boot"));
        if (known && java.util.Objects.equals(b, boot) && e < epoch) return false;
        boot = b;
        epoch = e;
        known = true;
        unsupported = false;
        table = new HashMap<>();
        Map<String, Object> y = Json.obj(c.get("yield"));
        if (y != null) for (Map.Entry<String, Object> en : y.entrySet()) {
            Map<String, Object> v = Json.obj(en.getValue());
            if (v == null) continue;
            Object lv = v.get("level");
            if (!"soft".equals(lv) && !"hard".equals(lv)) continue;
            List<String> why = new ArrayList<>();
            List<Object> w = Json.arr(v.get("why"));
            if (w != null) for (Object x : w) why.add(String.valueOf(x));
            table.put(norm(en.getKey()), new Entry((String) lv, at + Math.max(0, num(v.get("ttlMs"))), why));
        }
        frozen = new HashMap<>();
        Map<String, Object> fz = Json.obj(c.get("frozen"));
        if (fz != null) for (Map.Entry<String, Object> en : fz.entrySet()) {
            Map<String, Object> v = Json.obj(en.getValue());
            if (v != null && num(v.get("untilMs")) > 0)
                frozen.put(norm(en.getKey()), new Frozen(at + num(v.get("untilMs")), v.get("why") == null ? null : String.valueOf(v.get("why"))));
        }
        Map<String, Object> n = Json.obj(c.get("notice"));
        if (n != null && n.get("kind") != null && !"".equals(n.get("kind"))) {
            Map<String, Object> nn = new LinkedHashMap<>();
            nn.put("kind", String.valueOf(n.get("kind")));
            nn.put("inMs", (long) num(n.get("inMs")));
            nn.put("why", n.get("why") == null ? null : String.valueOf(n.get("why")));
            notice = nn;
        } else notice = null;
        advance(at);
        return true;
    }

    /** The agent has no control plane (control.state answered 501/404). Levels stay "unknown" — never "free". */
    public synchronized void setUnsupported() { unsupported = true; known = false; }

    public synchronized List<Map<String, Object>> drainAcks() {
        List<Map<String, Object>> a = acks;
        acks = new ArrayList<>();
        return a;
    }

    public synchronized boolean known() { return known; }
    public synchronized boolean unsupported() { return unsupported; }
    public synchronized Map<String, Object> notice() { return notice; }
    public synchronized Long epoch() { return known ? epoch : null; }
    public synchronized String boot() { return boot; }
}
