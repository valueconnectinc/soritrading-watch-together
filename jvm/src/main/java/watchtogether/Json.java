package watchtogether;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON for the protocol — no dependencies.
 * Objects are {@code Map<String,Object>} (insertion order kept), arrays {@code List<Object>},
 * integers {@code Long}, other numbers {@code Double}, plus {@code String}, {@code Boolean} and {@code null}.
 */
public final class Json {
    private Json() {}

    public static Object parse(String s) {
        P p = new P(s);
        p.ws();
        Object v = p.value(0);
        p.ws();
        if (p.i != s.length()) throw p.err("trailing data");
        return v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> obj(Object o) { return o instanceof Map ? (Map<String, Object>) o : null; }

    @SuppressWarnings("unchecked")
    public static List<Object> arr(Object o) { return o instanceof List ? (List<Object>) o : null; }

    public static String write(Object v) {
        StringBuilder b = new StringBuilder();
        write(b, v);
        return b.toString();
    }

    private static void write(StringBuilder b, Object v) {
        if (v == null) b.append("null");
        else if (v instanceof String) str(b, (String) v);
        else if (v instanceof Boolean) b.append(v.toString());
        else if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) b.append("null");
            else if (d == Math.rint(d) && Math.abs(d) < 1e15) b.append((long) d);
            else b.append(d);
        } else if (v instanceof Number) b.append(((Number) v).longValue());
        else if (v instanceof Map) {
            b.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                if (!first) b.append(',');
                first = false;
                str(b, String.valueOf(e.getKey()));
                b.append(':');
                write(b, e.getValue());
            }
            b.append('}');
        } else if (v instanceof List) {
            b.append('[');
            boolean first = true;
            for (Object x : (List<?>) v) {
                if (!first) b.append(',');
                first = false;
                write(b, x);
            }
            b.append(']');
        } else throw new IllegalArgumentException("cannot write " + v.getClass());
    }

    private static void str(StringBuilder b, String s) {
        b.append('"');
        for (int k = 0; k < s.length(); k++) {
            char c = s.charAt(k);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
            }
        }
        b.append('"');
    }

    private static final class P {
        final String s;
        int i;

        P(String s) { this.s = s; }

        IllegalArgumentException err(String m) { return new IllegalArgumentException("json: " + m + " at " + i); }

        void ws() { while (i < s.length() && " \t\r\n".indexOf(s.charAt(i)) >= 0) i++; }

        Object value(int depth) {
            if (depth > 64) throw err("too deep");
            if (i >= s.length()) throw err("unexpected end");
            char c = s.charAt(i);
            if (c == '{') {
                i++;
                Map<String, Object> m = new LinkedHashMap<>();
                ws();
                if (peek('}')) { i++; return m; }
                while (true) {
                    ws();
                    if (!peek('"')) throw err("expected key");
                    String k = string();
                    ws();
                    expect(':');
                    ws();
                    m.put(k, value(depth + 1));
                    ws();
                    if (peek(',')) { i++; continue; }
                    expect('}');
                    return m;
                }
            }
            if (c == '[') {
                i++;
                List<Object> a = new ArrayList<>();
                ws();
                if (peek(']')) { i++; return a; }
                while (true) {
                    ws();
                    a.add(value(depth + 1));
                    ws();
                    if (peek(',')) { i++; continue; }
                    expect(']');
                    return a;
                }
            }
            if (c == '"') return string();
            if (s.startsWith("true", i)) { i += 4; return Boolean.TRUE; }
            if (s.startsWith("false", i)) { i += 5; return Boolean.FALSE; }
            if (s.startsWith("null", i)) { i += 4; return null; }
            return number();
        }

        boolean peek(char c) { return i < s.length() && s.charAt(i) == c; }

        void expect(char c) {
            if (!peek(c)) throw err("expected '" + c + "'");
            i++;
        }

        String string() {
            expect('"');
            StringBuilder b = new StringBuilder();
            while (true) {
                if (i >= s.length()) throw err("unterminated string");
                char c = s.charAt(i++);
                if (c == '"') return b.toString();
                if (c != '\\') { b.append(c); continue; }
                if (i >= s.length()) throw err("bad escape");
                char e = s.charAt(i++);
                switch (e) {
                    case '"': b.append('"'); break;
                    case '\\': b.append('\\'); break;
                    case '/': b.append('/'); break;
                    case 'b': b.append('\b'); break;
                    case 'f': b.append('\f'); break;
                    case 'n': b.append('\n'); break;
                    case 'r': b.append('\r'); break;
                    case 't': b.append('\t'); break;
                    case 'u':
                        if (i + 4 > s.length()) throw err("bad \\u");
                        b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                        break;
                    default: throw err("bad escape");
                }
            }
        }

        Object number() {
            int st = i;
            if (peek('-')) i++;
            while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
            boolean frac = false;
            if (peek('.')) { frac = true; i++; while (i < s.length() && Character.isDigit(s.charAt(i))) i++; }
            if (peek('e') || peek('E')) {
                frac = true;
                i++;
                if (peek('+') || peek('-')) i++;
                while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
            }
            String t = s.substring(st, i);
            if (t.isEmpty() || t.equals("-")) throw err("unexpected character");
            if (!frac) {
                try { return Long.parseLong(t); } catch (NumberFormatException ex) { /* too big: fall through */ }
            }
            return Double.parseDouble(t);
        }
    }
}
