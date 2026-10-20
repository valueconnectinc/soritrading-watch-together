package watchtogether;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Runs the language-neutral vectors in ../conformance. No test framework: exit code 0 = pass. Arg: path to conformance/. */
public final class ConformanceTest {
    static int fails = 0, checks = 0;

    static void eq(Object got, Object want, String where) {
        checks++;
        if (!Objects.equals(got, want)) { fails++; System.out.println("FAIL " + where + ": got " + Json.write(got) + " want " + Json.write(want)); }
    }

    static List<Path> files(Path dir) throws Exception {
        try (Stream<Path> s = Files.list(dir)) { return s.filter(p -> p.toString().endsWith(".json")).sorted().collect(Collectors.toList()); }
    }

    static Map<String, Object> read(Path p) throws Exception { return Json.obj(Json.parse(new String(Files.readAllBytes(p), StandardCharsets.UTF_8))); }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args.length > 0 ? args[0] : "../conformance");
        int n = 0;
        for (Path f : files(root.resolve("control"))) {
            n++;
            Map<String, Object> v = read(f);
            List<String> uses = new ArrayList<>();
            List<Object> u = Json.arr(v.get("uses"));
            if (u != null) for (Object x : u) uses.add((String) x);
            ControlTracker t = new ControlTracker(uses);
            double last = Double.NEGATIVE_INFINITY;
            List<Object> steps = Json.arr(v.get("steps"));
            for (int i = 0; i < steps.size(); i++) {
                Map<String, Object> s = Json.obj(steps.get(i));
                double at = ((Number) s.get("at")).doubleValue();
                String where = f.getFileName() + " step " + i;
                if (at < last) { fails++; System.out.println("FAIL " + where + ": steps out of order"); }
                last = at;
                if (s.containsKey("control")) t.apply(Json.obj(s.get("control")), at);
                if (Boolean.TRUE.equals(s.get("unsupported"))) t.setUnsupported();
                Map<String, Object> e = Json.obj(s.get("expect"));
                if (e == null) continue;
                t.advance(at);
                if (e.containsKey("known")) eq(t.known(), e.get("known"), where + " known");
                Map<String, Object> lv = Json.obj(e.get("level"));
                if (lv != null) for (Map.Entry<String, Object> x : lv.entrySet()) eq(t.level(x.getKey(), at), x.getValue(), where + " level " + x.getKey());
                Map<String, Object> fz = Json.obj(e.get("frozen"));
                if (fz != null) for (Map.Entry<String, Object> x : fz.entrySet()) eq(t.isFrozen(x.getKey(), at), x.getValue(), where + " frozen " + x.getKey());
                if (e.containsKey("notice")) eq(t.notice(), e.get("notice"), where + " notice");
                if (e.containsKey("acks")) eq(new ArrayList<Object>(t.drainAcks()), e.get("acks"), where + " acks");
            }
        }
        if (n < 7) { fails++; System.out.println("FAIL only " + n + " control vectors found"); }
        for (Path f : files(root.resolve("sse"))) {
            Map<String, Object> v = read(f);
            List<Object> got = new ArrayList<>();
            SseParser p = new SseParser(e -> got.add(Client.map("event", e.event, "id", e.id, "data", e.data)));
            for (Object c : Json.arr(v.get("chunks"))) p.feed((String) c);
            eq(got, v.get("events"), f.getFileName().toString());
        }
        eq(new ArrayList<Object>(Client.OPS), Json.obj(Json.parse(new String(Files.readAllBytes(root.resolve("ops.json")), StandardCharsets.UTF_8))).get("ops"), "ops.json");
        System.out.println((fails == 0 ? "PASS " : "FAIL ") + (checks - fails) + "/" + checks + " checks, " + n + " control vectors");
        System.exit(fails == 0 ? 0 : 1);
    }
}
