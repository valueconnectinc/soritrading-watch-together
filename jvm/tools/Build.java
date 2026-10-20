import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/**
 * Build and test without Gradle or Maven (JDK 11+):
 *   java tools/Build.java          compile src/main + src/test into build/, then run the conformance vectors
 */
public final class Build {
    public static void main(String[] args) throws Exception {
        JavaCompiler jc = ToolProvider.getSystemJavaCompiler();
        if (jc == null) throw new IllegalStateException("needs a JDK (javax.tools compiler not found)");
        List<String> src = new ArrayList<>();
        for (String d : new String[] {"src/main/java", "src/test/java"}) {
            try (Stream<Path> s = Files.walk(Paths.get(d))) {
                src.addAll(s.filter(p -> p.toString().endsWith(".java")).map(Path::toString).collect(Collectors.toList()));
            }
        }
        List<String> a = new ArrayList<>(List.of("--release", "11", "-Xlint:all", "-Werror", "-d", "build/classes"));
        a.addAll(src);
        if (jc.run(null, null, null, a.toArray(new String[0])) != 0) System.exit(1);
        Process p = new ProcessBuilder("java", "-cp", "build/classes", "watchtogether.ConformanceTest", ".." + File.separator + "conformance").inheritIO().start();
        System.exit(p.waitFor());
    }
}
