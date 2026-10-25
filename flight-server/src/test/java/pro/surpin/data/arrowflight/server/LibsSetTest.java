package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Checks the runtime libs set (target/libs, release asset flight-server-libs): the Hadoop client libraries the
 * shaded jar leaves to the deployment, at the floors of docs/dev/DEPENDENCY-AUDIT.md, and enough to start the
 * server and read Parquet with the server's own libraries.
 */
@Tag("integration")
class LibsSetTest {

    /** Jar file name: artifact id, then a version starting with a digit. */
    private static final Pattern JAR = Pattern.compile("(.+?)-(\\d[^-]*(?:-[^-]+)*?)(?:-(linux-[a-z0-9_]+))?\\.jar");

    /** Artifacts that must not come back with the set (stale stand set, duplicates of shaded classes). */
    private static final List<String> BANNED = List.of(
            "netty-all", "netty", "zookeeper", "log4j-core", "log4j-1.2-api", "reload4j", "slf4j-reload4j",
            "bcprov-jdk18on", "bcpkix-jdk18on", "hadoop-client-api", "hadoop-client-runtime", "jersey-bundle",
            "lz4-java", "protobuf-java", "jackson-databind", "netty-codec-http2", "grpc-core", "arrow-vector",
            "duckdb_jdbc");

    private static final String STARTED = "Server Arrow Flight SQL started";

    @Test
    void setHoldsOneVersionPerLibraryAtTheFloors() throws IOException {
        Map<String, String> jars = jars(libsDir());
        assertTrue(jars.containsKey("hadoop-common"), "no hadoop-common: " + jars.keySet());
        assertTrue(jars.containsKey("hadoop-hdfs-client"), "no hadoop-hdfs-client: " + jars.keySet());
        assertEquals(jars.get("hadoop-common"), jars.get("hadoop-hdfs-client"), "one Hadoop version");
        for (String banned : BANNED) {
            assertFalse(jars.containsKey(banned), banned + " is in the libs set: " + jars);
        }
        assertAtLeast(jars, "hadoop-common", 3, 4, 0);
        assertAtLeast(jars, "commons-io", 2, 14, 0);
        assertAtLeast(jars, "dnsjava", 3, 6, 0);
        assertAtLeast(jars, "avro", 1, 11, 4);
        assertAtLeast(jars, "woodstox-core", 5, 4, 0);
        jars.forEach((artifact, version) -> {
            if (artifact.startsWith("jetty-")) {
                assertTrue(atLeast(version, 9, 4, 58), artifact + " " + version + " is below 9.4.58");
            }
        });
    }

    @Test
    void serverStartsOnTheLibsSetAndReadsParquet(@TempDir Path tmp) throws Exception {
        Path table = Files.createDirectories(tmp.resolve("data/test_schema/t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = duck.createStatement()) {
            statement.execute("COPY (SELECT range AS id FROM range(10)) TO '"
                    + table.resolve("part-0.parquet") + "' (FORMAT parquet)");
        }
        int port = freePort();
        Path log = tmp.resolve("server.log");
        Process server = start(classpath(true), tmp.resolve("data"), port, log);
        try {
            awaitStart(server, log);
            try (BufferAllocator allocator = new RootAllocator();
                 FlightClient client = FlightClient.builder(allocator,
                         Location.forGrpcInsecure("127.0.0.1", port)).build();
                 FlightSqlClient sql = new FlightSqlClient(client)) {
                FlightInfo info = sql.execute("SELECT count(*) AS n, CAST(sum(id) AS BIGINT) AS s FROM test_schema.t");
                long[] row = new long[2];
                for (FlightEndpoint endpoint : info.getEndpoints()) {
                    try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                        while (stream.next()) {
                            if (stream.getRoot().getRowCount() > 0) {
                                row[0] += ((BigIntVector) stream.getRoot().getVector(0)).get(0);
                                row[1] += ((BigIntVector) stream.getRoot().getVector(1)).get(0);
                            }
                        }
                    }
                }
                assertEquals(10, row[0], "rows");
                assertEquals(45, row[1], "sum of id");
            }
        } finally {
            stop(server);
        }
    }

    @Test
    void serverDoesNotStartWithoutTheLibsSet(@TempDir Path tmp) throws Exception {
        Files.createDirectories(tmp.resolve("data"));
        Path log = tmp.resolve("server.log");
        Process server = start(classpath(false), tmp.resolve("data"), freePort(), log);
        try {
            assertTrue(server.waitFor(120, TimeUnit.SECONDS), "the server should exit");
            assertNotEquals(0, server.exitValue());
            String out = Files.readString(log, StandardCharsets.UTF_8);
            assertTrue(out.contains("org/apache/hadoop/conf/Configuration"), out);
            assertFalse(out.contains(STARTED), out);
        } finally {
            stop(server);
        }
    }

    @Test
    void jarNamesSplitIntoArtifactAndVersion() {
        assertEquals(List.of("hadoop-common", "3.5.0"), split("hadoop-common-3.5.0.jar"));
        assertEquals(List.of("jetty-util-ajax", "9.4.58.v20250814"), split("jetty-util-ajax-9.4.58.v20250814.jar"));
        assertEquals(List.of("javassist", "3.30.2-GA"), split("javassist-3.30.2-GA.jar"));
        assertEquals(List.of("hadoop-shaded-protobuf_3_25", "1.5.0"), split("hadoop-shaded-protobuf_3_25-1.5.0.jar"));
        assertEquals(List.of("netty-transport-native-epoll", "4.2.19.Final"),
                split("netty-transport-native-epoll-4.2.19.Final-linux-x86_64.jar"));
        assertEquals(List.of("netty", "3.10.6.Final"), split("netty-3.10.6.Final.jar"));
    }

    /**
     * Returns the libs set directory the build copied.
     *
     * @return directory of the set (the test is skipped without one)
     */
    private static Path libsDir() {
        String dir = System.getProperty("libs.set.dir", "");
        assumeTrue(!dir.isBlank() && Files.isDirectory(Path.of(dir)), "no libs set (run through Maven)");
        return Path.of(dir);
    }

    /**
     * Maps every jar of a directory to its version.
     *
     * @param dir directory with jars
     * @return artifact id to version
     * @throws IOException when the directory cannot be listed
     */
    private static Map<String, String> jars(Path dir) throws IOException {
        Map<String, String> jars = new TreeMap<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.filter(f -> f.toString().endsWith(".jar")).sorted().toList()) {
                List<String> parts = split(file.getFileName().toString());
                String previous = jars.put(parts.get(0), parts.get(1));
                if (previous != null && !previous.equals(parts.get(1))) {
                    fail(parts.get(0) + " twice in the libs set: " + previous + " and " + parts.get(1));
                }
            }
        }
        assertFalse(jars.isEmpty(), "empty libs set " + dir);
        return jars;
    }

    /**
     * Splits a jar file name into artifact id and version.
     *
     * @param name file name such as hadoop-common-3.5.0.jar
     * @return artifact id and version
     */
    private static List<String> split(String name) {
        Matcher m = JAR.matcher(name);
        assertTrue(m.matches(), "not artifact-version.jar: " + name);
        return List.of(m.group(1), m.group(2));
    }

    /**
     * Builds the class path of a thin start: the server classes and libraries (what the shaded jar holds),
     * optionally followed by the libs set.
     *
     * @param withLibs whether to append the libs set
     * @return class path
     * @throws Exception when the server class path file cannot be read
     */
    private static String classpath(boolean withLibs) throws Exception {
        Path libs = libsDir();
        String serverClasspath = System.getProperty("libs.set.serverClasspath", "");
        assumeTrue(!serverClasspath.isBlank(), "no server class path file (run through Maven)");
        Path classes = Path.of(HadoopArrowFlightServer.class.getProtectionDomain().getCodeSource().getLocation()
                .toURI());
        List<String> entries = new ArrayList<>();
        entries.add(classes.toString());
        entries.add(Files.readString(Path.of(serverClasspath), StandardCharsets.UTF_8).trim());
        if (withLibs) {
            entries.add(libs.toAbsolutePath() + File.separator + "*");
        }
        return String.join(File.pathSeparator, entries);
    }

    /**
     * Starts the server in a JVM of its own.
     *
     * @param classpath class path of the JVM
     * @param dataDir data directory
     * @param port Flight port
     * @param log file receiving stdout and stderr
     * @return the server process
     * @throws IOException when the process cannot start
     */
    private static Process start(String classpath, Path dataDir, int port, Path log) throws IOException {
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        ProcessBuilder builder = new ProcessBuilder(java.toString(),
                // the documented alias must beat the bundled file's metricsEnabled=true
                "-Darrowflight.metrics.enabled=false",
                "-Dhazelcast.phone.home.enabled=false",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-cp", classpath,
                HadoopArrowFlightServer.class.getName(),
                "--data-dir", dataDir.toString(),
                "--localhost", "127.0.0.1",
                "--hosts", "127.0.0.1",
                "--port", String.valueOf(port),
                "--hazelcast-port", String.valueOf(freePort()));
        builder.environment().keySet().removeIf(k -> k.startsWith("FLIGHT_") || k.startsWith("DUCKDB_")
                || k.startsWith("HADOOP_") || k.equals("CLASSPATH"));
        builder.environment().put("FLIGHT_ALLOW_INSECURE", "true");
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        return builder.start();
    }

    /**
     * Waits until the server log says the Flight port is open.
     *
     * @param server the server process
     * @param log its log file
     * @throws Exception when the server exits or does not start in time
     */
    private static void awaitStart(Process server, Path log) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (System.nanoTime() < deadline) {
            String out = Files.readString(log, StandardCharsets.UTF_8);
            if (out.contains(STARTED)) {
                return;
            }
            if (!server.isAlive()) {
                fail("server exited with " + server.exitValue() + ":\n" + out);
            }
            Thread.sleep(200);
        }
        fail("server did not start in 120 s:\n" + Files.readString(log, StandardCharsets.UTF_8));
    }

    /**
     * Stops a server process.
     *
     * @param server the process
     * @throws InterruptedException when interrupted while waiting
     */
    private static void stop(Process server) throws InterruptedException {
        server.destroy();
        if (!server.waitFor(30, TimeUnit.SECONDS)) {
            server.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Picks a free TCP port.
     *
     * @return port number
     * @throws IOException when no port can be bound
     */
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Asserts that a library of the set is at least the given version.
     *
     * @param jars artifact id to version
     * @param artifact artifact id
     * @param want lowest version numbers
     */
    private static void assertAtLeast(Map<String, String> jars, String artifact, int... want) {
        String version = jars.get(artifact);
        assertTrue(version != null, artifact + " missing: " + jars.keySet());
        assertTrue(atLeast(version, want), artifact + " " + version + " is below the floor");
    }

    /**
     * Tells whether a dotted version is at least the given one.
     *
     * @param version version text such as 9.4.58.v20250814
     * @param want lowest version numbers, most significant first
     * @return true when the version is not below the given numbers
     */
    private static boolean atLeast(String version, int... want) {
        String[] parts = version.split("[.-]");
        for (int i = 0; i < want.length; i++) {
            int have = i < parts.length && parts[i].matches("\\d+") ? Integer.parseInt(parts[i]) : 0;
            if (have != want[i]) {
                return have > want[i];
            }
        }
        return true;
    }
}
