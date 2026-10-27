package pro.surpin.data.arrowflight.server.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.plugin.s3.FakeS3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Measures how DuckDB's httpfs reads Parquet from S3 when every request costs a round trip:
 * wall time and requests of a full scan, a point query and a filtered scan for DuckDB thread
 * counts and read settings (the s3 plugin's defaults first), against the fake S3 with a fixed
 * latency per object request ({@code -Ds3.perf.latencyMs}, default 10). Prints a table; the
 * asserts only check the answers. Needs the httpfs extension ({@code -Ds3.it.httpfs} or
 * {@code DUCKDB_HTTPFS_EXTENSION}).
 */
@Tag("perf")
class S3ReadPathPerfTest {

    private static final int FILES = 8;
    private static final int ROW_GROUPS = 4;
    private static final int ROWS_PER_GROUP = 100_000;

    @TempDir
    Path dir;

    private String httpfs;
    private FakeS3 s3;
    private String files;

    @BeforeEach
    void setUp() throws Exception {
        httpfs = System.getProperty("s3.it.httpfs", System.getenv("DUCKDB_HTTPFS_EXTENSION"));
        Assumptions.assumeTrue(httpfs != null && Files.isReadable(Path.of(httpfs)),
                "httpfs extension not configured (-Ds3.it.httpfs or DUCKDB_HTTPFS_EXTENSION)");
        s3 = FakeS3.start();
        Path data = dir.resolve("wide.parquet");
        StringBuilder list = new StringBuilder();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            for (int i = 0; i < FILES; i++) {
                long first = (long) i * ROW_GROUPS * ROWS_PER_GROUP;
                s.execute("COPY (SELECT " + first + " + range AS id, range % 1000 AS k, range * 0.5 AS v,"
                        + " md5(range::VARCHAR) AS pad FROM range(" + ROW_GROUPS * ROWS_PER_GROUP
                        + ")) TO '" + data + "' (FORMAT parquet, ROW_GROUP_SIZE " + ROWS_PER_GROUP + ")");
                s3.put("lake", "perf/w/x" + i + ".parquet", Files.readAllBytes(data));
                list.append(i == 0 ? "" : ", ").append("'s3://lake/perf/w/x").append(i).append(".parquet'");
            }
        }
        files = "read_parquet([" + list + "])";
        s3.latency(Long.getLong("s3.perf.latencyMs", 10));
    }

    @AfterEach
    void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    /**
     * Prints time and requests per query for each thread count and read setting.
     *
     * @throws Exception when DuckDB fails
     */
    @Test
    void readPathAtLatency() throws Exception {
        long rows = (long) FILES * ROW_GROUPS * ROWS_PER_GROUP;
        String scan = "SELECT count(*), sum(k), sum(v), count(DISTINCT pad) FROM " + files;
        String point = "SELECT count(*), sum(k) FROM " + files + " WHERE id = " + (rows / 2 + 17);
        String narrow = "SELECT count(*), sum(k) FROM " + files + " WHERE k = 7";
        // prefetch_all_parquet_files changes nothing on S3 (remote files are prefetched anyway).
        List<String> variants = List.of("", "SET enable_http_metadata_cache = false",
                "SET parquet_metadata_cache = false", "SET enable_external_file_cache = true");
        List<String> report = new ArrayList<>();
        for (int threads : new int[] {1, 4, 8}) {
            for (String variant : variants) {
                try (Connection conn = open(threads, variant)) {
                    for (String[] q : new String[][] {{"scan", scan}, {"point", point}, {"narrow", narrow}}) {
                        // First run reads footers too; the second shows the steady state.
                        for (int run = 0; run < 2; run++) {
                            s3.clearRequests();
                            long start = System.nanoTime();
                            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(q[1])) {
                                assertTrue(rs.next());
                                long count = rs.getLong(1);
                                switch (q[0]) {
                                    case "scan" -> assertEquals(rows, count);
                                    case "point" -> assertEquals(1, count);
                                    default -> assertEquals(rows / 1000, count);
                                }
                            }
                            long ms = (System.nanoTime() - start) / 1_000_000;
                            List<FakeS3.Request> requests = s3.requests();
                            long gets = requests.stream().filter(r -> r.method().equals("GET")).count();
                            long heads = requests.stream().filter(r -> r.method().equals("HEAD")).count();
                            report.add(String.format(Locale.ROOT,
                                    "threads=%2d %-40s %-6s run=%d %6d ms GET=%3d HEAD=%3d", threads,
                                    variant.isEmpty() ? "plugin defaults" : variant, q[0], run, ms, gets, heads));
                        }
                    }
                }
            }
        }
        report.forEach(System.out::println);
    }

    /**
     * Opens a DuckDB connection set up the way the s3 plugin sets up a server connection.
     *
     * @param threads DuckDB threads
     * @param variant statement run last, empty for none
     * @return connection
     * @throws Exception when DuckDB fails
     */
    private Connection open(int threads, String variant) throws Exception {
        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);
        try (Statement s = conn.createStatement()) {
            s.execute("LOAD '" + httpfs + "'");
            s.execute("SET threads = " + threads);
            s.execute("SET enable_external_file_cache = false");
            s.execute("SET httpfs_connection_caching = true");
            // The s3 plugin's defaults (metadataCache=true).
            s.execute("SET parquet_metadata_cache = true");
            s.execute("SET enable_http_metadata_cache = true");
            s.execute("CREATE SECRET perf (TYPE s3, KEY_ID 'k', SECRET 's', REGION 'us-east-1', ENDPOINT '"
                    + s3.endpoint().replace("http://", "") + "', URL_STYLE 'path', USE_SSL false)");
            if (!variant.isEmpty()) {
                s.execute(variant);
            }
        }
        return conn;
    }
}
