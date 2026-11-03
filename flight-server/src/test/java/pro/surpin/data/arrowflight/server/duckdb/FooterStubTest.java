package pro.surpin.data.arrowflight.server.duckdb;

import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests footer stubs: local copies of a Parquet footer without row groups. */
@Tag("unit")
class FooterStubTest {

    /** Verifies DuckDB describes a stub exactly like the stored file, partition columns included. */
    @Test
    void stubDescribesLikeTheFile(@TempDir Path dir) throws Exception {
        Path table = Files.createDirectories(dir.resolve("data/tpch/t/day=2024-01-02"));
        Path file = table.resolve("f.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1::UTINYINT AS u, 12.5::DECIMAL(18,3) AS d, 'x' AS v,"
                    + " TIMESTAMP '2024-01-02 03:04:05' AS ts, TIMESTAMPTZ '2024-01-02 03:04:05+00' AS tz,"
                    + " DATE '2024-01-02' AS dt, [1, 2] AS l, {'a': 1, 'b': 'c'} AS st,"
                    + " MAP {'k': 1} AS m, 'b'::BLOB AS bl, 1.5::FLOAT AS f, 2::HUGEINT AS h,"
                    + " gen_random_uuid() AS id FROM range(3)) TO '" + file + "' (FORMAT parquet)");
            Path root = dir.resolve("stubs");
            try (FooterStub stub = FooterStub.write(new LocalInputFile(file), root,
                    "hdfs://ns/data/tpch/t/day=2024-01-02/f.parquet")) {
                assertTrue(stub.file().startsWith(root));
                assertTrue(stub.file().toString().endsWith("data/tpch/t/day=2024-01-02/f.parquet"));
                assertTrue(Files.size(stub.file()) < Files.size(file));
                assertEquals(describe(s, file.toString()), describe(s, stub.file().toString()));
                assertTrue(describe(s, stub.file().toString()).contains("day DATE"),
                        describe(s, stub.file().toString()).toString());
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM read_parquet('" + stub.file() + "')")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getLong(1));
                }
            }
            try (java.util.stream.Stream<Path> left = Files.list(root)) {
                assertEquals(0, left.count(), "close removes the stub");
            }
        }
    }

    /** Verifies files that are not readable Parquet files fail with the location named. */
    @Test
    void refusesFilesThatAreNotParquet(@TempDir Path dir) throws Exception {
        Path text = dir.resolve("t.parquet");
        Files.writeString(text, "this is not a parquet file at all");
        IOException e = assertThrows(IOException.class, () -> FooterStub.write(new LocalInputFile(text),
                dir.resolve("stubs"), "hdfs://ns/d/t.parquet"));
        assertTrue(e.getMessage().contains("hdfs://ns/d/t.parquet"), e.getMessage());
        Path tiny = dir.resolve("tiny.parquet");
        Files.write(tiny, new byte[] {'P', 'A', 'R', '1'});
        assertThrows(IOException.class, () -> FooterStub.write(new LocalInputFile(tiny),
                dir.resolve("stubs"), "hdfs://ns/d/tiny.parquet"));
        Path badLength = dir.resolve("bad.parquet");
        Files.write(badLength, new byte[] {'P', 'A', 'R', '1', 0, 0, 0, 0, 1, 2, 3, 0x7f, 'P', 'A', 'R', '1'});
        assertThrows(IOException.class, () -> FooterStub.write(new LocalInputFile(badLength),
                dir.resolve("stubs"), "hdfs://ns/d/bad.parquet"));
        Path encrypted = dir.resolve("enc.parquet");
        Files.write(encrypted, new byte[] {'P', 'A', 'R', 'E', 1, 1, 1, 1, 4, 0, 0, 0, 'P', 'A', 'R', 'E'});
        IOException enc = assertThrows(IOException.class, () -> FooterStub.write(
                new LocalInputFile(encrypted), dir.resolve("stubs"), "hdfs://ns/d/enc.parquet"));
        assertTrue(enc.getMessage().contains("encrypted"), enc.getMessage());
        assertFalse(Files.exists(dir.resolve("stubs")) && hasFiles(dir.resolve("stubs")));
    }

    /** Verifies a location that would leave the stub directory is refused. */
    @Test
    void refusesLocationsOutsideTheStubDirectory(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("f.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1 AS x) TO '" + file + "' (FORMAT parquet)");
        }
        assertThrows(IOException.class, () -> FooterStub.write(new LocalInputFile(file),
                dir.resolve("stubs"), "hdfs://ns/../../escape.parquet"));
        assertThrows(IOException.class, () -> FooterStub.write(new LocalInputFile(file),
                dir.resolve("stubs"), "hdfs://ns"));
    }

    /** Verifies scheme and authority are dropped from locations. */
    @Test
    void pathBelowAuthority() {
        assertEquals("data/t/f.parquet", FooterStub.pathBelowAuthority("hdfs://ns/data/t/f.parquet"));
        assertEquals("data/t/f.parquet", FooterStub.pathBelowAuthority("hdfs://nn:8020//data/t/f.parquet"));
        assertEquals("data/t/f.parquet", FooterStub.pathBelowAuthority("/data/t/f.parquet"));
        assertEquals("", FooterStub.pathBelowAuthority("hdfs://ns"));
    }

    /**
     * Lists DuckDB's column descriptions of one file.
     *
     * @param s statement
     * @param path file
     * @return "name type" per column
     * @throws Exception on DuckDB failure
     */
    private static List<String> describe(Statement s, String path) throws Exception {
        List<String> columns = new ArrayList<>();
        try (ResultSet rs = s.executeQuery("DESCRIBE SELECT * FROM read_parquet('" + path + "')")) {
            while (rs.next()) {
                columns.add(rs.getString("column_name") + " " + rs.getString("column_type"));
            }
        }
        return columns;
    }

    /**
     * Tells whether a directory tree holds any regular file.
     *
     * @param dir directory
     * @return true when a file is left
     * @throws IOException on listing failure
     */
    private static boolean hasFiles(Path dir) throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.walk(dir)) {
            return paths.anyMatch(Files::isRegularFile);
        }
    }
}
