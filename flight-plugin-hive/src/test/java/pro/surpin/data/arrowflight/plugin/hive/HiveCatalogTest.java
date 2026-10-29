package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * The hive catalog against a real metastore (Hive 3.1.3 on embedded Derby in this JVM) over
 * tables in a local directory: names, types, skipped and hidden tables, partitions with their
 * values from the metastore (custom locations, special characters, NULL), file listing rules,
 * caching and pinning, a metastore outage, and the framed transport. The metastore runs on
 * libthrift 0.9.3 in its own class loader, the plugin on its newer libthrift: Hive 3.1.3 servers
 * in the field speak to this client the same way.
 */
class HiveCatalogTest {

    @TempDir
    static Path dir;
    static IsolatedHms hms;
    static String root;

    @BeforeAll
    static void start() throws Exception {
        hms = IsolatedHms.shared();
        root = dir.resolve("lake").toString();
        Files.createDirectories(dir.resolve("lake"));
        hms.createDatabase("hvt");
        // Unpartitioned, all types; Hive-style file names without extension, hidden files.
        Path types = Files.createDirectories(dir.resolve("lake/types"));
        parquet(types.resolve("000000_0"), "SELECT 1::TINYINT AS ti, 2::SMALLINT AS si, 3 AS i,"
                + " 4::BIGINT AS bi, true AS b, 1.5::FLOAT AS f, 2.5::DOUBLE AS d,"
                + " 12.34::DECIMAL(12,2) AS dec, 'x' AS s, 'v' AS vc, 'c' AS ch, DATE '2024-01-02' AS dt,"
                + " TIMESTAMP '2024-01-02 03:04:05' AS ts, 'ab'::BLOB AS bin, [1, 2] AS arr,"
                + " MAP {'k': 1} AS m, {'a': 1, 'b': 'z'} AS st");
        Files.writeString(types.resolve("_SUCCESS"), "");
        Files.writeString(types.resolve(".000000_0.crc"), "crc");
        Files.createFile(types.resolve("empty_file"));
        hms.createTable("hvt", "types", "file:" + types, "ti tinyint;si smallint;i int;bi bigint;"
                + "b boolean;f float;d double;dec decimal(12,2);s string;vc varchar(10);ch char(3);"
                + "dt date;ts timestamp;bin binary;arr array<int>;m map<string,int>;"
                + "st struct<a:int,b:string>", "", "parquet", null);
        // Partitioned: k=v directories, a custom location, special characters, NULL.
        Path parted = Files.createDirectories(dir.resolve("lake/parted"));
        hms.createTable("hvt", "parted", "file:" + parted, "id int;name string",
                "region string;day int", "parquet", null);
        String air = hms.addPartition("hvt", "parted", List.of("AIR", "1"), null);
        parquet(Path.of(air.substring("file:".length())).resolve("part-0.parquet"),
                "SELECT 1 AS id, 'a' AS name");
        Path custom = Files.createDirectories(dir.resolve("lake/elsewhere/mail-files"));
        hms.addPartition("hvt", "parted", List.of("MAIL", "2"), "file:" + custom);
        parquet(custom.resolve("f.parquet"), "SELECT 2 AS id, 'b' AS name");
        String special = hms.addPartition("hvt", "parted", List.of("a b/c:d=%'é", "3"), null);
        assertTrue(special.contains("%2F"), special);
        parquet(Path.of(special.substring("file:".length())).resolve("x.parquet"),
                "SELECT 3 AS id, 'c' AS name");
        String nulls = hms.addPartition("hvt", "parted", List.of("__HIVE_DEFAULT_PARTITION__", "4"),
                null);
        parquet(Path.of(nulls.substring("file:".length())).resolve("n.parquet"),
                "SELECT 4 AS id, 'd' AS name");
        // A directory with k=v name that is not a partition must not be read.
        parquet(Files.createDirectories(parted.resolve("region=SHIP/day=9")).resolve("g.parquet"),
                "SELECT 9 AS id, 'ghost' AS name");
        // Skipped and hidden tables.
        hms.createTable("hvt", "csv", "file:" + dir.resolve("lake/csv"), "a int", "", "text", null);
        hms.createTable("hvt", "orc", "file:" + dir.resolve("lake/orc"), "a int", "", "orc", null);
        hms.createTable("hvt", "acid", "file:" + dir.resolve("lake/acid"), "a int", "", "parquet",
                Map.of("transactional", "true", "transactional_properties", "insert_only"));
        hms.createTable("hvt", "uni", "file:" + dir.resolve("lake/uni"), "a uniontype<int,string>",
                "", "parquet", null);
        hms.createView("hvt", "v", "SELECT 1");
        Path secret = Files.createDirectories(dir.resolve("outside/secret"));
        parquet(secret.resolve("s.parquet"), "SELECT 42 AS a");
        hms.createTable("hvt", "secret", "file:" + secret, "a int", "", "parquet", null);
        Path half = Files.createDirectories(dir.resolve("lake/half"));
        hms.createTable("hvt", "half", "file:" + half, "a int", "k string", "parquet", null);
        hms.addPartition("hvt", "half", List.of("in"), null);
        hms.addPartition("hvt", "half", List.of("out"), "file:" + secret);
    }

    @Test
    void namesTypesAndSkippedTables() throws Exception {
        try (Catalog catalog = open(Map.of("schemaPrefix", "h_"))) {
            assertTrue(catalog.schemas().contains("h_hvt"), catalog.schemas().toString());
            List<String> tables = catalog.tables("h_hvt");
            assertTrue(tables.containsAll(List.of("parted", "types")), tables.toString());
            for (String skipped : List.of("csv", "orc", "acid", "uni", "v", "secret", "half")) {
                assertFalse(tables.contains(skipped), skipped + " in " + tables);
            }
            TableSnapshot types = catalog.current(new TableName("h_hvt", "types")).orElseThrow();
            List<String> declared = new ArrayList<>();
            for (ColumnSpec column : types.columns()) {
                declared.add(column.name() + " " + column.duckDbType());
            }
            // The Hive types travel along (SPI 1.8), so that CHAR(n) is told from string.
            assertEquals(List.of("string", "varchar(10)", "char(3)"), types.columns().stream()
                    .filter(column -> column.duckDbType().equals("VARCHAR")).map(ColumnSpec::sourceType).toList());
            assertEquals(List.of("ti TINYINT", "si SMALLINT", "i INTEGER", "bi BIGINT", "b BOOLEAN",
                    "f FLOAT", "d DOUBLE", "dec DECIMAL(12,2)", "s VARCHAR", "vc VARCHAR",
                    "ch VARCHAR", "dt DATE", "ts TIMESTAMP", "bin BLOB", "arr INTEGER[]",
                    "m MAP(VARCHAR, INTEGER)", "st STRUCT(\"a\" INTEGER, \"b\" VARCHAR)"), declared);
            assertEquals(List.of(), types.partitionColumns());
            assertEquals(new ReadSpec.ParquetFiles(false), types.read());
            // Only the visible, non-empty data file (no extension): no _SUCCESS, .crc, empty file.
            assertEquals(1, types.files().size(), types.files().toString());
            assertTrue(types.files().get(0).location().endsWith("/types/000000_0"));
            assertEquals(Optional.empty(), catalog.current(new TableName("h_hvt", "secret")));
            assertEquals(Optional.empty(), catalog.current(new TableName("h_hvt", "csv")));
            assertEquals(Optional.empty(), catalog.current(new TableName("h_hvt", "v")));
            assertEquals(List.of(root), catalog.readableRoots());
        }
    }

    @Test
    void partitionsComeFromTheMetastoreWithTheirValues() throws Exception {
        try (Catalog catalog = open(Map.of())) {
            TableSnapshot parted = catalog.current(new TableName("hvt", "parted")).orElseThrow();
            assertEquals(List.of("region", "day"), parted.partitionColumns());
            assertEquals("region", parted.columns().get(2).name());
            assertEquals("INTEGER", parted.columns().get(3).duckDbType());
            Map<String, Map<String, String>> byFile = new LinkedHashMap<>();
            for (TableFile file : parted.files()) {
                byFile.put(Path.of(file.location()).getFileName().toString(), file.partitionValues());
            }
            Map<String, String> nullRegion = new HashMap<>();
            nullRegion.put("region", null);
            nullRegion.put("day", "4");
            assertEquals(Map.of("region", "AIR", "day", "1"), byFile.get("part-0.parquet"));
            assertEquals(Map.of("region", "MAIL", "day", "2"), byFile.get("f.parquet"));
            assertEquals(Map.of("region", "a b/c:d=%'é", "day", "3"), byFile.get("x.parquet"));
            assertEquals(nullRegion, byFile.get("n.parquet"));
            assertFalse(byFile.containsKey("g.parquet"), "a directory that is no partition");
            assertEquals(4, parted.files().size());
            assertTrue(parted.snapshotId().startsWith("h"));
        }
    }

    @Test
    void aPartitionOutsideTheRootsHidesTheWholeTable() throws Exception {
        try (Catalog catalog = open(Map.of())) {
            assertFalse(catalog.tables("hvt").contains("half"), "hidden before any query");
            assertEquals(Optional.empty(), catalog.current(new TableName("hvt", "half")));
            assertFalse(catalog.tables("hvt").contains("half"));
        }
        try (Catalog catalog = open(Map.of("namesTtlSeconds", "0"))) {
            assertEquals(Optional.empty(), catalog.current(new TableName("hvt", "half")));
            assertFalse(catalog.tables("hvt").contains("half"), "still hidden once the mark expired");
            assertTrue(catalog.tables("hvt").contains("types"));
        }
    }

    @Test
    void snapshotsAreCachedAndPinnedByColumns() throws Exception {
        Path t = Files.createDirectories(dir.resolve("lake/grow"));
        hms.createTable("hvt", "grow", "file:" + t, "a int", "p int", "parquet", null);
        String first = hms.addPartition("hvt", "grow", List.of("1"), null);
        parquet(Path.of(first.substring(5)).resolve("a.parquet"), "SELECT 1 AS a");
        TableName name = new TableName("hvt", "grow");
        try (Catalog cached = open(Map.of("metadataTtlSeconds", "3600"));
                Catalog fresh = open(Map.of("metadataTtlSeconds", "0", "namesTtlSeconds", "0"))) {
            TableSnapshot before = fresh.current(name).orElseThrow();
            assertEquals(1, cached.current(name).orElseThrow().files().size());
            String second = hms.addPartition("hvt", "grow", List.of("2"), null);
            parquet(Path.of(second.substring(5)).resolve("b.parquet"), "SELECT 2 AS a");
            TableSnapshot after = fresh.current(name).orElseThrow();
            assertEquals(2, after.files().size());
            assertNotEquals(before.fingerprint(), after.fingerprint());
            assertEquals(before.snapshotId(), after.snapshotId(), "same columns, same snapshot ID");
            assertEquals(1, cached.current(name).orElseThrow().files().size(), "within the TTL");
            TableSnapshot pinned = fresh.pinnedColumns(name, before.snapshotId()).orElseThrow();
            assertEquals(before.columns(), pinned.columns());
            assertEquals(List.of(), pinned.files());
            assertEquals(Optional.empty(), fresh.pinnedColumns(name, "h0000000000000000"));
        }
    }

    @Test
    void anOutageIsAnErrorNotAnEmptyAnswer() throws Exception {
        IsolatedHms.Proxy proxy = hms.proxy();
        Catalog catalog = open(Map.of("uris", proxy.uri(), "metadataTtlSeconds", "0",
                "namesTtlSeconds", "0", "connectTimeoutMillis", "2000", "socketTimeoutMillis", "5000"));
        try {
            assertTrue(catalog.tables("hvt").contains("types"));
            proxy.close();
            IOException schemas = assertThrows(IOException.class, catalog::schemas);
            assertTrue(schemas.getMessage().contains("Hive Metastore unavailable"), schemas.getMessage());
            assertThrows(IOException.class, () -> catalog.tables("hvt"));
            IOException fast = assertThrows(IOException.class,
                    () -> catalog.current(new TableName("hvt", "types")));
            assertTrue(fast.getMessage().contains("not retried"), "fails at once: " + fast.getMessage());
        } finally {
            catalog.close();
        }
        IOException dead = assertThrows(IOException.class, () -> open(Map.of("uris",
                "thrift://127.0.0.1:1", "connectTimeoutMillis", "1000")));
        assertTrue(dead.getMessage().contains("thrift://127.0.0.1:1"), dead.getMessage());
    }

    @Test
    void theFramedTransportReachesTheMetastore() throws Exception {
        TableName name = new TableName("hvt", "parted");
        try (IsolatedHms.Proxy framed = hms.framedProxy();
                Catalog catalog = open(Map.of("uris", framed.uri(), "framedTransport", "true",
                        "metadataTtlSeconds", "0", "namesTtlSeconds", "0"));
                Catalog plain = open(Map.of())) {
            assertTrue(catalog.tables("hvt").contains("types"));
            TableSnapshot parted = catalog.current(name).orElseThrow();
            TableSnapshot expected = plain.current(name).orElseThrow();
            assertEquals(4, parted.files().size(), parted.files().toString());
            assertEquals(expected.columns(), parted.columns());
            assertEquals(expected.files(), parted.files());
        }
    }

    @Test
    void thriftIsAtTheFloor() {
        String version = org.apache.thrift.TBase.class.getPackage().getImplementationVersion();
        assertTrue(atLeast(version, 0, 24), "libthrift " + version
                + " of the plugin is below 0.24 (docs/dev/DEPENDENCY-AUDIT.md)");
        assertFalse(atLeast("0.9.3", 0, 24));
        assertTrue(atLeast("1.0.0", 0, 24));
        assertEquals(1 << 30, ThriftMetastore.thriftConfiguration().getMaxMessageSize());
        assertTrue(ThriftMetastore.thriftConfiguration().getMaxFrameSize() >= 16_384_000);
    }

    @Test
    void theFixtureRunsTheOldThrift() throws Exception {
        Class<?> base = Class.forName("org.apache.thrift.TBase", false, IsolatedHms.loader());
        assertEquals("0.9.3", base.getPackage().getImplementationVersion());
    }

    @Test
    void settingsAreChecked() {
        assertThrows(IllegalArgumentException.class, () -> open(Map.of("uris", "")));
        assertThrows(IllegalArgumentException.class, () -> open(Map.of("roots", "")));
        assertThrows(IllegalArgumentException.class, () -> open(Map.of("uris", "http://x:1")));
        IllegalArgumentException kerberos = assertThrows(IllegalArgumentException.class,
                () -> open(Map.of("authentication", "kerberos")));
        assertTrue(kerberos.getMessage().contains("metastorePrincipal is required"),
                kerberos.getMessage());
        assertThrows(IllegalArgumentException.class, () -> open(Map.of("icebergTables", "require")));
    }

    @Test
    void hiveTypesMapToDuckDb() {
        assertEquals("DECIMAL(10,0)", HiveTypes.duckDb("decimal"));
        assertEquals("DECIMAL(38,18)", HiveTypes.duckDb("DECIMAL(38, 18)"));
        assertEquals("TIMESTAMP WITH TIME ZONE", HiveTypes.duckDb("timestamp with local time zone"));
        assertEquals("STRUCT(\"a b\" INTEGER, \"c\" MAP(VARCHAR, DOUBLE[]))[]",
                HiveTypes.duckDb("array<struct<`a b`:int,c:map<string,array<double>>>>"));
        assertThrows(IllegalArgumentException.class, () -> HiveTypes.duckDb("uniontype<int,string>"));
        assertThrows(IllegalArgumentException.class, () -> HiveTypes.duckDb("decimal(40,2)"));
        assertThrows(IllegalArgumentException.class, () -> HiveTypes.duckDb("array<int"));
    }

    @Test
    void locationsAreNormalisedAndChecked() {
        assertEquals("s3://b/w", HiveLocations.normalize("s3a://b/w/"));
        assertEquals("/a/b", HiveLocations.normalize("file:/a/b"));
        assertEquals("/a/b", HiveLocations.normalize("file:///a/b"));
        assertTrue(HiveLocations.underAny("/a/b/c", List.of("/a/b")));
        assertFalse(HiveLocations.underAny("/a/bc", List.of("/a/b")));
        assertFalse(HiveLocations.underAny("/a/b/../c", List.of("/a/b")));
        assertNull(HiveTable.classify(new org.apache.hadoop.hive.metastore.api.Table(), List.of("/a"),
                false, false, false, false).table());
    }

    private static boolean atLeast(String version, int major, int minor) {
        String[] parts = version.split("\\.");
        int ma = Integer.parseInt(parts[0]);
        int mi = Integer.parseInt(parts[1]);
        return ma != major ? ma > major : mi >= minor;
    }

    private static Catalog open(Map<String, String> overrides) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("uris", hms.uri());
        values.put("roots", root);
        values.putAll(overrides);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new HiveCatalogProvider().open(new TestSupport.Context(
                new TestSupport.MapConfig("hive", values), stores, new SecretRedactor(), null));
        stores.freeze();
        return catalog;
    }

    private static void parquet(Path file, String select) throws Exception {
        Files.createDirectories(file.getParent());
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (" + select + ") TO '" + file.toString().replace("'", "''")
                    + "' (FORMAT parquet)");
        }
    }
}
