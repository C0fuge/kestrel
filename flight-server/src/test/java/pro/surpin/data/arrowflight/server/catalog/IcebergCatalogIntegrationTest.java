package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;

/**
 * Runs the server with the iceberg plugin over Iceberg tables written by Iceberg Java: an
 * iceberg-hadoop catalog (warehouse directory) and an iceberg-rest catalog (fake REST server over
 * a second warehouse), next to a Parquet directory table, under the DuckDB lockdown with the real
 * iceberg/avro extensions v1.5.6 ({@code -Diceberg.it.extensionDir}; skipped without it).
 * Complete answers are compared with DuckDB reading the same metadata directly; partial answers
 * with complete ones (also on a snapshot with deletes, file by file); snapshots with nested
 * changes, and joins, are answered completely; a planned snapshot stays pinned after a commit;
 * time travel reads an earlier snapshot.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IcebergCatalogIntegrationTest {

    private static final List<String> TABLES = List.of("types", "evolved", "parted", "deletes",
            "nested", "empty");

    @TempDir
    static Path dir;
    static String extensionDir;
    static String warehouse;
    static String restWarehouse;
    static Map<String, String> metadata;
    static Map<String, String> restMetadata;
    static AutoCloseable rest;
    static TestFlightServerHelper helper;

    @BeforeAll
    static void start() throws Exception {
        extensionDir = IcebergTestTables.extensionDir();
        Assumptions.assumeTrue(extensionDir != null,
                "needs -Diceberg.it.extensionDir with v1.5.6/linux_amd64/{iceberg,avro}.duckdb_extension");
        Assumptions.assumeTrue(IcebergTestTables.available(), "flight-plugin-iceberg is not built");
        warehouse = dir.resolve("wh").toString();
        restWarehouse = dir.resolve("wh-rest").toString();
        metadata = IcebergTestTables.write(warehouse);
        // The REST catalog's tables name their files file:/... in metadata and manifests, as
        // Spark and Gravitino write local tables; the roots stay plain paths.
        restMetadata = IcebergTestTables.write("file:" + restWarehouse);
        rest = IcebergTestTables.rest("file:" + restWarehouse);
        Path data = Files.createDirectories(dir.resolve("data/dirs/t"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range AS id, 'r' || range AS name FROM range(3)) TO '"
                    + data.resolve("part-0.parquet") + "' (FORMAT parquet)");
        }
        Path plugins = dir.resolve("plugins");
        IcebergTestTables.installPlugin(plugins);
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.duckdb.extensionDir", extensionDir);
        sys.put("arrowflight.iceberg-hadoop.warehouse", warehouse);
        sys.put("arrowflight.iceberg-hadoop.metadataTtlMillis", "0");
        sys.put("arrowflight.iceberg-rest.uri", IcebergTestTables.uri(rest));
        sys.put("arrowflight.iceberg-rest.roots", restWarehouse);
        sys.put("arrowflight.iceberg-rest.schemaPrefix", "rest_");
        sys.put("arrowflight.iceberg-rest.metadataTtlMillis", "0");
        Map<String, String> env = Map.of("FLIGHT_ICEBERG_REST_CREDENTIAL", "client-1:secret-1");
        ConfigSource source = new ConfigSource(sys::get, new Properties(), env, null);
        PluginSettings settings = new PluginSettings(plugins, null,
                "directory,iceberg-hadoop,iceberg-rest", false);
        helper = TestFlightServerHelper.builder().dataDir(dir.resolve("data").toString())
                .resultMode("complete")
                .registry(config -> StoreRegistry.open(config, new Configuration(), "localhost",
                        settings, source, List.of()))
                .start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (helper != null) {
            helper.close();
        }
        if (rest != null) {
            rest.close();
        }
    }

    @Test
    @Order(1)
    void catalogSchemasAndTablesAppearNextToTheDataDirectory() throws Exception {
        List<String> schemas = column(helper.sqlClient().getSchemas(null, null), "db_schema_name");
        assertTrue(schemas.containsAll(List.of("dirs", "ice", "rest_ice")), schemas.toString());
        List<String> tables = column(helper.sqlClient().getTables(null, "ice", null, null, false),
                "table_name");
        assertEquals(TABLES.stream().sorted().toList(), tables.stream().sorted().toList());
        assertEquals(6, column(helper.sqlClient().getTables(null, "rest_ice", null, null, false),
                "table_name").size());
        Schema types = tableSchema("ice", "types");
        assertEquals(18, types.getFields().size(), types.toString());
        assertEquals("tstz", types.getFields().get(10).getName());
        assertEquals(List.of("id", "label", "amount", "note"),
                tableSchema("ice", "evolved").getFields().stream().map(f -> f.getName()).toList());
        assertEquals(2, tableSchema("dirs", "t").getFields().size());
    }

    @Test
    @Order(2)
    void completeAnswersEqualDuckDbReadingTheTablesDirectly() throws Exception {
        for (String table : TABLES) {
            assertEquals(direct(metadata.get(table)), fingerprint("ice." + table), "ice." + table);
            assertEquals(direct(restMetadata.get(table)), fingerprint("rest_ice." + table),
                    "rest_ice." + table);
        }
        assertEquals(List.of("1|n1", "3|n3", "5|n5", "6|n6", "7|n7"),
                answer("SELECT id, label FROM ice.deletes ORDER BY id", "complete").rows());
        assertEquals(List.of("1|n1|1.25|null", "5|l5|10000005.75|note 5"),
                answer("SELECT * FROM ice.evolved WHERE id IN (1, 5) ORDER BY id", "complete").rows());
        assertEquals(List.of("0|r0|2|n2"), answer("SELECT d.id, d.name, i.id, i.label FROM dirs.t d"
                + " JOIN ice.evolved i ON i.id = d.id + 2 WHERE d.id = 0", "complete").rows());
        // A short name is ambiguous when two schemas have the table (ice and rest_ice).
        FlightRuntimeException ambiguous = assertThrows(FlightRuntimeException.class,
                () -> answer("SELECT count(*) FROM parted", "complete"));
        assertTrue(ambiguous.getMessage().contains("parted"), ambiguous.getMessage());
    }

    @Test
    @Order(3)
    void partialAnswersEqualCompleteOnes() throws Exception {
        for (String schema : List.of("ice", "rest_ice")) {
            for (String sql : List.of(
                    "SELECT * FROM " + schema + ".types",
                    "SELECT * FROM " + schema + ".evolved",
                    "SELECT id, region, ts, v FROM " + schema + ".parted WHERE v > 0.4",
                    "SELECT count(*) AS n, sum(amount) AS s, max(label) AS m FROM " + schema + ".evolved",
                    "SELECT region, count(*) AS n FROM " + schema + ".parted GROUP BY region")) {
                Answer partial = answer(sql, "partial");
                Answer complete = answer(sql, "complete");
                assertEquals(complete.schema(), partial.schema(), sql);
                // One node: one endpoint, so even aggregates in parts are the whole answer.
                assertEquals(complete.rows(), partial.rows(), sql);
                assertTrue(partial.endpoints() >= 1, sql);
            }
        }
        // Partial plans of tables without deletes carry the snapshot's files, one endpoint here
        // (one node), with a location.
        FlightInfo info = info("SELECT * FROM ice.evolved", "partial");
        assertEquals(1, info.getEndpoints().size());
        assertFalse(info.getEndpoints().get(0).getLocations().isEmpty());
    }

    @Test
    @Order(4)
    void nestedChangesJoinsAndEmptySnapshotsAreAnsweredCompletely() throws Exception {
        for (String sql : List.of(
                "SELECT * FROM ice.nested",
                "SELECT * FROM ice.empty",
                "SELECT a.id, b.label FROM ice.evolved a JOIN ice.deletes b ON a.id = b.id")) {
            FlightInfo info = info(sql, "partial");
            assertEquals(1, info.getEndpoints().size(), sql);
            assertTrue(info.getEndpoints().get(0).getLocations().isEmpty(),
                    "complete-answer endpoint: " + sql);
            assertEquals(answer(sql, "complete").rows(), answer(sql, "partial").rows(), sql);
        }
        assertEquals(List.of("1|{\"a\":10,\"c\":\"x1\"}", "2|{\"a\":20,\"c\":\"x2\"}",
                "3|{\"a\":30,\"c\":\"x3\"}", "4|{\"a\":40,\"c\":\"x4\"}"),
                answer("SELECT * FROM ice.nested", "partial").rows());
    }

    /**
     * A snapshot with position and equality deletes (Iceberg Java: the equality delete is older
     * than the second data file, so it applies to the first only) is read in parts: the plan has
     * located endpoints and no row count, and every data file read alone by a ticket naming only
     * that file gives its own surviving rows; together they are the complete answer.
     */
    @Test
    @Order(5)
    void deletesAreReadInPartsFileByFile() throws Exception {
        for (String schema : List.of("ice", "rest_ice")) {
            String sql = "SELECT id, label FROM " + schema + ".deletes";
            FlightInfo info = info(sql, "partial");
            assertFalse(info.getEndpoints().get(0).getLocations().isEmpty(), "partial endpoint: " + sql);
            assertEquals(-1, info.getRecords(), sql);
            assertEquals(List.of("1|n1", "3|n3", "5|n5", "6|n6", "7|n7"), answer(sql, "partial").rows());
            assertEquals(answer(sql, "complete").rows(), answer(sql, "partial").rows(), sql);
            CatalogTables.Resolved resolved = helper.parquetAdapter.catalogTables()
                    .current(schema, "deletes").orElseThrow();
            List<TableFile> files = resolved.snapshot().files();
            assertEquals(2, files.size(), schema);
            List<String> parts = new ArrayList<>();
            for (TableFile file : files) {
                String key = new CatalogFileKey(resolved.snapshot().snapshotId(), file.location()).encode();
                RowsListener listener = new RowsListener();
                helper.executionService.readParquet(helper.allocator, sql, new String[] {key}, listener, true);
                List<String> part = listener.rows;
                part.sort(null);
                assertEquals(file.deletes().isEmpty() ? List.of("6|n6", "7|n7") : List.of("1|n1", "3|n3", "5|n5"),
                        part, schema + " " + file.location());
                parts.addAll(part);
            }
            parts.sort(null);
            assertEquals(List.of("1|n1", "3|n3", "5|n5", "6|n6", "7|n7"), parts, schema);
        }
    }

    @Test
    @Order(6)
    void lockdownStillRefusesOtherFiles() {
        for (String sql : List.of("SELECT * FROM read_text('/etc/hostname')",
                "SELECT * FROM iceberg_scan('/etc/hostname')",
                "SELECT * FROM read_parquet('" + dir.resolve("plugins") + "/x.parquet')")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> answer(sql, "complete"), sql);
            // Refused by the lockdown, or (iceberg_scan) not even known where client SQL is
            // described: clients read Iceberg tables only through the catalog's views.
            String message = String.valueOf(e.getMessage());
            assertTrue(message.contains("ermission") || message.contains("disabled")
                    || message.contains("iceberg_scan\" is not in the catalog"), message);
        }
    }

    @Test
    @Order(7)
    void ticketsNamingFilesOutsideTheSnapshotAreRefused() throws Exception {
        String key = new CatalogFileKey("1.0", warehouse + "/ice/types/data/f1.parquet").encode();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        Exception e = assertThrows(Exception.class, () -> helper.executionService.readParquet(
                helper.allocator, "SELECT * FROM ice.types", new String[] {key}, listener, true));
        assertTrue(e.getMessage().contains("no longer available"), e.getMessage());
        // The right snapshot, but a file of another table.
        String snapshotId = helper.parquetAdapter.catalogTables().current("ice", "types")
                .orElseThrow().snapshot().snapshotId();
        String foreign = new CatalogFileKey(snapshotId, warehouse + "/ice/parted/data/x.parquet").encode();
        Exception e2 = assertThrows(IllegalArgumentException.class,
                () -> helper.executionService.readParquet(helper.allocator,
                        "SELECT * FROM ice.types", new String[] {foreign}, listener, true));
        assertTrue(e2.getMessage().contains("is not a data file of snapshot"), e2.getMessage());
    }

    /**
     * DuckDB 1.4.5's iceberg_scan applies equality deletes only when their columns are read:
     * without them "Equality deletes need the relevant columns to be selected", with them only
     * in a filter an INTERNAL error (Gravitino stand). The views keep those columns read.
     */
    @Test
    @Order(50)
    void equalityDeletesWithoutTheirColumnsSelected() throws Exception {
        for (String mode : List.of("complete", "partial")) {
            for (String schema : List.of("ice", "rest_ice")) {
                String table = schema + ".deletes";
                assertEquals(List.of("5"), answer("SELECT count(*) FROM " + table, mode).rows(),
                        mode + " " + table);
                assertEquals(List.of("5|5"), answer("SELECT count(label), count(*) FROM " + table
                        + " WHERE id > 0", mode).rows(), mode + " " + table);
                assertEquals(List.of("n3"), answer("SELECT label FROM " + table + " WHERE id = 3",
                        mode).rows(), mode + " " + table);
                // Queries on other columns only: every column stays read (with the position
                // delete and the rename, DuckDB fails on a subset) and filters stay above the scan
                // (a filter pushed into it gave wrong counts on the HMS stand).
                assertEquals(List.of("2|2"), answer("SELECT count(id), count(*) FROM " + table
                        + " WHERE label IN ('n3', 'n4', 'n5')", mode).rows(), mode + " " + table);
                assertEquals(List.of(), answer("SELECT id FROM " + table + " WHERE label = 'n4'",
                        mode).rows(), mode + " " + table);
                assertEquals(List.of("4"), answer("SELECT count(*) FROM " + table
                        + " WHERE label <> 'n1'", mode).rows(), mode + " " + table);
                assertEquals(List.of("n7"), answer("SELECT max(label) FROM " + table, mode).rows(),
                        mode + " " + table);
            }
        }
    }

    /**
     * Time travel: FOR VERSION AS OF / FOR TIMESTAMP AS OF read the first snapshot of evolved
     * (ids 1-4, before the rename and the added column) with that snapshot's columns, in complete
     * and partial mode (answered completely), next to the current snapshot in one query; points
     * without a snapshot and tables without history are refused.
     */
    @Test
    @Order(50)
    void timeTravelReadsTheFirstSnapshotWithItsColumns() throws Exception {
        for (String schema : List.of("ice", "rest_ice")) {
            String meta = (schema.equals("ice") ? metadata : restMetadata).get("evolved");
            long[] first = firstSnapshot(meta);
            String table = schema + ".evolved";
            String version = table + " FOR VERSION AS OF " + first[0];
            String at = table + " FOR TIMESTAMP AS OF TIMESTAMP '"
                    + java.time.Instant.ofEpochMilli(first[1]) + "'";
            for (String mode : List.of("complete", "partial")) {
                Answer old = answer("SELECT * FROM " + version + " ORDER BY id", mode);
                assertEquals(List.of("id", "name", "amount"), old.schema().getFields().stream()
                        .map(f -> f.getName()).toList(), mode + " " + table);
                assertEquals(4, old.rows().size(), mode + " " + old.rows());
                assertTrue(old.rows().get(0).startsWith("1|n1|"), old.rows().toString());
                assertEquals(1, old.endpoints(), mode);
                assertEquals(List.of("2|n2"), answer("SELECT e.id, e.name FROM " + version
                        + " e WHERE e.id = 2", mode).rows(), mode);
                assertEquals(List.of("4"), answer("SELECT count(*) FROM " + at, mode).rows(), mode);
                assertEquals(List.of("4|7"), answer("SELECT count(o.id), count(c.id) FROM " + table
                        + " c LEFT JOIN " + version + " AS o ON o.id = c.id", mode).rows(), mode);
            }
            // Deletes of the snapshot read are applied; nested renames cannot be read file by file.
            String deletes = schema + ".deletes FOR VERSION AS OF " + snapshot((schema.equals("ice")
                    ? metadata : restMetadata).get("deletes"), "DESC")[0];
            assertEquals(List.of("1|n1", "3|n3", "5|n5", "6|n6", "7|n7"),
                    answer("SELECT id, label FROM " + deletes + " ORDER BY id", "complete").rows());
            String nested = schema + ".nested FOR VERSION AS OF " + snapshot((schema.equals("ice")
                    ? metadata : restMetadata).get("nested"), "DESC")[0];
            FlightRuntimeException unreadable = assertThrows(FlightRuntimeException.class,
                    () -> answer("SELECT * FROM " + nested, "complete"));
            assertTrue(unreadable.getMessage().contains("cannot be read by time travel"),
                    unreadable.getMessage());
            assertEquals(List.of("7"), answer("SELECT count(*) FROM " + table
                    + " FOR TIMESTAMP AS OF TIMESTAMP '2999-01-01 00:00:00 UTC'", "complete").rows());
            for (String[] refused : new String[][] {
                    {table + " FOR VERSION AS OF 12345", "has no snapshot 12345"},
                    {table + " FOR TIMESTAMP AS OF TIMESTAMP '2000-01-01 00:00:00 UTC'",
                            "no snapshot committed at or before"},
                    {table + " FOR TIMESTAMP AS OF TIMESTAMP '2000-01-01 00:00:00'",
                            "needs a time zone"},
                    {"dirs.t FOR VERSION AS OF 1", "only tables of an Iceberg catalog"},
                    {schema + ".nope FOR VERSION AS OF 1", "nope"}}) {
                for (String mode : List.of("complete", "partial")) {
                    FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                            () -> answer("SELECT * FROM " + refused[0], mode), refused[0]);
                    // A missing table is NOT_FOUND, as DuckDB's own catalog error.
                    assertEquals(refused[0].contains(".nope ")
                            ? org.apache.arrow.flight.FlightStatusCode.NOT_FOUND
                            : org.apache.arrow.flight.FlightStatusCode.INVALID_ARGUMENT,
                            e.status().code(), refused[0] + ": " + e);
                    assertTrue(e.getMessage().contains(refused[1]), refused[0] + ": " + e.getMessage());
                }
            }
        }
    }

    @Test
    @Order(100)
    void plannedSnapshotStaysPinnedAfterACommit() throws Exception {
        String sql = "SELECT id, label FROM ice.evolved";
        FlightInfo before = info(sql, "partial");
        assertEquals(7, read(before).size());
        IcebergTestTables.appendToEvolved(warehouse, 100);
        // The endpoints planned before the commit still read the snapshot they were planned on.
        assertEquals(7, read(before).size());
        FlightInfo after = info(sql, "partial");
        List<String> rows = read(after);
        assertEquals(9, rows.size());
        assertTrue(rows.contains("100|l100"), rows.toString());
    }

    /**
     * The REST catalog goes away: complete answers keep the views made before; a partial query
     * (it asks the catalog for the current snapshot) is refused UNAVAILABLE naming the catalog,
     * not "Could not find Arrow schema" / "null" (Gravitino stand). Runs last: it stops the REST
     * catalog.
     */
    @Test
    @Order(1000)
    void restCatalogOutageIsUnavailableWithItsCause() throws Exception {
        assertEquals(List.of("5"), answer("SELECT count(*) FROM rest_ice.deletes", "complete").rows());
        rest.close();
        rest = null;
        assertEquals(List.of("5"), answer("SELECT count(*) FROM rest_ice.deletes", "complete").rows());
        for (String sql : List.of("SELECT id FROM rest_ice.evolved", "SELECT count(*) FROM rest_ice.evolved")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class, () -> info(sql, "partial"));
            assertEquals(org.apache.arrow.flight.FlightStatusCode.UNAVAILABLE, e.status().code(), sql + ": " + e);
            assertTrue(e.getMessage().contains("Iceberg REST catalog unavailable: http://"), e.getMessage());
            assertFalse(e.getMessage().contains("secret-1"), e.getMessage());
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /** Collects the rows an execution streams, as text. */
    private static final class RowsListener implements FlightProducer.ServerStreamListener {
        private final List<String> rows = new ArrayList<>();
        private VectorSchemaRoot root;

        @Override
        public void start(VectorSchemaRoot r, org.apache.arrow.vector.dictionary.DictionaryProvider p,
                org.apache.arrow.vector.ipc.message.IpcOption o) {
            this.root = r;
        }

        @Override
        public void putNext() {
            rows.addAll(rows(root));
        }

        @Override
        public void putNext(org.apache.arrow.memory.ArrowBuf metadata) {
            rows.addAll(rows(root));
        }

        @Override
        public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) {
            // Metadata frames carry no rows.
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable handler) {
            // Always ready.
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
            // Never cancelled.
        }

        @Override
        public void error(Throwable ex) {
            throw new IllegalStateException(ex);
        }

        @Override
        public void completed() {
            // The rows are read after the call returns.
        }
    }

    /**
     * An answer: announced schema, rows as text and number of endpoints.
     *
     * @param schema announced schema
     * @param rows rows, columns separated by |
     * @param endpoints endpoints read
     */
    private record Answer(Schema schema, List<String> rows, int endpoints) {
    }

    private static String fingerprintSql(String from) {
        return "SELECT count(*) AS n, md5(coalesce(string_agg(t::VARCHAR, '|' ORDER BY t::VARCHAR),"
                + " '')) AS h FROM " + from + " t";
    }

    private static String fingerprint(String table) throws Exception {
        return answer(fingerprintSql(table), "complete").rows().get(0);
    }

    private static String direct(String metadataLocation) throws Exception {
        Path ext = Path.of(extensionDir, "v1.5.6", "linux_amd64");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("LOAD '" + ext.resolve("avro.duckdb_extension") + "'");
            s.execute("LOAD '" + ext.resolve("iceberg.duckdb_extension") + "'");
            try (ResultSet rs = s.executeQuery(fingerprintSql("iceberg_scan('" + metadataLocation + "')"))) {
                rs.next();
                return rs.getLong(1) + "|" + rs.getString(2);
            }
        }
    }

    private static long[] firstSnapshot(String metadataLocation) throws Exception {
        return snapshot(metadataLocation, "ASC");
    }

    /**
     * Returns the first or last snapshot of a table, as DuckDB's iceberg extension lists it.
     *
     * @param metadataLocation metadata file
     * @param order ASC for the first, DESC for the last
     * @return snapshot ID and commit time in epoch milliseconds
     */
    private static long[] snapshot(String metadataLocation, String order) throws Exception {
        Path ext = Path.of(extensionDir, "v1.5.6", "linux_amd64");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("LOAD '" + ext.resolve("avro.duckdb_extension") + "'");
            s.execute("LOAD '" + ext.resolve("iceberg.duckdb_extension") + "'");
            try (ResultSet rs = s.executeQuery("SELECT snapshot_id, epoch_ms(timestamp_ms) FROM"
                    + " iceberg_snapshots('" + metadataLocation + "') ORDER BY sequence_number "
                    + order + " LIMIT 1")) {
                assertTrue(rs.next());
                return new long[] {rs.getLong(1), rs.getLong(2)};
            }
        }
    }

    private static Answer answer(String sql, String mode) throws Exception {
        FlightInfo info = info(sql, mode);
        Schema announced = info.getSchemaOptional().orElseThrow();
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                assertEquals(announced, stream.getSchema(), mode + " stream vs FlightInfo: " + sql);
                while (stream.next()) {
                    rows.addAll(rows(stream.getRoot()));
                }
            }
        }
        if (!sql.contains("ORDER BY")) {
            rows.sort(null);
        }
        return new Answer(announced, rows, info.getEndpoints().size());
    }

    private static List<String> read(FlightInfo info) throws Exception {
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    rows.addAll(rows(stream.getRoot()));
                }
            }
        }
        rows.sort(null);
        return rows;
    }

    private static List<String> rows(VectorSchemaRoot root) {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < root.getRowCount(); i++) {
            List<String> values = new ArrayList<>();
            for (FieldVector vector : root.getFieldVectors()) {
                Object value = vector.getObject(i);
                values.add(value instanceof byte[] bytes ? Arrays.toString(bytes) : String.valueOf(value));
            }
            rows.add(String.join("|", values));
        }
        return rows;
    }

    private static FlightInfo info(String sql, String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        return helper.sqlClient().execute(sql, new HeaderCallOption(headers));
    }

    private static List<String> column(FlightInfo info, String name) throws Exception {
        List<String> values = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VarCharVector vector = (VarCharVector) stream.getRoot().getVector(name);
                    for (int i = 0; i < stream.getRoot().getRowCount(); i++) {
                        values.add(new String(vector.get(i), java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
            }
        }
        return values;
    }

    private static Schema tableSchema(String schema, String table) throws Exception {
        FlightInfo info = helper.sqlClient().getTables(null, schema, table, null, true);
        try (FlightStream stream = helper.flightClient().getStream(info.getEndpoints().get(0).getTicket())) {
            assertTrue(stream.next());
            VarBinaryVector schemas = (VarBinaryVector) stream.getRoot().getVector("table_schema");
            return Schema.deserializeMessage(ByteBuffer.wrap(schemas.get(0)));
        }
    }
}
