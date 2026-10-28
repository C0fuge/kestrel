package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

/**
 * Answers through the server for tables of a real Iceberg REST catalog (Apache Polaris, the
 * Iceberg REST fixture, ...; started by the Catalogs CI workflow): the iceberg plugin's
 * iceberg-rest catalog (schemas rest_*) next to an iceberg-hadoop catalog over a reference
 * warehouse with the same tables, both written by Iceberg Java, under the DuckDB lockdown.
 * Complete answers of every REST table equal the reference's; partial answers equal complete
 * ones; a snapshot with deletes is planned in located parts. Skipped unless
 * {@code ICEBERG_REST_IT_URI} names the catalog and {@code ICEBERG_REST_IT_ROOT} its local
 * warehouse directory (S3 warehouses: not yet); optional {@code ICEBERG_REST_IT_WAREHOUSE},
 * {@code ICEBERG_REST_IT_CREDENTIAL} (OAuth2 id:secret), {@code ICEBERG_REST_IT_SCOPE}. Needs
 * {@code -Diceberg.it.extensionDir} and the iceberg plugin built.
 */
@Tag("rest-catalogs")
class RestCatalogServerConformanceTest {

    private static final List<String> TABLES = List.of("deletes", "empty", "evolved", "nested",
            "parted", "types");

    @TempDir
    static Path dir;
    static TestFlightServerHelper helper;

    @BeforeAll
    static void start() throws Exception {
        String uri = env("ICEBERG_REST_IT_URI").orElse(null);
        Assumptions.assumeTrue(uri != null, "no real REST catalog (ICEBERG_REST_IT_URI)");
        String root = env("ICEBERG_REST_IT_ROOT").orElse("");
        Assumptions.assumeTrue(Files.isDirectory(Path.of(root)),
                "ICEBERG_REST_IT_ROOT must be the catalog's local warehouse directory");
        String extensionDir = IcebergTestTables.extensionDir();
        Assumptions.assumeTrue(extensionDir != null,
                "needs -Diceberg.it.extensionDir with v1.5.6/linux_amd64/{iceberg,avro}.duckdb_extension");
        Assumptions.assumeTrue(IcebergTestTables.available(), "flight-plugin-iceberg is not built");

        String reference = dir.resolve("wh").toString();
        IcebergTestTables.write(reference);
        Map<String, String> writer = new HashMap<>(Map.of("uri", uri,
                "io-impl", "org.apache.iceberg.hadoop.HadoopFileIO"));
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.duckdb.extensionDir", extensionDir);
        sys.put("arrowflight.iceberg-hadoop.warehouse", reference);
        sys.put("arrowflight.iceberg-rest.uri", uri);
        sys.put("arrowflight.iceberg-rest.roots", root);
        sys.put("arrowflight.iceberg-rest.schemaPrefix", "rest_");
        sys.put("arrowflight.iceberg-rest.metadataTtlMillis", "0");
        Map<String, String> envs = new HashMap<>();
        env("ICEBERG_REST_IT_WAREHOUSE").ifPresent(v -> {
            writer.put("warehouse", v);
            sys.put("arrowflight.iceberg-rest.warehouse", v);
        });
        env("ICEBERG_REST_IT_SCOPE").ifPresent(v -> {
            writer.put("scope", v);
            sys.put("arrowflight.iceberg-rest.scope", v);
        });
        env("ICEBERG_REST_IT_CREDENTIAL").ifPresent(v -> {
            writer.put("credential", v);
            envs.put("FLIGHT_ICEBERG_REST_CREDENTIAL", v);
        });
        IcebergTestTables.writeRest(writer);

        Path plugins = dir.resolve("plugins");
        IcebergTestTables.installPlugin(plugins);
        Files.createDirectories(dir.resolve("data"));
        ConfigSource source = new ConfigSource(sys::get, new Properties(), envs, null);
        PluginSettings settings = new PluginSettings(plugins, null, "iceberg-hadoop,iceberg-rest", false);
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
    }

    private static Optional<String> env(String name) {
        return Optional.ofNullable(System.getenv(name)).filter(v -> !v.isBlank());
    }

    @Test
    void restTablesAppearNextToTheReference() throws Exception {
        List<String> schemas = column(helper.sqlClient().getSchemas(null, null), "db_schema_name");
        assertTrue(schemas.containsAll(List.of("ice", "rest_ice")), schemas.toString());
        assertEquals(TABLES, column(helper.sqlClient().getTables(null, "rest_ice", null, null, false),
                "table_name").stream().sorted().toList());
    }

    @Test
    void completeAnswersEqualTheReference() throws Exception {
        for (String table : TABLES) {
            String sql = "SELECT count(*) AS n, md5(coalesce(string_agg(t::VARCHAR, '|' ORDER BY"
                    + " t::VARCHAR), '')) AS h FROM %s." + table + " t";
            assertEquals(answer(sql.formatted("ice"), "complete").rows(),
                    answer(sql.formatted("rest_ice"), "complete").rows(), table);
        }
        assertEquals(List.of("1|n1", "3|n3", "5|n5", "6|n6", "7|n7"),
                answer("SELECT id, label FROM rest_ice.deletes ORDER BY id", "complete").rows());
    }

    @Test
    void partialAnswersEqualCompleteOnes() throws Exception {
        for (String sql : List.of(
                "SELECT * FROM rest_ice.types",
                "SELECT * FROM rest_ice.evolved",
                "SELECT * FROM rest_ice.nested",
                "SELECT * FROM rest_ice.empty",
                "SELECT id, label FROM rest_ice.deletes",
                "SELECT id, region, ts, v FROM rest_ice.parted WHERE v > 0.4",
                "SELECT region, count(*) AS n FROM rest_ice.parted GROUP BY region",
                "SELECT a.id, b.label FROM rest_ice.evolved a JOIN ice.deletes b ON a.id = b.id")) {
            Answer partial = answer(sql, "partial");
            Answer complete = answer(sql, "complete");
            assertEquals(complete.schema(), partial.schema(), sql);
            assertEquals(complete.rows(), partial.rows(), sql);
            assertEquals(answer(sql.replace("rest_ice.", "ice."), "complete").rows(), complete.rows(), sql);
        }
        for (String table : List.of("evolved", "deletes", "parted")) {
            FlightInfo info = info("SELECT * FROM rest_ice." + table, "partial");
            assertFalse(info.getEndpoints().get(0).getLocations().isEmpty(), "partial endpoint: " + table);
        }
    }

    /**
     * An answer: announced schema and rows as text.
     *
     * @param schema announced schema
     * @param rows rows, columns separated by |
     */
    private record Answer(Schema schema, List<String> rows) {
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
        return new Answer(announced, rows);
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
}
