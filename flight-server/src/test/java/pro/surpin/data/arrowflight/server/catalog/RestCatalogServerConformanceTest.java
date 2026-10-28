package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

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
 * ones; a snapshot with deletes is planned in located parts. On local warehouses the same
 * metadata files are also registered in an in-process Hive metastore (schema hms_ice, read by
 * the hive plugin through the iceberg plugin) as a further reference. Skipped unless
 * {@code ICEBERG_REST_IT_URI} names the catalog and {@code ICEBERG_REST_IT_ROOT} its warehouse:
 * a local directory, or an S3 location ({@code s3://bucket/path}, written with Iceberg's
 * S3FileIO and read through the real s3 plugin) with {@code ICEBERG_REST_IT_S3_ENDPOINT},
 * {@code ICEBERG_REST_IT_S3_ACCESS_KEY} and {@code ICEBERG_REST_IT_S3_SECRET_KEY} (path-style
 * URLs). Optional {@code ICEBERG_REST_IT_WAREHOUSE}, {@code ICEBERG_REST_IT_CREDENTIAL} (OAuth2
 * id:secret), {@code ICEBERG_REST_IT_SCOPE}. Needs {@code -Diceberg.it.extensionDir} (with
 * httpfs for S3) and the iceberg plugin built (and the s3 plugin for S3).
 */
@Tag("rest-catalogs")
class RestCatalogServerConformanceTest {

    private static final List<String> TABLES = List.of("deletes", "empty", "evolved", "nested",
            "parted", "types");

    /** The built plugins/s3 directory, for S3 warehouses. */
    private static final Path S3_PLUGIN = Path.of("../flight-plugin-s3/target/plugin/s3");

    /** Hive database of the metastore reference. */
    private static final String HMS = "hms_ice";

    @TempDir
    static Path dir;
    /** Whether the tables are also registered in a Hive metastore. */
    static boolean metastore;
    static TestFlightServerHelper helper;

    @BeforeAll
    static void start() throws Exception {
        String uri = env("ICEBERG_REST_IT_URI").orElse(null);
        Assumptions.assumeTrue(uri != null, "no real REST catalog (ICEBERG_REST_IT_URI)");
        String root = env("ICEBERG_REST_IT_ROOT").orElse("");
        boolean s3 = root.startsWith("s3://");
        Assumptions.assumeTrue(s3 || Files.isDirectory(Path.of(root)),
                "ICEBERG_REST_IT_ROOT must be the catalog's local warehouse directory or s3://...");
        String extensionDir = IcebergTestTables.extensionDir();
        Assumptions.assumeTrue(extensionDir != null,
                "needs -Diceberg.it.extensionDir with v1.5.6/linux_amd64/{iceberg,avro}.duckdb_extension");
        Assumptions.assumeTrue(IcebergTestTables.available(), "flight-plugin-iceberg is not built");
        Assumptions.assumeTrue(!s3 || Files.isDirectory(S3_PLUGIN), "flight-plugin-s3 is not built");

        String reference = dir.resolve("wh").toString();
        IcebergTestTables.write(reference);
        Map<String, String> writer = new HashMap<>(Map.of("uri", uri));
        Map<String, String> sys = new HashMap<>();
        Map<String, String> envs = new HashMap<>();
        if (s3) {
            String endpoint = env("ICEBERG_REST_IT_S3_ENDPOINT").orElseThrow();
            String accessKey = env("ICEBERG_REST_IT_S3_ACCESS_KEY").orElseThrow();
            String secretKey = env("ICEBERG_REST_IT_S3_SECRET_KEY").orElseThrow();
            writer.putAll(Map.of("io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                    "s3.endpoint", endpoint, "s3.path-style-access", "true",
                    "s3.access-key-id", accessKey, "s3.secret-access-key", secretKey,
                    "client.region", "us-east-1",
                    // Iceberg's "apache" client is the SDK's Apache 5 client, absent from our SDK.
                    "http-client.type", "urlconnection"));
            sys.putAll(Map.of("arrowflight.s3.endpoint", endpoint, "arrowflight.s3.urlStyle", "path",
                    "arrowflight.s3.region", "us-east-1"));
            envs.put("FLIGHT_S3_ACCESS_KEY", accessKey);
            envs.put("FLIGHT_S3_SECRET_KEY", secretKey);
        } else {
            writer.put("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");
        }
        sys.put("arrowflight.duckdb.extensionDir", extensionDir);
        sys.put("arrowflight.iceberg-hadoop.warehouse", reference);
        sys.put("arrowflight.iceberg-rest.uri", uri);
        sys.put("arrowflight.iceberg-rest.roots", root);
        sys.put("arrowflight.iceberg-rest.schemaPrefix", "rest_");
        sys.put("arrowflight.iceberg-rest.metadataTtlMillis", "0");
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
        Map<String, String> metadata = IcebergTestTables.writeRest(writer);

        Path plugins = dir.resolve("plugins");
        IcebergTestTables.installPlugin(plugins);
        if (s3) {
            Path target = Files.createDirectories(plugins.resolve("s3"));
            try (Stream<Path> jars = Files.list(S3_PLUGIN)) {
                for (Path jar : jars.toList()) {
                    Files.copy(jar, target.resolve(jar.getFileName().toString()));
                }
            }
        }
        String catalogs = "iceberg-hadoop,iceberg-rest";
        // A metastore over a local warehouse; the in-process one cannot place tables on S3.
        metastore = !s3 && HiveTestTables.available();
        if (metastore) {
            HiveTestTables.Hms hms = HiveTestTables.hms();
            hms.call("createDatabase", HMS);
            for (String table : TABLES) {
                String file = fileUri(metadata.get(table));
                hms.call("registerIceberg", HMS, table, file.substring(0, file.indexOf("/metadata/")),
                        file);
            }
            HiveTestTables.installPlugin(plugins);
            sys.put("arrowflight.hive.uris", hms.uri());
            sys.put("arrowflight.hive.roots", root);
            sys.put("arrowflight.hive.metadataTtlSeconds", "0");
            sys.put("arrowflight.hive.namesTtlSeconds", "0");
            catalogs += ",hive";
        }
        Files.createDirectories(dir.resolve("data"));
        ConfigSource source = new ConfigSource(sys::get, new Properties(), envs, null);
        PluginSettings settings = new PluginSettings(plugins, null, catalogs, false);
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

    private static String fileUri(String location) {
        return location.startsWith("/") ? "file:" + location
                : "file:" + Path.of(URI.create(location)).toString();
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

    @Test
    void metastoreTablesAnswerLikeTheRestCatalog() throws Exception {
        Assumptions.assumeTrue(metastore, "no metastore reference (S3 warehouse or hive plugin not built)");
        assertEquals(TABLES, column(helper.sqlClient().getTables(null, HMS, null, null, false),
                "table_name").stream().sorted().toList());
        for (String table : TABLES) {
            String sql = "SELECT * FROM %s." + table;
            Answer viaRest = answer(sql.formatted("rest_ice"), "complete");
            Answer viaHms = answer(sql.formatted(HMS), "complete");
            assertEquals(viaRest.schema(), viaHms.schema(), table);
            assertEquals(viaRest.rows(), viaHms.rows(), table);
            assertEquals(viaHms.rows(), answer(sql.formatted(HMS), "partial").rows(), table);
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
