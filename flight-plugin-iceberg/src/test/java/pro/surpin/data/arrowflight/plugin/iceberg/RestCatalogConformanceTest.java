package pro.surpin.data.arrowflight.plugin.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.plugin.s3.S3DataStoreProvider;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.storage.StoreUse;

/**
 * Conformance of the iceberg-rest catalog against a real Iceberg REST catalog (Apache Polaris,
 * the Iceberg REST fixture, ...; started by the Catalogs CI workflow). The test tables are
 * written through the catalog itself with Iceberg Java's RESTCatalog and, as the reference,
 * into a Hadoop-layout warehouse; the plugin must see both the same way: names, columns, files,
 * row and delete counts, partial-read problems and, with DuckDB's iceberg extension, the same
 * rows. Skipped unless {@code ICEBERG_REST_IT_URI} is set; the catalog's warehouse
 * ({@code ICEBERG_REST_IT_ROOT}) is a local directory shared with the catalog server, or an S3
 * location ({@code s3://bucket/path}) read and written with {@code ICEBERG_REST_IT_S3_ENDPOINT},
 * {@code ICEBERG_REST_IT_S3_ACCESS_KEY} and {@code ICEBERG_REST_IT_S3_SECRET_KEY} (path-style
 * URLs; the plugin reads through the s3 plugin's store). Optional:
 * {@code ICEBERG_REST_IT_WAREHOUSE}, {@code ICEBERG_REST_IT_CREDENTIAL} (OAuth2 id:secret),
 * {@code ICEBERG_REST_IT_SCOPE}, {@code ICEBERG_REST_IT_OAUTH_URI},
 * {@code ICEBERG_REST_IT_OMIT_COLUMNS} (columns of table {@code types} the catalog refuses, left
 * out of both warehouses), {@code ICEBERG_REST_IT_VENDED=true} (the catalog vends temporary S3
 * credentials: the plugin asks for them and every table must carry them).
 */
@Tag("rest-catalogs")
class RestCatalogConformanceTest {

    private static final List<String> TABLES = List.of("deletes", "empty", "evolved", "nested",
            "parted", "types");

    @TempDir
    static Path dir;
    static String reference;
    static Map<String, String> restMetadata;
    static Map<String, String> settings;
    /** Whether the catalog's warehouse is on S3. */
    static boolean s3;
    static String s3Root;
    /** Configuration of the s3 plugin's store for {@link #s3Root}. */
    static Map<String, String> s3Config;

    @BeforeAll
    static void writeTables() throws IOException {
        String uri = System.getenv("ICEBERG_REST_IT_URI");
        Assumptions.assumeTrue(uri != null && !uri.isBlank(),
                "no real REST catalog (ICEBERG_REST_IT_URI)");
        String root = System.getenv("ICEBERG_REST_IT_ROOT");
        s3 = root != null && root.startsWith("s3://");
        Assumptions.assumeTrue(root != null && (s3 || Files.isDirectory(Path.of(root))),
                "ICEBERG_REST_IT_ROOT must be the catalog's local warehouse directory or s3://...");
        reference = dir.resolve("wh").toString();
        IcebergFixtures.write(reference, omitted());

        settings = new HashMap<>(Map.of("uri", uri, "roots", root));
        Map<String, String> writer = new HashMap<>(Map.of("uri", uri));
        if (s3) {
            s3Root = root;
            s3Config = Map.of("endpoint", env("ICEBERG_REST_IT_S3_ENDPOINT").orElseThrow(),
                    "accessKey", env("ICEBERG_REST_IT_S3_ACCESS_KEY").orElseThrow(),
                    "secretKey", env("ICEBERG_REST_IT_S3_SECRET_KEY").orElseThrow(),
                    "region", "us-east-1", "urlStyle", "path");
            writer.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            writer.put("s3.endpoint", s3Config.get("endpoint"));
            writer.put("s3.path-style-access", "true");
            writer.put("s3.access-key-id", s3Config.get("accessKey"));
            writer.put("s3.secret-access-key", s3Config.get("secretKey"));
            writer.put("client.region", "us-east-1");
            // Iceberg's "apache" client is the SDK's Apache 5 client, newer than the s3 plugin's SDK.
            writer.put("http-client.type", "urlconnection");
        } else {
            writer.put("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");
        }
        env("ICEBERG_REST_IT_WAREHOUSE").ifPresent(v -> {
            settings.put("warehouse", v);
            writer.put("warehouse", v);
        });
        env("ICEBERG_REST_IT_CREDENTIAL").ifPresent(v -> {
            settings.put("credential", v);
            writer.put("credential", v);
        });
        env("ICEBERG_REST_IT_SCOPE").ifPresent(v -> {
            settings.put("scope", v);
            writer.put("scope", v);
        });
        env("ICEBERG_REST_IT_OAUTH_URI").ifPresent(v -> {
            settings.put("oauthUri", v);
            writer.put("oauth2-server-uri", v);
        });
        if (vended()) {
            settings.put("vendedCredentials", "true");
        }
        restMetadata = IcebergFixtures.writeRest(writer, omitted());
    }

    /**
     * Columns of table {@code types} the catalog refuses, from
     * {@code ICEBERG_REST_IT_OMIT_COLUMNS} (comma-separated).
     *
     * @return column names, empty when unset
     */
    static Set<String> omitted() {
        return env("ICEBERG_REST_IT_OMIT_COLUMNS").map(v -> Set.of(v.split("\\s*,\\s*")))
                .orElse(Set.of());
    }

    /**
     * Tells whether the catalog vends credentials and the plugin asks for them
     * ({@code ICEBERG_REST_IT_VENDED=true}).
     *
     * @return true for a vending catalog
     */
    private static boolean vended() {
        return env("ICEBERG_REST_IT_VENDED").map(Boolean::parseBoolean).orElse(false);
    }

    private static Optional<String> env(String name) {
        return Optional.ofNullable(System.getenv(name)).filter(v -> !v.isBlank());
    }

    private static Catalog rest() throws IOException {
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        if (s3) {
            stores.opened.add(new S3DataStoreProvider().open(new TestSupport.S3Context(
                    new TestSupport.MapConfig("s3", s3Config), new SecretRedactor()), s3Root,
                    StoreUse.CATALOG_ROOT));
        }
        Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.REST).open(
                new TestSupport.Context(new TestSupport.MapConfig("iceberg-rest", settings), stores,
                        new SecretRedactor()));
        stores.freeze();
        return catalog;
    }

    private static Catalog hadoop() throws IOException {
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.HADOOP).open(
                new TestSupport.Context(new TestSupport.MapConfig("iceberg-hadoop",
                        Map.of("warehouse", reference)), stores, new SecretRedactor()));
        stores.freeze();
        return catalog;
    }

    @Test
    void namesAreTheFixtureTables() throws IOException {
        try (Catalog catalog = rest()) {
            assertTrue(catalog.schemas().contains(IcebergFixtures.NAMESPACE), catalog.schemas().toString());
            assertEquals(TABLES, catalog.tables(IcebergFixtures.NAMESPACE).stream().sorted().toList());
            assertEquals(Optional.empty(), catalog.current(new TableName(IcebergFixtures.NAMESPACE, "nope")));
        }
    }

    @Test
    void snapshotsMatchTheReferenceWarehouse() throws IOException {
        try (Catalog viaRest = rest(); Catalog viaDir = hadoop()) {
            for (String table : TABLES) {
                TableName name = new TableName(IcebergFixtures.NAMESPACE, table);
                TableSnapshot expected = viaDir.current(name).orElseThrow();
                TableSnapshot actual = viaRest.current(name).orElseThrow(() -> new AssertionError(table));
                assertEquals(expected.columns(), actual.columns(), table);
                assertEquals(expected.partitionColumns(), actual.partitionColumns(), table);
                assertEquals(shape(expected), shape(actual), table);
                assertEquals(viaDir.partialReadProblem(expected).isPresent(),
                        viaRest.partialReadProblem(actual).isPresent(), table);
                ReadSpec.IcebergMetadata read = assertInstanceOf(ReadSpec.IcebergMetadata.class,
                        actual.read(), table);
                assertEquals(Locations.normalize(restMetadata.get(table)),
                        Locations.normalize(read.metadataLocation()), table);
                if (vended()) {
                    // Temporary credentials of the catalog's own, not the warehouse's static keys.
                    assertFalse(actual.credentials().isEmpty(), table);
                    for (StorageCredential credential : actual.credentials()) {
                        assertNotEquals(env("ICEBERG_REST_IT_S3_ACCESS_KEY").orElse(""),
                                credential.options().get("KEY_ID").value(), table);
                        assertTrue(credential.options().containsKey("SESSION_TOKEN"), table);
                    }
                } else {
                    assertEquals(List.of(), actual.credentials(), table);
                }
            }
        }
    }

    @Test
    void duckDbReadsTheSameRows() throws IOException, SQLException {
        String extensionDir = System.getProperty("iceberg.it.extensionDir", "");
        Path ext = Path.of(extensionDir.isBlank() ? "-" : extensionDir, "v1.5.6", "linux_amd64");
        Assumptions.assumeTrue(Files.isReadable(ext.resolve("iceberg.duckdb_extension")),
                "needs -Diceberg.it.extensionDir with v1.5.6/linux_amd64/{iceberg,avro}.duckdb_extension");
        try (Catalog viaRest = rest(); Catalog viaDir = hadoop();
                Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("LOAD '" + ext.resolve("avro.duckdb_extension") + "'");
            s.execute("LOAD '" + ext.resolve("iceberg.duckdb_extension") + "'");
            if (s3) {
                s.execute("LOAD '" + ext.resolve("httpfs.duckdb_extension") + "'");
                URI endpoint = URI.create(s3Config.get("endpoint"));
                s.execute("CREATE SECRET conformance (TYPE s3, KEY_ID '" + s3Config.get("accessKey")
                        + "', SECRET '" + s3Config.get("secretKey") + "', REGION 'us-east-1', ENDPOINT '"
                        + endpoint.getAuthority() + "', URL_STYLE 'path', USE_SSL "
                        + "https".equals(endpoint.getScheme()) + ")");
            }
            for (String table : TABLES) {
                if ("empty".equals(table)) {
                    continue;
                }
                TableName name = new TableName(IcebergFixtures.NAMESPACE, table);
                assertEquals(rows(s, viaDir.current(name).orElseThrow()),
                        rows(s, viaRest.current(name).orElseThrow()), table);
            }
        }
    }

    /**
     * Describes a snapshot's files without their locations: per data file its record count,
     * partition values and delete kinds, sorted.
     *
     * @param snapshot snapshot
     * @return description
     */
    private static List<String> shape(TableSnapshot snapshot) {
        List<String> result = new ArrayList<>();
        for (TableFile file : snapshot.files()) {
            List<String> deletes = new ArrayList<>();
            for (DeleteFile delete : file.deletes()) {
                deletes.add(delete.kind() + delete.equalityFieldIds().toString());
            }
            deletes.sort(null);
            result.add(file.recordCount() + " " + new java.util.TreeMap<>(file.partitionValues()) + " " + deletes);
        }
        result.sort(null);
        return result;
    }

    /**
     * Reads a table's rows with iceberg_scan, ordered by id.
     *
     * @param s statement
     * @param snapshot snapshot naming the metadata file
     * @return rows as text
     * @throws SQLException when DuckDB fails
     */
    private static List<String> rows(Statement s, TableSnapshot snapshot) throws SQLException {
        String metadata = ((ReadSpec.IcebergMetadata) snapshot.read()).metadataLocation();
        List<String> rows = new ArrayList<>();
        try (ResultSet rs = s.executeQuery("SELECT * FROM iceberg_scan('"
                + Locations.normalize(metadata).replace("'", "''") + "') ORDER BY id")) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columns; i++) {
                    row.append(i == 1 ? "" : "|").append(rs.getString(i));
                }
                rows.add(row.toString());
            }
        }
        return rows;
    }
}
