package pro.surpin.data.arrowflight.server.duckdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the checks and the statement order of the DuckDB setup written for plugins. */
@Tag("unit")
class DuckDbSetupValidatorTest {

    private final DuckDbSetupValidator validator =
            new DuckDbSetupValidator(name -> "/ext/" + name + ".duckdb_extension");

    @Test
    void ordersStoresThenCatalogsAndItemsPerContributor() {
        DuckDbSetupPlan plan = validator.plan(
                contribution("data", Set.of("s3"), List.of("s3://z/data/"), setup -> {
                    setup.set("s3_region", "");
                    setup.createSecret("data_secret", "s3", "s3://z/data/", Map.of(
                            "KEY_ID", SecretValue.plain("k")));
                    setup.loadExtension("httpfs");
                }),
                List.of(contribution("b", Set.of("s3"), List.of("s3://b/"), setup -> {
                    setup.loadExtension("httpfs");
                    setup.set("http_retries", 3L);
                }), contribution("a", Set.of("hdfs"), List.of("hdfs://ns/a"), setup -> {
                })),
                List.of(contribution("catalog", Set.of("s3"), List.of("s3://wh/"), setup -> {
                    setup.loadExtension("iceberg");
                    setup.refuseFunctions("Iceberg_Snapshots");
                })));

        assertEquals(List.of(
                "LOAD '/ext/httpfs.duckdb_extension'",
                "SET s3_region = ''",
                // An S3 catalog: tables written through S3A name objects s3a://, same secret.
                "CREATE TEMPORARY SECRET data_secret (TYPE s3, KEY_ID 'k', SCOPE ['s3://z/data/',"
                        + " 's3a://z/data/', 's3n://z/data/'])",
                "SET http_retries = 3",
                "LOAD '/ext/iceberg.duckdb_extension'"), plan.statements());
        assertEquals(List.of("s3://z/data/", "hdfs://ns/a", "s3://b/", "s3://wh/", "s3a://wh/",
                "s3n://wh/"), plan.roots());
        assertEquals("s3://z/data/", plan.secrets().get(0).scope());
        assertEquals(Set.of("iceberg_snapshots"), plan.extraRefusedFunctions());
        assertEquals(List.of("httpfs", "iceberg"), plan.extensions());
    }

    /**
     * Spark and Iceberg Java name local files {@code file:/...} in metadata and manifests; DuckDB
     * checks those names literally, so local catalog roots also appear as {@code file:} URIs
     * (Gravitino stand). The data directory's local root does not get a twin.
     */
    @Test
    void localCatalogRootsAlsoReadableAsFileUris() {
        DuckDbSetupPlan plan = validator.plan(
                contribution("data", Set.of("file"), List.of("/data/"), setup -> {
                }),
                List.of(contribution("hadoop", Set.of("file", "hdfs"), List.of("/wh/extra"),
                        setup -> {
                        })),
                List.of(contribution("catalog", Set.of("file", "hdfs"),
                        List.of("/wh/warehouse", "hdfs://ns/wh"),
                        setup -> setup.loadExtension("iceberg"))));

        assertEquals(List.of("/data/", "/wh/extra", "/wh/warehouse", "hdfs://ns/wh",
                "file:/wh/warehouse"), plan.roots());
    }

    /** Without an S3 catalog the S3 stores keep their single scope and roots (golden setup). */
    @Test
    void s3TwinsOnlyForS3Catalogs() {
        DuckDbSetupPlan plan = validator.plan(
                contribution("data", Set.of("s3"), List.of("s3://z/data/"), setup ->
                        setup.createSecret("data_secret", "s3", "s3://z/data/", Map.of(
                                "KEY_ID", SecretValue.plain("k")))),
                List.of(),
                List.of(contribution("catalog", Set.of("file"), List.of("/wh"),
                        setup -> setup.loadExtension("iceberg"))));

        assertEquals("CREATE TEMPORARY SECRET data_secret (TYPE s3, KEY_ID 'k',"
                + " SCOPE 's3://z/data/')", plan.statements().get(0));
        assertEquals(List.of("s3://z/data/", "/wh", "file:/wh"), plan.roots());
        assertEquals(List.of("s3://b/x/", "s3n://b/x/"), DuckDbSetupValidator.s3Twins("s3a://b/x/"));
        assertEquals(List.of(), DuckDbSetupValidator.s3Twins("hdfs://ns/x"));
    }

    @Test
    void settingsFollowTheirRules(@TempDir Path dir) throws Exception {
        Path ca = Files.writeString(dir.resolve("ca.pem"), "x");
        DuckDbSetupPlan plan = plan(setup -> {
            setup.set("s3_access_key_id", "");
            setup.set("s3_url_compatibility_mode", true);
            setup.set("enable_http_logging", false);
            setup.set("enable_server_cert_verification", false);
            setup.set("ca_cert_file", ca.toString());
            setup.set("hdfs_default_namenode", "nn:8020");
            setup.set("http_timeout", 30000L);
            setup.set("parquet_metadata_cache", true);
            setup.set("parquet_metadata_cache", true);
        });
        assertEquals(8, plan.statements().size());

        refused(setup -> setup.set("s3_access_key_id", "AKIA"));
        refused(setup -> setup.set("s3_endpoint", "evil:1"));
        refused(setup -> setup.set("s3_url_compatibility_mode", false));
        refused(setup -> setup.set("enable_http_logging", true));
        refused(setup -> setup.set("ca_cert_file", dir.resolve("missing.pem").toString()));
        refused(setup -> setup.set("http_timeout", -1L));
        refused(setup -> setup.set("parquet_metadata_cache", "true"));
        refused(setup -> {
            setup.set("parquet_metadata_cache", true);
            setup.set("parquet_metadata_cache", false);
        });
        for (String setting : List.of("enable_external_access", "lock_configuration",
                "allowed_directories", "allowed_paths", "allow_unsigned_extensions",
                "allow_community_extensions", "allow_persistent_secrets", "allow_unredacted_secrets",
                "autoinstall_known_extensions", "autoload_known_extensions", "extension_directory",
                "custom_extension_repository", "secret_directory", "temp_directory",
                "max_temp_directory_size", "threads", "preserve_insertion_order",
                "streaming_buffer_size",
                "memory_limit", "enable_logging", "logging_level", "some_unknown_setting",
                "Bad Name", "x;DROP")) {
            refused(setup -> setup.set(setting, true));
            refused(setup -> setup.set(setting, ""));
        }
    }

    @Test
    void secretsAreTypedScopedAndRedacted() {
        Map<String, SecretValue> options = new LinkedHashMap<>();
        options.put("KEY_ID", SecretValue.plain("AK"));
        options.put("SECRET", SecretValue.sensitive("s'k"));
        options.put("USE_SSL", SecretValue.of(false));
        DuckDbSetupPlan plan = plan(setup -> setup.createSecret("s", "S3", "s3://b/data/t/", options));

        assertEquals("CREATE TEMPORARY SECRET s (TYPE s3, KEY_ID 'AK', SECRET 's''k', USE_SSL false,"
                + " SCOPE 's3://b/data/t/')", plan.statements().get(0));
        assertEquals("a *** b", plan.redactor().redact("a s'k b"));
        assertEquals(List.of(new DuckDbSetupPlan.DeclaredSecret("s", "s3://b/data/t/")), plan.secrets());

        refused(setup -> setup.createSecret("s", "http", "s3://b/data/", Map.of()));
        refused(setup -> setup.createSecret("s", "s3", "s3://b/other/", Map.of()));
        refused(setup -> setup.createSecret("s", "s3", "s3://b/", Map.of()));
        refused(setup -> setup.createSecret("s", "s3", "s3://b/data/../x/", Map.of()));
        refused(setup -> setup.createSecret("s", "s3", null, Map.of()));
        refused(setup -> setup.createSecret("s", "s3", "s3://b/data/",
                Map.of("PROVIDER", SecretValue.plain("credential_chain"))));
        refused(setup -> setup.createSecret("s", "s3", "s3://b/data/",
                Map.of("USE_SSL", SecretValue.plain("yes"))));
        refused(setup -> setup.createSecret("bad name", "s3", "s3://b/data/", Map.of()));
        refused(setup -> {
            setup.createSecret("s", "s3", "s3://b/data/", Map.of());
            setup.createSecret("s", "s3", "s3://b/data/", Map.of());
        });
    }

    @Test
    void extensionsAndFunctionsAreNamesOnly() {
        refused(setup -> setup.loadExtension("/tmp/evil.duckdb_extension"));
        refused(setup -> setup.loadExtension("httpfs'"));
        refused(setup -> setup.refuseFunctions("read_csv; DROP"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new DuckDbSetupValidator(name -> {
                    throw new IllegalArgumentException("not configured: " + name);
                }).plan(contribution("p", Set.of("s3"), List.of("s3://b/data/"),
                        setup -> setup.loadExtension("httpfs")), List.of(), List.of()));
        assertTrue(e.getMessage().contains("httpfs"), e.getMessage());
    }

    @Test
    void rootsMustBeAbsoluteNormalisedAndOfTheStoresScheme() {
        for (String root : List.of("/", "s3://", "s3:///x", "relative/dir", "s3://b/../x",
                "s3://b/./x", "s3://b/x?y", "s3://b/x#y", "s3://b/*", "s3://b//x", " s3://b/x",
                "hdfs://ns/data", "file:/data")) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> validator.plan(contribution("p", Set.of("s3", "file"), List.of(root),
                            setup -> {
                            }), List.of(), List.of()), root);
            assertTrue(e.getMessage().contains("p"), e.getMessage());
        }
        assertEquals(List.of("/data/parquet", "/tmp/a b/c/"), validator.plan(
                contribution("p", Set.of("file"), List.of("/data/parquet", "/tmp/a b/c/"),
                        setup -> {
                        }), List.of(), List.of()).roots());
    }

    @Test
    void planDoesNotPrintStatements() {
        DuckDbSetupPlan plan = plan(setup -> setup.createSecret("s", "s3", "s3://b/data/",
                Map.of("SECRET", SecretValue.sensitive("hunter2"))));
        assertTrue(!plan.toString().contains("hunter2"), plan.toString());
    }

    private DuckDbSetupPlan plan(Consumer<DuckDbSetup> body) {
        return validator.plan(contribution("plugin", Set.of("s3"), List.of("s3://b/data/"), body),
                List.of(), List.of());
    }

    private void refused(Consumer<DuckDbSetup> body) {
        assertThrows(IllegalArgumentException.class, () -> plan(body));
    }

    private static DuckDbSetupValidator.Contribution contribution(String name, Set<String> schemes,
            List<String> roots, Consumer<DuckDbSetup> body) {
        return new DuckDbSetupValidator.Contribution(name, schemes, roots, body);
    }
}
