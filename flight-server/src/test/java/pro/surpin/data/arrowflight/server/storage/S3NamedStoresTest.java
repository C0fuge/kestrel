package pro.surpin.data.arrowflight.server.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.server.catalog.IcebergTestTables;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.spi.storage.Located;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A catalog on a second S3 store (Next steps 4): an Iceberg warehouse in another bucket with
 * other credentials than the S3 data directory, through the real s3 and iceberg plugins. Each
 * store gets its own DuckDB secret scoped to its root; a root outside every configured store is
 * still refused.
 */
@Tag("unit")
class S3NamedStoresTest {

    @TempDir
    Path dir;
    private S3PluginSupport support;
    private FakeS3 s3;

    @BeforeEach
    void setUp() throws Exception {
        Assumptions.assumeTrue(IcebergTestTables.available(), "flight-plugin-iceberg is not built");
        support = new S3PluginSupport(dir);
        IcebergTestTables.installPlugin(support.plugins);
        s3 = FakeS3.start();
        Path ext = Files.createDirectories(dir.resolve("ext/v1.4.5/linux_amd64"));
        for (String name : List.of("httpfs", "iceberg", "avro")) {
            Path file = Files.writeString(ext.resolve(name + ".duckdb_extension"), "x");
            support.properties.put("duckDb" + Character.toUpperCase(name.charAt(0))
                    + name.substring(1) + "Extension", file.toString());
        }
        support.properties.put("s3Endpoint", s3.endpoint());
        support.environment.put("FLIGHT_S3_ACCESS_KEY", "AKIAMAIN");
        support.environment.put("FLIGHT_S3_SECRET_KEY", "MAINSECRET");
        support.properties.put("arrowflight.iceberg-hadoop.warehouse", "s3://warehouse/iceberg");
    }

    @AfterEach
    void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    @Test
    void aWarehouseInAnotherBucketGetsItsOwnStoreAndSecret() throws Exception {
        support.properties.put("arrowflight.s3.stores", "wh");
        support.properties.put("arrowflight.s3.wh.root", "s3://warehouse");
        support.properties.put("arrowflight.s3.wh.endpoint", s3.endpoint());
        support.environment.put("FLIGHT_S3_WH_ACCESS_KEY", "AKIAWH");
        support.environment.put("FLIGHT_S3_WH_SECRET_KEY", "WHSECRET");

        try (StoreRegistry registry = support.open(StoreRegistryTest.config("s3://lake/parquet"),
                "iceberg-hadoop")) {
            DuckDbSetupPlan plan = registry.duckDbSetup();
            assertEquals(List.of(new DuckDbSetupPlan.DeclaredSecret("flight_s3", "s3://lake/parquet/"),
                    new DuckDbSetupPlan.DeclaredSecret("flight_s3_wh", "s3://warehouse/")),
                    plan.secrets());
            String main = secret(plan, "flight_s3 ");
            String wh = secret(plan, "flight_s3_wh ");
            assertTrue(main.contains("KEY_ID 'AKIAMAIN'") && main.contains("'MAINSECRET'"), main);
            assertTrue(wh.contains("KEY_ID 'AKIAWH'") && wh.contains("'WHSECRET'"), wh);
            assertFalse(wh.contains("MAIN"), wh);
            // Iceberg writers name the warehouse's objects as s3a:// too: the same secret.
            assertTrue(wh.contains("s3a://warehouse/"), wh);
            assertTrue(registry.readableRoots().contains("s3://warehouse/"),
                    registry.readableRoots().toString());

            Located table = registry.locate("s3://warehouse/iceberg/ns/t/data/f.parquet");
            assertEquals("iceberg/ns/t/data/f.parquet", table.relativePath());
            assertEquals("wh", table.store().configName());
            Located data = registry.locate("s3://lake/parquet/tpch/x.parquet");
            assertEquals("", data.store().configName());
            assertEquals("tpch/x.parquet", data.relativePath());
            assertEquals("a *** b ***", registry.redactor().redact("a WHSECRET b MAINSECRET"));
        }
    }

    /** Verifies the old refusal stays for a root no named store holds, and names the remedy. */
    @Test
    void aRootOutsideEveryStoreIsStillRefused() {
        support.properties.put("arrowflight.s3.stores", "wh");
        support.properties.put("arrowflight.s3.wh.root", "s3://other");
        support.environment.put("FLIGHT_S3_WH_ACCESS_KEY", "AKIAWH");
        support.environment.put("FLIGHT_S3_WH_SECRET_KEY", "WHSECRET");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> support.open(StoreRegistryTest.config("s3://lake/parquet"), "iceberg-hadoop"));
        assertTrue(e.getMessage().contains("needs a second s3 store"), e.getMessage());
        assertTrue(e.getMessage().contains("arrowflight.s3.stores"), e.getMessage());
    }

    /** Verifies a named store never falls back to the data directory's credentials. */
    @Test
    void aNamedStoreNeedsItsOwnCredentials() {
        support.properties.put("arrowflight.s3.stores", "wh");
        support.properties.put("arrowflight.s3.wh.root", "s3://warehouse/");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> support.open(StoreRegistryTest.config("s3://lake/parquet"), "iceberg-hadoop"));
        assertTrue(e.getMessage().contains("S3 store wh without credentials"), e.getMessage());
        assertTrue(e.getMessage().contains("FLIGHT_S3_WH_ACCESS_KEY"), e.getMessage());
    }

    /** Verifies a named secret passed as a property stops the start without showing it. */
    @Test
    void aNamedSecretPropertyIsRefused() {
        support.properties.put("arrowflight.s3.stores", "wh");
        support.properties.put("arrowflight.s3.wh.root", "s3://warehouse/");
        support.properties.put("arrowflight.s3.wh.secretKey", "LEAKED");
        support.environment.put("FLIGHT_S3_WH_ACCESS_KEY", "AKIAWH");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> support.open(StoreRegistryTest.config("s3://lake/parquet"), "iceberg-hadoop"));
        assertTrue(e.getMessage().contains("wh.secretKey"), e.getMessage());
        assertFalse(e.getMessage().contains("LEAKED"), e.getMessage());
    }

    /** Verifies a broken store list stops the start even when no catalog needs a store. */
    @Test
    void aBrokenStoreListStopsTheStart() {
        support.properties.put("arrowflight.s3.stores", "wh");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> support.open(StoreRegistryTest.config("s3://lake/parquet")));
        assertTrue(e.getMessage().contains("s3.wh.root"), e.getMessage());
    }

    private static String secret(DuckDbSetupPlan plan, String name) {
        return plan.statements().stream()
                .filter(s -> s.startsWith("CREATE TEMPORARY SECRET " + name)).findFirst()
                .orElseThrow(() -> new AssertionError("no secret " + name + " in " + plan));
    }
}
