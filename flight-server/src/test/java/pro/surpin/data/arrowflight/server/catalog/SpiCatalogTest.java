package pro.surpin.data.arrowflight.server.catalog;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import pro.surpin.data.arrowflight.server.duckdb.TableSecrets;
import pro.surpin.data.arrowflight.spi.catalog.AsOf;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class SpiCatalogTest {

    private static final TableName TABLE = new TableName("ns", "t");

    @Test
    void everySnapshotTheCatalogReturnsRegistersItsCredentials() throws Exception {
        TableSnapshot vended = snapshot("s3://lake/wh/ns/t");
        Catalog plugin = new Catalog() {
            @Override
            public List<String> schemas() {
                return List.of("ns");
            }

            @Override
            public List<String> tables(String schema) {
                return List.of("t");
            }

            @Override
            public Optional<TableSnapshot> current(TableName table) {
                return Optional.of(vended);
            }

            @Override
            public Optional<TableSnapshot> asOf(TableName table, AsOf point) {
                return Optional.of(vended);
            }

            @Override
            public List<String> readableRoots() {
                return List.of("s3://lake/wh");
            }
        };
        SpiCatalog catalog = new SpiCatalog(plugin, null, "rest");
        TableSecrets secrets = new TableSecrets();
        // Not registered yet: nothing is installed.
        assertEquals(vended, catalog.current(TABLE).orElseThrow());
        assertEquals(0, secrets.current().version());

        catalog.registerSecrets(secrets, "rest", List.of("s3://lake/wh"));
        catalog.current(TABLE);
        assertEquals(1, secrets.current().statements().size());
        catalog.pinned(TABLE, "1");
        catalog.pinnedColumns(TABLE, "1");
        catalog.asOf(TABLE, new AsOf.SnapshotId(1));
        assertEquals(1, secrets.current().version(), "the same credentials are not installed again");
        String sql = secrets.current().statements().values().iterator().next();
        assertTrue(sql.contains("SCOPE ['s3://lake/wh/ns/t/data/'"), sql);
    }

    @Test
    void credentialsOutsideTheCatalogsRootsFailTheTable() {
        Catalog plugin = new Catalog() {
            @Override
            public List<String> schemas() {
                return List.of("ns");
            }

            @Override
            public List<String> tables(String schema) {
                return List.of("t");
            }

            @Override
            public Optional<TableSnapshot> current(TableName table) {
                return Optional.of(snapshot("s3://lake"));
            }

            @Override
            public List<String> readableRoots() {
                return List.of("s3://lake/wh");
            }
        };
        SpiCatalog catalog = new SpiCatalog(plugin, null, "rest");
        TableSecrets secrets = new TableSecrets();
        catalog.registerSecrets(secrets, "rest", List.of("s3://lake/wh/ns/u"));
        assertThrows(IllegalArgumentException.class, () -> catalog.current(TABLE));
        assertEquals(0, secrets.current().version());
    }

    private static TableSnapshot snapshot(String prefix) {
        return new TableSnapshot(TABLE, "1", "f", null, null,
                List.of(new TableFile("s3://lake/wh/ns/t/data/1.parquet", 1, 0, null, Map.of(), -1,
                        List.of())), new ReadSpec.ParquetFiles(false),
                List.of(new StorageCredential(prefix, "s3",
                        Map.of("KEY_ID", SecretValue.plain("AKIAV")), null)));
    }
}
