package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.server.catalog.SpiCatalog;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.storage.HadoopDataStore;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Regression test: with a catalog configured, the first refresh of the inventory after its TTL
 * asked itself whether each data directory table's schema belongs to a catalog, which refreshed
 * again, until the stack overflowed (every query and metadata call failed after 30 s).
 */
class ParquetAdapterCatalogRefreshTest {

    @TempDir
    Path dir;

    /** A catalog with one empty schema. */
    private static final class OneSchema implements Catalog {
        @Override
        public List<String> schemas() {
            return List.of("cat");
        }

        @Override
        public List<String> tables(String schema) {
            return List.of();
        }

        @Override
        public Optional<TableSnapshot> current(TableName table) {
            return Optional.empty();
        }

        @Override
        public List<String> readableRoots() {
            return List.of("/nowhere");
        }
    }

    @Test
    void anExpiredInventoryRefreshesOnceWithDirectoryTablesAndCatalogs() throws Exception {
        Path table = Files.createDirectories(dir.resolve("data/dirs/t"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1 AS id) TO '" + table.resolve("p.parquet") + "' (FORMAT parquet)");
        }
        String data = dir.resolve("data").toString();
        AppConfig config = new AppConfig(
                3, 4096, 4, 65536, 2, 2, 2, null, false,
                null, null, null, null, null, false, 2_147_483_648L, 67108864, 67108864, 30000L,
                data, null, 31001, 5701, 120, 3, 500, 0, true, false, 64, null, null, "complete",
                true, true);
        CatalogTables catalogs = new CatalogTables(List.of(new CatalogTables.Entry(
                new SpiCatalog(new OneSchema(), null, "fake"), List.of("/nowhere"), false)));
        ParquetAdapter adapter = new ParquetAdapter(config, new HadoopDataStore(
                FileSystem.getLocal(new Configuration()), data, "localhost"), "localhost", catalogs);
        assertTrue(adapter.isCatalogSchema("cat"));
        adapter.expireCatalog();
        assertFalse(adapter.isCatalogSchema("dirs"));
        assertTrue(adapter.isCatalogSchema("cat"));
        assertEquals(Set.of("dirs", "cat"), adapter.getSchemas(null).keySet());
    }
}
