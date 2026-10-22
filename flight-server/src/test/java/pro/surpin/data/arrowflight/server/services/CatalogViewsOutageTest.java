package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;

/**
 * A catalog schema whose catalog cannot be read when the views are first made has no views;
 * queries naming it learn the catalog's error instead of "table does not exist".
 */
class CatalogViewsOutageTest {

    @Test
    void aSchemaWithoutViewsNamesItsCatalogsError() throws Exception {
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.getSchemas(null)).thenReturn(Map.of("hv", "catalog:hive"));
        when(parquet.isCatalogSchema("hv")).thenReturn(true);
        when(parquet.getTables("hv", null)).thenThrow(new UncheckedIOException(
                new IOException("Hive Metastore unavailable: thrift://hms:9083")));
        CatalogViews views = new CatalogViews(parquet, mock(DuckDbAdapter.class), true);
        views.current();
        assertEquals("java.io.IOException: Hive Metastore unavailable: thrift://hms:9083",
                views.unavailableCatalog("SELECT count(*) FROM HV.nation"));
        assertEquals("java.io.IOException: Hive Metastore unavailable: thrift://hms:9083",
                views.unavailableCatalog("SELECT * FROM \"hv\" . t"));
        assertNull(views.unavailableCatalog("SELECT * FROM xhv.t"));
        assertNull(views.unavailableCatalog("SELECT * FROM dirs.t"));
    }
}
