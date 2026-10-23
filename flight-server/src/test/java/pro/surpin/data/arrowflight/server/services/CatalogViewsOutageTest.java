package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.server.catalog.SpiCatalog;

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

    /** Verifies one refresh asks a failing catalog once, not once per schema and table. */
    @Test
    void aFailingCatalogIsAskedOncePerRefresh() throws Exception {
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        CatalogTables catalogs = mock(CatalogTables.class);
        CatalogTables.Entry hive = new CatalogTables.Entry(mock(SpiCatalog.class), List.of(), true);
        when(parquet.catalogTables()).thenReturn(catalogs);
        when(catalogs.owner(anyString())).thenReturn(Optional.of(hive));
        Map<String, String> schemas = new java.util.LinkedHashMap<>();
        for (String schema : List.of("a", "b", "c")) {
            schemas.put(schema, "catalog:hive");
            when(parquet.isCatalogSchema(schema)).thenReturn(true);
        }
        when(parquet.getSchemas(null)).thenReturn(schemas);
        when(parquet.getTables(anyString(), isNull())).thenThrow(new UncheckedIOException(
                new IOException("Hive Metastore unavailable: thrift://hms:9083")));
        CatalogViews views = new CatalogViews(parquet, mock(DuckDbAdapter.class), true);
        views.current();
        verify(parquet, times(1)).getTables(anyString(), isNull());
        for (String schema : List.of("a", "b", "c")) {
            assertEquals("java.io.IOException: Hive Metastore unavailable: thrift://hms:9083",
                    views.unavailableCatalog("SELECT * FROM " + schema + ".t"), schema);
        }
    }
}
