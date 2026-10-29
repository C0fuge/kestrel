package pro.surpin.data.arrowflight.plugin.jdbc;

import java.util.List;

import pro.surpin.data.arrowflight.spi.Plugin;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;

/**
 * The jdbc plugin: tables of PostgreSQL and MySQL databases as a table catalog of the server
 * ({@code catalog=jdbc}), read by DuckDB's postgres / mysql scanner extensions, read-only.
 */
public final class JdbcPlugin implements Plugin {

    @Override
    public List<CatalogProvider> catalogProviders() {
        return List.of(new JdbcCatalogProvider());
    }
}
