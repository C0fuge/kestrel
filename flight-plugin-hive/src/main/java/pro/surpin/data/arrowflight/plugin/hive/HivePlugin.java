package pro.surpin.data.arrowflight.plugin.hive;

import java.util.List;

import pro.surpin.data.arrowflight.spi.Plugin;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;

/**
 * The hive plugin: the tables of a Hive Metastore as a table catalog of the server
 * ({@code catalog=hive}).
 */
public final class HivePlugin implements Plugin {

    @Override
    public List<CatalogProvider> catalogProviders() {
        return List.of(new HiveCatalogProvider());
    }
}
