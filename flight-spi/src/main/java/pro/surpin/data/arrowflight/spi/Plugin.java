package pro.surpin.data.arrowflight.spi;

import java.util.List;

import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;
import pro.surpin.data.arrowflight.spi.catalog.TableFormatProvider;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;

/**
 * Entry point of a plugin, found with {@link java.util.ServiceLoader} in the plugin's own class
 * loader ({@code META-INF/services/pro.surpin.data.arrowflight.spi.Plugin}). Exactly one per
 * plugin directory. The server creates it only when its configuration needs the plugin, and
 * calls it with the thread context class loader set to the plugin's loader.
 */
public interface Plugin {

    /**
     * Lists the storage providers of this plugin, each chosen by URI scheme.
     *
     * @return storage providers, empty by default
     */
    default List<DataStoreProvider> dataStoreProviders() {
        return List.of();
    }

    /**
     * Lists the table catalog providers of this plugin, each chosen by name ({@code catalog=}).
     *
     * @return catalog providers, empty by default
     */
    default List<CatalogProvider> catalogProviders() {
        return List.of();
    }

    /**
     * Lists the table formats of this plugin that catalogs of other plugins may use, each chosen
     * by name ({@code provides.format} in the descriptor).
     *
     * @return table format providers, empty by default
     * @since SPI 1.1
     */
    default List<TableFormatProvider> tableFormatProviders() {
        return List.of();
    }
}
