package pro.surpin.data.arrowflight.spi.catalog;

import java.io.IOException;
import java.util.List;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;

/**
 * Opens a table catalog, chosen by name with {@code catalog=}.
 */
public interface CatalogProvider {

    /**
     * Names the catalog kind; also the namespace of its configuration keys.
     *
     * @return name, for example "directory", "hms", "iceberg-rest"
     */
    String name();

    /**
     * Declares the configuration keys of this provider.
     *
     * @return configuration keys
     */
    List<ConfigKey> configKeys();

    /**
     * Opens the catalog.
     *
     * @param context what the server hands the provider
     * @return open catalog; the server closes it
     * @throws IOException when the catalog cannot be reached
     */
    Catalog open(CatalogContext context) throws IOException;
}
