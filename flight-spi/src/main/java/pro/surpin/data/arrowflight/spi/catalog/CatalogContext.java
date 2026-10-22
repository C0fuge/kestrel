package pro.surpin.data.arrowflight.spi.catalog;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;

import pro.surpin.data.arrowflight.spi.config.PluginConfig;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * What the server hands a catalog provider when it opens a catalog.
 */
public interface CatalogContext {

    /**
     * Returns the provider's configuration (namespace = provider name).
     *
     * @return configuration
     */
    PluginConfig config();

    /**
     * Returns the open stores; every file access of a catalog goes through them.
     *
     * @return stores
     */
    Stores stores();

    /**
     * Returns the redactor that removes registered secret values from errors and logs.
     *
     * @return redactor
     */
    SecretRedactor redactor();

    /**
     * Names this server node, for logs.
     *
     * @return node name
     */
    String nodeName();

    /**
     * Creates daemon threads whose context class loader is the plugin's loader.
     *
     * @param name thread name
     * @return thread factory
     */
    ThreadFactory threadFactory(String name);

    /**
     * Opens the table format of another plugin, for tables a catalog lists but whose files that
     * plugin plans, such as Iceberg tables registered in a Hive Metastore. Only while the catalog
     * opens; the catalog closes the format when it closes.
     *
     * @param name format name, for example "iceberg"
     * @return a new instance of the format, empty when no installed plugin provides it
     * @throws IOException when the format's plugin is installed but cannot open it
     * @since SPI 1.1
     */
    default Optional<TableFormat> tableFormat(String name) throws IOException {
        return Optional.empty();
    }
}
