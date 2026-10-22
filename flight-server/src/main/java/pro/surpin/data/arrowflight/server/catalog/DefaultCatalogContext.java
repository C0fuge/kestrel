package pro.surpin.data.arrowflight.server.catalog;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;

import pro.surpin.data.arrowflight.server.plugin.PluginThreadFactory;
import pro.surpin.data.arrowflight.spi.catalog.CatalogContext;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.config.PluginConfig;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * What the server hands a catalog provider.
 *
 * @param config provider configuration
 * @param stores the server's stores
 * @param redactor redactor of secrets
 * @param nodeName this node, for logs
 * @param loader the plugin's class loader, for its threads
 * @param formats opens the table formats of other plugins
 */
public record DefaultCatalogContext(PluginConfig config, Stores stores, SecretRedactor redactor,
        String nodeName, ClassLoader loader, Formats formats) implements CatalogContext {

    /** Opens table formats by name. */
    @FunctionalInterface
    public interface Formats {

        /**
         * Opens a new instance of a table format.
         *
         * @param name format name
         * @return the format, empty when no plugin provides it
         * @throws IOException when it cannot be opened
         */
        Optional<TableFormat> open(String name) throws IOException;

        /**
         * Returns lookups that find no format.
         *
         * @return no formats
         */
        static Formats none() {
            return name -> Optional.empty();
        }
    }

    /**
     * Checks the parts.
     *
     * @param config provider configuration
     * @param stores stores
     * @param redactor redactor
     * @param nodeName node name
     * @param loader plugin loader
     * @param formats table formats
     */
    public DefaultCatalogContext {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(stores, "stores");
        Objects.requireNonNull(redactor, "redactor");
        Objects.requireNonNull(nodeName, "nodeName");
        formats = formats == null ? Formats.none() : formats;
    }

    @Override
    public ThreadFactory threadFactory(String name) {
        return new PluginThreadFactory(name, loader);
    }

    @Override
    public Optional<TableFormat> tableFormat(String name) throws IOException {
        return formats.open(name);
    }
}
