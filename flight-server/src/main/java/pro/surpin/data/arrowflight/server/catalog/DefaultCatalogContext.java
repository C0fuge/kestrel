package pro.surpin.data.arrowflight.server.catalog;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;

import pro.surpin.data.arrowflight.server.plugin.PluginThreadFactory;
import pro.surpin.data.arrowflight.spi.catalog.CatalogContext;
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
 */
public record DefaultCatalogContext(PluginConfig config, Stores stores, SecretRedactor redactor,
        String nodeName, ClassLoader loader) implements CatalogContext {

    /**
     * Checks the parts.
     *
     * @param config provider configuration
     * @param stores stores
     * @param redactor redactor
     * @param nodeName node name
     * @param loader plugin loader
     */
    public DefaultCatalogContext {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(stores, "stores");
        Objects.requireNonNull(redactor, "redactor");
        Objects.requireNonNull(nodeName, "nodeName");
    }

    @Override
    public ThreadFactory threadFactory(String name) {
        return new PluginThreadFactory(name, loader);
    }
}
