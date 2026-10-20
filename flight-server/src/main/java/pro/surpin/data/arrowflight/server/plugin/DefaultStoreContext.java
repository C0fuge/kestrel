package pro.surpin.data.arrowflight.server.plugin;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;

import pro.surpin.data.arrowflight.spi.config.PluginConfig;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.storage.StoreContext;

/**
 * What the server hands a storage provider.
 *
 * @param config the provider's configuration
 * @param nodeName this node, for logs
 * @param storageHost storage host colocated with this server
 * @param ioParallelism I/O threads of the server
 * @param redactor the server's redactor
 * @param loader the provider's plugin loader, or null for a built-in provider
 */
public record DefaultStoreContext(PluginConfig config, String nodeName, String storageHost,
        int ioParallelism, SecretRedactor redactor, ClassLoader loader) implements StoreContext {

    /**
     * Checks the parts.
     *
     * @param config configuration
     * @param nodeName node name
     * @param storageHost storage host
     * @param ioParallelism I/O threads
     * @param redactor redactor
     * @param loader plugin loader or null
     */
    public DefaultStoreContext {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(nodeName, "nodeName");
        Objects.requireNonNull(redactor, "redactor");
    }

    @Override
    public ThreadFactory threadFactory(String name) {
        return new PluginThreadFactory(name, loader);
    }
}
