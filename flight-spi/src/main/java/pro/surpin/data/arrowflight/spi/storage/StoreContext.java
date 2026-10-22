package pro.surpin.data.arrowflight.spi.storage;

import java.util.concurrent.ThreadFactory;

import pro.surpin.data.arrowflight.spi.config.PluginConfig;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * What the server hands a storage provider when it opens a store.
 */
public interface StoreContext {

    /**
     * Returns the provider's configuration (namespace = provider name).
     *
     * @return configuration
     */
    PluginConfig config();

    /**
     * Names this server node, for logs.
     *
     * @return node name
     */
    String nodeName();

    /**
     * Returns the storage host colocated with this server.
     *
     * @return storage host
     */
    String storageHost();

    /**
     * Returns the server's I/O parallelism, a sensible number of connections.
     *
     * @return I/O threads of the server
     */
    int ioParallelism();

    /**
     * Returns the redactor that removes registered secret values from errors and logs.
     *
     * @return redactor
     */
    SecretRedactor redactor();

    /**
     * Creates daemon threads whose context class loader is the plugin's loader.
     *
     * @param name thread name
     * @return thread factory
     */
    ThreadFactory threadFactory(String name);
}
