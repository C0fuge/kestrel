package pro.surpin.data.arrowflight.spi.metrics;

/**
 * Where a plugin publishes metrics: the server's Prometheus endpoint shows the samples of every
 * registered source. A name the server or another source already publishes is left out.
 *
 * @since SPI 1.3
 */
public interface PluginMetrics {

    /**
     * Adds a source.
     *
     * @param source source read on every scrape
     * @return handle; closing it removes the source
     */
    Registration register(MetricSource source);

    /**
     * Returns metrics that publish nothing, for servers and tests without an endpoint.
     *
     * @return a sink that drops every source
     */
    static PluginMetrics none() {
        return source -> () -> { };
    }

    /** Removes a registered source when closed. */
    interface Registration extends AutoCloseable {

        /** Removes the source; closing twice does nothing. */
        @Override
        void close();
    }
}
