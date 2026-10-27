package pro.surpin.data.arrowflight.spi.metrics;

import java.util.List;

/**
 * Supplies a plugin's current metric values; read on every scrape of the server's Prometheus
 * endpoint, so it must be cheap and must not block.
 *
 * @since SPI 1.3
 */
@FunctionalInterface
public interface MetricSource {

    /**
     * Returns the current values.
     *
     * @return samples, each name at most once
     */
    List<MetricSample> samples();
}
