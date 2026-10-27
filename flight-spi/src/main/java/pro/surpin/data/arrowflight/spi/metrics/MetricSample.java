package pro.surpin.data.arrowflight.spi.metrics;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * One value a plugin publishes on the server's Prometheus endpoint, without labels.
 *
 * @param name metric name, {@code arrowflight_} followed by lower-case letters, digits and
 *             underscores; a counter's name ends in {@code _total}
 * @param kind counter or gauge
 * @param help one-line description
 * @param value current value
 * @since SPI 1.3
 */
public record MetricSample(String name, Kind kind, String help, double value) {

    private static final Pattern NAME = Pattern.compile("arrowflight_[a-z0-9_]+");

    /** Prometheus metric types a plugin may publish. */
    public enum Kind {
        /** Only grows while the process runs. */
        COUNTER,
        /** Goes up and down. */
        GAUGE
    }

    /**
     * Checks the parts.
     *
     * @param name metric name
     * @param kind counter or gauge
     * @param help description
     * @param value value
     * @throws IllegalArgumentException when the name is not an {@code arrowflight_} name, a
     *         counter's name does not end in {@code _total}, or the help has a line break
     */
    public MetricSample {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(help, "help");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("metric name must match arrowflight_[a-z0-9_]+: " + name);
        }
        if (kind == Kind.COUNTER && !name.endsWith("_total")) {
            throw new IllegalArgumentException("counter name must end in _total: " + name);
        }
        if (help.indexOf('\n') >= 0 || help.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("metric help must be one line: " + name);
        }
    }

    /**
     * Creates a counter sample.
     *
     * @param name metric name ending in {@code _total}
     * @param help description
     * @param value value
     * @return sample
     */
    public static MetricSample counter(String name, String help, double value) {
        return new MetricSample(name, Kind.COUNTER, help, value);
    }

    /**
     * Creates a gauge sample.
     *
     * @param name metric name
     * @param help description
     * @param value value
     * @return sample
     */
    public static MetricSample gauge(String name, String help, double value) {
        return new MetricSample(name, Kind.GAUGE, help, value);
    }
}
