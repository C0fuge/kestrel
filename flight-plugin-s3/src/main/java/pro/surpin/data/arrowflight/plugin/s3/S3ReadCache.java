package pro.surpin.data.arrowflight.plugin.s3;

import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.storage.StoreContext;

/**
 * One store's way through the node-wide S3 read cache, for plugins built on this one (lakefs):
 * opens the route from the plugin's {@code cacheMegabytes} / {@code cacheCheckSeconds} /
 * {@code cacheAdmission} settings
 * and describes the DuckDB secret that reads through it.
 */
public final class S3ReadCache implements AutoCloseable {

    /** Size of the read cache in MiB; 0 = off. */
    public static final String CACHE_MEGABYTES_KEY = "cacheMegabytes";
    /** Seconds the read cache trusts an object's version before it looks it up again. */
    public static final String CACHE_CHECK_SECONDS_KEY = "cacheCheckSeconds";
    /** Default of {@link #CACHE_CHECK_SECONDS_KEY}. */
    public static final int DEFAULT_CACHE_CHECK_SECONDS = 10;
    /** Which missed blocks the read cache keeps: {@code always} or {@code reuse}. */
    public static final String CACHE_ADMISSION_KEY = "cacheAdmission";
    /** The read cache keys of a plugin namespace. */
    public static final List<ConfigKey> CONFIG_KEYS = List.of(
            ConfigKey.of(CACHE_MEGABYTES_KEY, "MiB of the node-wide S3 read cache DuckDB reads through"
                    + " (JVM heap, at most half of it); 0 = off, DuckDB reads S3 directly", "0"),
            ConfigKey.of(CACHE_CHECK_SECONDS_KEY, "Seconds the read cache trusts an object's version"
                    + " before it checks it on S3 again", Integer.toString(DEFAULT_CACHE_CHECK_SECONDS)),
            ConfigKey.of(CACHE_ADMISSION_KEY, "Which blocks read from S3 the read cache keeps: always, or"
                    + " reuse = only a block read again soon (scans larger than the cache pass through)",
                    "always"));

    private final S3CacheEndpoint.Route route;

    private S3ReadCache(S3CacheEndpoint.Route route) {
        this.route = Objects.requireNonNull(route, "route");
    }

    /**
     * Opens a store's route through the read cache when the plugin's settings turn it on; the
     * route's secret is added to the redactor.
     *
     * @param context what the server hands the provider; its config holds the cache settings
     * @param kind plugin name ({@code s3}, {@code lakefs}): names the settings, logs and metrics
     * @param root what the route may read
     * @param client S3 client with the store's credentials; not closed by the cache
     * @param settings store settings
     * @return the route, or empty when {@code cacheMegabytes} is 0
     * @throws IllegalArgumentException when a setting is out of range
     * @throws IllegalStateException when the cache endpoint cannot be started
     */
    public static Optional<S3ReadCache> open(StoreContext context, String kind, S3Uri root, S3Client client,
            S3Settings settings) {
        long bytes = cacheBytes(kind, context.config().getInt(CACHE_MEGABYTES_KEY, 0),
                Runtime.getRuntime().maxMemory());
        if (bytes == 0) {
            return Optional.empty();
        }
        boolean admitOnReuse = admitOnReuse(kind, context.config().get(CACHE_ADMISSION_KEY, "always"));
        S3CacheEndpoint.Route route;
        try {
            route = S3CacheEndpoint.register(bytes,
                    context.config().getInt(CACHE_CHECK_SECONDS_KEY, DEFAULT_CACHE_CHECK_SECONDS),
                    context.nodeName(), root, client, settings, context.metrics(), kind,
                    admitOnReuse);
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("Cannot start the S3 read cache: " + e.getMessage(), e);
        }
        context.redactor().add(route.secret());
        return Optional.of(new S3ReadCache(route));
    }

    /**
     * Checks the size of a read cache. Its blocks live on the JVM heap, so it may take at most
     * half of the heap.
     *
     * @param kind plugin name, for the message
     * @param megabytes {@code arrowflight.<kind>.cacheMegabytes}, 0 = off
     * @param maxHeapBytes largest heap of the JVM
     * @return cache size in bytes, 0 = off
     * @throws IllegalArgumentException when the size is negative or above half the heap
     */
    static long cacheBytes(String kind, int megabytes, long maxHeapBytes) {
        String key = "arrowflight." + kind + "." + CACHE_MEGABYTES_KEY;
        if (megabytes < 0) {
            throw new IllegalArgumentException(key + " must be >= 0: " + megabytes);
        }
        long bytes = (long) megabytes << 20;
        if (bytes > maxHeapBytes / 2) {
            throw new IllegalArgumentException(key + "=" + megabytes + " is more than half the JVM heap ("
                    + (maxHeapBytes >> 20) + " MiB): the cache lives on the heap; raise -Xmx or lower the cache");
        }
        return bytes;
    }

    /**
     * Reads the read cache's admission setting.
     *
     * @param kind plugin name, for the message
     * @param value {@code arrowflight.<kind>.cacheAdmission}
     * @return true for {@code reuse}, false for {@code always}
     * @throws IllegalArgumentException for any other value
     */
    static boolean admitOnReuse(String kind, String value) {
        String v = value == null ? "always" : value.trim().toLowerCase(Locale.ROOT);
        if ("always".equals(v) || v.isEmpty()) {
            return false;
        }
        if ("reuse".equals(v)) {
            return true;
        }
        throw new IllegalArgumentException("arrowflight." + kind + "." + CACHE_ADMISSION_KEY
                + " must be always or reuse: " + value);
    }

    /**
     * Returns the route this handle holds.
     *
     * @return route
     */
    S3CacheEndpoint.Route route() {
        return route;
    }

    /**
     * Describes the DuckDB setup that reads a root through the cache: the settings of
     * {@link S3DataStore#contribute(DuckDbSetup, S3Settings, S3Uri)}, with the secret pointing at
     * the cache's loopback endpoint and holding the route's random key.
     *
     * @param setup receives the extension, settings and secret
     * @param settings store settings
     * @param root scope of the secret
     */
    public void contribute(DuckDbSetup setup, S3Settings settings, S3Uri root) {
        S3DataStore.contributeCached(setup, settings, root, S3DataStore.secretName(""), route);
    }

    /** Unregisters the route; the last one stops the endpoint. */
    @Override
    public void close() {
        route.close();
    }
}
