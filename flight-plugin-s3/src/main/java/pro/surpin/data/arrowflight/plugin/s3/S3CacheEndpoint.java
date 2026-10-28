package pro.surpin.data.arrowflight.plugin.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.spi.metrics.MetricSample;
import pro.surpin.data.arrowflight.spi.metrics.PluginMetrics;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Node-wide S3 read cache for DuckDB (item 17, option A): an HTTP endpoint on 127.0.0.1 that
 * DuckDB's httpfs reads {@code s3://} through, answering ranged GETs from one byte-bounded
 * {@link S3BlockCache} shared by every DuckDB connection of the process, and reading misses from
 * S3 with the store's own client. Credentials stay in Java: DuckDB signs its requests with a
 * random per-route key the endpoint checks (SigV4), and a route reads only its own data directory.
 *
 * <p>Each object's version (ETag) is checked against S3 with a HEAD at most every
 * {@code checkSeconds}; a block missing from the cache is read with {@code If-Match} on that
 * version and the answer's ETag is compared with it (a gateway may ignore If-Match), so the bytes
 * of one answer always come from one version of an object. A block that
 * one request is already reading from S3 is not read again by a concurrent one: that request
 * waits for the first read (single flight). HEADs from DuckDB always go to S3. Listings and other
 * requests with a query are not served. With admission on reuse, a block is kept only when it
 * was missed before within the cache's reach (see {@link S3BlockCache}), so a scan larger than
 * the cache passes through without pushing out what small queries read again. Missed blocks are
 * streamed from S3 straight into the cache's pieces, never buffered whole per GET. There is one
 * endpoint per plugin class loader: the lakefs plugin, which bundles this code, runs its own
 * ({@code arrowflight_lakefs_cache_*}). With a disk tier ({@link S3DiskCache}), blocks read from S3
 * are also written to a local directory and a block missing from memory is read from its file
 * (and kept in memory again) before S3 is asked.
 */
final class S3CacheEndpoint {

    /** Bytes per cached block; requests are read from S3 in whole blocks. */
    static final int BLOCK_BYTES = 1 << 20;
    /** Most blocks read from S3 in one GET. */
    private static final int MAX_BLOCKS_PER_READ = 64;
    /** Most objects whose version is remembered; beyond, all are forgotten and looked up again. */
    private static final int MAX_VERSIONS = 100_000;
    private static final Logger LOGGER = LoggerFactory.getLogger(S3CacheEndpoint.class);
    private static final DateTimeFormatter HTTP_DATE = DateTimeFormatter.RFC_1123_DATE_TIME;
    private static final SecureRandom RANDOM = new SecureRandom();

    private static S3CacheEndpoint shared;

    private final HttpServer server;
    private final ExecutorService handlers;
    private final S3BlockCache cache;
    /** Disk tier, or null. */
    private final S3DiskCache disk;
    private final long checkNanos;
    private final LongSupplier nanoClock;
    private final String node;
    private final String kind;
    private final Map<String, Route> routes = new ConcurrentHashMap<>();
    private final Map<String, Version> versions = new ConcurrentHashMap<>();
    private final Map<S3BlockCache.BlockKey, CompletableFuture<S3BlockCache.Block>> reading = new ConcurrentHashMap<>();
    /** Held while a request claims the missing blocks of its span, so a span is claimed whole. */
    private final Object claiming = new Object();
    private final AtomicLong joined = new AtomicLong();
    private final AtomicLong s3Reads = new AtomicLong();
    private final AtomicLong s3ReadBytes = new AtomicLong();
    private final AtomicLong s3Lookups = new AtomicLong();
    private PluginMetrics.Registration metrics;

    /**
     * What one data directory reads through the endpoint, and the key DuckDB signs with.
     *
     * @param endpoint endpoint this route belongs to
     * @param keyId access key ID DuckDB sends
     * @param secret secret key DuckDB signs with
     * @param root data directory the route may read
     * @param client S3 client with the store's credentials
     * @param settings store settings, to redact errors
     */
    record Route(S3CacheEndpoint endpoint, String keyId, String secret, S3Uri root, S3Client client,
            S3Settings settings) implements AutoCloseable {

        /**
         * Names the endpoint the way a DuckDB S3 secret wants it.
         *
         * @return {@code 127.0.0.1:<port>}
         */
        String duckDbEndpoint() {
            return "127.0.0.1:" + endpoint.port();
        }

        @Override
        public void close() {
            endpoint.release(this);
        }

        @Override
        public String toString() {
            return "Route[keyId=" + keyId + ", root=" + root.root() + "]";
        }
    }

    /**
     * Version of an object as last looked up.
     *
     * @param etag ETag, quoted as S3 sends it
     * @param size size in bytes
     * @param lastModified Last-Modified header value
     * @param checkedNanos monotonic time of the lookup
     */
    private record Version(String etag, long size, String lastModified, long checkedNanos) {
    }

    /** The object changed while it was read (If-Match refused). */
    private static final class ChangedException extends Exception {
        private static final long serialVersionUID = 1L;

        ChangedException() {
            super(null, null, false, false);
        }
    }

    /**
     * Starts an endpoint.
     *
     * @param capacityBytes bytes the cache holds at most
     * @param checkSeconds seconds an object's version is trusted before it is looked up again
     * @param node node name, for logs
     * @param nanoClock monotonic clock in nanoseconds
     * @throws IOException when no loopback port can be bound
     */
    S3CacheEndpoint(long capacityBytes, int checkSeconds, String node, LongSupplier nanoClock) throws IOException {
        this(capacityBytes, checkSeconds, node, nanoClock, "s3");
    }

    /**
     * Starts an endpoint of a plugin.
     *
     * @param capacityBytes bytes the cache holds at most
     * @param checkSeconds seconds an object's version is trusted before it is looked up again
     * @param node node name, for logs
     * @param nanoClock monotonic clock in nanoseconds
     * @param kind plugin whose stores read through it ({@code s3}, {@code lakefs}): names its log
     *        lines, settings and metrics
     * @throws IOException when no loopback port can be bound
     */
    S3CacheEndpoint(long capacityBytes, int checkSeconds, String node, LongSupplier nanoClock, String kind)
            throws IOException {
        this(capacityBytes, checkSeconds, node, nanoClock, kind, false);
    }

    /**
     * Starts an endpoint of a plugin, choosing how blocks are admitted.
     *
     * @param capacityBytes bytes the cache holds at most
     * @param checkSeconds seconds an object's version is trusted before it is looked up again
     * @param node node name, for logs
     * @param nanoClock monotonic clock in nanoseconds
     * @param kind plugin whose stores read through it ({@code s3}, {@code lakefs}): names its log
     *        lines, settings and metrics
     * @param admitOnReuse keep a missed block only when it was missed before within the cache's
     *        reach; false keeps every block read
     * @throws IOException when no loopback port can be bound
     */
    S3CacheEndpoint(long capacityBytes, int checkSeconds, String node, LongSupplier nanoClock, String kind,
            boolean admitOnReuse) throws IOException {
        this(capacityBytes, checkSeconds, node, nanoClock, kind, admitOnReuse, null);
    }

    /**
     * Starts an endpoint of a plugin, choosing how blocks are admitted and whether a disk tier
     * backs the memory.
     *
     * @param capacityBytes bytes the cache holds at most in memory
     * @param checkSeconds seconds an object's version is trusted before it is looked up again
     * @param node node name, for logs
     * @param nanoClock monotonic clock in nanoseconds
     * @param kind plugin whose stores read through it ({@code s3}, {@code lakefs}): names its log
     *        lines, settings and metrics
     * @param admitOnReuse keep a missed block only when it was missed before within the cache's
     *        reach (each tier its own); false keeps every block read
     * @param diskSettings directory and size of the disk tier, or null for none
     * @throws IOException when no loopback port can be bound or the disk tier cannot be opened
     */
    S3CacheEndpoint(long capacityBytes, int checkSeconds, String node, LongSupplier nanoClock, String kind,
            boolean admitOnReuse, S3DiskCache.Settings diskSettings) throws IOException {
        this.kind = Objects.requireNonNull(kind, "kind");
        if (checkSeconds < 0) {
            throw new IllegalArgumentException("arrowflight." + kind + ".cacheCheckSeconds must be >= 0: "
                    + checkSeconds);
        }
        this.cache = new S3BlockCache(capacityBytes,
                admitOnReuse ? (int) Math.max(1, Math.min(Integer.MAX_VALUE, capacityBytes / BLOCK_BYTES)) : 0);
        this.checkNanos = TimeUnit.SECONDS.toNanos(checkSeconds);
        this.node = Objects.requireNonNull(node, "node");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.disk = diskSettings == null ? null : new S3DiskCache(diskSettings, kind, admitOnReuse);
        try {
            this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}),
                    0), 64);
        } catch (IOException | RuntimeException e) {
            if (disk != null) {
                disk.close();
            }
            throw e;
        }
        this.handlers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "s3-cache");
            t.setDaemon(true);
            return t;
        });
        server.createContext("/", this::handle);
        server.setExecutor(handlers);
        server.start();
    }

    /**
     * Registers a data directory with the process-wide endpoint, starting it on first use. The
     * first registration sets the capacity and check interval; later ones with other values get
     * a WARN and use the running endpoint.
     *
     * @param capacityBytes bytes the cache holds at most
     * @param checkSeconds seconds an object's version is trusted
     * @param node node name, for logs
     * @param root data directory
     * @param client S3 client of the store
     * @param settings store settings
     * @param pluginMetrics where the endpoint publishes its metrics while it runs
     * @return route; closing it unregisters the data directory, the last close stops the endpoint
     * @throws IOException when the endpoint cannot be started
     */
    static Route register(long capacityBytes, int checkSeconds, String node, S3Uri root,
            S3Client client, S3Settings settings, PluginMetrics pluginMetrics) throws IOException {
        return register(capacityBytes, checkSeconds, node, root, client, settings, pluginMetrics, "s3", false, null);
    }

    /**
     * Registers a data directory with the endpoint of this plugin's class loader; see
     * {@link #register(long, int, String, S3Uri, S3Client, S3Settings, PluginMetrics)}. The first
     * registration also sets the admission.
     *
     * @param capacityBytes bytes the cache holds at most
     * @param checkSeconds seconds an object's version is trusted
     * @param node node name, for logs
     * @param root data directory
     * @param client S3 client of the store
     * @param settings store settings
     * @param pluginMetrics where the endpoint publishes its metrics while it runs
     * @param kind plugin of the store ({@code s3}, {@code lakefs}), set by the first registration
     * @param admitOnReuse keep a missed block only on its second miss within the cache's reach
     * @param diskSettings directory and size of the disk tier, or null for none; set by the first
     *        registration
     * @return route; closing it unregisters the data directory, the last close stops the endpoint
     * @throws IOException when the endpoint cannot be started
     */
    static synchronized Route register(long capacityBytes, int checkSeconds, String node, S3Uri root,
            S3Client client, S3Settings settings, PluginMetrics pluginMetrics, String kind, boolean admitOnReuse,
            S3DiskCache.Settings diskSettings) throws IOException {
        if (shared == null) {
            shared = new S3CacheEndpoint(capacityBytes, checkSeconds, node, System::nanoTime, kind, admitOnReuse,
                    diskSettings);
            shared.metrics = pluginMetrics.register(shared::samples);
            LOGGER.info("node={} {}=cacheOpen endpoint=127.0.0.1:{} capacityBytes={} blockBytes={} checkSeconds={}"
                    + " admitOnReuse={} diskPath={} diskCapacityBytes={}", node, kind, shared.port(), capacityBytes,
                    BLOCK_BYTES, checkSeconds, admitOnReuse, diskSettings == null ? "" : diskSettings.path(),
                    diskSettings == null ? 0 : diskSettings.capacityBytes());
        } else if (shared.cache.capacityBytes() != capacityBytes
                || shared.checkNanos != TimeUnit.SECONDS.toNanos(checkSeconds)
                || shared.cache.admitOnReuse() != admitOnReuse
                || !Objects.equals(shared.disk == null ? null : shared.disk.settings(), diskSettings)) {
            LOGGER.warn("node={} {}=cacheSettingsIgnored capacityBytes={} checkSeconds={} admitOnReuse={}"
                    + " disk={} (the running cache keeps its own)", node, kind, capacityBytes, checkSeconds,
                    admitOnReuse, diskSettings);
        }
        return shared.route(root, client, settings);
    }

    /**
     * Adds a route with a fresh random key.
     *
     * @param root data directory
     * @param client S3 client of the store
     * @param settings store settings
     * @return route
     */
    Route route(S3Uri root, S3Client client, S3Settings settings) {
        Route route = new Route(this, "FLIGHTCACHE" + randomHex(8).toUpperCase(Locale.ROOT),
                randomHex(32), root, client, settings);
        routes.put(route.keyId(), route);
        return route;
    }

    /**
     * Removes a route; stops the shared endpoint after its last route.
     *
     * @param route route to remove
     */
    private void release(Route route) {
        synchronized (S3CacheEndpoint.class) {
            if (routes.remove(route.keyId()) != null && routes.isEmpty() && shared == this) {
                shared = null;
                close();
            }
        }
    }

    /**
     * Returns the port the endpoint listens on.
     *
     * @return loopback port
     */
    int port() {
        return server.getAddress().getPort();
    }

    /**
     * Returns the disk tier.
     *
     * @return disk tier, or null when the cache is in memory only
     */
    S3DiskCache disk() {
        return disk;
    }

    /**
     * Describes the cache's use so far.
     *
     * @return statistics line; {@code joined} counts blocks a request took from another request's
     *         read instead of reading them from S3 itself
     */
    String stats() {
        return cache.stats() + " joined=" + joined.get() + (disk == null ? "" : " " + disk.stats());
    }

    /**
     * Returns the endpoint's metrics for the server's Prometheus endpoint.
     *
     * @return cache size, block counts and the reads sent to S3
     */
    List<MetricSample> samples() {
        S3BlockCache.Use use = cache.use();
        String prefix = "arrowflight_" + kind + "_cache_";
        List<MetricSample> samples = new ArrayList<>(List.of(
                MetricSample.gauge(prefix + "capacity_bytes",
                        "Most bytes the S3 read cache holds", cache.capacityBytes()),
                MetricSample.gauge(prefix + "used_bytes",
                        "Bytes held by the S3 read cache", use.usedBytes()),
                MetricSample.gauge(prefix + "blocks",
                        "Blocks held by the S3 read cache", use.blocks()),
                MetricSample.counter(prefix + "hits_total",
                        "Blocks the S3 read cache answered from memory", use.hits()),
                MetricSample.counter(prefix + "misses_total",
                        "Blocks the S3 read cache did not hold when asked", use.misses()),
                MetricSample.counter(prefix + "evictions_total",
                        "Blocks dropped from the S3 read cache to make room", use.evictions()),
                MetricSample.counter(prefix + "joined_total",
                        "Missed blocks taken from another request's S3 read", joined.get()),
                MetricSample.counter(prefix + "s3_reads_total",
                        "GET requests the S3 read cache sent to S3", s3Reads.get()),
                MetricSample.counter(prefix + "s3_read_bytes_total",
                        "Bytes the S3 read cache read from S3", s3ReadBytes.get()),
                MetricSample.counter(prefix + "s3_lookups_total",
                        "HEAD requests the S3 read cache sent to S3", s3Lookups.get()),
                MetricSample.counter(prefix + "not_admitted_total",
                        "Missed blocks the S3 read cache read but did not keep (admission on reuse)",
                        use.notAdmitted())));
        if (disk != null) {
            S3DiskCache.Use d = disk.use();
            String p = prefix + "disk_";
            samples.addAll(List.of(
                    MetricSample.gauge(p + "capacity_bytes", "Most bytes the S3 read cache's disk tier holds",
                            d.capacityBytes()),
                    MetricSample.gauge(p + "used_bytes", "Bytes of block files in the disk tier", d.usedBytes()),
                    MetricSample.gauge(p + "blocks", "Block files in the disk tier", d.blocks()),
                    MetricSample.counter(p + "hits_total", "Blocks read from the disk tier instead of S3", d.hits()),
                    MetricSample.counter(p + "misses_total", "Blocks the disk tier did not hold when asked",
                            d.misses()),
                    MetricSample.counter(p + "writes_total", "Blocks written to the disk tier", d.writes()),
                    MetricSample.counter(p + "evictions_total", "Block files deleted to make room", d.evictions()),
                    MetricSample.counter(p + "dropped_total",
                            "Blocks not written because the disk tier's write queue was full", d.dropped()),
                    MetricSample.counter(p + "errors_total",
                            "Block files that failed to write or read or did not match their checksum", d.errors()),
                    MetricSample.counter(p + "not_admitted_total",
                            "Blocks the disk tier did not write (admission on reuse)", d.notAdmitted())));
        }
        return samples;
    }

    /** Stops the endpoint. */
    void close() {
        if (metrics != null) {
            metrics.close();
        }
        LOGGER.info("node={} {}=cacheClosed {}", node, kind, stats());
        server.stop(0);
        handlers.shutdownNow();
        if (disk != null) {
            disk.close();
        }
    }

    /**
     * Answers one request.
     *
     * @param exchange request and response
     */
    private void handle(HttpExchange exchange) {
        try {
            answer(exchange);
        } finally {
            exchange.close();
        }
    }

    /**
     * Answers one request; errors of S3 become S3 error answers.
     *
     * @param exchange request and response, closed by the caller
     */
    private void answer(HttpExchange exchange) {
        try {
            URI uri = exchange.getRequestURI();
            String method = exchange.getRequestMethod();
            Optional<String> keyId = S3SigV4.verify(method, uri.getRawPath(), uri.getRawQuery(),
                    name -> exchange.getRequestHeaders().getFirst(name),
                    id -> {
                        Route r = routes.get(id);
                        return r == null ? null : r.secret();
                    }, Instant.now());
            Route route = keyId.map(routes::get).orElse(null);
            if (route == null) {
                error(exchange, method, 403, "AccessDenied", "signature not valid for the read cache");
                return;
            }
            String path = URLDecoder.decode(uri.getRawPath().replace("+", "%2B"), StandardCharsets.UTF_8);
            String trimmed = path.startsWith("/") ? path.substring(1) : path;
            int slash = trimmed.indexOf('/');
            String bucket = slash < 0 ? trimmed : trimmed.substring(0, slash);
            String key = slash < 0 ? "" : trimmed.substring(slash + 1);
            if (uri.getRawQuery() != null && !uri.getRawQuery().isEmpty()) {
                error(exchange, method, 501, "NotImplemented", "the S3 read cache serves object reads only,"
                        + " no listings: name files, not globs, or turn arrowflight." + kind + ".cacheMegabytes off");
                return;
            }
            if (!bucket.equals(route.root().bucket()) || !key.startsWith(route.root().prefix())
                    || !plainKey(key)) {
                error(exchange, method, 403, "AccessDenied", "outside the data directory");
                return;
            }
            if ("HEAD".equals(method)) {
                head(exchange, route, key);
            } else if ("GET".equals(method)) {
                get(exchange, route, key, exchange.getRequestHeaders().getFirst("Range"));
            } else {
                error(exchange, method, 405, "MethodNotAllowed", "read only");
            }
        } catch (NoSuchFileException e) {
            sendQuietly(exchange, 404, "NoSuchKey", "missing");
        } catch (StorageException e) {
            LOGGER.warn("node={} {}=cacheReadFailed error={}", node, kind, e.getMessage());
            sendQuietly(exchange, 503, "SlowDown", "S3 read failed");
        } catch (IOException | RuntimeException e) {
            LOGGER.debug("node={} {}=cacheRequestFailed error={}", node, kind, e.toString());
        }
    }

    /**
     * Tells whether a key names an object without dot segments a store might resolve.
     *
     * @param key object key
     * @return false for an empty key or one with an empty, "." or ".." segment
     */
    static boolean plainKey(String key) {
        for (String segment : key.split("/", -1)) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Answers a HEAD with the object's current version, always looked up on S3.
     *
     * @param exchange request and response
     * @param route route of the request
     * @param key object key
     * @throws IOException when the answer cannot be sent
     * @throws NoSuchFileException when the object does not exist
     * @throws StorageException when S3 cannot be read
     */
    private void head(HttpExchange exchange, Route route, String key)
            throws IOException, NoSuchFileException, StorageException {
        Version version = lookUp(route, key);
        headers(exchange, version);
        exchange.getResponseHeaders().set("Content-Length", Long.toString(version.size()));
        exchange.sendResponseHeaders(200, -1);
    }

    /**
     * Answers a GET from cached blocks, reading the missing ones from S3.
     *
     * @param exchange request and response
     * @param route route of the request
     * @param key object key
     * @param range Range header, or null for the whole object
     * @throws IOException when the answer cannot be sent
     * @throws NoSuchFileException when the object does not exist
     * @throws StorageException when S3 cannot be read
     */
    private void get(HttpExchange exchange, Route route, String key, String range)
            throws IOException, NoSuchFileException, StorageException {
        Version version = known(route, key);
        S3BlockCache.Block[] blocks;
        long[] span;
        try {
            span = span(range, version.size());
            blocks = blocks(route, key, version, span);
        } catch (ChangedException e) {
            version = lookUp(route, key);
            span = span(range, version.size());
            try {
                blocks = blocks(route, key, version, span);
            } catch (ChangedException again) {
                throw new StorageException("object " + key + " changed twice while it was read");
            }
        }
        if (span == null) {
            exchange.getResponseHeaders().set("Content-Range", "bytes */" + version.size());
            error(exchange, "GET", 416, "InvalidRange", "range not satisfiable");
            return;
        }
        headers(exchange, version);
        long length = span[1] - span[0] + 1;
        if (range != null) {
            exchange.getResponseHeaders().set("Content-Range",
                    "bytes " + span[0] + "-" + span[1] + "/" + version.size());
        }
        exchange.sendResponseHeaders(range == null ? 200 : 206, length == 0 ? -1 : length);
        if (length == 0) {
            return;
        }
        long first = span[0] / BLOCK_BYTES;
        try (OutputStream out = exchange.getResponseBody()) {
            for (int i = 0; i < blocks.length; i++) {
                long blockStart = (first + i) * BLOCK_BYTES;
                int from = (int) Math.max(0, span[0] - blockStart);
                int to = (int) Math.min(blocks[i].length(), span[1] - blockStart + 1);
                blocks[i].write(out, from, to);
            }
        }
    }

    /**
     * Turns a Range header into the first and last byte to send.
     *
     * @param range Range header, or null for the whole object
     * @param size object size
     * @return first and last byte (last = first - 1 for an empty object), or null when the range
     *         cannot be satisfied
     */
    static long[] span(String range, long size) {
        if (range == null) {
            return new long[] {0, size - 1};
        }
        if (!range.startsWith("bytes=") || range.indexOf(',') >= 0) {
            return null;
        }
        String[] parts = range.substring("bytes=".length()).trim().split("-", -1);
        try {
            if (parts.length != 2) {
                return null;
            }
            if (parts[0].isEmpty()) {
                long suffix = Long.parseLong(parts[1]);
                return suffix <= 0 || size == 0 ? null : new long[] {Math.max(0, size - suffix), size - 1};
            }
            long start = Long.parseLong(parts[0]);
            long end = parts[1].isEmpty() ? size - 1 : Math.min(size - 1, Long.parseLong(parts[1]));
            return start < 0 || start >= size || end < start ? null : new long[] {start, end};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Collects the blocks of a span, reading the missing ones from S3. A missing block another
     * request is reading already is taken from that read; the blocks this request reads itself
     * are read first, so two requests never wait for each other.
     *
     * @param route route of the request
     * @param key object key
     * @param version object version to read
     * @param span first and last byte, or null
     * @return the blocks covering the span, in order
     * @throws ChangedException when the object is no longer that version
     * @throws NoSuchFileException when the object does not exist
     * @throws StorageException when S3 cannot be read
     */
    private S3BlockCache.Block[] blocks(Route route, String key, Version version, long[] span)
            throws ChangedException, NoSuchFileException, StorageException {
        if (span == null || span[1] < span[0]) {
            return new S3BlockCache.Block[0];
        }
        String bucket = route.root().bucket();
        long first = span[0] / BLOCK_BYTES;
        long last = span[1] / BLOCK_BYTES;
        int count = (int) (last - first + 1);
        S3BlockCache.Block[] blocks = new S3BlockCache.Block[count];
        S3BlockCache.BlockKey[] keys = new S3BlockCache.BlockKey[count];
        List<CompletableFuture<S3BlockCache.Block>> reads = new ArrayList<>(Collections.nCopies(count, null));
        boolean[] mine = new boolean[count];
        // Claimed under one lock: two requests for the same blocks must not split them into two
        // reads (one claiming the first block, the other the next before the first gets to it).
        synchronized (claiming) {
            for (int i = 0; i < count; i++) {
                keys[i] = new S3BlockCache.BlockKey(bucket, key, version.etag(), first + i);
                blocks[i] = cache.get(keys[i]);
                if (blocks[i] != null) {
                    continue;
                }
                CompletableFuture<S3BlockCache.Block> read = new CompletableFuture<>();
                CompletableFuture<S3BlockCache.Block> other = reading.putIfAbsent(keys[i], read);
                if (other != null) {
                    reads.set(i, other);
                    continue;
                }
                // A read that ended between the cache miss and the claim left its block in the cache.
                blocks[i] = cache.peek(keys[i]);
                if (blocks[i] != null) {
                    reading.remove(keys[i], read);
                    read.complete(blocks[i]);
                    continue;
                }
                mine[i] = true;
                reads.set(i, read);
            }
        }
        try {
            readOwn(route, key, version, first, blocks, keys, reads, mine);
        } finally {
            for (int i = 0; i < count; i++) {
                if (mine[i]) {
                    reads.get(i).completeExceptionally(new StorageException("read of " + key + " abandoned"));
                    reading.remove(keys[i], reads.get(i));
                }
            }
        }
        for (int i = 0; i < count; i++) {
            if (blocks[i] == null) {
                blocks[i] = await(reads.get(i));
                joined.incrementAndGet();
            }
        }
        return blocks;
    }

    /**
     * Reads the blocks this request claimed from the disk tier, else from S3 (consecutive ones
     * together), caching each and handing it to the requests waiting for it; a failure is handed
     * to them too.
     *
     * @param route route of the request
     * @param key object key
     * @param version object version to read
     * @param first number of the span's first block
     * @param blocks blocks of the span, filled in
     * @param keys names of the span's blocks
     * @param reads reads of the span's blocks, own or another request's
     * @param mine which blocks this request reads
     * @throws ChangedException when the object is no longer that version
     * @throws NoSuchFileException when the object does not exist
     * @throws StorageException when S3 cannot be read
     */
    private void readOwn(Route route, String key, Version version, long first, S3BlockCache.Block[] blocks,
            S3BlockCache.BlockKey[] keys, List<CompletableFuture<S3BlockCache.Block>> reads, boolean[] mine)
            throws ChangedException, NoSuchFileException, StorageException {
        if (disk != null) {
            for (int b = 0; b < blocks.length; b++) {
                if (mine[b]) {
                    S3BlockCache.Block block = disk.get(keys[b]);
                    if (block != null) {
                        // Read again: kept in memory whatever the admission.
                        cache.put(keys[b], block);
                        blocks[b] = block;
                        reads.get(b).complete(block);
                    }
                }
            }
        }
        int i = 0;
        while (i < blocks.length) {
            if (!mine[i] || blocks[i] != null) {
                i++;
                continue;
            }
            int end = i;
            while (end + 1 < blocks.length && mine[end + 1] && blocks[end + 1] == null
                    && end + 1 - i < MAX_BLOCKS_PER_READ) {
                end++;
            }
            long from = (first + i) * BLOCK_BYTES;
            long to = Math.min(version.size(), (first + end + 1) * BLOCK_BYTES) - 1;
            try (ResponseInputStream<GetObjectResponse> in = open(route, key, version, from, to)) {
                // Each block is handed over as it arrives; the GET is never held whole in memory.
                for (int b = i; b <= end; b++) {
                    long blockStart = (first + b) * BLOCK_BYTES;
                    int length = (int) (Math.min(to + 1, blockStart + BLOCK_BYTES) - blockStart);
                    S3BlockCache.Block block = S3BlockCache.Block.read(in, length);
                    s3ReadBytes.addAndGet(length);
                    cache.offer(keys[b], block);
                    if (disk != null) {
                        disk.offer(keys[b], block);
                    }
                    blocks[b] = block;
                    reads.get(b).complete(block);
                }
                if (in.read() >= 0) {
                    // A store that ignores Range sent the rest of the object: drop it.
                    in.abort();
                }
            } catch (ChangedException | NoSuchFileException | StorageException | RuntimeException e) {
                failOwn(reads, mine, i, e);
                throw e;
            } catch (IOException e) {
                StorageException failed = new StorageException("short read of " + key + ": "
                        + route.settings().redact(String.valueOf(e.getMessage())));
                failOwn(reads, mine, i, failed);
                throw failed;
            }
            i = end + 1;
        }
    }

    /**
     * Waits for another request's read of a block.
     *
     * @param read that read
     * @return the block
     * @throws ChangedException when that read found the object changed
     * @throws NoSuchFileException when that read found the object missing
     * @throws StorageException when that read failed otherwise, or the wait was interrupted
     */
    private static S3BlockCache.Block await(CompletableFuture<S3BlockCache.Block> read)
            throws ChangedException, NoSuchFileException, StorageException {
        try {
            return read.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("interrupted while waiting for a block read");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ChangedException changed) {
                throw changed;
            }
            if (cause instanceof NoSuchFileException missing) {
                throw missing;
            }
            if (cause instanceof StorageException failed) {
                throw new StorageException(failed.getMessage());
            }
            throw new StorageException("block read failed: " + cause.getClass().getSimpleName());
        }
    }

    /**
     * Fails the waiters of this request's blocks that are not read yet.
     *
     * @param reads reads of the span's blocks
     * @param mine which blocks this request reads
     * @param from first block not read
     * @param e the failure
     */
    private static void failOwn(List<CompletableFuture<S3BlockCache.Block>> reads, boolean[] mine, int from,
            Exception e) {
        for (int b = from; b < mine.length; b++) {
            if (mine[b]) {
                reads.get(b).completeExceptionally(e);
            }
        }
    }

    /**
     * Opens a read of bytes of one object version on S3; the answer's body starts at the first
     * byte asked for, also from a store that ignores Range.
     *
     * @param route route of the request
     * @param key object key
     * @param version version that must still be current
     * @param from first byte
     * @param to last byte
     * @return the answer's body, positioned at {@code from}; the caller closes it
     * @throws ChangedException when S3 refuses the version (412) or answers with another ETag
     * @throws NoSuchFileException when the object does not exist
     * @throws StorageException when S3 cannot be read
     */
    private ResponseInputStream<GetObjectResponse> open(Route route, String key, Version version, long from, long to)
            throws ChangedException, NoSuchFileException, StorageException {
        ResponseInputStream<GetObjectResponse> in;
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(route.root().bucket()).key(key)
                    .range("bytes=" + from + "-" + to).ifMatch(version.etag()).build();
            s3Reads.incrementAndGet();
            in = route.client().getObject(request);
        } catch (NoSuchKeyException e) {
            versions.remove(versionKey(route, key));
            throw new NoSuchFileException();
        } catch (S3Exception e) {
            if (e.statusCode() == 412) {
                versions.remove(versionKey(route, key));
                throw new ChangedException();
            }
            if (e.statusCode() == 404) {
                versions.remove(versionKey(route, key));
                throw new NoSuchFileException();
            }
            throw new StorageException("read " + key + ": " + route.settings().redact(e.getMessage()));
        } catch (SdkException e) {
            throw new StorageException("read " + key + ": " + route.settings().redact(e.getMessage()));
        }
        GetObjectResponse response = in.response();
        String etag = response.eTag();
        if (etag != null && !etag.equals(version.etag())) {
            // A store that ignores If-Match (a gateway) sent another version.
            in.abort();
            versions.remove(versionKey(route, key));
            throw new ChangedException();
        }
        Long sent = response.contentLength();
        if (from > 0 && sent != null && sent > to - from + 1) {
            // A store that ignores Range sends the whole object.
            try {
                in.skipNBytes(from);
            } catch (IOException e) {
                in.abort();
                throw new StorageException("short read of " + key + ": the object ended before byte " + from);
            }
        }
        return in;
    }

    /**
     * Returns the version of an object, looking it up when it is unknown or older than the check
     * interval.
     *
     * @param route route of the request
     * @param key object key
     * @return version
     * @throws NoSuchFileException when the object does not exist
     * @throws StorageException when S3 cannot be read
     */
    private Version known(Route route, String key) throws NoSuchFileException, StorageException {
        Version version = versions.get(versionKey(route, key));
        if (version != null && nanoClock.getAsLong() - version.checkedNanos() < checkNanos) {
            return version;
        }
        return lookUp(route, key);
    }

    /**
     * Looks up the current version of an object on S3 and remembers it.
     *
     * @param route route of the request
     * @param key object key
     * @return version
     * @throws NoSuchFileException when the object does not exist
     * @throws StorageException when S3 cannot be read
     */
    private Version lookUp(Route route, String key) throws NoSuchFileException, StorageException {
        try {
            s3Lookups.incrementAndGet();
            HeadObjectResponse head = route.client().headObject(r -> r.bucket(route.root().bucket()).key(key));
            Instant modified = head.lastModified() == null ? Instant.EPOCH : head.lastModified();
            Version version = new Version(head.eTag() == null ? "\"\"" : head.eTag(), head.contentLength(),
                    HTTP_DATE.format(modified.atOffset(ZoneOffset.UTC)), nanoClock.getAsLong());
            if (versions.size() >= MAX_VERSIONS) {
                versions.clear();
            }
            versions.put(versionKey(route, key), version);
            return version;
        } catch (NoSuchKeyException e) {
            versions.remove(versionKey(route, key));
            throw new NoSuchFileException();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                versions.remove(versionKey(route, key));
                throw new NoSuchFileException();
            }
            throw new StorageException("look up " + key + ": " + route.settings().redact(e.getMessage()));
        } catch (SdkException e) {
            throw new StorageException("look up " + key + ": " + route.settings().redact(e.getMessage()));
        }
    }

    /**
     * Names an object across routes; two routes over one bucket share versions and blocks only
     * when they read the same endpoint's bucket, which the bucket name alone cannot tell, so the
     * route's endpoint is part of the name.
     *
     * @param route route of the request
     * @param key object key
     * @return version map key
     */
    private static String versionKey(Route route, String key) {
        return route.settings().endpointUri() + "|" + route.root().bucket() + "|" + key;
    }

    /**
     * Sets the headers httpfs reads from an object answer.
     *
     * @param exchange response
     * @param version object version
     */
    private static void headers(HttpExchange exchange, Version version) {
        exchange.getResponseHeaders().set("ETag", version.etag());
        exchange.getResponseHeaders().set("Last-Modified", version.lastModified());
        exchange.getResponseHeaders().set("Accept-Ranges", "bytes");
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
    }

    /**
     * Sends an S3 error answer.
     *
     * @param exchange response
     * @param method request method; a HEAD answer has no body
     * @param status HTTP status
     * @param code S3 error code
     * @param message message
     * @throws IOException when the answer cannot be sent
     */
    private static void error(HttpExchange exchange, String method, int status, String code, String message)
            throws IOException {
        if ("HEAD".equals(method)) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + code + "</Code><Message>"
                + message + "</Message></Error>").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/xml");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /**
     * Sends an error answer, ignoring a failure to send it.
     *
     * @param exchange response
     * @param status HTTP status
     * @param code S3 error code
     * @param message message
     */
    private static void sendQuietly(HttpExchange exchange, int status, String code, String message) {
        try {
            error(exchange, exchange.getRequestMethod(), status, code, message);
        } catch (IOException | RuntimeException e) {
            // The client went away; nothing to answer.
        }
    }

    /**
     * Returns random bytes as hex.
     *
     * @param bytes number of random bytes
     * @return hex text, twice as long
     */
    private static String randomHex(int bytes) {
        byte[] random = new byte[bytes];
        RANDOM.nextBytes(random);
        return HexFormat.of().formatHex(random);
    }

    /** The object does not exist. */
    private static final class NoSuchFileException extends Exception {
        private static final long serialVersionUID = 1L;

        NoSuchFileException() {
            super(null, null, false, false);
        }
    }

    /** S3 could not be read; the message is redacted. */
    private static final class StorageException extends Exception {
        private static final long serialVersionUID = 1L;

        StorageException(String message) {
            super(message);
        }
    }
}
