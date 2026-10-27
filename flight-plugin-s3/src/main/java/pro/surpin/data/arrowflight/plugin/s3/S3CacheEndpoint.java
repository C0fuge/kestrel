package pro.surpin.data.arrowflight.plugin.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
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
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 * version, so the bytes of one answer always come from one version of an object. HEADs from
 * DuckDB always go to S3. Listings and other requests with a query are not served.
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
    private final long checkNanos;
    private final LongSupplier nanoClock;
    private final String node;
    private final Map<String, Route> routes = new ConcurrentHashMap<>();
    private final Map<String, Version> versions = new ConcurrentHashMap<>();

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
        if (checkSeconds < 0) {
            throw new IllegalArgumentException("arrowflight.s3.cacheCheckSeconds must be >= 0: " + checkSeconds);
        }
        this.cache = new S3BlockCache(capacityBytes);
        this.checkNanos = TimeUnit.SECONDS.toNanos(checkSeconds);
        this.node = Objects.requireNonNull(node, "node");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 64);
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
     * @return route; closing it unregisters the data directory, the last close stops the endpoint
     * @throws IOException when the endpoint cannot be started
     */
    static synchronized Route register(long capacityBytes, int checkSeconds, String node, S3Uri root,
            S3Client client, S3Settings settings) throws IOException {
        if (shared == null) {
            shared = new S3CacheEndpoint(capacityBytes, checkSeconds, node, System::nanoTime);
            LOGGER.info("node={} s3=cacheOpen endpoint=127.0.0.1:{} capacityBytes={} blockBytes={} checkSeconds={}",
                    node, shared.port(), capacityBytes, BLOCK_BYTES, checkSeconds);
        } else if (shared.cache.capacityBytes() != capacityBytes
                || shared.checkNanos != TimeUnit.SECONDS.toNanos(checkSeconds)) {
            LOGGER.warn("node={} s3=cacheSettingsIgnored capacityBytes={} checkSeconds={} (the running cache"
                    + " keeps its own)", node, capacityBytes, checkSeconds);
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
     * Describes the cache's use so far.
     *
     * @return statistics line
     */
    String stats() {
        return cache.stats();
    }

    /** Stops the endpoint. */
    void close() {
        LOGGER.info("node={} s3=cacheClosed {}", node, cache.stats());
        server.stop(0);
        handlers.shutdownNow();
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
                        + " no listings: name files, not globs, or turn arrowflight.s3.cacheMegabytes off");
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
            LOGGER.warn("node={} s3=cacheReadFailed error={}", node, e.getMessage());
            sendQuietly(exchange, 503, "SlowDown", "S3 read failed");
        } catch (IOException | RuntimeException e) {
            LOGGER.debug("node={} s3=cacheRequestFailed error={}", node, e.toString());
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
        byte[][] blocks;
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
                int to = (int) Math.min(blocks[i].length, span[1] - blockStart + 1);
                out.write(blocks[i], from, to - from);
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
     * Collects the blocks of a span, reading the missing ones from S3.
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
    private byte[][] blocks(Route route, String key, Version version, long[] span)
            throws ChangedException, NoSuchFileException, StorageException {
        if (span == null || span[1] < span[0]) {
            return new byte[0][];
        }
        String bucket = route.root().bucket();
        long first = span[0] / BLOCK_BYTES;
        long last = span[1] / BLOCK_BYTES;
        byte[][] blocks = new byte[(int) (last - first + 1)][];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = cache.get(new S3BlockCache.BlockKey(bucket, key, version.etag(), first + i));
        }
        int i = 0;
        while (i < blocks.length) {
            if (blocks[i] != null) {
                i++;
                continue;
            }
            int end = i;
            while (end + 1 < blocks.length && blocks[end + 1] == null && end + 1 - i < MAX_BLOCKS_PER_READ) {
                end++;
            }
            long from = (first + i) * BLOCK_BYTES;
            long to = Math.min(version.size(), (first + end + 1) * BLOCK_BYTES) - 1;
            byte[] bytes = read(route, key, version, from, to);
            for (int b = i; b <= end; b++) {
                int offset = (int) ((first + b) * BLOCK_BYTES - from);
                int length = (int) Math.min(BLOCK_BYTES, bytes.length - offset);
                byte[] block = Arrays.copyOfRange(bytes, offset, offset + length);
                cache.put(new S3BlockCache.BlockKey(bucket, key, version.etag(), first + b), block);
                blocks[b] = block;
            }
            i = end + 1;
        }
        return blocks;
    }

    /**
     * Reads bytes of one object version from S3.
     *
     * @param route route of the request
     * @param key object key
     * @param version version that must still be current
     * @param from first byte
     * @param to last byte
     * @return exactly the bytes from..to
     * @throws ChangedException when S3 refuses the version (412)
     * @throws NoSuchFileException when the object does not exist
     * @throws StorageException when S3 cannot be read or sends too few bytes
     */
    private byte[] read(Route route, String key, Version version, long from, long to)
            throws ChangedException, NoSuchFileException, StorageException {
        int count = (int) (to - from + 1);
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(route.root().bucket()).key(key)
                    .range("bytes=" + from + "-" + to).ifMatch(version.etag()).build();
            byte[] bytes = route.client().getObjectAsBytes(request).asByteArray();
            if (bytes.length > count && bytes.length > to) {
                // A store that ignores Range sends the whole object.
                return Arrays.copyOfRange(bytes, (int) from, (int) from + count);
            }
            if (bytes.length != count) {
                throw new StorageException("short read of " + key + ": " + bytes.length + " of " + count + " bytes");
            }
            return bytes;
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
