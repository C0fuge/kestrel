package pro.surpin.data.arrowflight.plugin.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A small S3 server on the JDK's HTTP server, path style only: ListObjectsV2, HEAD and GET with
 * one byte range. It checks no signature but records every request, so tests can count calls
 * and look at the headers.
 */
public final class FakeS3 implements AutoCloseable {

    /** One object. */
    public record StoredObject(byte[] data, Instant lastModified) {
    }

    /** One request as received. */
    public record Request(String method, String path, Map<String, String> query, Map<String, String> headers) {
    }

    private final HttpServer server;
    private final String scheme;
    private final NavigableMap<String, StoredObject> objects = new ConcurrentSkipListMap<>();
    private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger pageSize = new AtomicInteger(1000);
    private final AtomicLong latencyMillis = new AtomicLong();

    private FakeS3(HttpServer server, String scheme) {
        this.server = server;
        this.scheme = scheme;
        server.createContext("/", this::handle);
        // Enough handlers that an injected latency delays requests side by side, as on S3.
        server.setExecutor(Executors.newFixedThreadPool(32, r -> {
            Thread t = new Thread(r, "fake-s3");
            t.setDaemon(true);
            return t;
        }));
        server.start();
    }

    /**
     * Starts a plain HTTP server on a free loopback port.
     *
     * @return running server
     * @throws IOException when no port can be bound
     */
    public static FakeS3 start() throws IOException {
        return new FakeS3(HttpServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0), "http");
    }

    /**
     * Starts an HTTPS server on a free loopback port.
     *
     * @param tls server TLS context
     * @return running server
     * @throws IOException when no port can be bound
     */
    public static FakeS3 startTls(SSLContext tls) throws IOException {
        HttpsServer server = HttpsServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        return new FakeS3(server, "https");
    }

    /**
     * Returns the endpoint URL.
     *
     * @return {@code http[s]://127.0.0.1:port}
     */
    public String endpoint() {
        return scheme + "://127.0.0.1:" + server.getAddress().getPort();
    }

    /**
     * Stores an object.
     *
     * @param bucket bucket
     * @param key key
     * @param data content
     * @param lastModified modification time
     */
    public void put(String bucket, String key, byte[] data, Instant lastModified) {
        objects.put(bucket + "/" + key, new StoredObject(data, lastModified));
    }

    /**
     * Stores an object modified at a fixed time.
     *
     * @param bucket bucket
     * @param key key
     * @param data content
     */
    public void put(String bucket, String key, byte[] data) {
        put(bucket, key, data, Instant.parse("2026-10-17T08:00:00Z"));
    }

    /**
     * Sets how many keys a listing page holds at most.
     *
     * @param size keys per page
     */
    public void pageSize(int size) {
        pageSize.set(size);
    }

    /**
     * Delays every later object request (HEAD, GET) before it is answered, as the round trip to
     * a remote S3 endpoint does; listings are not delayed.
     *
     * @param millis delay per request, 0 for none
     */
    public void latency(long millis) {
        latencyMillis.set(millis);
    }

    /**
     * Returns the requests received so far.
     *
     * @return requests in order
     */
    public List<Request> requests() {
        synchronized (requests) {
            return List.copyOf(requests);
        }
    }

    /**
     * Counts the requests of a method.
     *
     * @param method HTTP method
     * @return number of requests
     */
    public long count(String method) {
        return requests().stream().filter(r -> r.method().equals(method)).count();
    }

    /** Forgets the requests received so far. */
    public void clearRequests() {
        requests.clear();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            URI uri = exchange.getRequestURI();
            String path = URLDecoder.decode(uri.getRawPath(), StandardCharsets.UTF_8);
            Map<String, String> query = parseQuery(uri.getRawQuery());
            Map<String, String> headers = new LinkedHashMap<>();
            exchange.getRequestHeaders().forEach((name, values) ->
                    headers.put(name.toLowerCase(java.util.Locale.ROOT), String.join(",", values)));
            String method = exchange.getRequestMethod();
            requests.add(new Request(method, path, query, headers));
            String trimmed = path.startsWith("/") ? path.substring(1) : path;
            int slash = trimmed.indexOf('/');
            String bucket = slash < 0 ? trimmed : trimmed.substring(0, slash);
            String key = slash < 0 ? "" : trimmed.substring(slash + 1);
            if (key.isEmpty() && "GET".equals(method) && "2".equals(query.get("list-type"))) {
                list(exchange, bucket, query);
            } else if ("HEAD".equals(method) || "GET".equals(method)) {
                delay();
                object(exchange, bucket + "/" + key, "GET".equals(method), headers.get("range"));
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    private void delay() {
        long millis = latencyMillis.get();
        if (millis > 0) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void list(HttpExchange exchange, String bucket, Map<String, String> query)
            throws IOException {
        if (objects.keySet().stream().noneMatch(k -> k.startsWith(bucket + "/"))) {
            send(exchange, 404, "<Error><Code>NoSuchBucket</Code><Message>missing</Message></Error>");
            return;
        }
        String prefix = query.getOrDefault("prefix", "");
        String after = query.get("continuation-token");
        int max = Math.min(pageSize.get(),
                Integer.parseInt(query.getOrDefault("max-keys", "1000")));
        StringBuilder contents = new StringBuilder();
        int count = 0;
        String last = null;
        boolean truncated = false;
        for (Map.Entry<String, StoredObject> e : objects.tailMap(bucket + "/" + prefix, true).entrySet()) {
            String full = e.getKey();
            if (!full.startsWith(bucket + "/" + prefix)) {
                break;
            }
            String key = full.substring(bucket.length() + 1);
            if (after != null && key.compareTo(after) <= 0) {
                continue;
            }
            if (count == max) {
                truncated = true;
                break;
            }
            contents.append("<Contents><Key>").append(xml(key)).append("</Key><LastModified>")
                    .append(e.getValue().lastModified()).append("</LastModified><ETag>\"e")
                    .append(e.getValue().data().length).append("\"</ETag><Size>")
                    .append(e.getValue().data().length)
                    .append("</Size><StorageClass>STANDARD</StorageClass></Contents>");
            count++;
            last = key;
        }
        String body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<Name>" + xml(bucket) + "</Name><Prefix>" + xml(prefix) + "</Prefix>"
                + "<KeyCount>" + count + "</KeyCount><MaxKeys>" + max + "</MaxKeys>"
                + "<IsTruncated>" + truncated + "</IsTruncated>"
                + (truncated ? "<NextContinuationToken>" + xml(last) + "</NextContinuationToken>" : "")
                + contents + "</ListBucketResult>";
        send(exchange, 200, body);
    }

    private void object(HttpExchange exchange, String name, boolean withBody, String range)
            throws IOException {
        StoredObject object = objects.get(name);
        if (object == null) {
            if (withBody) {
                send(exchange, 404, "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>");
            } else {
                exchange.sendResponseHeaders(404, -1);
            }
            return;
        }
        byte[] data = object.data();
        int start = 0;
        int end = data.length - 1;
        int status = 200;
        if (range != null && range.startsWith("bytes=")) {
            String[] parts = range.substring(6).split("-", -1);
            start = Integer.parseInt(parts[0]);
            if (!parts[1].isEmpty()) {
                end = Math.min(end, Integer.parseInt(parts[1]));
            }
            status = 206;
            exchange.getResponseHeaders().set("Content-Range",
                    "bytes " + start + "-" + end + "/" + data.length);
        }
        exchange.getResponseHeaders().set("Last-Modified",
                DateTimeFormatter.RFC_1123_DATE_TIME.format(object.lastModified().atOffset(ZoneOffset.UTC)));
        exchange.getResponseHeaders().set("ETag", "\"e" + data.length + "\"");
        exchange.getResponseHeaders().set("Accept-Ranges", "bytes");
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        int length = end - start + 1;
        if (!withBody) {
            exchange.getResponseHeaders().set("Content-Length", String.valueOf(data.length));
            exchange.sendResponseHeaders(200, -1);
            return;
        }
        exchange.sendResponseHeaders(status, length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(data, start, length);
        }
    }

    private static void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/xml");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> result = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) {
            return result;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String name = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            result.put(name, value);
        }
        return result;
    }

    private static String xml(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
