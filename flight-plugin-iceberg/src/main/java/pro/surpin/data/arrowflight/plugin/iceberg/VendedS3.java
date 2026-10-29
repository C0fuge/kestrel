package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

/**
 * Reads S3 objects with one set of vended credentials: HEAD for the length and GET with one
 * byte range, signed with AWS Signature Version 4 (JDK HTTP client, no AWS SDK). Used for the
 * metadata files of a table whose catalog vended credentials (manifest lists and manifests), so
 * that the server's own S3 keys need not read the warehouse. Secret values never appear in
 * exceptions or logs.
 */
final class VendedS3 {

    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    /** SHA-256 of an empty payload. */
    private static final String EMPTY_SHA256 =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final DateTimeFormatter AMZ_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final String DEFAULT_REGION = "us-east-1";

    private final String prefix;
    private final String keyId;
    private final String secret;
    private final String token;
    private final String region;
    /** {@code host[:port]} of a configured endpoint, or null for AWS. */
    private final String endpoint;
    private final boolean ssl;
    private final boolean pathStyle;
    private final HttpClient http;
    private final Duration timeout;
    private final Clock clock;

    /**
     * Creates a reader.
     *
     * @param credential s3 credential
     * @param http HTTP client
     * @param timeout request timeout
     * @param clock clock for the request time
     */
    private VendedS3(StorageCredential credential, HttpClient http, Duration timeout, Clock clock) {
        Map<String, SecretValue> options = credential.options();
        this.prefix = Locations.normalize(credential.prefix());
        this.keyId = value(options, "KEY_ID");
        this.secret = value(options, "SECRET");
        this.token = value(options, "SESSION_TOKEN");
        String configuredRegion = value(options, "REGION");
        this.region = configuredRegion == null ? DEFAULT_REGION : configuredRegion;
        this.endpoint = value(options, "ENDPOINT");
        String useSsl = value(options, "USE_SSL");
        this.ssl = useSsl == null || !"false".equalsIgnoreCase(useSsl);
        String urlStyle = value(options, "URL_STYLE");
        this.pathStyle = "path".equalsIgnoreCase(urlStyle);
        this.http = http;
        this.timeout = timeout;
        this.clock = clock;
    }

    /**
     * Creates a reader for a credential.
     *
     * @param credential vended credential
     * @param http HTTP client
     * @param timeout request timeout
     * @param clock clock for the request time
     * @return reader, empty for a credential that is not S3 or has no key
     */
    static Optional<VendedS3> of(StorageCredential credential, HttpClient http, Duration timeout,
            Clock clock) {
        if (!"s3".equalsIgnoreCase(credential.type())
                || !Locations.normalize(credential.prefix()).startsWith("s3://")
                || value(credential.options(), "KEY_ID") == null
                || value(credential.options(), "SECRET") == null) {
            return Optional.empty();
        }
        return Optional.of(new VendedS3(credential, http, timeout, clock));
    }

    /**
     * Returns the normalised prefix the credential applies to.
     *
     * @return prefix
     */
    String prefix() {
        return prefix;
    }

    /**
     * Tells whether a location lies below the credential's prefix. A prefix is a key prefix,
     * not necessarily a directory, as vended by the catalog.
     *
     * @param location normalised location
     * @return true when it does
     */
    boolean covers(String location) {
        return location.startsWith(prefix);
    }

    /**
     * Returns an object's length.
     *
     * @param location normalised {@code s3://} location
     * @return length in bytes
     * @throws FileNotFoundException when the object does not exist
     * @throws IOException on another failure
     */
    long length(String location) throws IOException {
        HttpResponse<Void> response = send(location, "HEAD", null, HttpResponse.BodyHandlers.discarding());
        check(response.statusCode(), location);
        return response.headers().firstValueAsLong("content-length")
                .orElseThrow(() -> new IOException("S3 sent no Content-Length for " + location));
    }

    /**
     * Tells whether an object exists.
     *
     * @param location normalised {@code s3://} location
     * @return true when HEAD finds it
     * @throws IOException on a failure other than "not found"
     */
    boolean exists(String location) throws IOException {
        try {
            length(location);
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    /**
     * Reads a byte range of an object.
     *
     * @param location normalised {@code s3://} location
     * @param position first byte
     * @param target buffer
     * @param offset buffer offset
     * @param length bytes to read
     * @throws IOException when the range cannot be read in full
     */
    void readFully(String location, long position, byte[] target, int offset, int length)
            throws IOException {
        if (length == 0) {
            return;
        }
        String range = "bytes=" + position + "-" + (position + length - 1);
        HttpResponse<byte[]> response = send(location, "GET", range,
                HttpResponse.BodyHandlers.ofByteArray());
        check(response.statusCode(), location);
        byte[] body = response.body();
        if (response.statusCode() == 200) {
            // A store that ignores Range sends the whole object.
            if (body.length < position + length) {
                throw new IOException("S3 sent " + body.length + " bytes of " + location
                        + ", fewer than the range " + range + " needs");
            }
            System.arraycopy(body, (int) position, target, offset, length);
            return;
        }
        if (response.statusCode() != 206 || body.length != length) {
            throw new IOException("S3 answered " + response.statusCode() + " with " + body.length
                    + " bytes to the range " + range + " of " + location);
        }
        System.arraycopy(body, 0, target, offset, length);
    }

    /**
     * Sends a signed request.
     *
     * @param location object location
     * @param method HEAD or GET
     * @param range Range header value, or null
     * @param handler body handler
     * @param <T> body type
     * @return response
     * @throws IOException on a transport failure
     */
    private <T> HttpResponse<T> send(String location, String method, String range,
            HttpResponse.BodyHandler<T> handler) throws IOException {
        Target target = target(location);
        String amzDate = AMZ_DATE.format(clock.instant());
        Map<String, String> signed = new TreeMap<>();
        signed.put("host", target.host());
        if (range != null) {
            signed.put("range", range);
        }
        signed.put("x-amz-content-sha256", EMPTY_SHA256);
        signed.put("x-amz-date", amzDate);
        if (token != null) {
            signed.put("x-amz-security-token", token);
        }
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(target.url()))
                .timeout(timeout).method(method, HttpRequest.BodyPublishers.noBody());
        for (Map.Entry<String, String> header : signed.entrySet()) {
            if (!"host".equals(header.getKey())) {
                request.header(header.getKey(), header.getValue());
            }
        }
        request.header("Authorization", authorization(method, target.path(), signed, amzDate));
        try {
            return http.send(request.build(), handler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading " + location, e);
        }
    }

    /**
     * Where a request for an object goes.
     *
     * @param url full URL
     * @param host Host header value
     * @param path encoded path, the canonical URI
     */
    record Target(String url, String host, String path) {
    }

    /**
     * Builds the URL of an object.
     *
     * @param location normalised {@code s3://bucket/key}
     * @return target
     * @throws IOException when the location names no object
     */
    Target target(String location) throws IOException {
        if (!location.startsWith("s3://")) {
            throw new IOException("Not an S3 location: " + location);
        }
        String rest = location.substring("s3://".length());
        int slash = rest.indexOf('/');
        if (slash <= 0 || slash == rest.length() - 1) {
            throw new IOException("Not an S3 object: " + location);
        }
        String bucket = rest.substring(0, slash);
        String key = encodeKey(rest.substring(slash + 1));
        String server = endpoint != null ? endpoint
                : DEFAULT_REGION.equals(region) ? "s3.amazonaws.com" : "s3." + region + ".amazonaws.com";
        String host = withoutDefaultPort(server);
        String path;
        if (pathStyle || bucket.contains(".") && endpoint == null && ssl) {
            path = "/" + encodeSegment(bucket) + "/" + key;
        } else {
            host = bucket + "." + host;
            path = "/" + key;
        }
        return new Target((ssl ? "https://" : "http://") + host + path, host, path);
    }

    /**
     * Drops a port that the scheme implies, as the JDK client leaves it out of its Host header.
     *
     * @param server {@code host[:port]}
     * @return host and a port other than the scheme's default
     */
    private String withoutDefaultPort(String server) {
        String suffix = ssl ? ":443" : ":80";
        return server.endsWith(suffix) ? server.substring(0, server.length() - suffix.length()) : server;
    }

    /**
     * Builds the Authorization header.
     *
     * @param method HTTP method
     * @param path canonical URI
     * @param signed signed headers by lower-case name, sorted
     * @param amzDate request time
     * @return header value
     */
    private String authorization(String method, String path, Map<String, String> signed,
            String amzDate) {
        StringBuilder canonical = new StringBuilder();
        canonical.append(method).append('\n').append(path).append('\n').append('\n');
        for (Map.Entry<String, String> header : signed.entrySet()) {
            canonical.append(header.getKey()).append(':').append(header.getValue().strip()).append('\n');
        }
        String names = String.join(";", signed.keySet());
        canonical.append('\n').append(names).append('\n').append(EMPTY_SHA256);
        String date = amzDate.substring(0, 8);
        String scope = date + "/" + region + "/s3/aws4_request";
        String toSign = ALGORITHM + "\n" + amzDate + "\n" + scope + "\n" + hex(sha256(canonical.toString()));
        byte[] key = hmac(("AWS4" + secret).getBytes(StandardCharsets.UTF_8), date);
        key = hmac(key, region);
        key = hmac(key, "s3");
        key = hmac(key, "aws4_request");
        return ALGORITHM + " Credential=" + keyId + "/" + scope + ", SignedHeaders=" + names
                + ", Signature=" + hex(hmac(key, toSign));
    }

    /**
     * Maps a status to an exception.
     *
     * @param status HTTP status
     * @param location object location
     * @throws IOException for anything but 2xx
     */
    private static void check(int status, String location) throws IOException {
        if (status == 404) {
            throw new FileNotFoundException(location);
        }
        if (status == 403) {
            throw new IOException("S3 refused the catalog's vended credentials for " + location
                    + " (403)");
        }
        if (status / 100 != 2) {
            throw new IOException("S3 answered " + status + " for " + location);
        }
    }

    /**
     * Encodes a key for the canonical URI: every byte but unreserved characters and "/".
     *
     * @param key object key
     * @return encoded key
     */
    static String encodeKey(String key) {
        String[] segments = key.split("/", -1);
        for (int i = 0; i < segments.length; i++) {
            segments[i] = encodeSegment(segments[i]);
        }
        return String.join("/", segments);
    }

    /**
     * Percent-encodes a path segment as SigV4 requires (RFC 3986 unreserved kept).
     *
     * @param segment segment
     * @return encoded segment
     */
    private static String encodeSegment(String segment) {
        StringBuilder result = new StringBuilder();
        for (byte b : segment.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if (c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
                    || c == '-' || c == '.' || c == '_' || c == '~') {
                result.append((char) c);
            } else {
                result.append('%').append(String.format(Locale.ROOT, "%02X", c));
            }
        }
        return result.toString();
    }

    /**
     * Reads a non-blank option value.
     *
     * @param options secret options
     * @param name option name
     * @return value, null when missing or blank
     */
    private static String value(Map<String, SecretValue> options, String name) {
        SecretValue value = options.get(name);
        return value == null || value.value() == null || value.value().isBlank() ? null
                : value.value().strip();
    }

    /**
     * Hashes a text with SHA-256.
     *
     * @param text UTF-8 text
     * @return digest
     */
    private static byte[] sha256(String text) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /**
     * Computes an HMAC-SHA256.
     *
     * @param key key
     * @param data UTF-8 text
     * @return MAC
     */
    private static byte[] hmac(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 not available", e);
        }
    }

    /**
     * Formats bytes as lower-case hex.
     *
     * @param bytes bytes
     * @return hex text
     */
    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    @Override
    public String toString() {
        return "VendedS3[" + prefix + "]";
    }
}
