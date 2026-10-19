package pro.surpin.data.arrowflight.server.storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * How the server reaches an S3 data directory: the endpoint, the credentials and the DuckDB
 * httpfs extension that reads the data. The secret key never comes from a system property or the
 * properties file, which other users of the host can read; {@link #toString()} leaves it out.
 *
 * @param endpoint endpoint URL ({@code https://host:port}); null for AWS in {@code region}
 * @param region signing region (for Ceph RGW the zonegroup name)
 * @param urlStyle "path" ({@code host/bucket/key}) or "vhost" ({@code bucket.host/key})
 * @param caFile PEM file with the only certificates trusted for TLS, or null for the system ones
 * @param verifyTls false trusts any certificate; for lab use only
 * @param accessKey access key ID, or null when not configured
 * @param secretKey secret access key, or null when not configured
 * @param sessionToken session token of temporary credentials, or null
 * @param listingTtlSeconds how long a listing of the data directory is used before it is
 *                          refreshed in the background; 0 lists on every use
 * @param maxConnections HTTP connections of the Java client
 * @param httpfsExtension path of DuckDB's httpfs extension file
 * @param metadataCache whether DuckDB caches Parquet footers and HTTP metadata
 */
public record S3Settings(String endpoint, String region, String urlStyle, String caFile,
        boolean verifyTls, String accessKey, String secretKey, String sessionToken,
        int listingTtlSeconds, int maxConnections, String httpfsExtension, boolean metadataCache) {

    /** Region when none is configured. */
    public static final String DEFAULT_REGION = "us-east-1";
    /** Seconds a listing is used when nothing is configured. */
    public static final int DEFAULT_LISTING_TTL_SECONDS = 30;
    /** Settings of a server whose data directory is not on S3. */
    public static final S3Settings NONE = new S3Settings(null, DEFAULT_REGION, "path", null, true,
            null, null, null, DEFAULT_LISTING_TTL_SECONDS, 1, null, true);

    /** Environment variable with the access key ID. */
    public static final String ACCESS_KEY_ENV = "FLIGHT_S3_ACCESS_KEY";
    /** Environment variable with the secret access key. */
    public static final String SECRET_KEY_ENV = "FLIGHT_S3_SECRET_KEY";
    /** Environment variable with the session token of temporary credentials. */
    public static final String SESSION_TOKEN_ENV = "FLIGHT_S3_SESSION_TOKEN";
    /**
     * Property names under which somebody might try to pass credentials. A system property shows
     * in the process list; the properties file sits in the jar or next to it.
     */
    private static final List<String> REFUSED_PROPERTIES = List.of(
            "s3AccessKey", "s3SecretKey", "s3SessionToken",
            "arrowflight.s3.accessKey", "arrowflight.s3.secretKey", "arrowflight.s3.sessionToken");
    /** Environment variables the AWS tools and DuckDB's httpfs read on their own. */
    private static final List<String> AWS_ENVIRONMENT = List.of(
            "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION",
            "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE",
            "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "DUCKDB_S3_ACCESS_KEY_ID",
            "DUCKDB_S3_SECRET_ACCESS_KEY", "DUCKDB_S3_SESSION_TOKEN", "DUCKDB_S3_ENDPOINT",
            "DUCKDB_S3_REGION");
    private static final String REDACTED = "***";

    /**
     * Checks the settings.
     *
     * @param endpoint endpoint URL, or null for AWS
     * @param region signing region
     * @param urlStyle "path" or "vhost"
     * @param caFile PEM file of trusted certificates, or null
     * @param verifyTls whether certificates are checked
     * @param accessKey access key ID, or null
     * @param secretKey secret access key, or null
     * @param sessionToken session token, or null
     * @param listingTtlSeconds listing lifetime in seconds
     * @param maxConnections HTTP connections of the Java client
     * @param httpfsExtension path of the httpfs extension, or null
     * @param metadataCache whether DuckDB caches metadata
     */
    public S3Settings {
        endpoint = blankToNull(endpoint);
        region = blankToNull(region) == null ? DEFAULT_REGION : region.trim();
        urlStyle = blankToNull(urlStyle) == null ? "path" : urlStyle.trim().toLowerCase(Locale.ROOT);
        caFile = blankToNull(caFile);
        accessKey = blankToNull(accessKey);
        secretKey = blankToNull(secretKey);
        sessionToken = blankToNull(sessionToken);
        httpfsExtension = blankToNull(httpfsExtension);
        if (!"path".equals(urlStyle) && !"vhost".equals(urlStyle)) {
            throw new IllegalArgumentException("s3UrlStyle must be path or vhost: " + urlStyle);
        }
        if (!region.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid s3Region: " + region);
        }
        if (listingTtlSeconds < 0) {
            throw new IllegalArgumentException("s3ListingTtlSeconds must not be negative: "
                    + listingTtlSeconds);
        }
        if (maxConnections <= 0) {
            throw new IllegalArgumentException("s3MaxConnections must be positive: " + maxConnections);
        }
        if (endpoint != null) {
            endpoint = normalizeEndpoint(endpoint);
        }
        if ((accessKey == null) != (secretKey == null)) {
            throw new IllegalArgumentException("S3 credentials need both an access key and a secret key");
        }
    }

    /**
     * Tells whether connections use TLS.
     *
     * @return false only for an {@code http://} endpoint
     */
    public boolean useSsl() {
        return endpoint == null || endpoint.startsWith("https://");
    }

    /**
     * Returns the endpoint the Java client talks to: the configured one, or the regional AWS
     * endpoint. It is always set explicitly, so that no environment variable redirects it.
     *
     * @return endpoint URI without a path
     */
    public URI endpointUri() {
        return URI.create(endpoint != null ? endpoint : "https://s3." + region + ".amazonaws.com");
    }

    /**
     * Names the endpoint the way DuckDB's secret takes it: host and port, no scheme.
     *
     * @return {@code host[:port]}
     */
    public String duckDbEndpoint() {
        URI uri = endpointUri();
        return uri.getPort() < 0 ? uri.getHost() : uri.getHost() + ":" + uri.getPort();
    }

    /**
     * Tells whether objects are named as {@code host/bucket/key}.
     *
     * @return true for path style
     */
    public boolean pathStyle() {
        return "path".equals(urlStyle);
    }

    /**
     * Tells whether credentials are configured.
     *
     * @return true when an access key and a secret key are set
     */
    public boolean hasCredentials() {
        return accessKey != null && secretKey != null;
    }

    /**
     * Returns these settings with other credentials.
     *
     * @param credentials credentials to use
     * @return settings with the credentials
     */
    public S3Settings withCredentials(Credentials credentials) {
        return new S3Settings(endpoint, region, urlStyle, caFile, verifyTls,
                credentials.accessKey(), credentials.secretKey(), credentials.sessionToken(),
                listingTtlSeconds, maxConnections, httpfsExtension, metadataCache);
    }

    /**
     * Refuses settings an S3 data directory cannot be read with.
     *
     * @throws IllegalArgumentException naming the first missing or unusable setting
     */
    public void requireUsable() {
        if (!hasCredentials()) {
            throw new IllegalArgumentException("S3 data directory without credentials: set "
                    + ACCESS_KEY_ENV + " and " + SECRET_KEY_ENV + " or s3CredentialsFile");
        }
        if (httpfsExtension == null) {
            throw new IllegalArgumentException("S3 data directory without duckDbHttpfsExtension:"
                    + " DuckDB reads S3 through its httpfs extension file");
        }
        if (!Files.isReadable(Path.of(httpfsExtension))) {
            throw new IllegalArgumentException("duckDbHttpfsExtension is not a readable file: "
                    + httpfsExtension);
        }
        if (caFile != null && !Files.isReadable(Path.of(caFile))) {
            throw new IllegalArgumentException("s3CaFile is not a readable file: " + caFile);
        }
    }

    /**
     * Replaces the secret key and the session token in a text, for error messages that might
     * quote them.
     *
     * @param text text that may hold a secret
     * @return text without the secret values, null for null
     */
    public String redact(String text) {
        String result = text;
        for (String secret : new String[] {secretKey, sessionToken}) {
            if (result != null && secret != null && !secret.isEmpty()) {
                result = result.replace(secret, REDACTED);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "S3Settings[endpoint=" + endpoint + ", region=" + region + ", urlStyle=" + urlStyle
                + ", caFile=" + caFile + ", verifyTls=" + verifyTls
                + ", accessKey=" + (accessKey == null ? null : maskAccessKey(accessKey))
                + ", secretKey=" + (secretKey == null ? null : REDACTED)
                + ", sessionToken=" + (sessionToken == null ? null : REDACTED)
                + ", listingTtlSeconds=" + listingTtlSeconds + ", maxConnections=" + maxConnections
                + ", httpfsExtension=" + httpfsExtension + ", metadataCache=" + metadataCache + "]";
    }

    /**
     * Refuses credentials passed as system properties or in the properties file.
     *
     * @param lookup reads a property from one source (system properties, the properties file)
     * @throws IllegalArgumentException when such a property is set
     */
    public static void requireNoCredentialProperties(UnaryOperator<String> lookup) {
        for (String name : REFUSED_PROPERTIES) {
            String value = lookup.apply(name);
            if (value != null && !value.isBlank()) {
                throw new IllegalArgumentException("S3 credentials are not read from properties ("
                        + name + "): -D values show in the process list. Set " + ACCESS_KEY_ENV
                        + " and " + SECRET_KEY_ENV + " in the environment or use s3CredentialsFile");
            }
        }
    }

    /**
     * Names the AWS and DuckDB environment variables that are set; this server ignores them.
     *
     * @param environment process environment
     * @return names of the variables that are set
     */
    public static List<String> awsEnvironmentVariables(Map<String, String> environment) {
        List<String> result = new ArrayList<>();
        for (String name : AWS_ENVIRONMENT) {
            String value = environment.get(name);
            if (value != null && !value.isEmpty()) {
                result.add(name);
            }
        }
        return result;
    }

    /**
     * Reads the credentials: from the credentials file when one is configured, otherwise from
     * the environment.
     *
     * @param credentialsFile properties file with accessKey, secretKey and optionally
     *                        sessionToken, or null
     * @param environment process environment
     * @return credentials, empty when none are configured
     * @throws IOException when the credentials file cannot be read
     * @throws IllegalArgumentException when the file is readable by every user or incomplete
     */
    public static Credentials loadCredentials(String credentialsFile, Map<String, String> environment)
            throws IOException {
        if (blankToNull(credentialsFile) != null) {
            return readCredentialsFile(Path.of(credentialsFile.trim()));
        }
        return new Credentials(environment.get(ACCESS_KEY_ENV), environment.get(SECRET_KEY_ENV),
                environment.get(SESSION_TOKEN_ENV));
    }

    /**
     * Reads a credentials file: Java properties with accessKey, secretKey and sessionToken.
     *
     * @param file credentials file
     * @return credentials
     * @throws IOException when the file cannot be read
     * @throws IllegalArgumentException when every user may read the file or it lacks a key
     */
    static Credentials readCredentialsFile(Path file) throws IOException {
        PosixFileAttributeView posix = Files.getFileAttributeView(file, PosixFileAttributeView.class);
        if (posix != null) {
            Set<PosixFilePermission> permissions = posix.readAttributes().permissions();
            if (permissions.contains(PosixFilePermission.OTHERS_READ)) {
                throw new IllegalArgumentException("s3CredentialsFile " + file
                        + " is readable by every user; chmod o-r it");
            }
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(file)) {
            properties.load(input);
        }
        Credentials credentials = new Credentials(properties.getProperty("accessKey"),
                properties.getProperty("secretKey"), properties.getProperty("sessionToken"));
        if (credentials.accessKey() == null || credentials.secretKey() == null) {
            throw new IllegalArgumentException("s3CredentialsFile " + file
                    + " needs accessKey= and secretKey=");
        }
        return credentials;
    }

    /**
     * Checks an endpoint URL and drops a trailing "/".
     *
     * @param endpoint endpoint as configured; without a scheme it is https
     * @return {@code http[s]://host[:port]}
     */
    private static String normalizeEndpoint(String endpoint) {
        String value = endpoint.trim();
        if (!value.contains("://")) {
            value = "https://" + value;
        }
        URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid s3Endpoint: " + endpoint, e);
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new IllegalArgumentException("s3Endpoint must be http:// or https://: " + endpoint);
        }
        if (uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                || uri.getRawFragment() != null
                || (uri.getRawPath() != null && !uri.getRawPath().isEmpty()
                        && !"/".equals(uri.getRawPath()))) {
            throw new IllegalArgumentException("s3Endpoint must be scheme://host[:port] only: "
                    + endpoint);
        }
        return scheme + "://" + uri.getRawAuthority();
    }

    /**
     * Shows the start of an access key ID only.
     *
     * @param accessKey access key ID
     * @return masked key
     */
    private static String maskAccessKey(String accessKey) {
        return accessKey.length() <= 4 ? REDACTED : accessKey.substring(0, 4) + REDACTED;
    }

    /**
     * Turns a blank value into null and trims the others.
     *
     * @param value value or null
     * @return trimmed value, or null when blank
     */
    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    /**
     * S3 credentials.
     *
     * @param accessKey access key ID, or null
     * @param secretKey secret access key, or null
     * @param sessionToken session token, or null
     */
    public record Credentials(String accessKey, String secretKey, String sessionToken) {

        /**
         * Trims the values; blank ones become null.
         *
         * @param accessKey access key ID
         * @param secretKey secret access key
         * @param sessionToken session token
         */
        public Credentials {
            accessKey = blankToNull(accessKey);
            secretKey = blankToNull(secretKey);
            sessionToken = blankToNull(sessionToken);
        }

        @Override
        public String toString() {
            return "Credentials[accessKey=" + (accessKey == null ? null : maskAccessKey(accessKey))
                    + ", secretKey=" + (secretKey == null ? null : REDACTED)
                    + ", sessionToken=" + (sessionToken == null ? null : REDACTED) + "]";
        }
    }
}
