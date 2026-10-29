package pro.surpin.data.arrowflight.auth.jwt;

import com.nimbusds.jose.JWSAlgorithm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validated settings of the JWT authentication provider, read from {@code FLIGHT_JWT_*}.
 *
 * @param jwksUrl HTTPS address of the issuer's key set, or null with a key set file
 * @param jwksFile key set file, or null with a key set address
 * @param issuer exact issuer ({@code iss}) a token must carry
 * @param audiences accepted audiences; a token's {@code aud} must contain one of them
 * @param algorithms accepted signature algorithms, all asymmetric
 * @param userClaim claim naming the caller's identity
 * @param groupsClaim claim (dotted path into nested objects) listing the caller's groups, or null
 * @param clockSkewSeconds tolerated clock difference for expiry and not-before
 * @param jwksCacheMillis how long a fetched key set is used before it is fetched again
 * @param jwksRefreshLimitMillis minimum time between key set fetches triggered by unknown keys
 * @param trustStore trust store file for the key set address, or null for the JVM default trust
 * @param trustStoreType trust store type
 * @param trustStorePassword trust store password, or null
 * @param connectTimeoutMillis connect timeout of a key set fetch
 * @param readTimeoutMillis read timeout of a key set fetch
 * @param basicPassword whether a Basic credential whose password is a token is accepted
 */
record JwtSettings(URL jwksUrl, Path jwksFile, String issuer, Set<String> audiences,
        Set<JWSAlgorithm> algorithms, String userClaim, String groupsClaim, int clockSkewSeconds,
        long jwksCacheMillis, long jwksRefreshLimitMillis, Path trustStore, String trustStoreType,
        String trustStorePassword, int connectTimeoutMillis, int readTimeoutMillis, boolean basicPassword) {

    /** Asymmetric algorithms a token may be signed with; HMAC and unsigned tokens are never accepted. */
    static final Set<JWSAlgorithm> SUPPORTED = Set.of(JWSAlgorithm.RS256, JWSAlgorithm.RS384,
            JWSAlgorithm.RS512, JWSAlgorithm.PS256, JWSAlgorithm.PS384, JWSAlgorithm.PS512,
            JWSAlgorithm.ES256, JWSAlgorithm.ES384, JWSAlgorithm.ES512);
    private static final Pattern CLAIM = Pattern.compile("[A-Za-z0-9_:/-]+(\\.[A-Za-z0-9_:/-]+)*");

    /**
     * Copies the collections. The copies answer {@code contains(null)} with false: the nimbus claims verifier asks that,
     * and an immutable {@code Set.copyOf} throws on it (older nimbus versions on the server class path).
     */
    JwtSettings {
        audiences = Collections.unmodifiableSet(new LinkedHashSet<>(audiences));
        algorithms = Collections.unmodifiableSet(new LinkedHashSet<>(algorithms));
    }

    /**
     * Reads and validates the provider settings.
     *
     * @param environment deployment settings
     * @return validated settings
     * @throws IllegalArgumentException when a setting is missing, malformed or insecure
     */
    static JwtSettings fromEnvironment(Map<String, String> environment) {
        String url = optional(environment, "FLIGHT_JWT_JWKS_URL");
        String file = optional(environment, "FLIGHT_JWT_JWKS_FILE");
        if ((url == null) == (file == null)) {
            throw new IllegalArgumentException("Set exactly one of FLIGHT_JWT_JWKS_URL and FLIGHT_JWT_JWKS_FILE");
        }
        String issuer = required(environment, "FLIGHT_JWT_ISSUER");
        Set<String> audiences = new LinkedHashSet<>(List.of(
                required(environment, "FLIGHT_JWT_AUDIENCE").split("[\\s,]+")));
        audiences.remove("");
        String userClaim = optional(environment, "FLIGHT_JWT_USER_CLAIM");
        if (userClaim == null) {
            userClaim = "sub";
        }
        if (!CLAIM.matcher(userClaim).matches() || userClaim.contains(".")) {
            throw new IllegalArgumentException("Malformed FLIGHT_JWT_USER_CLAIM: " + userClaim);
        }
        String groupsClaim = optional(environment, "FLIGHT_JWT_GROUPS_CLAIM");
        if (groupsClaim != null && !CLAIM.matcher(groupsClaim).matches()) {
            throw new IllegalArgumentException("Malformed FLIGHT_JWT_GROUPS_CLAIM: " + groupsClaim);
        }
        String store = optional(environment, "FLIGHT_JWT_TRUSTSTORE");
        String storeType = optional(environment, "FLIGHT_JWT_TRUSTSTORE_TYPE");
        if (url == null && store != null) {
            throw new IllegalArgumentException("FLIGHT_JWT_TRUSTSTORE belongs to FLIGHT_JWT_JWKS_URL");
        }
        return new JwtSettings(url == null ? null : httpsUrl(url), file == null ? null : Path.of(file), issuer,
                audiences, algorithms(optional(environment, "FLIGHT_JWT_ALGORITHMS")), userClaim, groupsClaim,
                bounded(environment, "FLIGHT_JWT_CLOCK_SKEW_SECONDS", 60, 0, 600),
                bounded(environment, "FLIGHT_JWT_JWKS_CACHE_MINUTES", 15, 1, 1440) * 60_000L,
                bounded(environment, "FLIGHT_JWT_JWKS_REFRESH_LIMIT_SECONDS", 30, 1, 3600) * 1_000L,
                store == null ? null : Path.of(store), storeType == null ? "PKCS12" : storeType,
                secret(environment, "FLIGHT_JWT_TRUSTSTORE_PASSWORD"),
                bounded(environment, "FLIGHT_JWT_CONNECT_TIMEOUT_MS", 5_000, 100, 120_000),
                bounded(environment, "FLIGHT_JWT_READ_TIMEOUT_MS", 10_000, 100, 300_000),
                flag(environment, "FLIGHT_JWT_BASIC_PASSWORD"));
    }

    /**
     * Parses the key set address, which must be HTTPS (keys fetched in clear text could be replaced).
     *
     * @param value configured address
     * @return URL
     */
    private static URL httpsUrl(String value) {
        try {
            URI uri = new URI(value);
            if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getUserInfo() != null) {
                throw new IllegalArgumentException("FLIGHT_JWT_JWKS_URL must be https://host/...: " + value);
            }
            return uri.toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            throw new IllegalArgumentException("Malformed FLIGHT_JWT_JWKS_URL: " + value, e);
        }
    }

    /**
     * Parses the accepted algorithms.
     *
     * @param value comma- or space-separated names, or null for all supported
     * @return algorithms
     */
    private static Set<JWSAlgorithm> algorithms(String value) {
        if (value == null) {
            return SUPPORTED;
        }
        Set<JWSAlgorithm> algorithms = new LinkedHashSet<>();
        for (String name : value.split("[\\s,]+")) {
            JWSAlgorithm algorithm = JWSAlgorithm.parse(name.toUpperCase(Locale.ROOT));
            if (!SUPPORTED.contains(algorithm)) {
                throw new IllegalArgumentException("FLIGHT_JWT_ALGORITHMS: " + name + " is not accepted; use "
                        + "RS256/384/512, PS256/384/512 or ES256/384/512");
            }
            algorithms.add(algorithm);
        }
        return algorithms;
    }

    /**
     * Reads a secret from {@code NAME} or from the file named by {@code NAME_FILE}.
     *
     * @param environment deployment settings
     * @param name setting name
     * @return secret, or null when neither is set
     */
    private static String secret(Map<String, String> environment, String name) {
        String value = optional(environment, name);
        String file = optional(environment, name + "_FILE");
        if (value != null && file != null) {
            throw new IllegalArgumentException("Set " + name + " or " + name + "_FILE, not both");
        }
        if (file == null) {
            return value;
        }
        try {
            return Files.readString(Path.of(file), StandardCharsets.UTF_8).strip();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + name + "_FILE", e);
        }
    }

    /**
     * Reads a required non-blank setting.
     *
     * @param environment deployment settings
     * @param name setting name
     * @return value
     */
    private static String required(Map<String, String> environment, String name) {
        String value = optional(environment, name);
        if (value == null) {
            throw new IllegalArgumentException("JWT authentication requires " + name);
        }
        return value;
    }

    /**
     * Reads an optional setting.
     *
     * @param environment deployment settings
     * @param name setting name
     * @return trimmed value, or null when unset or blank
     */
    private static String optional(Map<String, String> environment, String name) {
        String value = environment.get(name);
        return value == null || value.isBlank() ? null : value.trim();
    }

    /**
     * Reads a boolean setting, refusing anything but true and false.
     *
     * @param environment deployment settings
     * @param name setting name
     * @return value, false when unset
     */
    private static boolean flag(Map<String, String> environment, String name) {
        String value = optional(environment, name);
        if (value == null || value.equalsIgnoreCase("false")) {
            return false;
        }
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        throw new IllegalArgumentException(name + " must be true or false");
    }

    /**
     * Reads a bounded integer setting.
     *
     * @param environment deployment settings
     * @param name setting name
     * @param fallback value when unset
     * @param min smallest accepted value
     * @param max largest accepted value
     * @return value
     */
    private static int bounded(Map<String, String> environment, String name, int fallback, int min, int max) {
        String value = optional(environment, name);
        if (value == null) {
            return fallback;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
        if (parsed < min || parsed > max) {
            throw new IllegalArgumentException(name + " must be between " + min + " and " + max);
        }
        return parsed;
    }

    @Override
    public String toString() {
        return "jwks=" + (jwksUrl != null ? jwksUrl : jwksFile) + " issuer=" + issuer + " audiences=" + audiences
                + " userClaim=" + userClaim + (groupsClaim == null ? "" : " groupsClaim=" + groupsClaim);
    }
}
