package pro.surpin.data.arrowflight.plugin.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

/**
 * Reads the storage credentials an Iceberg REST catalog vends with loadTable
 * ({@code X-Iceberg-Access-Delegation: vended-credentials}): the {@code storage-credentials}
 * list (prefix + config) or, from catalogs that send only that, the S3 keys of the response's
 * {@code config} for the table location. Only S3 credentials are used; they become DuckDB
 * {@code s3} secret options. Values are never logged.
 */
final class VendedCredentials {

    private static final Logger LOGGER = LoggerFactory.getLogger(VendedCredentials.class);

    /** Not instantiable. */
    private VendedCredentials() {
    }

    /**
     * Reads the credentials of a loadTable response.
     *
     * @param body loadTable response
     * @param tableLocation the table's location, the prefix of credentials in {@code config}
     * @return credentials, empty when the catalog vended none usable
     */
    static List<StorageCredential> of(JsonNode body, String tableLocation) {
        List<StorageCredential> result = new ArrayList<>();
        JsonNode list = body.path("storage-credentials");
        if (list.isArray() && !list.isEmpty()) {
            for (JsonNode entry : list) {
                String prefix = entry.path("prefix").asText("");
                if (!prefix.isBlank()) {
                    s3(prefix, entry.path("config")).ifPresent(result::add);
                }
            }
            return List.copyOf(result);
        }
        if (tableLocation != null && !tableLocation.isBlank()) {
            s3(tableLocation, body.path("config")).ifPresent(result::add);
        }
        return List.copyOf(result);
    }

    /**
     * Maps one credential's config to an S3 secret.
     *
     * @param prefix location prefix
     * @param config Iceberg S3FileIO properties
     * @return credential, empty for another storage or without an access key
     */
    private static java.util.Optional<StorageCredential> s3(String prefix, JsonNode config) {
        String scheme = prefix.toLowerCase(Locale.ROOT);
        if (!scheme.startsWith("s3://") && !scheme.startsWith("s3a://")
                && !scheme.startsWith("s3n://")) {
            LOGGER.debug("iceberg=vendedCredentialsSkipped prefix={} reason=notS3", prefix);
            return java.util.Optional.empty();
        }
        Map<String, String> props = new LinkedHashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it = config.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> field = it.next();
            if (field.getValue().isValueNode() && !field.getValue().isNull()) {
                props.put(field.getKey(), field.getValue().asText());
            }
        }
        String keyId = props.get("s3.access-key-id");
        String secret = props.get("s3.secret-access-key");
        if (keyId == null || keyId.isBlank() || secret == null || secret.isBlank()) {
            return java.util.Optional.empty();
        }
        Map<String, SecretValue> options = new LinkedHashMap<>();
        options.put("KEY_ID", SecretValue.plain(keyId));
        options.put("SECRET", SecretValue.sensitive(secret));
        String token = props.get("s3.session-token");
        if (token != null && !token.isBlank()) {
            options.put("SESSION_TOKEN", SecretValue.sensitive(token));
        }
        String region = props.getOrDefault("client.region", props.get("s3.region"));
        if (region != null && !region.isBlank()) {
            options.put("REGION", SecretValue.plain(region));
        }
        String endpoint = props.get("s3.endpoint");
        if (endpoint != null && !endpoint.isBlank()) {
            String lower = endpoint.toLowerCase(Locale.ROOT);
            String hostPort = endpoint.replaceFirst("^[A-Za-z]+://", "");
            while (hostPort.endsWith("/")) {
                hostPort = hostPort.substring(0, hostPort.length() - 1);
            }
            options.put("ENDPOINT", SecretValue.plain(hostPort));
            options.put("USE_SSL", SecretValue.of(!lower.startsWith("http://")));
        }
        if ("true".equalsIgnoreCase(props.get("s3.path-style-access"))) {
            options.put("URL_STYLE", SecretValue.plain("path"));
        }
        Instant expiresAt = null;
        String expires = props.get("s3.session-token-expires-at-ms");
        if (expires != null && !expires.isBlank()) {
            try {
                expiresAt = Instant.ofEpochMilli(Long.parseLong(expires.strip()));
            } catch (NumberFormatException e) {
                LOGGER.warn("iceberg=vendedCredentialsExpiryUnreadable prefix={}", prefix);
            }
        }
        return java.util.Optional.of(new StorageCredential(prefix, "s3", options, expiresAt));
    }

    /**
     * Returns the earliest expiry of some credentials.
     *
     * @param credentials credentials
     * @return earliest expiry in epoch milliseconds, Long.MAX_VALUE when none expires
     */
    static long earliestExpiryMillis(List<StorageCredential> credentials) {
        long earliest = Long.MAX_VALUE;
        for (StorageCredential credential : credentials) {
            if (credential.expiresAt() != null) {
                earliest = Math.min(earliest, credential.expiresAt().toEpochMilli());
            }
        }
        return earliest;
    }
}
