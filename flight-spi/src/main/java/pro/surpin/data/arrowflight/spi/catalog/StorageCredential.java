package pro.surpin.data.arrowflight.spi.catalog;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

/**
 * Temporary storage credentials a catalog received for one table (for example vended by an
 * Iceberg REST catalog). The server installs them as a temporary DuckDB secret before it reads
 * the table's files, scoped to the directory that holds the snapshot's files below
 * {@code prefix}, and only when that lies below one of the catalog's readable roots. Values
 * marked sensitive never appear in errors or logs.
 *
 * @param prefix URI prefix the credentials apply to, as the catalog returned it
 * @param type DuckDB secret type: s3, gcs or r2
 * @param options secret options in the order they are written, as for
 *                {@link pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup#createSecret}
 * @param expiresAt when the credentials expire, or null when the catalog did not say; the
 *                  server does not install expired credentials
 * @since SPI 1.7
 */
public record StorageCredential(String prefix, String type, Map<String, SecretValue> options,
        Instant expiresAt) {

    /**
     * Checks the parts and copies the options, keeping their order.
     *
     * @param prefix URI prefix
     * @param type secret type
     * @param options secret options
     * @param expiresAt expiry, or null
     */
    public StorageCredential {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(options, "options");
        options = Collections.unmodifiableMap(new LinkedHashMap<>(options));
    }
}
