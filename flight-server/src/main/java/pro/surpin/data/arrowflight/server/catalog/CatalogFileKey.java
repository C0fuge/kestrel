package pro.surpin.data.arrowflight.server.catalog;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Names a data file of a catalog table in a partial-result ticket together with the snapshot it
 * belongs to: {@code @snapshot:<url-encoded snapshot id>@<absolute location>}. The executing
 * node reads the file with the columns of that snapshot, never with the table's current ones.
 * Files of snapshots read as Parquet files (Hive) carry their partition values too, so that the
 * executing node never lists partitions again:
 * {@code @snapshot:<id>@?<key>=<value>&<key>@<location>} (URL-encoded; a key without "=" has a
 * NULL value; {@code @?@} for a table without partitions).
 *
 * @param snapshotId snapshot ID from the planning node
 * @param location absolute file location
 * @param partitionValues partition column to value (null values allowed), in column order; null
 *                        when the key carries none (Iceberg)
 */
public record CatalogFileKey(String snapshotId, String location,
        Map<String, String> partitionValues) {

    private static final String PREFIX = "@snapshot:";

    /**
     * Copies the partition values, keeping their order and null values.
     *
     * @param snapshotId snapshot ID
     * @param location file location
     * @param partitionValues partition values, or null
     */
    public CatalogFileKey {
        partitionValues = partitionValues == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(partitionValues));
    }

    /**
     * Names a file without partition values (Iceberg).
     *
     * @param snapshotId snapshot ID
     * @param location file location
     */
    public CatalogFileKey(String snapshotId, String location) {
        this(snapshotId, location, null);
    }

    /**
     * Tells whether the key carries the file's partition values (files read as Parquet files).
     *
     * @return true when it does
     */
    public boolean carriesPartitionValues() {
        return partitionValues != null;
    }

    /**
     * Writes the key.
     *
     * @return ticket file entry
     */
    public String encode() {
        StringBuilder result = new StringBuilder(PREFIX)
                .append(URLEncoder.encode(snapshotId, StandardCharsets.UTF_8)).append('@');
        if (partitionValues != null) {
            result.append('?');
            boolean first = true;
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                result.append(first ? "" : "&")
                        .append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
                if (entry.getValue() != null) {
                    result.append('=')
                            .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
                }
                first = false;
            }
            result.append('@');
        }
        return result.append(location).toString();
    }

    /**
     * Tells whether a ticket file entry names a catalog file.
     *
     * @param entry ticket file entry
     * @return true for a catalog file key
     */
    public static boolean isKey(String entry) {
        return entry != null && entry.startsWith(PREFIX);
    }

    /**
     * Reads a key.
     *
     * @param entry ticket file entry
     * @return key
     * @throws IllegalArgumentException when the entry is not a catalog file key
     */
    public static CatalogFileKey decode(String entry) {
        if (!isKey(entry)) {
            throw new IllegalArgumentException("Not a catalog file entry: " + entry);
        }
        int at = entry.indexOf('@', PREFIX.length());
        if (at < 0 || at == entry.length() - 1) {
            throw new IllegalArgumentException("Bad catalog file entry: " + entry);
        }
        String snapshotId = URLDecoder.decode(entry.substring(PREFIX.length(), at),
                StandardCharsets.UTF_8);
        String rest = entry.substring(at + 1);
        if (!rest.startsWith("?")) {
            return new CatalogFileKey(snapshotId, rest, null);
        }
        int end = rest.indexOf('@');
        if (end < 0 || end == rest.length() - 1) {
            throw new IllegalArgumentException("Bad catalog file entry: " + entry);
        }
        Map<String, String> values = new LinkedHashMap<>();
        String query = rest.substring(1, end);
        if (!query.isEmpty()) {
            for (String pair : query.split("&", -1)) {
                int eq = pair.indexOf('=');
                String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq),
                        StandardCharsets.UTF_8);
                String value = eq < 0 ? null
                        : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
                if (key.isEmpty() || values.containsKey(key)) {
                    throw new IllegalArgumentException("Bad partition values in catalog file entry: "
                            + entry);
                }
                values.put(key, value);
            }
        }
        return new CatalogFileKey(snapshotId, rest.substring(end + 1), values);
    }
}
