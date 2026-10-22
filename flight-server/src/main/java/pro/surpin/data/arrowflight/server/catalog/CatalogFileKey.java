package pro.surpin.data.arrowflight.server.catalog;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Names a data file of a catalog table in a partial-result ticket together with the snapshot it
 * belongs to: {@code @snapshot:<url-encoded snapshot id>@<absolute location>}. The executing
 * node reads the file with the columns of that snapshot, never with the table's current ones.
 *
 * @param snapshotId snapshot ID from the planning node
 * @param location absolute file location
 */
public record CatalogFileKey(String snapshotId, String location) {

    private static final String PREFIX = "@snapshot:";

    /**
     * Writes the key.
     *
     * @return ticket file entry
     */
    public String encode() {
        return PREFIX + URLEncoder.encode(snapshotId, StandardCharsets.UTF_8) + "@" + location;
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
        return new CatalogFileKey(URLDecoder.decode(entry.substring(PREFIX.length(), at),
                StandardCharsets.UTF_8), entry.substring(at + 1));
    }
}
