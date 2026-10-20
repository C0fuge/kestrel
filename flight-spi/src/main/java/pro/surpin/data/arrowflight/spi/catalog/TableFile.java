package pro.surpin.data.arrowflight.spi.catalog;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * One data file of a table snapshot.
 *
 * @param location path relative to the data directory (directory catalog) or absolute URI
 *                 (other catalogs)
 * @param size file length in bytes
 * @param modificationTime modification time in milliseconds since the epoch, 0 when unknown
 * @param hosts storage hosts that hold a block of the file; empty without locality
 * @param partitionValues values of partition columns that are not stored in the file
 * @param recordCount rows in the file, -1 when unknown
 * @param deletes delete files that apply to this file
 */
public record TableFile(String location, long size, long modificationTime, Set<String> hosts,
        Map<String, String> partitionValues, long recordCount, List<DeleteFile> deletes) {

    /**
     * Checks the parts and copies the collections, keeping their order.
     *
     * @param location file location
     * @param size length in bytes
     * @param modificationTime modification time
     * @param hosts block hosts
     * @param partitionValues partition values
     * @param recordCount row count or -1
     * @param deletes delete files
     */
    public TableFile {
        Objects.requireNonNull(location, "location");
        hosts = hosts == null || hosts.isEmpty()
                ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(hosts));
        partitionValues = partitionValues == null || partitionValues.isEmpty()
                ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(partitionValues));
        deletes = deletes == null ? List.of() : List.copyOf(deletes);
    }
}
