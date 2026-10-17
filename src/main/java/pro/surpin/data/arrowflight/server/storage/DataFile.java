package pro.surpin.data.arrowflight.server.storage;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * One data file as its store lists it.
 *
 * @param relativePath path below the data directory, the name used in tickets and inventories
 * @param location the store's own name for the file, used to open it and to name it to DuckDB
 * @param size file length in bytes
 * @param modificationTime modification time in milliseconds since the epoch
 * @param hosts storage hosts that hold a block of the file, most blocks last; empty without
 *              locality
 */
public record DataFile(String relativePath, String location, long size, long modificationTime,
        Set<String> hosts) {

    /**
     * Creates a data file and keeps its own copy of the hosts, in their order.
     *
     * @param relativePath path below the data directory
     * @param location the store's own name for the file
     * @param size file length in bytes
     * @param modificationTime modification time in milliseconds since the epoch
     * @param hosts storage hosts that hold a block of the file
     */
    public DataFile {
        Objects.requireNonNull(relativePath, "relativePath");
        Objects.requireNonNull(location, "location");
        hosts = hosts == null || hosts.isEmpty()
                ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(hosts));
    }
}
