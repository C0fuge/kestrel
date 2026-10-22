package pro.surpin.data.arrowflight.spi.catalog;

import java.util.List;
import java.util.Objects;

/**
 * A file that deletes rows of a data file (Iceberg v2 and v3).
 *
 * @param location absolute URI of the delete file
 * @param size file length in bytes
 * @param kind how rows are named
 * @param equalityFieldIds field IDs compared by an equality delete; empty otherwise
 */
public record DeleteFile(String location, long size, Kind kind, List<Integer> equalityFieldIds) {

    /** How a delete file names the rows it deletes. */
    public enum Kind {
        /** File path and row position. */
        POSITION,
        /** Values of some columns. */
        EQUALITY,
        /** A bitmap of row positions (Iceberg v3). */
        DELETION_VECTOR
    }

    /**
     * Checks the parts and copies the field IDs.
     *
     * @param location absolute URI
     * @param size length in bytes
     * @param kind kind of delete
     * @param equalityFieldIds field IDs of an equality delete
     */
    public DeleteFile {
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(kind, "kind");
        equalityFieldIds = equalityFieldIds == null ? List.of() : List.copyOf(equalityFieldIds);
    }
}
