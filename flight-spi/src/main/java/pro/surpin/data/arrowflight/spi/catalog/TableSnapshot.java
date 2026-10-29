package pro.surpin.data.arrowflight.spi.catalog;

import java.util.List;
import java.util.Objects;

/**
 * The readable content of a table at one point in time.
 *
 * @param name table name
 * @param snapshotId opaque snapshot ID (Iceberg snapshot ID; for other catalogs a hash of the
 *                   files); another node reads the same snapshot through it
 * @param fingerprint changes whenever the readable content changes; views and caches key on it
 * @param columns declared columns; empty when the server should read the first file's footer
 * @param partitionColumns partition columns whose values are not stored in the files; each one is
 *                         also declared in {@code columns}, with its type
 * @param files data files, in a stable order
 * @param read how the files are read
 * @param credentials temporary storage credentials for the files (since SPI 1.7); empty when
 *                    the server's own store credentials read them
 */
public record TableSnapshot(TableName name, String snapshotId, String fingerprint,
        List<ColumnSpec> columns, List<String> partitionColumns, List<TableFile> files,
        ReadSpec read, List<StorageCredential> credentials) {

    /**
     * Checks the parts and copies the lists.
     *
     * @param name table name
     * @param snapshotId snapshot ID
     * @param fingerprint content fingerprint
     * @param columns declared columns
     * @param partitionColumns partition columns
     * @param files data files
     * @param read read specification
     * @param credentials storage credentials
     */
    public TableSnapshot {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(snapshotId, "snapshotId");
        Objects.requireNonNull(fingerprint, "fingerprint");
        Objects.requireNonNull(read, "read");
        columns = columns == null ? List.of() : List.copyOf(columns);
        partitionColumns = partitionColumns == null ? List.of() : List.copyOf(partitionColumns);
        files = files == null ? List.of() : List.copyOf(files);
        credentials = credentials == null ? List.of() : List.copyOf(credentials);
    }

    /**
     * Creates a snapshot read with the server's own store credentials.
     *
     * @param name table name
     * @param snapshotId snapshot ID
     * @param fingerprint content fingerprint
     * @param columns declared columns
     * @param partitionColumns partition columns
     * @param files data files
     * @param read read specification
     */
    public TableSnapshot(TableName name, String snapshotId, String fingerprint,
            List<ColumnSpec> columns, List<String> partitionColumns, List<TableFile> files,
            ReadSpec read) {
        this(name, snapshotId, fingerprint, columns, partitionColumns, files, read, List.of());
    }

    /**
     * Returns this snapshot with other files, keeping everything else.
     *
     * @param otherFiles files of the copy
     * @return snapshot with those files
     * @since SPI 1.7
     */
    public TableSnapshot withFiles(List<TableFile> otherFiles) {
        return new TableSnapshot(name, snapshotId, fingerprint, columns, partitionColumns,
                otherFiles, read, credentials);
    }
}
