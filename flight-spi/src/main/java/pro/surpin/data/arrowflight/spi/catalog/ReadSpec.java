package pro.surpin.data.arrowflight.spi.catalog;

import java.util.Objects;

/**
 * How the server reads a table snapshot.
 */
public sealed interface ReadSpec {

    /**
     * The snapshot's files are read with {@code read_parquet}: the declared columns by name (a
     * column missing in a file reads NULL), each partition column a constant of its declared
     * type from {@link TableFile#partitionValues()} (a missing value is NULL). In partial mode
     * the planning node ships every file's partition values in the ticket, so executing nodes
     * never list partitions again (see {@link Catalog#pinnedColumns}).
     *
     * @param hivePartitionedPaths whether every path has {@code key=value} segments for the
     *                             partition columns
     */
    record ParquetFiles(boolean hivePartitionedPaths) implements ReadSpec {
    }

    /**
     * DuckDB's iceberg extension may read the table itself (complete answers only).
     *
     * @param metadataLocation absolute URI of the table metadata file
     */
    record IcebergMetadata(String metadataLocation) implements ReadSpec {

        /**
         * Checks the location.
         *
         * @param metadataLocation metadata file
         */
        public IcebergMetadata {
            Objects.requireNonNull(metadataLocation, "metadataLocation");
        }
    }

    /**
     * The catalog reads the snapshot's files itself and hands the rows to the server as Arrow
     * streams through {@link Catalog#exportArrowScan}, for file formats DuckDB cannot read (ORC).
     * The server opens one stream per reference of the table in a query and asks for the
     * columns and filters of that reference ({@link ArrowScan}). Partition columns come in the
     * stream as text (UTF-8, NULL for a missing value), the server converts them to their
     * declared types as for {@link ParquetFiles}; every other column comes as an Arrow type
     * DuckDB reads as the column's declared type. In partial mode the planning node ships every
     * file's partition values in the ticket, as for {@link ParquetFiles}.
     *
     * @param format name of the file format, for messages (for example {@code orc})
     * @since SPI 1.4
     */
    record ArrowStream(String format) implements ReadSpec {

        /**
         * Checks the format name.
         *
         * @param format file format
         */
        public ArrowStream {
            Objects.requireNonNull(format, "format");
        }
    }
}
