package pro.surpin.data.arrowflight.spi.catalog;

import java.util.Objects;

/**
 * How the server reads a table snapshot.
 */
public sealed interface ReadSpec {

    /**
     * The snapshot's files are read with {@code read_parquet}.
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
}
