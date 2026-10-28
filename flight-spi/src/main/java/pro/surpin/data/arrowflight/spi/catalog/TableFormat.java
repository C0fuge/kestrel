package pro.surpin.data.arrowflight.spi.catalog;

import java.io.IOException;
import java.util.Optional;

import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;

/**
 * Plans the snapshots of tables of one format (Iceberg) for a catalog of another plugin that
 * only knows where a table's metadata lies, for example a Hive Metastore that registers Iceberg
 * tables with their metadata file. The format reads that metadata through the server's stores.
 */
public interface TableFormat extends AutoCloseable {

    /**
     * Plans a snapshot of a table from its current metadata file.
     *
     * @param table table name, as the catalog shows it
     * @param metadataLocation absolute URI of the table's current metadata file
     * @param snapshotId snapshot ID this format returned before (pinning), or null for the
     *                   current snapshot
     * @return snapshot, empty when the metadata has no such snapshot
     * @throws IOException when the metadata cannot be read
     */
    Optional<TableSnapshot> snapshot(TableName table, String metadataLocation, String snapshotId)
            throws IOException;

    /**
     * Plans the snapshot of a table at an earlier point (time travel), with the columns that
     * snapshot was written with, as {@link Catalog#asOf}. A catalog pins it later through
     * {@link #snapshot} with the returned snapshot ID. A point the metadata has no snapshot for
     * is an error, never the current snapshot. The default refuses.
     *
     * @param table table name, as the catalog shows it
     * @param metadataLocation absolute URI of the table's current metadata file
     * @param point the point to read
     * @return snapshot
     * @throws IOException when the metadata cannot be read
     * @throws IllegalArgumentException when the metadata has no snapshot for the point
     * @throws UnsupportedOperationException when the format keeps no snapshot history
     * @since SPI 1.5
     */
    default TableSnapshot asOf(TableName table, String metadataLocation, AsOf point)
            throws IOException {
        throw new UnsupportedOperationException("the table format keeps no snapshot history");
    }

    /**
     * Tells why the data files of a snapshot this format planned cannot each be read on their
     * own, as {@link Catalog#partialReadProblem(TableSnapshot)}.
     *
     * @param snapshot a snapshot this format returned
     * @return the reason, empty when every file can be read on its own
     */
    default Optional<String> partialReadProblem(TableSnapshot snapshot) {
        return Optional.of("the table format did not say whether its files can be read one by one");
    }

    /**
     * Describes what DuckDB needs to read this format's tables (extensions); a catalog that
     * uses the format adds it to its own contribution.
     *
     * @param setup receives the items
     */
    default void contributeDuckDb(DuckDbSetup setup) {
    }

    /**
     * Releases threads and caches.
     *
     * @throws IOException when releasing fails
     */
    @Override
    default void close() throws IOException {
    }
}
