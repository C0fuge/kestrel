package pro.surpin.data.arrowflight.spi.catalog;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;

/**
 * Names the schemas and tables the server shows and the files of each table.
 */
public interface Catalog extends AutoCloseable {

    /**
     * Lists the schemas.
     *
     * @return schema names
     * @throws IOException when the catalog cannot be read
     */
    List<String> schemas() throws IOException;

    /**
     * Lists the tables of a schema.
     *
     * @param schema schema name
     * @return table names
     * @throws IOException when the catalog cannot be read
     */
    List<String> tables(String schema) throws IOException;

    /**
     * Returns the current snapshot of a table.
     *
     * @param table table name
     * @return snapshot, empty when the table does not exist
     * @throws IOException when the catalog cannot be read
     */
    Optional<TableSnapshot> current(TableName table) throws IOException;

    /**
     * Returns the snapshot the planning node used, for an executing node in partial mode.
     *
     * @param table table name
     * @param snapshotId snapshot ID from the planning node
     * @return that snapshot, empty when it is not available
     * @throws IOException when the catalog cannot be read
     */
    default Optional<TableSnapshot> pinned(TableName table, String snapshotId) throws IOException {
        return current(table).filter(s -> s.snapshotId().equals(snapshotId));
    }

    /**
     * Returns the columns of the snapshot the planning node used, for an executing node whose
     * ticket names the snapshot's files with their partition values (snapshots read as
     * {@link ReadSpec.ParquetFiles} or {@link ReadSpec.ArrowStream}): such a node never lists the
     * files again, so the returned snapshot may come without files. The default returns
     * {@link #pinned}.
     *
     * @param table table name
     * @param snapshotId snapshot ID from the planning node
     * @return a snapshot with the same columns, empty when the columns changed since
     * @throws IOException when the catalog cannot be read
     * @since SPI 1.1
     */
    default Optional<TableSnapshot> pinnedColumns(TableName table, String snapshotId)
            throws IOException {
        return pinned(table, snapshotId);
    }

    /**
     * Lists the views of a schema the catalog can show. A view's name is never also a table name
     * of the schema ({@link #tables} lists tables only). The default has none.
     *
     * @param schema schema name
     * @return views, empty for an unknown schema
     * @throws IOException when the catalog cannot be read
     * @since SPI 1.6
     */
    default List<CatalogView> views(String schema) throws IOException {
        return List.of();
    }

    /**
     * Returns the snapshot of a table at an earlier point (time travel), planned with the columns
     * that snapshot was written with. Another node pins it through {@link #pinned} like the
     * current one. A point the table has no snapshot for (an unknown or expired snapshot ID, an
     * unknown reference, an instant before the first commit) is an error, never the current
     * snapshot. The default refuses: catalogs without snapshot history do not support it.
     *
     * @param table table name
     * @param point the point to read
     * @return snapshot, empty when the table does not exist
     * @throws IOException when the catalog cannot be read
     * @throws IllegalArgumentException when the table has no snapshot for the point
     * @throws UnsupportedOperationException when the catalog keeps no snapshot history
     * @since SPI 1.2
     */
    default Optional<TableSnapshot> asOf(TableName table, AsOf point) throws IOException {
        throw new UnsupportedOperationException("the catalog keeps no snapshot history");
    }

    /**
     * Tells why the data files of a snapshot cannot each be read on their own with
     * {@code read_parquet} and the snapshot's columns (field IDs), so that the server answers
     * queries of the table completely instead of in parts: for example a nested column whose
     * fields were renamed, or data files that are not Parquet. Delete files are seen by the
     * server itself.
     *
     * @param snapshot a snapshot this catalog returned
     * @return the reason, empty when every file can be read on its own
     */
    default Optional<String> partialReadProblem(TableSnapshot snapshot) {
        return Optional.empty();
    }

    /**
     * Reads some data files of a snapshot read as {@link ReadSpec.ArrowStream} into an Arrow C
     * stream the server allocated: the plugin fills the {@code ArrowArrayStream} at the address
     * (for example with Arrow Java's {@code Data.exportArrayStream}) and must not keep it; the
     * server consumes and releases it, from another thread. Only plugins that take Arrow from
     * the server ({@code share=arrow} in the descriptor) can implement this. The batches carry
     * exactly the columns of the request, in its order, with the files' rows in the files'
     * order. Errors while reading surface through the stream. The stream's callbacks run on
     * DuckDB's threads, without the plugin's context class loader. The default refuses.
     *
     * @param snapshot a snapshot this catalog returned, read as {@link ReadSpec.ArrowStream}
     * @param files files of the snapshot to read, with their partition values
     * @param scan columns and filter hints
     * @param streamAddress memory address of an empty {@code ArrowArrayStream} C struct
     * @param allocator the server's {@code org.apache.arrow.memory.BufferAllocator} for the
     *                  stream's buffers; the plugin may make a child allocator of it
     * @throws IOException when the stream cannot be opened
     * @throws UnsupportedOperationException when the catalog has no tables read this way
     * @since SPI 1.4
     */
    default void exportArrowScan(TableSnapshot snapshot, List<TableFile> files, ArrowScan scan,
            long streamAddress, Object allocator) throws IOException {
        throw new UnsupportedOperationException("the catalog has no tables read as Arrow streams");
    }

    /**
     * Returns the roots every file and metadata location of this catalog lies under; the server
     * hides tables with locations elsewhere.
     *
     * @return readable roots
     */
    List<String> readableRoots();

    /**
     * Describes what DuckDB needs to read this catalog's tables; checked and written out by the
     * server like a store's contribution.
     *
     * @param setup receives the items
     */
    default void contributeDuckDb(DuckDbSetup setup) {
    }

    /**
     * Tells whether schema and table names never change while the server runs, so that the
     * server lists them once.
     *
     * @return false by default
     */
    default boolean namesAreStatic() {
        return false;
    }

    /**
     * Releases clients and threads of the catalog.
     *
     * @throws IOException when releasing fails
     */
    @Override
    default void close() throws IOException {
    }
}
