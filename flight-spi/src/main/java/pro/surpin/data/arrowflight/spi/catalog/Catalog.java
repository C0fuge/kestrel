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
