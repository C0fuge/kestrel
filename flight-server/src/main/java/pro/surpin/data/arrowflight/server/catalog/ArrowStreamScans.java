package pro.surpin.data.arrowflight.server.catalog;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.BufferAllocator;
import org.duckdb.DuckDBConnection;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * The Arrow streams of one query over tables read as {@link ReadSpec.ArrowStream}, registered on
 * the query's DuckDB connection. DuckDB's {@code registerArrowStream} makes a temporary view over
 * a stream that can be read once, by one table reference of one statement: so every reference of
 * such a table gets its own stream under a name of its own, and the statement must be prepared
 * and run once (never taken from a statement cache). Closing drops the views and releases the
 * streams DuckDB did not read.
 */
public final class ArrowStreamScans implements AutoCloseable {

    /** Prefix of the registered view names. */
    static final String NAME_PREFIX = "__kestrel_arrow_";
    private static final AtomicLong NEXT = new AtomicLong();

    private final Connection connection;
    private final BufferAllocator allocator;
    private final Map<String, ArrowArrayStream> streams = new LinkedHashMap<>();

    /**
     * Starts the streams of one query.
     *
     * @param connection the DuckDB connection the query runs on
     * @param allocator allocator of the streams' buffers (the query's)
     */
    public ArrowStreamScans(Connection connection, BufferAllocator allocator) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    /**
     * Opens one stream of some files of a snapshot and names it as a FROM source with the
     * requested columns, each of its declared type (partition columns converted from text as
     * {@link CatalogSources#typedConstant} does).
     *
     * @param catalog the snapshot's catalog
     * @param snapshot snapshot read as {@link ReadSpec.ArrowStream}
     * @param files files to read
     * @param scan columns and filter hints
     * @return FROM source, to be read by one table reference of one statement
     * @throws IOException when the catalog cannot open the stream
     * @throws SQLException when DuckDB refuses the stream
     * @throws IllegalArgumentException when the snapshot is not read as Arrow streams or a
     *         requested column is not declared
     */
    public String source(Catalog catalog, TableSnapshot snapshot, List<TableFile> files,
            ArrowScan scan) throws IOException, SQLException {
        if (!(snapshot.read() instanceof ReadSpec.ArrowStream)) {
            throw new IllegalArgumentException(snapshot.name() + " is not read as Arrow streams");
        }
        String projection = projection(snapshot, scan);
        String name = NAME_PREFIX + NEXT.incrementAndGet();
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
        streams.put(name, stream);
        catalog.exportArrowScan(snapshot, files, scan, stream.memoryAddress(), allocator);
        connection.unwrap(DuckDBConnection.class).registerArrowStream(name, stream);
        return "(SELECT " + projection + " FROM " + name + ")";
    }

    /**
     * Writes the select list of a stream: the requested columns, partition columns converted to
     * their declared types.
     *
     * @param snapshot snapshot with its columns
     * @param scan requested columns
     * @return select list
     * @throws IllegalArgumentException when a requested column is not declared
     */
    static String projection(TableSnapshot snapshot, ArrowScan scan) {
        Map<String, ColumnSpec> declared = new LinkedHashMap<>();
        for (ColumnSpec column : snapshot.columns()) {
            declared.put(column.name(), column);
        }
        List<String> items = new ArrayList<>(scan.columns().size());
        for (String name : scan.columns()) {
            ColumnSpec column = declared.get(name);
            if (column == null) {
                throw new IllegalArgumentException("Column " + name + " is not declared by "
                        + snapshot.name());
            }
            String quoted = DuckDbAdapter.quoteIdentifier(name);
            if (!snapshot.partitionColumns().contains(name)
                    || "VARCHAR".equalsIgnoreCase(column.duckDbType())) {
                items.add(quoted);
            } else {
                items.add("TRY_CAST(" + quoted + " AS " + column.duckDbType() + ") AS " + quoted);
            }
        }
        return String.join(", ", items);
    }

    /**
     * Counts the streams opened so far.
     *
     * @return streams
     */
    public int size() {
        return streams.size();
    }

    /**
     * Drops the views and releases the streams; the first error is thrown after all are done.
     *
     * @throws SQLException when a view cannot be dropped
     */
    @Override
    public void close() throws SQLException {
        SQLException error = null;
        try (Statement statement = connection.createStatement()) {
            for (String name : streams.keySet()) {
                try {
                    statement.execute("DROP VIEW IF EXISTS " + name);
                } catch (SQLException e) {
                    if (error == null) {
                        error = e;
                    }
                }
            }
        } catch (SQLException e) {
            error = e;
        } finally {
            for (ArrowArrayStream stream : streams.values()) {
                // A stream DuckDB read is released already; release() then does nothing.
                stream.release();
                stream.close();
            }
            streams.clear();
        }
        if (error != null) {
            throw error;
        }
    }
}
