package pro.surpin.data.arrowflight.server.adapters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.server.LogUtil;

/**
 * Connections to the node's shared DuckDB that got {@code streaming_buffer_size} before the
 * lockdown ({@code duckDbSharedDatabase} with {@code duckDbStreamingBufferSize}). DuckDB keeps
 * that setting per connection and refuses it once the configuration is locked, so connections
 * opened later keep DuckDB's default; these are opened up front, lent to sessions and taken
 * back when a session ends.
 */
final class BufferedConnections {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedConnections.class);

    private final Connection root;
    private final int size;
    private final ArrayDeque<Connection> idle;
    private long missed;
    private boolean closed;

    /**
     * Creates the pool of one shared DuckDB.
     *
     * @param root        connection that owns the shared DuckDB
     * @param connections connections to it with the buffer size set, all idle
     */
    BufferedConnections(Connection root, List<Connection> connections) {
        this.root = root;
        this.size = connections.size();
        this.idle = new ArrayDeque<>(connections);
    }

    /**
     * Returns the connection of the shared DuckDB these connections belong to.
     *
     * @return owning connection
     */
    Connection root() {
        return root;
    }

    /**
     * Returns the number of connections opened for this pool.
     *
     * @return pool size
     */
    int size() {
        return size;
    }

    /**
     * Lends an idle connection.
     *
     * @return connection, or null when all are lent or the pool is closed
     */
    synchronized Connection take() {
        if (closed || idle.isEmpty()) {
            if (!closed && missed++ == 0) {
                LOGGER.warn("node={} duckdb=sharedStreamingBuffer exhausted=true connections={}"
                        + " reason=\"more sessions than duckDbSharedBufferedConnections; further"
                        + " sessions use DuckDB's default buffer\"", LogUtil.node(), size);
            }
            return null;
        }
        return idle.poll();
    }

    /**
     * Takes back a lent connection after dropping the temporary views a session left; the
     * caller has already detached the session's catalog.
     *
     * @param conn connection lent by {@link #take()}
     * @return true when the pool keeps it, false when the caller must close it
     */
    boolean give(Connection conn) {
        synchronized (this) {
            if (closed) {
                return false;
            }
        }
        try {
            dropTemporaryViews(conn);
        } catch (SQLException e) {
            LOGGER.debug("DuckDB connection not reusable", e);
            return false;
        }
        synchronized (this) {
            if (closed) {
                return false;
            }
            idle.push(conn);
            return true;
        }
    }

    /**
     * Returns the number of idle connections.
     *
     * @return idle connections
     */
    synchronized int idle() {
        return idle.size();
    }

    /**
     * Returns how often a session found no idle connection.
     *
     * @return sessions without a buffered connection
     */
    synchronized long missed() {
        return missed;
    }

    /**
     * Closes the idle connections; connections given back later are refused.
     */
    synchronized void close() {
        closed = true;
        for (Connection conn : idle) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.warn("Failed to close DuckDB resource", e);
            }
        }
        idle.clear();
    }

    /**
     * Drops the temporary views of a connection (join views, Arrow streams) so the next session
     * starts without them.
     *
     * @param conn connection to clean
     * @throws SQLException when a view cannot be listed or dropped
     */
    static void dropTemporaryViews(Connection conn) throws SQLException {
        List<String> views = new ArrayList<>();
        try (Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("SELECT schema_name, view_name FROM duckdb_views()"
                        + " WHERE temporary AND database_name = 'temp'")) {
            while (rs.next()) {
                views.add("temp." + quote(rs.getString(1)) + "." + quote(rs.getString(2)));
            }
        }
        try (Statement s = conn.createStatement()) {
            for (String view : views) {
                s.execute("DROP VIEW IF EXISTS " + view);
            }
        }
    }

    /**
     * Quotes a DuckDB identifier.
     *
     * @param name identifier
     * @return quoted identifier
     */
    private static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
