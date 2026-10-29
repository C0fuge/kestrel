package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallStatus;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.services.QueryTables;
import pro.surpin.data.arrowflight.server.services.TimeTravelClause;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Decides access to Flight schemas, tables, and SQL statements. */
public interface FlightAuthorization extends AutoCloseable {
    /**
     * Checks schema discovery access.
     *
     * @param principal authenticated caller identity
     * @param schema schema name
     * @return true when visible
     */
    boolean canSeeSchema(String principal, String schema);

    /**
     * Checks table read access.
     *
     * @param principal authenticated caller identity
     * @param schema schema name
     * @param table table name
     * @return true when readable
     */
    boolean canReadTable(String principal, String schema, String table);

    /**
     * Tells whether this provider may restrict tables with row filters or column masks, so the
     * server asks {@link #restrictTable} for each table a query reads. A provider that does
     * answers {@link #canReadTable} true for a table it allows only with restrictions (listings
     * and the table checks of {@link #requireQuery} use it).
     *
     * @return false by default: tables are read whole or not at all
     */
    default boolean restrictsTables() {
        return false;
    }

    /**
     * Returns what a principal may read of one table a query reads; asked only when
     * {@link #restrictsTables} is true.
     *
     * @param principal authenticated caller identity
     * @param schema schema name
     * @param table table name
     * @param columns every column of the table, name to DuckDB type, in table order
     * @return the restriction; by default {@link TableRestriction#UNRESTRICTED} when
     *         {@link #canReadTable} allows the table, else {@link TableRestriction#DENIED}
     */
    default TableRestriction restrictTable(String principal, String schema, String table,
            java.util.Map<String, String> columns) {
        return canReadTable(principal, schema, table)
                ? TableRestriction.UNRESTRICTED : TableRestriction.DENIED;
    }

    /**
     * Requires access to each table read by a query.
     *
     * @param principal authenticated caller identity
     * @param query SQL statement
     * @throws org.apache.arrow.flight.FlightRuntimeException UNAUTHORIZED when a table may not be
     *         read, INVALID_ARGUMENT when the query's tables cannot be named
     */
    default void requireQuery(String principal, String query) {
        // A table read at an earlier snapshot (FOR VERSION AS OF) is checked like the table.
        ParquetQueryParser parsed = null;
        List<ParquetQueryParser.JoinTable> named;
        try {
            String text = TimeTravelClause.withoutClauses(query);
            if (ParquetQueryParser.hasNestedQuery(text)) {
                // Subqueries, CTEs and set operations: every table from DuckDB's own parse.
                named = QueryTables.of(text);
            } else {
                parsed = ParquetQueryParser.parse(text);
                // Tables the parse may not name (quoted join tables).
                named = ParquetQueryParser.checkedTables(text);
            }
        } catch (RuntimeException e) {
            // Refused as without authorization (a query that cannot be checked is not run).
            Throwable reason = e.getCause() instanceof IllegalArgumentException ? e.getCause() : e;
            throw CallStatus.INVALID_ARGUMENT.withDescription(String.valueOf(reason.getMessage()))
                    .withCause(e).toRuntimeException();
        }
        Set<String> checked = new HashSet<>();
        if (parsed == null) {
            if (named.isEmpty()) {
                throw denied();
            }
        } else if (parsed.isJoin) {
            if (parsed.joinTables.isEmpty()) {
                throw denied();
            }
            for (ParquetQueryParser.JoinTable table : parsed.joinTables) {
                requireTable(principal, table.schema(), table.table(), checked);
            }
        } else {
            requireTable(principal, parsed.schema, parsed.table, checked);
        }
        for (ParquetQueryParser.JoinTable table : named) {
            requireTable(principal, table.schema(), table.table(), checked);
        }
    }

    /**
     * Requires access to one query table.
     *
     * @param principal authenticated caller identity
     * @param schema schema name
     * @param table table name
     * @param checked already checked table keys
     */
    private void requireTable(String principal, String schema, String table,
            Set<String> checked) {
        if (schema == null || table == null) {
            throw denied();
        }
        String key = schema + "\u0000" + table;
        if (checked.add(key) && !canReadTable(principal, schema, table)) {
            throw denied();
        }
    }

    /**
     * Creates a generic denial status.
     *
     * @return unauthorized Flight error
     */
    private static RuntimeException denied() {
        return CallStatus.UNAUTHORIZED
                .withDescription("Schema or table access denied")
                .toRuntimeException();
    }

    @Override
    default void close() {
    }
}
