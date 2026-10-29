package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.CallStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.TableRestriction;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;

/**
 * Applies the row filters and column masks of an authorization provider to a query: each
 * reference to a restricted table ({@link TableReferences}: after FROM, JOIN or a comma of a FROM
 * list) becomes {@code (SELECT * REPLACE (CAST((<mask>) AS <type>) AS c, ...) FROM s.t WHERE
 * (<filter>)) AS <alias or t>}. Fails closed: when a restricted table is also read where the
 * rewrite cannot reach (a reference the finder misses, an earlier snapshot), the query is refused
 * as UNAUTHORIZED, never run unrestricted.
 */
public final class QueryRestrictions {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRestrictions.class);

    /** Schema the completeness check gives the rewritten sources; no table is read under it. */
    private static final String CHECK_SCHEMA = " kestrel restricted source ";

    /**
     * A query after its restrictions.
     *
     * @param sql the SQL to run: the query itself when nothing of it is restricted
     * @param restricted whether a filter or mask was applied
     */
    public record Result(String sql, boolean restricted) {
    }

    private QueryRestrictions() {
    }

    /**
     * Applies the restrictions of every table a query reads.
     *
     * @param principal caller identity, or null
     * @param sql query with its catalog views already replaced by their queries
     * @param authorization the server's provider
     * @param columns takes a schema and a table name and returns the table's columns, name to
     *        DuckDB type in table order; called only when the provider restricts tables
     * @return the SQL to run and whether it is restricted
     * @throws org.apache.arrow.flight.FlightRuntimeException UNAUTHORIZED when a table may not be
     *         read or its restriction cannot be applied, INVALID_ARGUMENT when the query's tables
     *         cannot be named
     */
    public static Result apply(String principal, String sql, FlightAuthorization authorization,
            BiFunction<String, String, Map<String, String>> columns) {
        if (!authorization.restrictsTables()) {
            return new Result(sql, false);
        }
        Map<String, TableRestriction> restricted = new LinkedHashMap<>();
        Map<String, Map<String, String>> types = new LinkedHashMap<>();
        Set<String> seen = new HashSet<>();
        for (ParquetQueryParser.JoinTable table : tables(sql)) {
            if (table.schema() == null || table.table() == null) {
                throw denied("a table without schema");
            }
            String key = key(table.schema(), table.table());
            if (!seen.add(key)) {
                continue;
            }
            Map<String, String> tableColumns = columns.apply(table.schema(), table.table());
            TableRestriction restriction;
            try {
                restriction = authorization.restrictTable(principal, table.schema(), table.table(),
                        tableColumns);
            } catch (RuntimeException e) {
                LOGGER.warn("node={} query=denied reason='restriction of {}.{} failed' error='{}'",
                        LogUtil.node(), table.schema(), table.table(), e.toString());
                throw denied("restriction failed");
            }
            if (restriction == null || !restriction.allowed()) {
                throw denied("table " + table.schema() + "." + table.table());
            }
            if (restriction.restricts()) {
                restricted.put(key, restriction);
                types.put(key, tableColumns);
            }
        }
        if (restricted.isEmpty()) {
            return new Result(sql, false);
        }
        if (TimeTravelClause.present(sql)) {
            throw denied("a restricted query reads an earlier snapshot");
        }
        List<TableReferences.Reference> references = TableReferences.find(sql,
                (schema, table) -> restricted.containsKey(key(schema, table)));
        List<String> sources = new ArrayList<>(references.size());
        List<String> checks = new ArrayList<>(references.size());
        for (TableReferences.Reference reference : references) {
            String key = key(reference.schema(), reference.table());
            String alias = reference.aliased() ? ""
                    : " AS " + DuckDbAdapter.quoteIdentifier(reference.table());
            String table = DuckDbAdapter.quoteIdentifier(reference.table());
            try {
                sources.add(source(DuckDbAdapter.quoteIdentifier(reference.schema()) + "." + table,
                        restricted.get(key), types.get(key)) + alias);
                checks.add(source(DuckDbAdapter.quoteIdentifier(CHECK_SCHEMA) + "." + table,
                        restricted.get(key), types.get(key)) + alias);
            } catch (IllegalArgumentException e) {
                throw denied(e.getMessage());
            }
        }
        // Every read of a restricted table must now be one of the rewritten sources.
        for (ParquetQueryParser.JoinTable table : tables(TableReferences.rewrite(sql, references, checks))) {
            if (table.schema() != null && restricted.containsKey(key(table.schema(), table.table()))) {
                throw denied("a reference to " + table.schema() + "." + table.table()
                        + " cannot be restricted");
            }
        }
        LOGGER.debug("node={} query=restricted tables={} references={}", LogUtil.node(),
                restricted.size(), references.size());
        return new Result(TableReferences.rewrite(sql, references, sources), true);
    }

    /**
     * Builds the restricted source of one table.
     *
     * @param name quoted schema and table name
     * @param restriction its restriction
     * @param columns its columns, name to DuckDB type
     * @return parenthesised query of the allowed rows and values
     * @throws IllegalArgumentException when a mask names no column of the table
     */
    static String source(String name, TableRestriction restriction, Map<String, String> columns) {
        StringBuilder out = new StringBuilder("(SELECT *");
        if (!restriction.masks().isEmpty()) {
            Map<String, String> byName = new LinkedHashMap<>();
            columns.forEach((column, type) -> byName.put(column.toLowerCase(Locale.ROOT), type));
            List<String> replaced = new ArrayList<>();
            for (Map.Entry<String, String> mask : restriction.masks().entrySet()) {
                String type = byName.get(mask.getKey().toLowerCase(Locale.ROOT));
                if (type == null) {
                    throw new IllegalArgumentException("mask of an unknown column");
                }
                replaced.add("CAST((" + mask.getValue() + ") AS " + type + ") AS "
                        + DuckDbAdapter.quoteIdentifier(mask.getKey()));
            }
            out.append(" REPLACE (").append(String.join(", ", replaced)).append(')');
        }
        out.append(" FROM ").append(name);
        if (restriction.rowFilter() != null) {
            out.append(" WHERE (").append(restriction.rowFilter()).append(')');
        }
        return out.append(')').toString();
    }

    /**
     * Names the tables a query reads.
     *
     * @param sql query
     * @return tables in parse order
     * @throws org.apache.arrow.flight.FlightRuntimeException INVALID_ARGUMENT when they cannot be
     *         named
     */
    private static List<ParquetQueryParser.JoinTable> tables(String sql) {
        try {
            return QueryTables.of(TimeTravelClause.withoutClauses(sql));
        } catch (RuntimeException e) {
            throw CallStatus.INVALID_ARGUMENT.withDescription(String.valueOf(e.getMessage()))
                    .withCause(e).toRuntimeException();
        }
    }

    /**
     * Returns a table's key, as DuckDB compares names.
     *
     * @param schema schema name
     * @param table table name
     * @return lower-case key
     */
    private static String key(String schema, String table) {
        return (schema + "\u0000" + table).toLowerCase(Locale.ROOT);
    }

    /**
     * Logs a refusal and creates the generic denial status.
     *
     * @param reason why, for the log only
     * @return unauthorized Flight error
     */
    private static RuntimeException denied(String reason) {
        LOGGER.info("node={} query=denied reason='{}'", LogUtil.node(), reason);
        return CallStatus.UNAUTHORIZED.withDescription("Schema or table access denied")
                .toRuntimeException();
    }
}
