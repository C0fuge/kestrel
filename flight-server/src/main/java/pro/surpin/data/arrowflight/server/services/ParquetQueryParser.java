package pro.surpin.data.arrowflight.server.services;

import org.jooq.*;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.metrics.CacheMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses SQL SELECT queries against Parquet tables, extracting schema, table, columns, filters, and join info.
 */
public class ParquetQueryParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetQueryParser.class);
    private static final int PARSE_CACHE_CAPACITY = 1024;
    private static final Map<String, ParquetQueryParser> PARSE_CACHE =
            Collections.synchronizedMap(
                    new LinkedHashMap<>(PARSE_CACHE_CAPACITY, 0.75f, true));
    /**
     * Parses by the text as callers pass it, before {@link #normalized} preparation; holds only
     * texts that parsed, so a refused text is checked (and refused) again every time.
     */
    private static final Map<String, ParquetQueryParser> RAW_PARSE_CACHE =
            Collections.synchronizedMap(
                    new LinkedHashMap<>(PARSE_CACHE_CAPACITY, 0.75f, true));

    /**
     * A table reference in a JOIN, with optional schema and alias.
     */
    public record JoinTable(String schema, String table, String alias) {}

    /**
     * A selected expression in a query, describing its aggregation function and column references.
     */
    public static final class SelectExpr {
        /**
         * Supported aggregation function types.
         */
        public enum AggFunc { COLUMN, COUNT_STAR, COUNT, SUM, MIN, MAX }

        public final AggFunc func;
        public final String inputColumn;
        public final String inputExpression;
        public final List<String> inputColumns;
        public final Integer decimalScale;
        public final Integer decimalPrecision;
        public final String outputName;
        /** True for {@code count(DISTINCT x)} and {@code sum(DISTINCT x)}. */
        public final boolean distinct;
        /**
         * True for a computed GROUP BY key (item 55 (c)): a {@link AggFunc#COLUMN} item whose
         * value is {@link #inputExpression} (quoted SQL over the table's columns), named
         * {@link #outputName}; {@link #inputColumn} is the output name, not a table column.
         */
        public final boolean computed;

        /**
         * @param func aggregation function type
         * @param inputColumn raw column expression text
         * @param outputName rendered column alias
         */
        SelectExpr(AggFunc func, String inputColumn, String outputName) {
            this(func, inputColumn, inputColumn,
                    inputColumn == null ? Collections.emptyList() : List.of(inputColumn),
                    null, null, outputName);
        }

        /**
         * Creates a parsed select expression with its executable SQL and physical inputs.
         *
         * @param func aggregation function type
         * @param inputColumn unquoted input text used by simple-column paths
         * @param inputExpression quoted SQL expression used by DuckDB
         * @param inputColumns physical columns referenced by the expression
         * @param decimalScale decimal input scale, or null for non-decimal inputs
         * @param decimalPrecision decimal input precision, or null for non-decimal inputs
         * @param outputName rendered column alias
         */
        SelectExpr(AggFunc func, String inputColumn, String inputExpression,
                List<String> inputColumns, Integer decimalScale, Integer decimalPrecision,
                String outputName) {
            this(func, inputColumn, inputExpression, inputColumns, decimalScale,
                    decimalPrecision, outputName, false);
        }

        /**
         * Creates a parsed select expression, possibly over distinct input values.
         *
         * @param func aggregation function type
         * @param inputColumn unquoted input text used by simple-column paths
         * @param inputExpression quoted SQL expression used by DuckDB
         * @param inputColumns physical columns referenced by the expression
         * @param decimalScale decimal input scale, or null for non-decimal inputs
         * @param decimalPrecision decimal input precision, or null for non-decimal inputs
         * @param outputName rendered column alias
         * @param distinct whether the aggregate counts or sums distinct values only
         */
        @SuppressWarnings("java:S107") // Mirrors the fields of one immutable value object.
        SelectExpr(AggFunc func, String inputColumn, String inputExpression,
                List<String> inputColumns, Integer decimalScale, Integer decimalPrecision,
                String outputName, boolean distinct) {
            this(func, inputColumn, inputExpression, inputColumns, decimalScale, decimalPrecision,
                    outputName, distinct, false);
        }

        /**
         * Creates a computed GROUP BY key: its value is a deterministic expression of the
         * table's columns, its name the output name.
         *
         * @param expression quoted SQL expression used by DuckDB
         * @param outputName output column name
         * @return the select item
         */
        static SelectExpr computedKey(String expression, String outputName) {
            return new SelectExpr(AggFunc.COLUMN, outputName, expression, referencedColumns(expression),
                    null, null, outputName, false, true);
        }

        @SuppressWarnings("java:S107") // Mirrors the fields of one immutable value object.
        private SelectExpr(AggFunc func, String inputColumn, String inputExpression,
                List<String> inputColumns, Integer decimalScale, Integer decimalPrecision,
                String outputName, boolean distinct, boolean computed) {
            this.computed = computed;
            this.distinct = distinct;
            this.func = func;
            this.inputColumn = inputColumn;
            this.inputExpression = inputExpression;
            this.inputColumns = List.copyOf(inputColumns);
            this.decimalScale = decimalScale;
            this.decimalPrecision = decimalPrecision;
            this.outputName = outputName;
        }
    }

    public final String schema;
    public final String table;
    public final List<String> columns;
    public final String filter;
    /**
     * The WHERE condition {@link #filter} was rendered from, or null when not kept. Shared by
     * every user of the cached parse: read it, never change it. File pruning reads it instead of
     * parsing {@link #filter} again (item 40 (b)).
     */
    public final Condition where;

    public final boolean hasAggregation;
    public final List<String> groupByColumnNames;
    public final List<SelectExpr> selectExprs;

    public final boolean isJoin;
    public final List<JoinTable> joinTables;
    public final String duckDbSql;

    /** ORDER BY list as executable SQL, or null. Applied by every endpoint to its own rows. */
    public final String orderBy;
    /** LIMIT row count, or null. Applied by every endpoint to its own rows. */
    public final Long limit;
    /** OFFSET row count, or null. */
    public final Long offset;
    /** True for {@code SELECT DISTINCT}. Applied by every endpoint to its own rows. */
    public final boolean distinct;
    /** HAVING condition as executable SQL, or null. */
    public final String having;
    /**
     * Why each endpoint cannot answer this query for its own files, or null when it can. Set for
     * select items other than plain columns and plain count, sum, min and max (an expression over
     * an aggregate, avg, an aggregate with FILTER or OVER, any other expression) and for GROUP BY
     * expressions: the per-endpoint SQL cannot render them, and the client could not merge the
     * per-endpoint values of most of them.
     */
    public final String completeAnswerReason;
    /**
     * Why the endpoint answers of this query cannot be merged on one node (distributed complete
     * mode), or null when they can. The same as {@link #completeAnswerReason}, except that
     * computed GROUP BY keys ({@link SelectExpr#computed}) are no reason: every endpoint computes
     * them, and the merge groups by their output columns (item 55 (c)). The partial result mode
     * keeps refusing them (its clients merge the endpoint answers themselves).
     */
    public final String mergeReason;
    /**
     * Output names of computed GROUP BY keys named in the GROUP BY clause by their select alias
     * ({@code GROUP BY g} for {@code id % 6 AS g}). DuckDB binds such a name to a table column
     * of that name first: a merge must check that the table has none.
     */
    public final List<String> aliasGroupKeys;

    /**
     * @param schema table schema name
     * @param table table name
     * @param columns selected column names
     * @param filter WHERE clause expression
     * @param hasAggregation whether query uses aggregation
     * @param groupByColumnNames GROUP BY column names
     * @param selectExprs parsed select expressions
     */
    private ParquetQueryParser(String schema, String table, List<String> columns, String filter,
            boolean hasAggregation, List<String> groupByColumnNames, List<SelectExpr> selectExprs) {
        this(schema, table, columns, filter, hasAggregation, groupByColumnNames, selectExprs,
                false, Collections.emptyList(), null, null, null, null, false, null, null, null, null,
                Collections.emptyList());
    }

    /**
     * @param schema table schema name
     * @param table primary table name
     * @param columns selected column names
     * @param filter WHERE clause expression
     * @param hasAggregation whether query uses aggregation
     * @param groupByColumnNames GROUP BY column names
     * @param selectExprs parsed select expressions
     * @param isJoin whether query involves JOINs
     * @param joinTables list of joined tables
     * @param duckDbSql rewritten SQL for DuckDB execution
     * @param orderBy ORDER BY list as executable SQL, or null
     * @param limit LIMIT row count, or null
     * @param offset OFFSET row count, or null
     * @param distinct whether the query is SELECT DISTINCT
     * @param having HAVING condition as executable SQL, or null
     * @param completeAnswerReason why endpoints cannot answer for their own files, or null
     * @param where the WHERE condition the filter was rendered from, or null
     * @param mergeReason why endpoint answers cannot be merged on one node, or null
     * @param aliasGroupKeys computed GROUP BY keys named by their select alias
     */
    @SuppressWarnings("java:S107") // One immutable value object built in a single place.
    private ParquetQueryParser(String schema, String table, List<String> columns, String filter,
            boolean hasAggregation, List<String> groupByColumnNames, List<SelectExpr> selectExprs,
            boolean isJoin, List<JoinTable> joinTables, String duckDbSql,
            String orderBy, Long limit, Long offset, boolean distinct, String having,
            String completeAnswerReason, Condition where, String mergeReason,
            List<String> aliasGroupKeys) {
        this.mergeReason = mergeReason;
        this.aliasGroupKeys = List.copyOf(aliasGroupKeys);
        this.schema = schema;
        this.table = table;
        this.columns = List.copyOf(columns);
        this.filter = filter;
        this.hasAggregation = hasAggregation;
        this.groupByColumnNames = List.copyOf(groupByColumnNames);
        this.selectExprs = List.copyOf(selectExprs);
        this.isJoin = isJoin;
        this.joinTables = List.copyOf(joinTables);
        this.duckDbSql = duckDbSql;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
        this.distinct = distinct;
        this.having = having;
        this.completeAnswerReason = completeAnswerReason;
        this.where = where;
    }

    /**
     * Names the feature that only gives a correct answer when one node sees all the rows.
     *
     * @return feature name, or null when every node can answer for its own files
     */
    public String singleNodeFeature() {
        if (offset != null && offset > 0) {
            return "OFFSET";
        }
        if (having != null) {
            return "HAVING";
        }
        for (SelectExpr expr : selectExprs) {
            if (expr.distinct) {
                return "An aggregate over DISTINCT values";
            }
        }
        return null;
    }

    /**
     * Returns this query without its ORDER BY and LIMIT, as a part of its input must answer it
     * when the parts' answers are merged and ordered and limited afterwards.
     *
     * @return the same query without ORDER BY and LIMIT, or this query when it has neither
     */
    public ParquetQueryParser withoutOrderAndLimit() {
        if (orderBy == null && limit == null) {
            return this;
        }
        return new ParquetQueryParser(schema, table, columns, filter, hasAggregation,
                groupByColumnNames, selectExprs, isJoin, joinTables, duckDbSql, null, null, offset,
                distinct, having, completeAnswerReason, where, mergeReason, aliasGroupKeys);
    }

    /**
     * Returns this query with one more condition ANDed to its WHERE clause, as a client's join
     * runtime filters narrow a plain scan ({@link RuntimeFilters}). File pruning keeps reading
     * the original condition ({@link #where}), which accepts every row the new one does.
     *
     * @param condition executable SQL condition over the table's columns
     * @return the narrowed query
     */
    public ParquetQueryParser withExtraCondition(String condition) {
        String narrowed = filter == null || filter.isBlank()
                ? condition : "(" + filter + ") AND (" + condition + ")";
        return new ParquetQueryParser(schema, table, columns, narrowed, hasAggregation,
                groupByColumnNames, selectExprs, isJoin, joinTables, duckDbSql, orderBy, limit,
                offset, distinct, having, completeAnswerReason, where, mergeReason, aliasGroupKeys);
    }

    /**
     * Names the table column a plain output column of this query reads.
     *
     * @param outputName output column name as the result schema names it
     * @return the quoted source column, or null when the output is not one plain column
     */
    public String sourceColumnOf(String outputName) {
        if (selectExprs.isEmpty()) {
            return quoted(outputName);
        }
        String found = null;
        for (SelectExpr expr : selectExprs) {
            String name = expr.outputName != null && !expr.outputName.isBlank()
                    ? expr.outputName : expr.inputColumn;
            if (outputName.equals(name)) {
                if (found != null || expr.func != SelectExpr.AggFunc.COLUMN
                        || expr.inputColumn == null || expr.computed) {
                    return null;
                }
                found = quoted(expr.inputColumn);
            }
        }
        return found;
    }

    private static String quoted(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Returns whether the query restricts or orders its result rows.
     *
     * @return true when ORDER BY, LIMIT or OFFSET is present
     */
    public boolean hasOrderOrLimit() {
        return orderBy != null || limit != null || offset != null;
    }

    /**
     * Renders the ORDER BY, LIMIT and OFFSET clauses for DuckDB.
     *
     * @return SQL suffix starting with a space, or an empty string
     */
    public String orderAndLimitSql() {
        StringBuilder sql = new StringBuilder();
        if (orderBy != null) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        if (limit != null) {
            sql.append(" LIMIT ").append(limit);
        }
        if (offset != null) {
            sql.append(" OFFSET ").append(offset);
        }
        return sql.toString();
    }

    /**
     * @param query SQL SELECT query string
     * @return parsed query representation
     * @throws IllegalArgumentException if query is not a SELECT
     * @throws RuntimeException if parsing fails
     */
    public static ParquetQueryParser parse(String query) {
        // Item 40(b): a request parses its text several times (describe, plan, DoGet); the
        // preparation below is a pure function of the text, so a text seen before skips it too.
        ParquetQueryParser seen = RAW_PARSE_CACHE.get(query);
        if (seen != null) {
            return seen;
        }
        String raw = query;
        query = castTypedLiterals(withoutTrailingSemicolon(query));
        ParquetQueryParser parsed;
        try {
            rejectParameterMarkers(query);
            parsed = parseChecked(query);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid SQL query: " + query, e);
        }
        if (QueryCacheLimits.cacheable(raw)) {
            synchronized (RAW_PARSE_CACHE) {
                RAW_PARSE_CACHE.put(raw, parsed);
                if (RAW_PARSE_CACHE.size() > PARSE_CACHE_CAPACITY) {
                    RAW_PARSE_CACHE.remove(RAW_PARSE_CACHE.keySet().iterator().next());
                }
            }
        }
        return parsed;
    }

    /**
     * Prepares SQL text as {@link #parse} does before the SQL parser reads it.
     *
     * @param query SQL text
     * @return SQL text as the SQL parser reads it
     */
    static String normalized(String query) {
        return flattenSubqueryWrapper(castTypedLiterals(withoutTrailingSemicolon(query)));
    }

    /**
     * Drops the statement terminator many clients append.
     *
     * @param query SQL text
     * @return SQL text without trailing semicolons and whitespace
     */
    static String withoutTrailingSemicolon(String query) {
        String q = query.trim();
        while (q.endsWith(";")) {
            q = q.substring(0, q.length() - 1).trim();
        }
        return q;
    }

    /**
     * Rewrites {@code TIME '...'} and {@code TIMESTAMPTZ '...'} literals as casts of the string.
     * The SQL parser drops the fraction of a TIME literal ({@code 12:34:56.789} becomes
     * {@code 12:34:56}, so an equality filter silently matches nothing) and does not know the
     * TIMESTAMPTZ keyword; a cast keeps the text as written and DuckDB converts it exactly.
     *
     * @param sql SQL text
     * @return SQL text with those literals written as casts
     */
    static String castTypedLiterals(String sql) {
        StringBuilder out = new StringBuilder(sql.length() + 16);
        char quote = 0;
        int i = 0;
        Matcher m = null;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (quote != 0) {
                out.append(c);
                if (c == quote) {
                    quote = 0;
                }
                i++;
                continue;
            }
            // Both keywords start with t; one matcher, tried only there (item 40(b)).
            boolean wordStart = (c == 't' || c == 'T')
                    && (i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1))
                    && sql.charAt(i - 1) != '_');
            if (wordStart && m == null) {
                m = TYPED_LITERAL.matcher(sql);
            }
            if (wordStart && m.region(i, sql.length()).lookingAt()) {
                String type = m.group(1).equalsIgnoreCase("time")
                        ? "TIME" : "TIMESTAMP WITH TIME ZONE";
                out.append("CAST('").append(m.group(2)).append("' AS ").append(type).append(')');
                i = m.end();
                continue;
            }
            if (c == '\'' || c == '"') {
                quote = c;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    private static final Pattern TYPED_LITERAL = Pattern.compile(
            "(?i)(time|timestamptz)\\s+'((?:[^']|'')*)'");


    /**
     * Refuses statements with parameter markers, which would otherwise be read as NULL.
     *
     * @param query SQL text
     * @throws IllegalArgumentException if a question mark occurs outside quotes
     */
    static void rejectParameterMarkers(String query) {
        if (withoutQuotedText(query).indexOf('?') >= 0) {
            throw new IllegalArgumentException(
                    "Statement parameters (?) are not supported; put the values into the SQL text");
        }
    }

    /**
     * Blanks out string literals and quoted identifiers so keywords can be searched safely.
     *
     * @param sql SQL text
     * @return the text with every quoted span replaced by a single space
     */
    static String withoutQuotedText(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        char quote = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                        i++;
                    } else {
                        quote = 0;
                    }
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
                out.append(' ');
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /**
     * Tells whether a query holds a nested query (a subquery, a CTE, a set operation) besides the
     * plain FROM wrapper this parser flattens.
     *
     * @param query SQL text without time travel clauses
     * @return true when the text holds a nested query
     */
    public static boolean hasNestedQuery(String query) {
        return TableReferences.nestedQuery(flattenSubqueryWrapper(query));
    }

    /**
     * Names every table a query reads, for access checks: each schema-qualified name right after
     * FROM, JOIN or a comma of a FROM list, quoted names and names after comments included. A
     * nested query (a subquery anywhere but the plain FROM wrapper this parser flattens) is
     * refused: the parse names only the outer query's tables; {@link QueryTables} names those.
     *
     * @param query SQL text without time travel clauses
     * @return the named tables, in text order
     * @throws IllegalArgumentException when the query holds a subquery
     */
    public static List<JoinTable> checkedTables(String query) {
        if (hasNestedQuery(query)) {
            throw new IllegalArgumentException("Subqueries are named by QueryTables, not by this parser");
        }
        List<JoinTable> tables = new ArrayList<>();
        for (TableReferences.Reference reference : TableReferences.find(query, (schema, table) -> true)) {
            tables.add(new JoinTable(reference.schema(), reference.table(), reference.table()));
        }
        return tables;
    }

    /**
     * Tells whether the parse of a query is cached.
     *
     * @param query SQL text as passed to {@link #parse(String)}
     * @return true when cached
     */
    static boolean isCached(String query) {
        return PARSE_CACHE.containsKey(flattenSubqueryWrapper(query));
    }

    /**
     * Parses a query that has no terminator and no parameter markers.
     *
     * @param query SQL SELECT query string
     * @return parsed query representation
     */
    private static ParquetQueryParser parseChecked(String query) {
        // Flatten subquery wrappers before jOOQ parsing (regex is more robust)
        String flattened = flattenSubqueryWrapper(query);
        if (!flattened.equals(query)) {
            LOGGER.debug("Flattened subquery wrapper; parsing: {}", flattened);
            query = flattened;
        }
        ParquetQueryParser cached = PARSE_CACHE.get(query);
        if (cached != null) {
            return cached;
        }

        Settings settings = new Settings()
                .withRenderFormatted(true)
                .withParseNameCase(ParseNameCase.AS_IS);
        DSLContext ctx = DSL.using(SQLDialect.DEFAULT, settings);

        try {
            Query parsedQuery = parseStatement(ctx.settings(), query);

            if (parsedQuery instanceof Select<?> select) {
                LOGGER.debug("Parsing SQL query: {}", query);
                ParquetQueryParser parser = parseSelect(select, query);
                if (holdsKeyFunction(query) && !keyFunctionsOnlyInKeys(parser)) {
                    throw new IllegalArgumentException("A " + KEY_FUNCTIONS
                            + " call is not an aliased GROUP BY key: " + query);
                }
                if (!QueryCacheLimits.cacheable(query)) {
                    CacheMetrics.oversize(CacheMetrics.Cache.PARSE);
                } else {
                    synchronized (PARSE_CACHE) {
                        PARSE_CACHE.put(query, parser);
                        if (PARSE_CACHE.size() > PARSE_CACHE_CAPACITY) {
                            PARSE_CACHE.remove(PARSE_CACHE.keySet().iterator().next());
                            CacheMetrics.eviction(CacheMetrics.Cache.PARSE);
                        }
                    }
                }
                LOGGER.debug("Parsed query: {}", parser);
                return parser;
            } else {
                throw new IllegalArgumentException("Only SELECT queries are supported: " + query);
            }

        } catch (Exception e) {
            // Callers fall back or report the failure; multi-table queries come here routinely (item 33 (E)).
            LOGGER.debug("Error parsing SQL query", e);
            throw new RuntimeException("Invalid SQL query: " + query, e);
        }
    }

    /** Functions jOOQ's parser does not know that a computed GROUP BY key may call (item 55 (c)). */
    static final Set<String> KEY_FUNCTIONS = Set.of("strftime", "time_bucket");

    /** A call of a {@link #KEY_FUNCTIONS} function in SQL text, quoted or not. */
    private static final Pattern KEY_FUNCTION_CALL = Pattern.compile(
            "(?i)(?<![\\w$])\"?(?:strftime|time_bucket)\"?\\s*\\(");

    /** A call of a function jOOQ does not know, as rendered with every name quoted. */
    private static final Pattern UNKNOWN_FUNCTION_CALL = Pattern.compile("\"((?:[^\"]|\"\")+)\"\\s*\\(");

    /**
     * Parses one SQL statement with jOOQ. jOOQ refuses functions it does not know; a statement
     * that calls {@link #KEY_FUNCTIONS} ({@code strftime}, {@code time_bucket}) is parsed again
     * with unknown functions allowed, and accepted only when every function jOOQ did not know is
     * one of them (item 55 (c)). Callers must still make sure those calls end up only where they
     * render them themselves ({@link #keyText}).
     *
     * @param settings parse settings (name case and so on)
     * @param sql statement text
     * @return the parsed statement
     * @throws org.jooq.impl.ParserException when jOOQ cannot parse it
     */
    static Query parseStatement(Settings settings, String sql) {
        DSLContext ctx = DSL.using(SQLDialect.DEFAULT, settings);
        try {
            return ctx.parser().parseQuery(sql);
        } catch (org.jooq.impl.ParserException e) {
            if (!holdsKeyFunction(sql)) {
                throw e;
            }
            Settings lenient = ((Settings) settings.clone())
                    .withParseUnknownFunctions(org.jooq.conf.ParseUnknownFunctions.IGNORE);
            Query parsed;
            try {
                parsed = DSL.using(SQLDialect.DEFAULT, lenient).parser().parseQuery(sql);
            } catch (org.jooq.impl.ParserException again) {
                throw e;
            }
            Matcher call = UNKNOWN_FUNCTION_CALL.matcher(KEY_CTX.renderInlined(parsed));
            while (call.find()) {
                if (!KEY_FUNCTIONS.contains(call.group(1).toLowerCase(Locale.ROOT))) {
                    throw e;
                }
            }
            return parsed;
        }
    }

    /**
     * Tells whether SQL text may call one of the {@link #KEY_FUNCTIONS}.
     *
     * @param sql SQL text
     * @return true when it may
     */
    static boolean holdsKeyFunction(String sql) {
        return sql != null && KEY_FUNCTION_CALL.matcher(sql).find();
    }

    /**
     * Tells whether every {@link #KEY_FUNCTIONS} call of a parsed query is inside a computed
     * GROUP BY key, the one place rendered from the parse by {@link #keyText}; everywhere else
     * (WHERE, HAVING, ORDER BY, aggregates, other select items) jOOQ would render it, and its
     * rendering of an INTERVAL is no DuckDB SQL.
     *
     * @param parsed parsed query
     * @return true when the calls are only in computed keys
     */
    private static boolean keyFunctionsOnlyInKeys(ParquetQueryParser parsed) {
        if (holdsKeyFunction(parsed.filter) || holdsKeyFunction(parsed.having)
                || holdsKeyFunction(parsed.orderBy)) {
            return false;
        }
        Set<String> keyNames = new HashSet<>();
        for (SelectExpr expr : parsed.selectExprs) {
            if (expr.computed) {
                keyNames.add(expr.outputName.toLowerCase(Locale.ROOT));
            } else if (holdsKeyFunction(expr.inputColumn) || holdsKeyFunction(expr.inputExpression)) {
                return false;
            }
        }
        for (String group : parsed.groupByColumnNames) {
            if (!keyNames.contains(group.toLowerCase(Locale.ROOT)) && holdsKeyFunction(group)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Resolves a GROUP BY ordinal ({@code GROUP BY 1}) to the column it names in the select
     * list, so that it groups like the column named (item 33 (B)); anything else is returned as
     * it is.
     *
     * @param gf GROUP BY element
     * @param selectList the select list
     * @return the column at that position (without its alias), or gf
     */
    static GroupField groupByOrdinal(GroupField gf, List<? extends SelectFieldOrAsterisk> selectList) {
        if (!(gf instanceof org.jooq.Param<?> param) || !(param.getValue() instanceof Number n)
                || n instanceof Double || n instanceof Float
                || n instanceof java.math.BigDecimal decimal && decimal.scale() > 0) {
            return gf;
        }
        long position = n.longValue();
        if (position < 1 || position > selectList.size()) {
            return gf;
        }
        SelectFieldOrAsterisk item = selectList.get((int) position - 1);
        if (item instanceof org.jooq.impl.QOM.FieldAlias<?> alias) {
            item = alias.$field();
        }
        return item instanceof TableField<?, ?> column ? column : gf;
    }

    /**
     * Finds the select item a GROUP BY element names when that item is a computed key every
     * endpoint can compute for its own rows (item 55 (c)): {@code GROUP BY 1}, {@code GROUP BY g}
     * for {@code ... AS g}, or the same expression as a select item, where the item is a
     * {@link #computable} expression of the table's columns.
     *
     * @param listed the GROUP BY element as written
     * @param gf the element after {@link #groupByOrdinal}
     * @param selectList the select list
     * @param quotedCtx renderer for executable DuckDB SQL
     * @return the select position of the key, or -1
     */
    static int computedGroupKey(GroupField listed, GroupField gf,
            List<? extends SelectFieldOrAsterisk> selectList, DSLContext quotedCtx) {
        if (listed instanceof org.jooq.Param<?> param && param.getValue() instanceof Number n
                && gf == listed) {
            long position = n.longValue();
            if (n instanceof Double || n instanceof Float || position < 1 || position > selectList.size()
                    || n instanceof java.math.BigDecimal decimal && decimal.scale() > 0) {
                return -1;
            }
            return computedItem(selectList.get((int) position - 1)) ? (int) position - 1 : -1;
        }
        if (gf instanceof TableField<?, ?> named) {
            if (named.getQualifiedName().getName().length != 1) {
                return -1;
            }
            int found = -1;
            for (int i = 0; i < selectList.size(); i++) {
                if (selectList.get(i) instanceof org.jooq.impl.QOM.FieldAlias<?> alias
                        && alias.getName().equalsIgnoreCase(named.getName())) {
                    if (found >= 0 || !computedItem(alias)) {
                        return -1;
                    }
                    found = i;
                }
            }
            return found;
        }
        String text = gf instanceof org.jooq.Field<?> expression ? keyText(expression, quotedCtx) : null;
        if (text == null) {
            return -1;
        }
        for (int i = 0; i < selectList.size(); i++) {
            SelectFieldOrAsterisk item = selectList.get(i);
            org.jooq.Field<?> field = item instanceof org.jooq.impl.QOM.FieldAlias<?> alias ? alias.$field()
                    : item instanceof org.jooq.Field<?> plain ? plain : null;
            if (field != null && computedItem(item) && text.equals(keyText(field, quotedCtx))) {
                return i;
            }
        }
        return -1;
    }

    /** Renders {@code date_trunc} with its date part (jOOQ's default dialect drops the part). */
    private static final DSLContext DATE_TRUNC_CTX = DSL.using(SQLDialect.POSTGRES,
            new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));

    /** jOOQ's default-dialect rendering of {@code date_trunc(part, x)}: {@code trunc(x, null)}. */
    private static final Pattern DEFAULT_TRUNC_DATE = Pattern.compile("(?s)^trunc\\((.*), null\\)$");

    /** The PostgreSQL rendering of {@code date_trunc}, whose part is kept. */
    private static final Pattern POSTGRES_TRUNC_DATE = Pattern.compile("^date_trunc\\('([a-z]+)', ");

    /** A column, or a CAST of a column, as {@code date_trunc}'s argument of a computed key. */
    private static final Pattern TRUNC_DATE_ARGUMENT = Pattern.compile(
            "^(?:\"[^\"]+\"|cast\\(\"[^\"]+\" as [a-z ]+\\))$");

    /** Date parts of a {@code date_trunc} key; DuckDB reads each as PostgreSQL does. */
    private static final Set<String> TRUNC_DATE_PARTS = Set.of("year", "quarter", "month", "week", "day",
            "hour", "minute", "second");

    /** jOOQ's parse node of {@code date_trunc}, which has no public model type. */
    private static final String TRUNC_DATE_CLASS = "org.jooq.impl.TruncDate";

    /**
     * Renders a computed GROUP BY key as executable DuckDB SQL (item 55 (c)). A
     * {@link #computable} expression renders as jOOQ renders it; {@code date_trunc('<part>', x)},
     * which jOOQ's default dialect renders as {@code trunc(x, null)} (silently dropping the part),
     * is accepted only as the whole key, with a column or a CAST of a column as {@code x} and one
     * of year, quarter, month, week, day, hour, minute or second as the part, and rendered as
     * {@code date_trunc('<part>', x)} from jOOQ's PostgreSQL rendering of the part.
     *
     * @param field key expression without its alias
     * @param quotedCtx renderer for executable DuckDB SQL
     * @return the key's SQL, or null when no endpoint may compute it
     */
    static String keyText(org.jooq.Field<?> field, DSLContext quotedCtx) {
        if (field.getClass().getName().equals(TRUNC_DATE_CLASS)) {
            Matcher argument = DEFAULT_TRUNC_DATE.matcher(quotedCtx.renderInlined(field).trim());
            Matcher part = POSTGRES_TRUNC_DATE.matcher(DATE_TRUNC_CTX.renderInlined(field).trim());
            if (!argument.matches() || !part.lookingAt() || !TRUNC_DATE_PARTS.contains(part.group(1))
                    || !TRUNC_DATE_ARGUMENT.matcher(argument.group(1)).matches()) {
                return null;
            }
            return "date_trunc('" + part.group(1) + "', " + argument.group(1) + ")";
        }
        if (keyFunction(field)) {
            return keyFunctionText((org.jooq.impl.QOM.Function<?>) field, quotedCtx);
        }
        return computable(field) ? quotedCtx.renderInlined(field).trim() : null;
    }

    /** jOOQ's parse node of a function it does not know. */
    private static final String UNKNOWN_FUNCTION_CLASS = "org.jooq.impl.Function";

    /**
     * Tells whether an expression is a call of one of the {@link #KEY_FUNCTIONS}, which jOOQ
     * parses only as an unknown function ({@link #parseStatement}).
     *
     * @param field expression
     * @return true for such a call
     */
    static boolean keyFunction(org.jooq.Field<?> field) {
        if (!(field instanceof org.jooq.impl.QOM.Function<?> function)
                || !field.getClass().getName().equals(UNKNOWN_FUNCTION_CLASS)
                || function.$name().getName().length != 1) {
            return false;
        }
        String name = functionName(function);
        return name != null && KEY_FUNCTIONS.contains(name);
    }

    /**
     * Returns the lower-case last part of a function's name.
     *
     * @param function the call
     * @return the name, or null when jOOQ holds none
     */
    private static String functionName(org.jooq.impl.QOM.Function<?> function) {
        String last = function.$name().last();
        return last == null ? null : last.toLowerCase(Locale.ROOT);
    }

    /**
     * Renders a {@code strftime} or {@code time_bucket} key from its parse (item 55 (c)). Accepted:
     * {@code strftime(x, '<format>')} and {@code time_bucket(<width>, x)}, where {@code x} is a
     * column or a CAST of a column and the width is a string literal (passed to DuckDB as written)
     * or an INTERVAL of months only or of whole seconds (days and time) only (rendered as
     * {@code INTERVAL '<n> months'} or {@code INTERVAL '<n> seconds'}, which DuckDB buckets alike; jOOQ's
     * own rendering of an INTERVAL is no DuckDB SQL). Anything else, such as {@code time_bucket}'s
     * origin or offset argument, is refused.
     *
     * @param function the call
     * @param quotedCtx renderer for executable DuckDB SQL
     * @return the key's SQL, or null when no endpoint may compute it
     */
    private static String keyFunctionText(org.jooq.impl.QOM.Function<?> function, DSLContext quotedCtx) {
        List<? extends org.jooq.Field<?>> args = function.$args();
        String name = functionName(function);
        if (name == null || args.size() != 2) {
            return null;
        }
        boolean strftime = name.equals("strftime");
        String argument = quotedCtx.renderInlined(args.get(strftime ? 0 : 1)).trim();
        if (!TRUNC_DATE_ARGUMENT.matcher(argument).matches()) {
            return null;
        }
        org.jooq.Field<?> other = args.get(strftime ? 1 : 0);
        Object value = other instanceof org.jooq.Param<?> param ? param.getValue() : null;
        if (strftime) {
            return value instanceof String format && !format.isEmpty()
                    ? "strftime(" + argument + ", " + quotedCtx.renderInlined(other).trim() + ")" : null;
        }
        String width = bucketWidth(value, quotedCtx, other);
        return width == null ? null : "time_bucket(" + width + ", " + argument + ")";
    }

    /** Microseconds per day. */
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    /**
     * Renders a {@code time_bucket} width as DuckDB SQL.
     *
     * @param value parsed literal value
     * @param quotedCtx renderer for executable DuckDB SQL
     * @param literal the literal itself
     * @return the width, or null when it is refused
     */
    static String bucketWidth(Object value, DSLContext quotedCtx, org.jooq.Field<?> literal) {
        if (value instanceof String text) {
            return text.isBlank() ? null : quotedCtx.renderInlined(literal).trim();
        }
        org.jooq.types.YearToMonth months = null;
        org.jooq.types.DayToSecond time = null;
        if (value instanceof org.jooq.types.YearToSecond both) {
            months = both.getYearToMonth();
            time = both.getDayToSecond();
        } else if (value instanceof org.jooq.types.YearToMonth only) {
            months = only;
        } else if (value instanceof org.jooq.types.DayToSecond only) {
            time = only;
        } else {
            return null;
        }
        long monthCount = months == null ? 0 : (long) months.getYears() * 12 + months.getMonths();
        long micros = time == null ? 0 : time.getDays() * MICROS_PER_DAY + time.getHours() * 3_600_000_000L
                + time.getMinutes() * 60_000_000L + time.getSeconds() * 1_000_000L + time.getNano() / 1000;
        boolean negative = months != null && months.getSign() < 0 || time != null && time.getSign() < 0;
        // Whole seconds only: jOOQ parses '<n> seconds' back (a part query is parsed again), not microseconds.
        if (negative || time != null && time.getNano() != 0 || (monthCount > 0) == (micros > 0)) {
            return null;
        }
        return monthCount > 0 ? "INTERVAL '" + monthCount + " months'"
                : "INTERVAL '" + micros / 1_000_000L + " seconds'";
    }

    /**
     * Tells whether rendered SQL holds a {@code date_trunc} as jOOQ's default dialect renders it
     * ({@code trunc(x, null)}), which means something else to DuckDB.
     *
     * @param rendered rendered SQL
     * @return true when it may hold one
     */
    static boolean holdsDefaultTruncDate(String rendered) {
        return rendered != null && TRUNC_DATE_IN_TEXT.matcher(rendered).find();
    }

    /** A {@code date_trunc} anywhere in default-dialect SQL. */
    private static final Pattern TRUNC_DATE_IN_TEXT = Pattern.compile("(?is)\\btrunc\\(.*, null\\)");

    /**
     * Tells whether a select item is a computed expression (not a plain column) that
     * {@link #computable} accepts.
     *
     * @param item select item
     * @return true for such an item
     */
    private static boolean computedItem(SelectFieldOrAsterisk item) {
        org.jooq.Field<?> field = item instanceof org.jooq.impl.QOM.FieldAlias<?> alias ? alias.$field()
                : item instanceof org.jooq.Field<?> plain ? plain : null;
        if (field == null || field instanceof TableField<?, ?>) {
            return false;
        }
        // An unaliased date_trunc would be named after jOOQ's wrong rendering, not as DuckDB names it.
        // So would a strftime / time_bucket key, named after jOOQ's rendering of an unknown function.
        boolean truncDate = field.getClass().getName().equals(TRUNC_DATE_CLASS) || keyFunction(field);
        return (!truncDate || item instanceof org.jooq.impl.QOM.FieldAlias<?>)
                && keyText(field, KEY_CTX) != null;
    }

    /** Renderer for {@link #keyText} checks outside a parse. */
    private static final DSLContext KEY_CTX = DSL.using(SQLDialect.DEFAULT,
            new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));

    /** Date parts whose {@code extract} jOOQ renders as DuckDB reads them. */
    private static final Set<DatePart> KEY_DATE_PARTS = Set.of(DatePart.YEAR, DatePart.MONTH,
            DatePart.DAY, DatePart.HOUR);

    /**
     * Tells whether an expression can be a computed GROUP BY key that every endpoint computes as
     * the client's DuckDB would: only columns, literals, arithmetic ({@code + - * / %}),
     * {@code CAST}, {@code extract} of year / month / day / hour, {@code lower}, {@code upper},
     * {@code floor}, {@code ceil}, {@code abs}, {@code substring} and {@code coalesce}. These are
     * deterministic, and jOOQ renders them back unchanged in meaning; other functions it may
     * translate (it renders {@code date_trunc('day', ts)} as {@code trunc("ts", null)}) or they
     * are not deterministic ({@code random()}), so they keep the single-node path.
     *
     * @param field expression
     * @return true when the expression is accepted
     */
    static boolean computable(org.jooq.Field<?> field) {
        if (field instanceof TableField<?, ?>) {
            return true;
        }
        if (field instanceof org.jooq.Param<?> param) {
            return param.getValue() != null;
        }
        List<?> args = operatorArguments(field);
        if (args != null) {
            return computableArguments(args);
        }
        if (field instanceof org.jooq.impl.QOM.Cast<?> cast) {
            return computable(cast.$field());
        }
        if (field instanceof org.jooq.impl.QOM.Extract extract) {
            return KEY_DATE_PARTS.contains(extract.$datePart()) && computable(extract.$field());
        }
        return false;
    }

    /**
     * Returns the arguments of an operator {@link #computable} accepts.
     *
     * @param field expression
     * @return its arguments, or null when it is no accepted operator
     */
    private static List<?> operatorArguments(org.jooq.Field<?> field) {
        return switch (field) {
            case org.jooq.impl.QOM.Add<?> op -> op.$args();
            case org.jooq.impl.QOM.Sub<?> op -> op.$args();
            case org.jooq.impl.QOM.Mul<?> op -> op.$args();
            case org.jooq.impl.QOM.Div<?> op -> op.$args();
            case org.jooq.impl.QOM.Mod<?> op -> op.$args();
            case org.jooq.impl.QOM.Neg<?> op -> op.$args();
            case org.jooq.impl.QOM.Lower op -> op.$args();
            case org.jooq.impl.QOM.Upper op -> op.$args();
            case org.jooq.impl.QOM.Floor<?> op -> op.$args();
            case org.jooq.impl.QOM.Ceil<?> op -> op.$args();
            case org.jooq.impl.QOM.Abs<?> op -> op.$args();
            case org.jooq.impl.QOM.Substring op -> op.$args();
            case org.jooq.impl.QOM.Coalesce<?> op -> op.$args();
            default -> null;
        };
    }

    /**
     * Tells whether every argument of an accepted operator is {@link #computable}.
     *
     * @param args operator arguments: fields, lists of fields, or null for omitted ones
     * @return true when all are accepted
     */
    private static boolean computableArguments(List<?> args) {
        for (Object arg : args) {
            if (arg == null) {
                continue;
            }
            if (arg instanceof org.jooq.Field<?> f) {
                if (!computable(f)) {
                    return false;
                }
            } else if (arg instanceof List<?> list) {
                if (!computableArguments(list)) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Names a select item's output column as the parsed select items name it.
     *
     * @param selected select item
     * @param noQuoteCtx renderer with quoted names disabled
     * @return its alias, or its rendered text
     */
    private static String outputName(org.jooq.Field<?> selected, DSLContext noQuoteCtx) {
        return selected instanceof org.jooq.impl.QOM.FieldAlias<?> ? selected.getName()
                : noQuoteCtx.renderInlined(selected).trim();
    }

    private static final Pattern FROM_PAREN = Pattern.compile("(?i)\\bfrom\\s*\\(");

    /**
     * Flattens "SELECT ... FROM (SELECT ...) alias" → "SELECT ... FROM innerTable".
     * Regex-based, operates on the raw SQL string before jOOQ parsing.
     *
     * @param query raw SQL query string
     * @return flattened query, or original if no subquery wrapper detected
     */
    @SuppressWarnings("java:S3776")
    private static String flattenSubqueryWrapper(String query) {
        String q = query.trim();

        Matcher fromParen = FROM_PAREN.matcher(q);
        if (!fromParen.find()) {
            return q;
        }

        int openParen = fromParen.end() - 1;
        String beforeFrom = q.substring(0, fromParen.start());

        int depth = 1;
        int i = openParen + 1;
        while (i < q.length() && depth > 0) {
            char c = q.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            i++;
        }
        if (depth != 0) {
            return q;
        }

        String innerSQL = q.substring(openParen + 1, i - 1).trim();
        String afterParen = q.substring(i).trim();

        Matcher aliasM = Pattern.compile("^(?:(?i:as)\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)(.*)$",
                Pattern.DOTALL).matcher(afterParen);
        if (!aliasM.matches()) {
            return q;
        }
        // The alias disappears with the wrapper, so "alias.column" becomes "column".
        String aliasPrefix = "(?i)\\b" + Pattern.quote(aliasM.group(1)) + "\\.";
        beforeFrom = beforeFrom.replaceAll(aliasPrefix, "");
        String outerRest = aliasM.group(2).trim().replaceAll(aliasPrefix, "");

        if (!innerSQL.toLowerCase(Locale.ROOT).startsWith("select")) {
            return q;
        }

        // The wrapper can be replaced by its table only when the inner query is a plain column
        // list (or *) over one table with an optional WHERE. Aggregates, GROUP BY, ORDER BY or
        // LIMIT inside would be lost, and the query would return a wrong result.
        String innerShape = withoutQuotedText(innerSQL).toLowerCase(Locale.ROOT);
        Matcher innerList = Pattern.compile("(?is)^select\\s+(.+?)\\s+from\\s+\\S+(\\s+where\\b.*)?$")
                .matcher(innerSQL);
        boolean plainInner = innerList.matches()
                && innerList.group(1).trim().matches("\\*|[\\w\"]+(\\s*,\\s*[\\w\"]+)*")
                && !Pattern.compile("\\b(group\\s+by|order\\s+by|having|limit|offset|union|intersect"
                        + "|except|join|distinct)\\b|\\bselect\\b.*\\bselect\\b", Pattern.DOTALL)
                        .matcher(innerShape).find();
        if (!plainInner) {
            throw new IllegalArgumentException("A subquery in FROM is supported only in the form "
                    + "(SELECT columns FROM table [WHERE ...]) alias");
        }
        String innerColumns = innerList.group(1).trim();
        if (!innerColumns.equals("*") && beforeFrom.trim().matches("(?i)select\\s+\\*")) {
            // SELECT * over the wrapper means the inner column list.
            beforeFrom = beforeFrom.replaceFirst("\\*\\s*$", Matcher.quoteReplacement(innerColumns) + " ");
        }

        Matcher innerFrom = Pattern.compile("(?i)\\bfrom\\s+(\\S+)").matcher(innerSQL);
        if (!innerFrom.find()) {
            return q;
        }
        String realTableRef = innerFrom.group(1).replaceAll("\\s*$", "");

        String innerWhere = "";
        Matcher innerWhereM = Pattern.compile("(?i)\\bwhere\\b(.+)$",
                Pattern.DOTALL).matcher(innerSQL);
        if (innerWhereM.find()) {
            innerWhere = innerWhereM.group(1).trim();
        }

        StringBuilder flat = new StringBuilder(beforeFrom);
        flat.append("from ").append(realTableRef);

        // The outer query may go on after its WHERE: GROUP BY ... ORDER BY ... LIMIT (this is
        // how BI tools such as Superset wrap a dataset). That tail is kept as it is.
        int tailStart = topLevelMatch(outerRest, OUTER_TAIL);
        String tail = tailStart < 0 ? "" : outerRest.substring(tailStart).trim();
        String outerHead = tailStart < 0 ? outerRest : outerRest.substring(0, tailStart).trim();
        String outerWhere = "";
        if (outerHead.toLowerCase(Locale.ROOT).startsWith("where ")) {
            outerWhere = outerHead.substring(5).trim();
        } else if (!outerHead.isEmpty()) {
            return q;
        }

        boolean hasInner = !innerWhere.isEmpty();
        boolean hasOuter = !outerWhere.isEmpty();
        if (hasInner && hasOuter) {
            flat.append(" where (").append(innerWhere).append(") and (").append(outerWhere).append(")");
        } else if (hasInner) {
            flat.append(" where ").append(innerWhere);
        } else if (hasOuter) {
            flat.append(" where ").append(outerWhere);
        }
        if (!tail.isEmpty()) {
            flat.append(' ').append(tail);
        }

        return flat.toString();
    }

    private static final Pattern OUTER_TAIL = Pattern.compile(
            "(?i)\\b(group\\s+by|having|order\\s+by|limit|offset)\\b");

    /**
     * Finds the first match of a pattern outside quotes and parentheses.
     *
     * @param sql SQL text
     * @param pattern keyword pattern
     * @return start index of the match, or -1
     */
    static int topLevelMatch(String sql, Pattern pattern) {
        boolean[] nested = new boolean[sql.length()];
        char quote = 0;
        int depth = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (quote != 0) {
                nested[i] = true;
                if (c == quote) {
                    quote = 0;
                }
                continue;
            }
            if (c == '\'' || c == '"') {
                quote = c;
                nested[i] = true;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
            }
            nested[i] = nested[i] || depth > 0;
        }
        Matcher m = pattern.matcher(sql);
        while (m.find()) {
            if (!nested[m.start()]) {
                return m.start();
            }
        }
        return -1;
    }

    /**
     * @param select jOOQ Select query object
     * @param originalSql original SQL string for fallback parsing
     * @return parsed query representation
     */
    private static ParquetQueryParser parseSelect(Select<?> select, String originalSql) {
        List<? extends Table<?>> fromTables = select.$from();

        DSLContext noQuoteCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.NEVER));

        if (fromTables.size() > 1) {
            // The client knows its SQL and the refusal log line carries it once (item 37 (c)).
            throw new IllegalArgumentException(
                    "Can only select from ONE table, but got " + fromTables.size() + " tables");
        }
        rejectUnsupportedClauses(select, originalSql);

        // Detect JOIN via jOOQ rendered output
        boolean isJoin = hasJoinKeywords(select);

        if (isJoin) {
            return parseJoinSelect(select, noQuoteCtx, originalSql);
        }
        return parseSingleTableSelect(select, fromTables, noQuoteCtx);
    }

    /**
     * Refuses clauses that the single-table execution path does not carry to DuckDB. Without
     * this check they were dropped and the query returned a wrong result.
     *
     * @param select parsed query
     * @param originalSql SQL text as received
     * @throws IllegalArgumentException if the query uses such a clause
     */
    private static void rejectUnsupportedClauses(Select<?> select, String originalSql) {
        String shape = withoutQuotedText(originalSql).toLowerCase(Locale.ROOT);
        if (Pattern.compile("\\b(union|intersect|except)\\b").matcher(shape).find()) {
            throw new IllegalArgumentException("UNION, INTERSECT and EXCEPT are not supported");
        }
        if (!select.$distinctOn().isEmpty()) {
            throw new IllegalArgumentException("DISTINCT ON is not supported");
        }
        if (select.$qualify() != null || !select.$window().isEmpty()) {
            throw new IllegalArgumentException("Window clauses are not supported");
        }
        if (select.$with() != null) {
            throw new IllegalArgumentException("WITH (common table expressions) is not supported");
        }
    }

    /**
     * Detects JOIN by checking whether the (single) FROM table renders with "join" keyword.
     * jOOQ rendering is deterministic for all JOIN types.
     *
     * @param select jOOQ Select query object
     * @return true if query contains JOIN
     */
    private static boolean hasJoinKeywords(Select<?> select) {
        List<? extends Table<?>> from = select.$from();
        if (from.size() > 1) {
            return true;
        }
        if (from.size() == 1) {
            String rendered = DSL.using(SQLDialect.DEFAULT)
                    .render(from.get(0)).toLowerCase(Locale.ROOT);
            return rendered.contains(" join ");
        }
        return false;
    }

    /**
     * Extracts table references from a JOIN SQL string using regex.
     * Only matches table references immediately after FROM/JOIN keywords,
     * avoiding false matches in ON clause column references.
     */
    private static final Pattern JOIN_KEYWORD = Pattern.compile(
            "\\b(?:FROM|(?:(?:LEFT|RIGHT|FULL(?:\\s+OUTER)?|INNER|CROSS)\\s+)?JOIN)\\s+",
            Pattern.CASE_INSENSITIVE);

    // The optional alias must not be the next keyword: in "FROM s.a JOIN s.b ON ..." the word
    // after "s.a" is JOIN, not an alias.
    // A name part is a word or a double-quoted identifier ("" inside it is one quote).
    private static final String NAME_PART = "(?:\\w+|\"(?:[^\"]|\"\")+\")";

    private static final Pattern TABLE_REF = Pattern.compile(
            "^(" + NAME_PART + ")(?:\\.(" + NAME_PART + "))?"
                    + "(?:\\s+(?:AS\\s+)?(?!(?:JOIN|ON|USING|WHERE|LEFT|RIGHT|FULL|INNER"
                    + "|CROSS|OUTER|NATURAL|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|WINDOW)\\b)(\\w+))?",
            Pattern.CASE_INSENSITIVE);

    /** A plain identifier DuckDB reads without quotes. */
    private static final Pattern PLAIN_NAME = Pattern.compile("[A-Za-z_]\\w*");

    /**
     * @param sql SQL query string
     * @return list of extracted join tables
     */
    private static List<JoinTable> extractJoinTables(String sql) {
        List<JoinTable> tables = new ArrayList<>();
        Matcher kw = JOIN_KEYWORD.matcher(sql);
        while (kw.find()) {
            String rest = sql.substring(kw.end()).trim();
            Matcher tr = TABLE_REF.matcher(rest);
            if (tr.find()) {
                String schema = tr.group(2) != null ? unquoteName(tr.group(1)) : null;
                String table = unquoteName(tr.group(2) != null ? tr.group(2) : tr.group(1));
                String alias = tr.group(3);
                if (alias == null) {
                    alias = table;
                }
                tables.add(new JoinTable(schema, table, alias));
            }
        }
        return tables;
    }

    /**
     * Returns a name part as DuckDB reads it: a double-quoted identifier without its quotes.
     *
     * @param part a word or a double-quoted identifier
     * @return the name
     */
    private static String unquoteName(String part) {
        return part.startsWith("\"") ? part.substring(1, part.length() - 1).replace("\"\"", "\"") : part;
    }

    /**
     * Returns a pattern matching a name part as written: bare or double-quoted, any case.
     *
     * @param name the name
     * @return regular expression
     */
    private static String namePattern(String name) {
        return "(?:" + Pattern.quote(name) + "|" + Pattern.quote(DuckDbAdapter.quoteIdentifier(name)) + ")";
    }

    /**
     * @param select jOOQ Select query object
     * @param noQuoteCtx DSL context with quoted names disabled
     * @param originalSql original SQL string for regex-based join extraction
     * @return parsed query representation with join info
     */
    private static ParquetQueryParser parseJoinSelect(Select<?> select,
            DSLContext noQuoteCtx, String originalSql) {

        List<JoinTable> joinTables = extractJoinTables(originalSql);
        if (joinTables.isEmpty()) {
            throw new IllegalArgumentException("Could not extract table references from JOIN: " + originalSql);
        }

        List<String> columns = new ArrayList<>();
        List<SelectExpr> exprs = new ArrayList<>();
        for (SelectFieldOrAsterisk sfoa : select.$select()) {
            if (!(sfoa instanceof org.jooq.Field<?>)) {
                continue;
            }
            org.jooq.Field<?> f = (org.jooq.Field<?>) sfoa;
            columns.add(f.getName());
            String outputName = noQuoteCtx.renderInlined(f).trim();
            exprs.add(new SelectExpr(SelectExpr.AggFunc.COLUMN, outputName, outputName));
        }

        Condition where = select.$where();
        DSLContext quotedCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));
        String filter = quotedCtx.renderInlined(where);

        String duckDbSql = rewriteTableRefs(originalSql, joinTables);

        JoinTable primary = joinTables.get(0);
        return new ParquetQueryParser(primary.schema, primary.table, columns, filter,
                false, Collections.emptyList(), exprs, true, joinTables, duckDbSql,
                renderOrderBy(select, quotedCtx, null), rowCount(select.$limit()),
                rowCount(select.$offset()), false, null, null, where, null, Collections.emptyList());
    }

    /**
     * @param select jOOQ Select query object
     * @param fromTables list of FROM tables from the query
     * @param noQuoteCtx DSL context with quoted names disabled
     * @return parsed query representation for a single-table query
     */
    @SuppressWarnings("java:S3776")
    private static ParquetQueryParser parseSingleTableSelect(Select<?> select,
            List<? extends Table<?>> fromTables, DSLContext noQuoteCtx) {

        if (fromTables.size() != 1) {
            throw new IllegalArgumentException(
                    "Can only select from ONE table, but got " + fromTables.size() + " tables");
        }

        String schema = Optional.ofNullable(fromTables.get(0).getSchema()).map(Schema::getName).orElse(null);
        String table = fromTables.get(0).getName();

        DSLContext quotedCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));
        List<String> gbCols = new ArrayList<>();
        String completeAnswerReason = null;
        String mergeReason = null;
        // Select positions of computed GROUP BY keys (item 55 (c)), and those named by alias.
        Set<Integer> computedKeys = new HashSet<>();
        List<String> aliasGroupKeys = new ArrayList<>();
        for (GroupField listed : select.$groupBy()) {
            GroupField gf = groupByOrdinal(listed, select.$select());
            int key = computedGroupKey(listed, gf, select.$select(), quotedCtx);
            String rendered = noQuoteCtx.renderInlined(gf).trim();
            if (key >= 0) {
                computedKeys.add(key);
                String keyName = outputName((org.jooq.Field<?>) select.$select().get(key), noQuoteCtx);
                gbCols.add(keyName);
                if (gf instanceof TableField<?, ?>) {
                    aliasGroupKeys.add(keyName);
                }
            } else {
                gbCols.add(noQuoteCtx.render(gf).trim());
            }
            if (!(gf instanceof TableField<?, ?>)) {
                String reason = "The GROUP BY expression '" + rendered + "' is not a column";
                if (completeAnswerReason == null) {
                    completeAnswerReason = reason;
                }
                if (mergeReason == null && key < 0) {
                    mergeReason = reason;
                }
            }
        }

        List<String> columns = new ArrayList<>();
        List<SelectExpr> exprs = new ArrayList<>();
        boolean hasAgg = !gbCols.isEmpty();
        int position = -1;

        for (SelectFieldOrAsterisk sfoa : select.$select()) {
            position++;
            if (!(sfoa instanceof org.jooq.Field<?>)) {
                continue;
            }
            org.jooq.Field<?> selected = (org.jooq.Field<?>) sfoa;
            org.jooq.Field<?> f = selected;
            String outputName = noQuoteCtx.renderInlined(selected).trim();
            if (selected instanceof org.jooq.impl.QOM.FieldAlias<?> alias) {
                f = alias.$field();
                outputName = selected.getName();
            }
            columns.add(selected.getName());
            String itemReason = completeAnswerReason(f, noQuoteCtx);
            if (completeAnswerReason == null) {
                completeAnswerReason = itemReason;
            }
            boolean computedKey = computedKeys.contains(position);
            if (mergeReason == null && !computedKey) {
                mergeReason = itemReason;
            }

            if (computedKey) {
                exprs.add(SelectExpr.computedKey(keyText(f, quotedCtx), outputName));
            } else if (f instanceof org.jooq.impl.QOM.Count count) {
                hasAgg = true;
                org.jooq.Field<?> arg = count.$field();
                if (arg == null || arg instanceof org.jooq.Param<?>) {
                    exprs.add(new SelectExpr(SelectExpr.AggFunc.COUNT_STAR, null, outputName));
                } else {
                    exprs.add(aggregateExpression(SelectExpr.AggFunc.COUNT,
                            arg, outputName, noQuoteCtx, quotedCtx, count.$distinct()));
                }
            } else if (f instanceof org.jooq.impl.QOM.Sum sum) {
                hasAgg = true;
                exprs.add(aggregateExpression(SelectExpr.AggFunc.SUM,
                        sum.$field(), outputName, noQuoteCtx, quotedCtx, sum.$distinct()));
            } else if (f instanceof org.jooq.impl.QOM.Min<?> min) {
                hasAgg = true;
                exprs.add(aggregateExpression(SelectExpr.AggFunc.MIN,
                        min.$field(), outputName, noQuoteCtx, quotedCtx, false));
            } else if (f instanceof org.jooq.impl.QOM.Max<?> max) {
                hasAgg = true;
                exprs.add(aggregateExpression(SelectExpr.AggFunc.MAX,
                        max.$field(), outputName, noQuoteCtx, quotedCtx, false));
            } else {
                String inputColumn = noQuoteCtx.renderInlined(f).trim();
                exprs.add(new SelectExpr(SelectExpr.AggFunc.COLUMN, inputColumn, outputName));
            }
        }

        Condition where = select.$where();
        String filter = quotedCtx.renderInlined(where);

        Condition having = select.$having();
        String orderBy = renderOrderBy(select, quotedCtx, exprs);
        if (mergeReason == null && !computedKeys.isEmpty() && holdsDefaultTruncDate(orderBy)) {
            // Merged rows would be sorted by jOOQ's rendering of date_trunc, which drops its part.
            mergeReason = "The ORDER BY '" + orderBy + "' holds a date_trunc; order by its alias or position";
        }
        return new ParquetQueryParser(schema, table, columns, filter, hasAgg, gbCols, exprs,
                false, Collections.emptyList(), null,
                orderBy, rowCount(select.$limit()),
                rowCount(select.$offset()), select.$distinct(),
                having == null ? null : quotedCtx.renderInlined(having), completeAnswerReason, where,
                mergeReason, aliasGroupKeys);
    }

    /**
     * Tells why one select item keeps endpoints from answering for their own files. Each endpoint
     * renders plain columns and plain count, sum, min and max only; anything else used to be
     * quoted as a column name (DuckDB: "Referenced column ... not found") or, for FILTER and OVER,
     * silently dropped.
     *
     * @param field select item without its alias
     * @param noQuoteCtx renderer for messages
     * @return reason, or null when every endpoint can compute the item
     */
    private static String completeAnswerReason(org.jooq.Field<?> field, DSLContext noQuoteCtx) {
        if (field instanceof TableField<?, ?>) {
            return null;
        }
        boolean plainAggregate = field instanceof org.jooq.impl.QOM.Count
                || field instanceof org.jooq.impl.QOM.Sum
                || field instanceof org.jooq.impl.QOM.Min<?>
                || field instanceof org.jooq.impl.QOM.Max<?>;
        String text = noQuoteCtx.renderInlined(field).trim();
        if (!plainAggregate) {
            return "The select item '" + text + "' is not a column or a plain count, sum, min or max";
        }
        org.jooq.impl.QOM.AggregateFunction<?, ?> aggregate = (org.jooq.impl.QOM.AggregateFunction<?, ?>) field;
        if (aggregate.$filterWhere() != null || aggregate.$windowSpecification() != null
                || aggregate.$windowDefinition() != null || aggregate.$windowName() != null) {
            return "The aggregate '" + text + "' has a FILTER or OVER clause";
        }
        return null;
    }

    /**
     * Renders the ORDER BY list of a query as executable SQL.
     *
     * @param select parsed query
     * @param quotedCtx renderer for executable DuckDB SQL
     * @param exprs parsed select items of a one-table query, or null to keep every item as written
     * @return comma-separated sort list, or null when the query has no ORDER BY
     */
    private static String renderOrderBy(Select<?> select, DSLContext quotedCtx, List<SelectExpr> exprs) {
        List<String> parts = new ArrayList<>();
        for (SortField<?> sortField : select.$orderBy()) {
            String rendered = quotedCtx.renderInlined(sortField).trim();
            parts.add(aggregateOrdinal(select, sortField, rendered, quotedCtx, exprs));
        }
        return parts.isEmpty() ? null : String.join(", ", parts);
    }

    /**
     * Rewrites an ORDER BY item that is a plain count, sum, min or max also selected by the query
     * as that select item's position (item 55 (c)): {@code ORDER BY count(*) DESC} orders exactly
     * as {@code ORDER BY 2 DESC} when {@code count(*)} is the second select item, and merged
     * answers (distributed complete mode, batches of copies) can order by a position but not by
     * an aggregate of the merged rows. Any other item, and an aggregate whose input columns share
     * a name with a select alias (DuckDB might bind it differently), is kept as it is.
     *
     * @param select parsed query
     * @param sortField ORDER BY item
     * @param rendered the item rendered as executable SQL
     * @param quotedCtx renderer for executable DuckDB SQL
     * @param exprs parsed select items, in select-list order, or null
     * @return the item, or its select-list position with its direction and null order
     */
    private static String aggregateOrdinal(Select<?> select, SortField<?> sortField, String rendered,
            DSLContext quotedCtx, List<SelectExpr> exprs) {
        org.jooq.Field<?> sorted = sortField.$field();
        boolean aggregate = sorted instanceof org.jooq.impl.QOM.Count || sorted instanceof org.jooq.impl.QOM.Sum
                || sorted instanceof org.jooq.impl.QOM.Min<?> || sorted instanceof org.jooq.impl.QOM.Max<?>;
        String text = quotedCtx.renderInlined(sorted).trim();
        if (exprs == null || !aggregate || !rendered.startsWith(text)) {
            return rendered;
        }
        List<? extends SelectFieldOrAsterisk> items = select.$select();
        if (items.size() != exprs.size()) {
            return rendered;
        }
        Set<String> aliases = new HashSet<>();
        for (SelectFieldOrAsterisk item : items) {
            if (item instanceof org.jooq.impl.QOM.FieldAlias<?> alias) {
                aliases.add(alias.getName().toLowerCase(Locale.ROOT));
            }
        }
        for (int i = 0; i < items.size(); i++) {
            org.jooq.Field<?> field = (org.jooq.Field<?>) items.get(i);
            if (field instanceof org.jooq.impl.QOM.FieldAlias<?> alias) {
                field = alias.$field();
            }
            if (!text.equals(quotedCtx.renderInlined(field).trim())) {
                continue;
            }
            boolean shadowed = exprs.get(i).inputColumns.stream()
                    .anyMatch(c -> aliases.contains(c.replace("\"", "").toLowerCase(Locale.ROOT)));
            return shadowed ? rendered : (i + 1) + rendered.substring(text.length());
        }
        return rendered;
    }

    /**
     * Reads a literal LIMIT or OFFSET value.
     *
     * @param field parsed LIMIT or OFFSET expression, or null
     * @return non-negative row count, or null when absent
     * @throws IllegalArgumentException if the value is not a non-negative integer literal
     */
    private static Long rowCount(org.jooq.Field<? extends Number> field) {
        if (field == null) {
            return null;
        }
        String rendered = DSL.using(SQLDialect.DEFAULT).renderInlined(field).trim();
        try {
            long value = Long.parseLong(rendered);
            if (value < 0) {
                throw new NumberFormatException(rendered);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "LIMIT and OFFSET must be non-negative integer literals: " + rendered, e);
        }
    }

    /**
     * Creates an aggregate expression while retaining safe SQL and referenced columns.
     *
     * @param func aggregate function
     * @param input aggregate input field
     * @param outputName aggregate output name
     * @param noQuoteCtx renderer for compatibility names
     * @param quotedCtx renderer for executable DuckDB SQL
     * @param distinct whether the aggregate takes distinct input values only
     * @return parsed aggregate expression
     */
    private static SelectExpr aggregateExpression(SelectExpr.AggFunc func,
            org.jooq.Field<?> input, String outputName,
            DSLContext noQuoteCtx, DSLContext quotedCtx, boolean distinct) {
        String inputColumn = noQuoteCtx.renderInlined(input).trim();
        String inputExpression = quotedCtx.renderInlined(input).trim();
        boolean decimalSum = func == SelectExpr.AggFunc.SUM && input.getDataType().isDecimal();
        Integer decimalScale = decimalSum ? input.getDataType().scale() : null;
        Integer decimalPrecision = decimalSum ? input.getDataType().precision() : null;
        return new SelectExpr(func, inputColumn, inputExpression,
                referencedColumns(inputExpression), decimalScale, decimalPrecision, outputName,
                distinct);
    }

    /**
     * Extracts quoted physical column references from a rendered expression.
     *
     * @param expression quoted SQL expression
     * @return referenced column names in encounter order
     */
    private static List<String> referencedColumns(String expression) {
        List<String> result = new ArrayList<>();
        Matcher matcher = Pattern.compile("\"([^\"]+)\"").matcher(expression);
        while (matcher.find()) {
            String column = matcher.group(1).replace("\"\"", "\"");
            if (!result.contains(column)) {
                result.add(column);
            }
        }
        return result;
    }

    // ── JOIN helper methods ───────────────────────────────────────────────

    private static final String JOIN_ANY_RE = "(?:FROM|(?:(?:LEFT|RIGHT|FULL(?:\\s+OUTER)?|INNER|CROSS)\\s+)?JOIN)";

    /**
     * @param sql original SQL string
     * @param joinTables list of join tables with their aliases
     * @return SQL string with qualified table refs rewritten to aliases
     */
    static String rewriteTableRefs(String sql, List<JoinTable> joinTables) {
        String result = sql;
        for (JoinTable jt : joinTables) {
            String qualified = jt.schema() != null
                    ? namePattern(jt.schema()) + "\\." + namePattern(jt.table()) : namePattern(jt.table());
            if (!jt.alias().equals(jt.table())) {
                result = result.replaceAll(
                        "(?i)(\\b" + JOIN_ANY_RE + "\\s+)" + qualified
                                + "\\s+(?:AS\\s+)?" + Pattern.quote(jt.alias()),
                        "$1" + Matcher.quoteReplacement(jt.alias()));
            } else {
                String view = PLAIN_NAME.matcher(jt.table()).matches()
                        ? jt.table() : DuckDbAdapter.quoteIdentifier(jt.table());
                result = result.replaceAll(
                        "(?i)(\\b" + JOIN_ANY_RE + "\\s+|,\\s*)" + qualified + "(?![\\w\"])",
                        "$1" + Matcher.quoteReplacement(view));
            }
        }
        return result;
    }

    // ── public accessors ──────────────────────────────────────────────────

    /**
     * @return schema name, or null if not specified
     */
    public String getSchema() {
        return schema;
    }

    /**
     * @return table name
     */
    public String getTable() {
        return table;
    }

    /**
     * @return selected column names
     */
    public List<String> getColumns() {
        return columns;
    }

    /**
     * @return WHERE clause filter expression
     */
    public String getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        if (isJoin) {
            return "ParquetQueryParser{isJoin=true, joinTables=" + joinTables
                    + ", columns=" + columns + ", filter='" + filter + "'}";
        }
        return "ParquetQueryParser{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", columns=" + columns +
                ", filter='" + filter + '\'' +
                ", hasAggregation=" + hasAggregation +
                ", groupByColumnNames=" + groupByColumnNames +
                '}';
    }

}
