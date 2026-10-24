package pro.surpin.data.arrowflight.server.services;

import org.jooq.*;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.server.metrics.CacheMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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
                false, Collections.emptyList(), null, null, null, null, false, null, null);
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
     */
    @SuppressWarnings("java:S107") // One immutable value object built in a single place.
    private ParquetQueryParser(String schema, String table, List<String> columns, String filter,
            boolean hasAggregation, List<String> groupByColumnNames, List<SelectExpr> selectExprs,
            boolean isJoin, List<JoinTable> joinTables, String duckDbSql,
            String orderBy, Long limit, Long offset, boolean distinct, String having,
            String completeAnswerReason) {
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
        query = castTypedLiterals(withoutTrailingSemicolon(query));
        try {
            rejectParameterMarkers(query);
            return parseChecked(query);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid SQL query: " + query, e);
        }
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
            boolean wordStart = i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1))
                    && sql.charAt(i - 1) != '_';
            Matcher m = TYPED_LITERAL.matcher(sql).region(i, sql.length());
            if (wordStart && m.lookingAt()) {
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
            Query parsedQuery = ctx.parser().parseQuery(query);

            if (parsedQuery instanceof Select<?> select) {
                LOGGER.debug("Parsing SQL query: {}", query);
                ParquetQueryParser parser = parseSelect(select, query);
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
            LOGGER.error("Error parsing SQL query", e);
            throw new RuntimeException("Invalid SQL query: " + query, e);
        }
    }

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

        Matcher fromParen = Pattern.compile("(?i)\\bfrom\\s*\\(").matcher(q);
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
            throw new IllegalArgumentException(
                    "Can only select from ONE table, but got query: " + select.getSQL());
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
    private static final Pattern TABLE_REF = Pattern.compile(
            "^(\\w+(?:\\.\\w+)?)(?:\\s+(?:AS\\s+)?(?!(?:JOIN|ON|USING|WHERE|LEFT|RIGHT|FULL|INNER"
                    + "|CROSS|OUTER|NATURAL|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|WINDOW)\\b)(\\w+))?",
            Pattern.CASE_INSENSITIVE);

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
                String qualified = tr.group(1);
                String alias = tr.group(2);
                int dot = qualified.indexOf('.');
                String schema = dot > 0 ? qualified.substring(0, dot) : null;
                String table = dot > 0 ? qualified.substring(dot + 1) : qualified;
                if (alias == null) {
                    alias = table;
                }
                tables.add(new JoinTable(schema, table, alias));
            }
        }
        return tables;
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
                renderOrderBy(select, quotedCtx), rowCount(select.$limit()),
                rowCount(select.$offset()), false, null, null);
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
            throw new IllegalArgumentException("Can only select from ONE table, but got query: " + select.getSQL());
        }

        String schema = Optional.ofNullable(fromTables.get(0).getSchema()).map(Schema::getName).orElse(null);
        String table = fromTables.get(0).getName();

        List<String> gbCols = new ArrayList<>();
        String completeAnswerReason = null;
        for (GroupField gf : select.$groupBy()) {
            gbCols.add(noQuoteCtx.render(gf).trim());
            if (completeAnswerReason == null && !(gf instanceof TableField<?, ?>)) {
                completeAnswerReason = "The GROUP BY expression '" + noQuoteCtx.renderInlined(gf).trim()
                        + "' is not a column";
            }
        }

        List<String> columns = new ArrayList<>();
        List<SelectExpr> exprs = new ArrayList<>();
        boolean hasAgg = !gbCols.isEmpty();
        DSLContext quotedCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));

        for (SelectFieldOrAsterisk sfoa : select.$select()) {
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
            if (completeAnswerReason == null) {
                completeAnswerReason = completeAnswerReason(f, noQuoteCtx);
            }

            if (f instanceof org.jooq.impl.QOM.Count count) {
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
        return new ParquetQueryParser(schema, table, columns, filter, hasAgg, gbCols, exprs,
                false, Collections.emptyList(), null,
                renderOrderBy(select, quotedCtx), rowCount(select.$limit()),
                rowCount(select.$offset()), select.$distinct(),
                having == null ? null : quotedCtx.renderInlined(having), completeAnswerReason);
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
     * @return comma-separated sort list, or null when the query has no ORDER BY
     */
    private static String renderOrderBy(Select<?> select, DSLContext quotedCtx) {
        List<String> parts = new ArrayList<>();
        for (SortField<?> sortField : select.$orderBy()) {
            parts.add(quotedCtx.renderInlined(sortField).trim());
        }
        return parts.isEmpty() ? null : String.join(", ", parts);
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
                    ? jt.schema() + "." + jt.table() : jt.table();
            if (!jt.alias().equals(jt.table())) {
                result = result.replaceAll(
                        "(?i)(\\b" + JOIN_ANY_RE + "\\s+)" + Pattern.quote(qualified)
                                + "\\s+(?:AS\\s+)?" + Pattern.quote(jt.alias()),
                        "$1" + Matcher.quoteReplacement(jt.alias()));
            } else {
                result = result.replaceAll(
                        "(?i)(\\b" + JOIN_ANY_RE + "\\s+|,\\s*)" + Pattern.quote(qualified) + "\\b",
                        "$1" + Matcher.quoteReplacement(jt.table()));
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
