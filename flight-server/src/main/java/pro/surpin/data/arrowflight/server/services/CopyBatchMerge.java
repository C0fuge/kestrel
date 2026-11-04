package pro.surpin.data.arrowflight.server.services;

import org.jooq.DSLContext;
import org.jooq.GroupField;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.TableField;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.QOM;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser.SelectExpr;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser.SelectExpr.AggFunc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Merges the answers one node computes for batches of a ticket's files into the ticket's answer
 * (item 30, local copies larger than the cache). Accepts what {@link CompleteAnswerMerge} merges
 * from part answers, plus two shapes only one node can answer: an aggregate with HAVING (the
 * parts answer it without HAVING, with every aggregate the condition reads as an extra column;
 * the condition is applied to the merged groups) and count or sum over DISTINCT values of one
 * column (the parts also group by that column, so each part keeps its distinct values; the
 * merge counts or sums them again over distinct values).
 */
public final class CopyBatchMerge {

    /** Name prefix of a part answer column holding an aggregate the HAVING condition reads. */
    static final String HAVING_PREFIX = "__kestrel_having_";
    /** Name prefix of a part answer column holding the values of a DISTINCT aggregate. */
    static final String DISTINCT_PREFIX = "__kestrel_distinct_";

    private static final DSLContext QUOTED = DSL.using(SQLDialect.DEFAULT,
            new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));

    private final ParquetQueryParser original;
    private final ParquetQueryParser part;
    private final String partSql;
    private final List<AggFunc> havingFuncs;
    private final HavingNode having;

    /**
     * Creates the merge of one query.
     *
     * @param original parsed client query
     * @param part parsed query every batch answers
     * @param partSql SQL of the query every batch answers
     * @param havingFuncs per extra HAVING column, the aggregate it holds
     * @param having HAVING condition over the merged answer, or null
     */
    private CopyBatchMerge(ParquetQueryParser original, ParquetQueryParser part, String partSql,
            List<AggFunc> havingFuncs, HavingNode having) {
        this.original = original;
        this.part = part;
        this.partSql = partSql;
        this.havingFuncs = List.copyOf(havingFuncs);
        this.having = having;
    }

    /**
     * Plans the merge of a query's answers for batches of its files.
     *
     * @param query client SQL
     * @param parsed the parsed client query
     * @return the merge, or null when the batches' answers cannot be merged into its answer
     */
    public static CopyBatchMerge of(String query, ParquetQueryParser parsed) {
        if (parsed.isJoin) {
            return null;
        }
        boolean distinctAggregate = parsed.selectExprs.stream().anyMatch(e -> e.distinct);
        if (parsed.having == null && !distinctAggregate) {
            if (CompleteAnswerMerge.partsRefusal(parsed) != null
                    || !(parsed.hasAggregation || parsed.distinct || parsed.limit != null)) {
                return null;
            }
            ParquetQueryParser part = CompleteAnswerMerge.partQuery(parsed);
            return new CopyBatchMerge(parsed, part, query, List.of(), null);
        }
        try {
            return rewrite(query, parsed);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Rewrites a query with HAVING or DISTINCT aggregates into the query its batches answer.
     *
     * @param query client SQL
     * @param parsed the parsed client query
     * @return the merge, or null when the shape is not accepted
     */
    @SuppressWarnings("java:S3776") // One pass over the select list and the HAVING condition.
    private static CopyBatchMerge rewrite(String query, ParquetQueryParser parsed) {
        if (TimeTravelClause.present(query) || (parsed.offset != null && parsed.offset > 0)
                || parsed.completeAnswerReason != null || CompleteAnswerMerge.orderRefusal(parsed) != null
                || parsed.selectExprs.isEmpty()) {
            return null;
        }
        for (String groupColumn : parsed.groupByColumnNames) {
            if (selectedColumn(parsed, groupColumn) < 0) {
                return null;
            }
        }
        DSLContext parser = DSL.using(SQLDialect.DEFAULT,
                new Settings().withParseNameCase(ParseNameCase.AS_IS));
        Query statement = parser.parser().parseQuery(ParquetQueryParser.normalized(query));
        if (!(statement instanceof Select<?> select) || select.$select().size() != parsed.selectExprs.size()) {
            return null;
        }
        List<SelectFieldOrAsterisk> items = new ArrayList<>();
        List<GroupField> groups = new ArrayList<>(select.$groupBy());
        String distinctColumn = null;
        org.jooq.Field<?> distinctField = null;
        int position = 0;
        for (SelectFieldOrAsterisk item : select.$select()) {
            if (!(item instanceof org.jooq.Field<?> field)) {
                return null;
            }
            org.jooq.Field<?> inner = field instanceof QOM.FieldAlias<?> alias ? alias.$field() : field;
            SelectExpr expr = parsed.selectExprs.get(position);
            if (expr.distinct) {
                org.jooq.Field<?> argument = inner instanceof QOM.Count count ? count.$field()
                        : inner instanceof QOM.Sum sum ? sum.$field() : null;
                if (!(argument instanceof TableField<?, ?> column)) {
                    return null;
                }
                String name = column.getName();
                if (distinctColumn != null && !distinctColumn.equalsIgnoreCase(name)) {
                    // Two DISTINCT inputs would need the parts grouped by both.
                    return null;
                }
                if (distinctColumn == null) {
                    distinctColumn = name;
                    distinctField = DSL.field(DSL.name(name));
                }
                items.add(distinctField.as(DISTINCT_PREFIX + position));
            } else {
                items.add(item);
            }
            position++;
        }
        if (distinctField != null) {
            groups.add(distinctField);
        }
        List<AggFunc> havingFuncs = new ArrayList<>();
        HavingNode havingNode = null;
        if (select.$having() != null) {
            Map<String, Integer> aggregates = new LinkedHashMap<>();
            havingNode = havingNode(select.$having(), parsed, aggregates, havingFuncs);
            if (havingNode == null) {
                return null;
            }
            int j = 0;
            for (String aggregate : aggregates.keySet()) {
                items.add(DSL.field(aggregate).as(HAVING_PREFIX + j));
                j++;
            }
        }
        // The batches answer every group in full: HAVING, ORDER BY and LIMIT apply to the merge.
        Select<?> partSelect = select.$select(items).$groupBy(groups).$having(null)
                .$orderBy(List.of()).$limit(null);
        String partSql = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.EXPLICIT_DEFAULT_QUOTED))
                .renderInlined(partSelect);
        ParquetQueryParser part = ParquetQueryParser.parse(partSql);
        if (part.completeAnswerReason != null || part.singleNodeFeature() != null
                || part.selectExprs.size() != parsed.selectExprs.size() + havingFuncs.size()) {
            return null;
        }
        return new CopyBatchMerge(parsed, part, partSql, havingFuncs, havingNode);
    }

    /**
     * Returns the parsed query every batch answers.
     *
     * @return part query
     */
    public ParquetQueryParser partQuery() {
        return part;
    }

    /**
     * Returns the SQL every batch answers; the client query itself for the shapes
     * {@link CompleteAnswerMerge} merges.
     *
     * @return part query SQL
     */
    public String partSql() {
        return partSql;
    }

    /**
     * Returns the number of extra columns the part answers carry beyond the answer's columns.
     *
     * @return extra column count
     */
    public int extraColumns() {
        return havingFuncs.size();
    }

    /**
     * Tells whether the merge takes the shapes only one node can answer (HAVING, DISTINCT
     * aggregates) rather than the ones {@link CompleteAnswerMerge} merges.
     *
     * @return true for a rewritten query
     */
    public boolean rewritten() {
        return having != null || original.selectExprs.stream().anyMatch(e -> e.distinct);
    }

    /**
     * Renders the DuckDB SQL that computes the answer from the union of the batches' answers.
     *
     * @param parts SQL relation holding the rows of all part answers
     * @param partColumns column names of the part answers, in order
     * @param outputNames column names of the answer, in order
     * @return merge SQL
     * @throws IllegalArgumentException when the part answers do not fit the query or an ORDER BY
     *         item is not an output column
     */
    public String mergeSql(String parts, List<String> partColumns, List<String> outputNames) {
        if (!rewritten()) {
            return CompleteAnswerMerge.mergePartsSql(original, parts, partColumns, outputNames);
        }
        int n = original.selectExprs.size();
        if (partColumns.size() != n + havingFuncs.size() || outputNames.size() != n) {
            throw new IllegalArgumentException("Part answers have " + partColumns.size()
                    + " columns, expected " + (n + havingFuncs.size()));
        }
        List<String> items = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        for (int i = 0; i < partColumns.size(); i++) {
            String column = DuckDbAdapter.quoteIdentifier(partColumns.get(i));
            SelectExpr expr = i < n ? original.selectExprs.get(i) : null;
            AggFunc func = expr != null ? expr.func : havingFuncs.get(i - n);
            if (expr != null && expr.distinct) {
                String name = func == AggFunc.COUNT ? "count" : "sum";
                items.add(name + "(DISTINCT " + column + ") AS " + column);
                continue;
            }
            switch (func) {
                case COLUMN -> {
                    items.add(column);
                    groups.add(column);
                }
                case COUNT, COUNT_STAR -> items.add("CAST(sum(" + column + ") AS BIGINT) AS " + column);
                case SUM -> items.add("sum(" + column + ") AS " + column);
                case MIN -> items.add("min(" + column + ") AS " + column);
                case MAX -> items.add("max(" + column + ") AS " + column);
                default -> throw new IllegalArgumentException("Unknown aggregate " + func);
            }
        }
        StringBuilder inner = new StringBuilder("SELECT ");
        if (original.distinct) {
            inner.append("DISTINCT ");
        }
        inner.append(String.join(", ", items)).append(" FROM ").append(parts);
        if (!groups.isEmpty()) {
            inner.append(" GROUP BY ").append(String.join(", ", groups));
        }
        List<String> renamed = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            renamed.add("merged." + DuckDbAdapter.quoteIdentifier(partColumns.get(i)) + " AS "
                    + DuckDbAdapter.quoteIdentifier(outputNames.get(i)));
        }
        StringBuilder sql = new StringBuilder("SELECT ").append(String.join(", ", renamed))
                .append(" FROM (").append(inner).append(") AS merged");
        if (having != null) {
            sql.append(" WHERE ").append(having.render(partColumns, n));
        }
        if (original.orderBy == null && original.limit == null) {
            return sql.toString();
        }
        // Ordered by the answer's names: a DISTINCT aggregate's part column has another name.
        StringBuilder outer = new StringBuilder("SELECT * FROM (").append(sql).append(") AS answer");
        if (original.orderBy != null) {
            outer.append(" ORDER BY ").append(CompleteAnswerMerge.orderBy(original, outputNames, "answer"));
        }
        if (original.limit != null) {
            outer.append(" LIMIT ").append(original.limit);
        }
        return outer.toString();
    }

    /**
     * Finds the select item that is a plain column of a name.
     *
     * @param parsed parsed client query
     * @param column column name, possibly qualified
     * @return position of the select item, or -1
     */
    private static int selectedColumn(ParquetQueryParser parsed, String column) {
        String name = unqualified(column);
        for (int i = 0; i < parsed.selectExprs.size(); i++) {
            SelectExpr expr = parsed.selectExprs.get(i);
            if (expr.func == AggFunc.COLUMN && expr.inputColumn != null
                    && unqualified(expr.inputColumn).equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Strips qualifiers and quotes from a column reference.
     *
     * @param reference column reference
     * @return column name
     */
    private static String unqualified(String reference) {
        String name = reference.replace("\"", "").trim();
        int dot = name.lastIndexOf('.');
        return dot >= 0 ? name.substring(dot + 1) : name;
    }

    /**
     * Translates a HAVING condition (or one of its operands) into a node over the merged answer.
     *
     * @param part condition or operand
     * @param parsed parsed client query
     * @param aggregates rendered aggregate SQL to its extra column position, filled in
     * @param funcs per extra column, its aggregate, filled in
     * @return the node, or null when the condition holds something the merge does not accept
     */
    @SuppressWarnings({"java:S3776", "rawtypes"})
    private static HavingNode havingNode(org.jooq.QueryPart part, ParquetQueryParser parsed,
            Map<String, Integer> aggregates, List<AggFunc> funcs) {
        if (part instanceof QOM.And and) {
            return binary(and.$arg1(), and.$arg2(), " AND ", parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Or or) {
            return binary(or.$arg1(), or.$arg2(), " OR ", parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Not not) {
            HavingNode operand = havingNode(not.$condition(), parsed, aggregates, funcs);
            return operand == null ? null : (cols, n) -> "(NOT " + operand.render(cols, n) + ")";
        }
        if (part instanceof QOM.Eq eq) {
            return binary((org.jooq.QueryPart) eq.$arg1(), (org.jooq.QueryPart) eq.$arg2(), " = ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Ne ne) {
            return binary((org.jooq.QueryPart) ne.$arg1(), (org.jooq.QueryPart) ne.$arg2(), " <> ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Lt lt) {
            return binary((org.jooq.QueryPart) lt.$arg1(), (org.jooq.QueryPart) lt.$arg2(), " < ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Le le) {
            return binary((org.jooq.QueryPart) le.$arg1(), (org.jooq.QueryPart) le.$arg2(), " <= ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Gt gt) {
            return binary((org.jooq.QueryPart) gt.$arg1(), (org.jooq.QueryPart) gt.$arg2(), " > ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Ge ge) {
            return binary((org.jooq.QueryPart) ge.$arg1(), (org.jooq.QueryPart) ge.$arg2(), " >= ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Add add) {
            return binary((org.jooq.QueryPart) add.$arg1(), (org.jooq.QueryPart) add.$arg2(), " + ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Sub sub) {
            return binary((org.jooq.QueryPart) sub.$arg1(), (org.jooq.QueryPart) sub.$arg2(), " - ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof QOM.Mul mul) {
            return binary((org.jooq.QueryPart) mul.$arg1(), (org.jooq.QueryPart) mul.$arg2(), " * ",
                    parsed, aggregates, funcs);
        }
        if (part instanceof org.jooq.Param<?> param) {
            String literal = QUOTED.renderInlined(param).trim();
            return (cols, n) -> literal;
        }
        if (part instanceof TableField<?, ?> column) {
            int position = selectedColumn(parsed, column.getName());
            return position < 0 ? null
                    : (cols, n) -> "merged." + DuckDbAdapter.quoteIdentifier(cols.get(position));
        }
        AggFunc func = plainAggregate(part);
        if (func == null) {
            return null;
        }
        String sql = QUOTED.renderInlined(part).trim();
        Integer position = aggregates.get(sql);
        if (position == null) {
            position = aggregates.size();
            aggregates.put(sql, position);
            funcs.add(func);
        }
        int extra = position;
        return (cols, n) -> "merged." + DuckDbAdapter.quoteIdentifier(cols.get(n + extra));
    }

    /**
     * Translates a binary operation of a HAVING condition.
     *
     * @param left left operand
     * @param right right operand
     * @param operator SQL operator with spaces
     * @param parsed parsed client query
     * @param aggregates rendered aggregate SQL to its extra column position, filled in
     * @param funcs per extra column, its aggregate, filled in
     * @return the node, or null when an operand is not accepted
     */
    private static HavingNode binary(org.jooq.QueryPart left, org.jooq.QueryPart right, String operator,
            ParquetQueryParser parsed, Map<String, Integer> aggregates, List<AggFunc> funcs) {
        HavingNode l = havingNode(left, parsed, aggregates, funcs);
        HavingNode r = l == null ? null : havingNode(right, parsed, aggregates, funcs);
        if (r == null) {
            return null;
        }
        return (cols, n) -> "(" + l.render(cols, n) + operator + r.render(cols, n) + ")";
    }

    /**
     * Names the aggregate of a plain count, sum, min or max (no DISTINCT, FILTER or OVER).
     *
     * @param part query part
     * @return the aggregate, or null for anything else
     */
    private static AggFunc plainAggregate(org.jooq.QueryPart part) {
        if (!(part instanceof QOM.AggregateFunction<?, ?> aggregate)
                || aggregate.$filterWhere() != null || aggregate.$windowSpecification() != null
                || aggregate.$windowDefinition() != null || aggregate.$windowName() != null) {
            return null;
        }
        if (part instanceof QOM.Count count) {
            if (count.$distinct()) {
                return null;
            }
            return count.$field() == null || count.$field() instanceof org.jooq.Param<?>
                    ? AggFunc.COUNT_STAR : AggFunc.COUNT;
        }
        if (part instanceof QOM.Sum sum) {
            return sum.$distinct() ? null : AggFunc.SUM;
        }
        if (part instanceof QOM.Min<?>) {
            return AggFunc.MIN;
        }
        if (part instanceof QOM.Max<?>) {
            return AggFunc.MAX;
        }
        return null;
    }

    /** A HAVING condition, or one of its operands, rendered over the merged answer. */
    @FunctionalInterface
    private interface HavingNode {
        /**
         * Renders the node as DuckDB SQL over the relation {@code merged}.
         *
         * @param partColumns column names of the part answers
         * @param answerColumns number of the answer's columns (extra columns follow)
         * @return SQL
         */
        String render(List<String> partColumns, int answerColumns);
    }

    @Override
    public String toString() {
        return "CopyBatchMerge{part=" + partSql.toLowerCase(Locale.ROOT) + "}";
    }
}
