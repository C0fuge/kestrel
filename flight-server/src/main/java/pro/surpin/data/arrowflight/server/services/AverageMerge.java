package pro.surpin.data.arrowflight.server.services;

import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.QOM;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Lets distributed complete mode merge plain {@code avg} items (lane 3, D10). Endpoints cannot
 * answer an average for their own files, so the query is planned as a part query where each
 * {@code avg(x)} is {@code sum(x)} and {@code count(x)}; the merge sums both and divides as
 * DuckDB's avg does, so DECIMAL and integer averages equal the single-node answer bit for bit
 * while the unscaled sum stays below 2^53 (beyond it, and for floating-point inputs as for their
 * sums, the last bit may differ). Only the opt-in distributed path uses the part query.
 */
public final class AverageMerge {

    /** Name prefix of the part query's sum column of a select item's average. */
    static final String SUM_PREFIX = "__kestrel_avg_sum_";
    /** Name prefix of the part query's count column of a select item's average. */
    static final String COUNT_PREFIX = "__kestrel_avg_count_";

    private final ParquetQueryParser original;
    private final String partQuery;
    private final List<Boolean> averages;

    /**
     * Creates the rewrite of one query.
     *
     * @param original parsed client query
     * @param partQuery query the endpoints answer
     * @param averages per select item of the client query, whether it is an average
     */
    private AverageMerge(ParquetQueryParser original, String partQuery, List<Boolean> averages) {
        this.original = original;
        this.partQuery = partQuery;
        this.averages = List.copyOf(averages);
    }

    /**
     * Rewrites a query whose select list has plain averages into its part query.
     *
     * @param query client SQL
     * @return the rewrite, or null when the query has no average, an average with DISTINCT,
     *         FILTER or OVER, or a shape the merge refuses besides its averages
     */
    public static AverageMerge of(String query) {
        if (TimeTravelClause.present(query)) {
            return null;
        }
        Select<?> select;
        ParquetQueryParser original;
        try {
            DSLContext parser = DSL.using(SQLDialect.DEFAULT,
                    new Settings().withParseNameCase(ParseNameCase.AS_IS));
            Query parsed = parser.parser().parseQuery(ParquetQueryParser.normalized(query));
            if (!(parsed instanceof Select<?> s)) {
                return null;
            }
            select = s;
            original = ParquetQueryParser.parse(query);
        } catch (RuntimeException e) {
            return null;
        }
        List<SelectFieldOrAsterisk> items = new ArrayList<>();
        List<Boolean> averages = new ArrayList<>();
        for (SelectFieldOrAsterisk item : select.$select()) {
            if (!(item instanceof org.jooq.Field<?> field)) {
                return null;
            }
            org.jooq.Field<?> inner = field instanceof QOM.FieldAlias<?> alias ? alias.$field() : field;
            if (inner instanceof QOM.Avg avg) {
                if (avg.$distinct() || avg.$filterWhere() != null || avg.$windowSpecification() != null
                        || avg.$windowDefinition() != null || avg.$windowName() != null) {
                    return null;
                }
                int position = averages.size();
                items.add(DSL.sum(avg.$field()).as(SUM_PREFIX + position));
                items.add(DSL.count(avg.$field()).as(COUNT_PREFIX + position));
                averages.add(true);
            } else {
                items.add(item);
                averages.add(false);
            }
        }
        if (!averages.contains(true) || original.isJoin || original.singleNodeFeature() != null
                || original.selectExprs.size() != averages.size()
                || CompleteAnswerMerge.orderRefusal(original) != null) {
            return null;
        }
        // The endpoints need no order: the merge orders the complete answer.
        Select<?> part = select.$select(items).$orderBy(List.of());
        String partSql = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.EXPLICIT_DEFAULT_QUOTED))
                .renderInlined(part);
        return new AverageMerge(original, partSql, averages);
    }

    /**
     * Returns the query the endpoints answer.
     *
     * @return part query SQL
     */
    public String partQuery() {
        return partQuery;
    }

    /**
     * Renders the DuckDB SQL that computes the complete answer from the union of the part
     * query's endpoint answers: the part query's merge, then each average divided out, named and
     * ordered as the complete answer.
     *
     * @param part parsed part query
     * @param parts SQL relation holding the rows of all endpoint answers
     * @param partColumns column names of the endpoint answers, in order
     * @param sumScales per endpoint answer column, the decimal scale of a DECIMAL column, 0 for
     *        an integer column, null for any other type
     * @param outputNames column names of the complete answer, in order
     * @return merge SQL
     * @throws IllegalArgumentException when the endpoint answers do not fit the rewrite or an
     *         ORDER BY item is not an output column
     */
    public String mergeSql(ParquetQueryParser part, String parts, List<String> partColumns,
            List<Integer> sumScales, List<String> outputNames) {
        long averageCount = averages.stream().filter(Boolean::booleanValue).count();
        if (outputNames.size() != averages.size()
                || partColumns.size() != averages.size() + averageCount
                || sumScales.size() != partColumns.size()) {
            throw new IllegalArgumentException("Endpoint answers have " + partColumns.size()
                    + " columns for " + averages.size() + " select items with " + averageCount
                    + " averages");
        }
        String inner = CompleteAnswerMerge.mergeSql(part, parts, partColumns);
        List<String> items = new ArrayList<>(averages.size());
        int column = 0;
        for (int i = 0; i < averages.size(); i++) {
            String name = DuckDbAdapter.quoteIdentifier(outputNames.get(i));
            String value = "merged." + DuckDbAdapter.quoteIdentifier(partColumns.get(column));
            if (averages.get(i)) {
                String count = "merged." + DuckDbAdapter.quoteIdentifier(partColumns.get(column + 1));
                items.add(average(value, count, sumScales.get(column)) + " AS " + name);
                column += 2;
            } else {
                items.add(value + " AS " + name);
                column++;
            }
        }
        String sql = "SELECT " + String.join(", ", items) + " FROM (" + inner + ") AS merged";
        if (original.orderBy == null) {
            return sql;
        }
        return "SELECT * FROM (" + sql + ") AS answer ORDER BY "
                + CompleteAnswerMerge.orderBy(original, outputNames, "answer");
    }

    /**
     * Divides a merged sum by its merged count as DuckDB's avg finalizes its state: the unscaled
     * sum as DOUBLE over the count times ten to the scale; floating-point sums over the count.
     *
     * @param sum merged sum column
     * @param count merged count column
     * @param scale decimal scale of the sum, 0 for an integer sum, null for a floating-point sum
     * @return SQL expression of the average, NULL when the count is 0
     */
    static String average(String sum, String count, Integer scale) {
        String quotient;
        if (scale == null) {
            quotient = "CAST(" + sum + " AS DOUBLE) / CAST(" + count + " AS DOUBLE)";
        } else if (scale == 0) {
            quotient = "CAST(CAST(" + sum + " AS HUGEINT) AS DOUBLE) / CAST(" + count + " AS DOUBLE)";
        } else {
            String power = java.math.BigInteger.TEN.pow(scale).toString();
            // Exact unscaled value: integer part and fraction scaled apart, no DECIMAL overflow.
            String unscaled = "(CAST(trunc(" + sum + ") AS HUGEINT) * " + power + " + CAST((" + sum
                    + " - trunc(" + sum + ")) * " + power + " AS HUGEINT))";
            quotient = "CAST(" + unscaled + " AS DOUBLE) / (CAST(" + count + " AS DOUBLE) * CAST("
                    + power + " AS DOUBLE))";
        }
        return "CASE WHEN " + count + " = 0 THEN NULL ELSE " + quotient + " END";
    }
}
