package pro.surpin.data.arrowflight.server.services;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser.SelectExpr;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser.SelectExpr.AggFunc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rebuilds the complete answer of a query from the answers its partial-mode endpoints give for
 * their own files (item 18, distributed complete mode). Only query shapes whose merge is
 * provably the complete answer are accepted; every other query keeps today's path, where one
 * node computes the complete answer alone.
 */
public final class CompleteAnswerMerge {

    /** One ORDER BY item that names a column: optional qualifiers, direction, null order. */
    private static final Pattern ORDER_ITEM = Pattern.compile(
            "(?:\"(?:[^\"]|\"\")+\"\\.)*\"((?:[^\"]|\"\")+)\"((?:\\s+(?i:asc|desc))?"
                    + "(?:\\s+(?i:nulls\\s+(?:first|last)))?)");

    /** One ORDER BY item that is a select-list position. */
    private static final Pattern ORDER_ORDINAL = Pattern.compile(
            "([1-9][0-9]*)((?:\\s+(?i:asc|desc))?(?:\\s+(?i:nulls\\s+(?:first|last)))?)");

    private CompleteAnswerMerge() {
    }

    /**
     * Tells why the endpoint answers of a query cannot be merged into its complete answer.
     *
     * @param parsed parsed client query
     * @return reason, or null when {@link #mergeSql} accepts the query shape (it still refuses
     *         an ORDER BY name that is not an output column of the endpoint answers)
     */
    public static String refusal(ParquetQueryParser parsed) {
        if (parsed.isJoin) {
            return "A JOIN";
        }
        if (parsed.singleNodeFeature() != null) {
            return parsed.singleNodeFeature();
        }
        if (parsed.completeAnswerReason != null) {
            return parsed.completeAnswerReason;
        }
        if (parsed.orderBy != null) {
            for (String item : parsed.orderBy.split(",")) {
                String trimmed = item.trim();
                if (!ORDER_ITEM.matcher(trimmed).matches() && !ORDER_ORDINAL.matcher(trimmed).matches()) {
                    return "ORDER BY " + trimmed;
                }
            }
        }
        if (parsed.hasAggregation) {
            if (parsed.limit != null) {
                // Each endpoint would keep its first groups only, not the groups' full values.
                return "LIMIT on an aggregate";
            }
            if (parsed.selectExprs.isEmpty()) {
                return "An aggregate without a select list";
            }
            for (String groupColumn : parsed.groupByColumnNames) {
                if (parsed.selectExprs.stream().noneMatch(e -> e.func == AggFunc.COLUMN
                        && sameColumn(e.inputColumn, groupColumn))) {
                    return "The GROUP BY column '" + groupColumn + "' is not selected";
                }
            }
        }
        return null;
    }

    /**
     * Renders the DuckDB SQL that computes a query's complete answer from the union of its
     * endpoint answers.
     *
     * @param parsed parsed client query
     * @param parts SQL relation holding the rows of all endpoint answers
     * @param partColumns column names of the endpoint answers, in order
     * @return merge SQL whose columns have the endpoint answers' names
     * @throws IllegalArgumentException when the query shape cannot be merged
     */
    public static String mergeSql(ParquetQueryParser parsed, String parts, List<String> partColumns) {
        String reason = refusal(parsed);
        if (reason != null) {
            throw new IllegalArgumentException(reason + " cannot be merged from endpoint answers");
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        if (parsed.distinct) {
            sql.append("DISTINCT ");
        }
        if (parsed.hasAggregation) {
            appendAggregateMerge(sql, parsed.selectExprs, parts, partColumns);
        } else {
            sql.append("* FROM ").append(parts);
        }
        if (parsed.orderBy == null && parsed.limit == null) {
            return sql.toString();
        }
        StringBuilder outer = new StringBuilder("SELECT * FROM (").append(sql).append(") AS merged");
        if (parsed.orderBy != null) {
            outer.append(" ORDER BY ").append(orderBy(parsed, partColumns));
        }
        if (parsed.limit != null) {
            outer.append(" LIMIT ").append(parsed.limit);
        }
        return outer.toString();
    }

    /**
     * Appends the re-aggregation of grouped endpoint answers: counts are summed, sums summed,
     * minima and maxima taken again, grouped by every selected column.
     *
     * @param sql SQL being built, after SELECT
     * @param exprs select items of the client query
     * @param parts SQL relation holding the endpoint answers
     * @param partColumns column names of the endpoint answers
     */
    private static void appendAggregateMerge(StringBuilder sql, List<SelectExpr> exprs,
            String parts, List<String> partColumns) {
        if (exprs.size() != partColumns.size()) {
            throw new IllegalArgumentException("Endpoint answers have " + partColumns.size()
                    + " columns, the query selects " + exprs.size());
        }
        requireDistinctNames(partColumns);
        List<String> items = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        for (int i = 0; i < exprs.size(); i++) {
            String column = DuckDbAdapter.quoteIdentifier(partColumns.get(i));
            switch (exprs.get(i).func) {
                case COLUMN -> {
                    items.add(column);
                    groups.add(column);
                }
                case COUNT, COUNT_STAR -> items.add("CAST(sum(" + column + ") AS BIGINT) AS " + column);
                case SUM -> items.add("sum(" + column + ") AS " + column);
                case MIN -> items.add("min(" + column + ") AS " + column);
                case MAX -> items.add("max(" + column + ") AS " + column);
                default -> throw new IllegalArgumentException("Unknown aggregate " + exprs.get(i).func);
            }
        }
        sql.append(String.join(", ", items)).append(" FROM ").append(parts);
        if (!groups.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groups));
        }
    }

    /**
     * Rewrites the ORDER BY of a query over the merged answer, where only output names exist.
     *
     * @param parsed parsed client query
     * @param partColumns column names of the endpoint answers
     * @return ORDER BY items over the output names
     * @throws IllegalArgumentException when an item is not an output column
     */
    private static String orderBy(ParquetQueryParser parsed, List<String> partColumns) {
        requireDistinctNames(partColumns);
        List<String> items = new ArrayList<>();
        for (String item : parsed.orderBy.split(",")) {
            String trimmed = item.trim();
            Matcher ordinal = ORDER_ORDINAL.matcher(trimmed);
            if (ordinal.matches() && Integer.parseInt(ordinal.group(1)) <= partColumns.size()) {
                items.add(ordinal.group(1) + ordinal.group(2));
                continue;
            }
            Matcher named = ORDER_ITEM.matcher(trimmed);
            if (!named.matches()) {
                throw new IllegalArgumentException("ORDER BY " + trimmed
                        + " is not an output column and cannot be merged from endpoint answers");
            }
            String name = named.group(1).replace("\"\"", "\"");
            if (partColumns.stream().noneMatch(name::equalsIgnoreCase) || namesAnotherInput(parsed, name)) {
                throw new IllegalArgumentException("ORDER BY " + trimmed
                        + " is not an output column and cannot be merged from endpoint answers");
            }
            items.add(DuckDbAdapter.quoteIdentifier(name) + named.group(2));
        }
        return String.join(", ", items);
    }

    /**
     * Tells whether a name used in ORDER BY is also an input column of a select item that has
     * another output name, so that it could mean the input instead of the output.
     *
     * @param parsed parsed client query
     * @param name ORDER BY column name
     * @return true when the name is ambiguous
     */
    private static boolean namesAnotherInput(ParquetQueryParser parsed, String name) {
        for (SelectExpr expr : parsed.selectExprs) {
            boolean readsName = expr.inputColumns.stream().anyMatch(c -> sameColumn(c, name));
            if (readsName && !sameColumn(name, expr.outputName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Refuses endpoint answers with two columns of one name, which the merge could not tell apart.
     *
     * @param partColumns column names of the endpoint answers
     * @throws IllegalArgumentException when a name repeats
     */
    private static void requireDistinctNames(List<String> partColumns) {
        Set<String> seen = new HashSet<>();
        for (String column : partColumns) {
            if (!seen.add(column.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Endpoint answers have two columns named " + column);
            }
        }
    }

    /**
     * Compares two column references by their unqualified, unquoted names, ignoring case.
     *
     * @param a column reference, possibly qualified or quoted
     * @param b column reference, possibly qualified or quoted
     * @return true when both name the same column
     */
    private static boolean sameColumn(String a, String b) {
        return a != null && b != null && unqualified(a).equalsIgnoreCase(unqualified(b));
    }

    /**
     * Strips the qualifiers and quotes of a column reference.
     *
     * @param reference column reference
     * @return the column name
     */
    private static String unqualified(String reference) {
        String name = reference.trim();
        int dot = name.lastIndexOf('.');
        if (dot >= 0 && !name.endsWith("\"")) {
            name = name.substring(dot + 1);
        } else if (name.endsWith("\"")) {
            int open = name.lastIndexOf('"', name.length() - 2);
            name = open >= 0 ? name.substring(open + 1, name.length() - 1) : name;
        }
        return name.replace("\"", "");
    }
}
