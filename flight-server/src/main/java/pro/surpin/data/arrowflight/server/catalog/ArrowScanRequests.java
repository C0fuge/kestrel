package pro.surpin.data.arrowflight.server.catalog;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.SQLDialect;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.QOM;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Builds the {@link ArrowScan} of a query of one table read as Arrow streams: the declared
 * columns the query's SQL names (all of them when it selects {@code *}; the first one when it
 * names none, as for {@code count(*)}) and the simple predicates of the top-level conjunction
 * of its WHERE clause. A column too many only costs reading it; a column too few makes DuckDB
 * refuse the query, never answer it wrongly. Predicates are hints the catalog may use to skip
 * data that cannot match: only conditions every returned row must pass are handed over, with
 * values of the column's declared type, and anything else is left to DuckDB alone.
 */
public final class ArrowScanRequests {

    /** A column reference as jOOQ renders it with quoted names: "c" or "t"."c". */
    private static final Pattern COLUMN = Pattern.compile("^(?:\"[^\"]*\"\\.)*\"([^\"]*)\"$");
    /** {@code count(*)}, whose star names no column. */
    private static final Pattern COLUMNS_EXPRESSION = Pattern.compile("(?i)\\bcolumns\\s*\\(");
    private static final Pattern COUNT_STAR = Pattern.compile("(?i)count\\s*\\(\\s*\\*\\s*\\)");
    /** Words after which a star starts a select list rather than multiplying a value. */
    private static final Set<String> KEYWORDS = Set.of("select", "distinct", "all", "by", "on",
            "and", "or", "not", "where", "when", "then", "else", "case", "in", "is", "like",
            "between", "having", "qualify", "returning", "as", "from", "join", "using", "over",
            "partition", "limit", "offset", "union", "except", "intersect", "with");
    /** Words that may follow a select-list star. */
    private static final Set<String> STAR_MODIFIERS = Set.of("from", "exclude", "replace",
            "rename", "like", "ilike", "similar", "glob", "as", "into", "where", "group",
            "order", "limit", "union", "except", "intersect", "window", "qualify", "having");
    private static final Set<String> INTEGERS = Set.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT");
    private static final DSLContext RENDER = DSL.using(SQLDialect.DEFAULT,
            new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));

    /** Not instantiated. */
    private ArrowScanRequests() {
    }

    /**
     * Builds the scan of one table reference of a query.
     *
     * @param snapshot snapshot with its declared columns
     * @param sql the query's SQL, with the table's source under any name that is no column
     * @param filter the query's WHERE clause as the query parser rendered it, or null
     * @return columns and predicate hints
     * @throws IllegalArgumentException when the snapshot declares no columns
     */
    public static ArrowScan of(TableSnapshot snapshot, String sql, String filter) {
        return of(snapshot, sql, filter, false);
    }

    /**
     * Builds the scan of one table reference of a query, with all declared columns when the
     * query may read the whole row.
     *
     * @param snapshot snapshot with its declared columns
     * @param sql the query's SQL, with the table's source under any name that is no column
     * @param filter the query's WHERE clause as the query parser rendered it, or null
     * @param allColumns whether every declared column is read whatever the SQL names
     * @return columns and predicate hints
     * @throws IllegalArgumentException when the snapshot declares no columns
     */
    public static ArrowScan of(TableSnapshot snapshot, String sql, String filter,
            boolean allColumns) {
        if (snapshot.columns().isEmpty()) {
            throw new IllegalArgumentException("The catalog declares no columns for "
                    + snapshot.name());
        }
        List<String> columns = allColumns
                ? snapshot.columns().stream().map(ColumnSpec::name).toList()
                : columns(snapshot, sql);
        return new ArrowScan(columns, predicates(snapshot, filter));
    }

    /**
     * Lists the declared columns a query names, in their declared order.
     *
     * @param snapshot snapshot with its declared columns
     * @param sql query SQL
     * @return columns, at least one
     */
    static List<String> columns(TableSnapshot snapshot, String sql) {
        List<String> declared = snapshot.columns().stream().map(ColumnSpec::name).toList();
        Set<String> named = new LinkedHashSet<>();
        boolean star = identifiers(COUNT_STAR.matcher(sql).replaceAll("count()"), named);
        // COLUMNS('regex') selects columns by a pattern the names do not show.
        if (star || COLUMNS_EXPRESSION.matcher(sql).find()) {
            return declared;
        }
        List<String> result = new ArrayList<>();
        for (String column : declared) {
            if (named.contains(column.toLowerCase(Locale.ROOT))) {
                result.add(column);
            }
        }
        return result.isEmpty() ? List.of(declared.get(0)) : result;
    }

    /**
     * Collects the identifiers of a SQL text, lower case, outside string literals and comments.
     *
     * @param sql SQL text
     * @param into receives the identifiers
     * @return true when the text holds a star outside string literals that is no product
     */
    private static boolean identifiers(String sql, Set<String> into) {
        boolean star = false;
        // Whether the last token ends a value (a column, a literal, a closing parenthesis).
        boolean operand = false;
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i = skipQuoted(sql, i, '\'');
                operand = true;
            } else if (c == '"') {
                int end = skipQuoted(sql, i, '"');
                into.add(sql.substring(i + 1, end - 1).replace("\"\"", "\"")
                        .toLowerCase(Locale.ROOT));
                i = end;
                operand = true;
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i);
                i = eol < 0 ? n : eol + 1;
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
            } else if (Character.isLetter(c) || c == '_') {
                int start = i;
                i = wordEnd(sql, i);
                String word = sql.substring(start, i).toLowerCase(Locale.ROOT);
                into.add(word);
                operand = !KEYWORDS.contains(word);
            } else if (Character.isDigit(c)) {
                i = wordEnd(sql, i);
                operand = true;
            } else if (Character.isWhitespace(c)) {
                i++;
            } else {
                // A star between two values is a product; any other star may select columns.
                star |= c == '*' && !(operand && operandFollows(sql, i + 1));
                operand = c == ')';
                i++;
            }
        }
        return star;
    }

    /**
     * Finds the end of a word or a number.
     *
     * @param sql SQL text
     * @param start index of its first character
     * @return index after it
     */
    private static int wordEnd(String sql, int start) {
        int i = start;
        while (i < sql.length() && (Character.isLetterOrDigit(sql.charAt(i))
                || sql.charAt(i) == '_' || sql.charAt(i) == '$')) {
            i++;
        }
        return i;
    }

    /**
     * Tells whether a value starts after a star: a column, a literal, a parenthesis or a sign,
     * but no clause keyword (as in {@code DISTINCT ON (a) * FROM t}).
     *
     * @param sql SQL text
     * @param from index after the star
     * @return true when the star is followed by a value
     */
    private static boolean operandFollows(String sql, int from) {
        int i = from;
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) {
            i++;
        }
        if (i >= sql.length()) {
            return false;
        }
        char c = sql.charAt(i);
        if (Character.isLetter(c) || c == '_') {
            return !STAR_MODIFIERS.contains(sql.substring(i, wordEnd(sql, i))
                    .toLowerCase(Locale.ROOT));
        }
        return Character.isDigit(c) || c == '(' || c == '"' || c == '\'' || c == '+'
                || c == '-' && !(i + 1 < sql.length() && sql.charAt(i + 1) == '-');
    }

    /**
     * Skips a quoted text whose quote is doubled inside.
     *
     * @param sql SQL text
     * @param start index of the opening quote
     * @param quote quote character
     * @return index after the closing quote (the end of the text when it is not closed)
     */
    private static int skipQuoted(String sql, int start, char quote) {
        int i = start + 1;
        while (i < sql.length()) {
            if (sql.charAt(i) == quote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return sql.length();
    }

    /**
     * Turns the simple conditions of a WHERE clause's top-level conjunction into predicates.
     *
     * @param snapshot snapshot with its declared columns
     * @param filter WHERE clause, or null
     * @return predicates, possibly none
     */
    static List<ArrowScan.Predicate> predicates(TableSnapshot snapshot, String filter) {
        if (filter == null || filter.isBlank()) {
            return List.of();
        }
        Condition condition;
        try {
            condition = DSL.using(SQLDialect.DEFAULT,
                    new Settings().withParseNameCase(ParseNameCase.AS_IS)).parser()
                    .parseCondition(filter);
        } catch (RuntimeException e) {
            return List.of();
        }
        Map<String, ColumnSpec> columns = new LinkedHashMap<>();
        for (ColumnSpec column : snapshot.columns()) {
            columns.putIfAbsent(column.name().toLowerCase(Locale.ROOT), column);
        }
        List<ArrowScan.Predicate> result = new ArrayList<>();
        List<Condition> conjuncts = new ArrayList<>();
        conjuncts(condition, conjuncts);
        for (Condition conjunct : conjuncts) {
            try {
                predicate(conjunct, columns, result);
            } catch (RuntimeException e) {
                // Not handed over: DuckDB applies it anyway.
            }
        }
        return result;
    }

    /**
     * Splits a condition into the parts of its top-level AND.
     *
     * @param condition condition
     * @param into receives the parts
     */
    private static void conjuncts(Condition condition, List<Condition> into) {
        if (condition instanceof QOM.And and) {
            conjuncts(and.$arg1(), into);
            conjuncts(and.$arg2(), into);
        } else {
            into.add(condition);
        }
    }

    /**
     * Adds the predicates of one condition when it is a simple one on a declared column.
     *
     * @param condition condition
     * @param columns declared columns by lower-case name
     * @param into receives the predicates
     */
    private static void predicate(Condition condition, Map<String, ColumnSpec> columns,
            List<ArrowScan.Predicate> into) {
        if (condition instanceof QOM.IsNull isNull) {
            ColumnSpec column = column(isNull.$field(), columns);
            if (column != null) {
                into.add(new ArrowScan.Predicate(column.name(), ArrowScan.Op.IS_NULL, List.of()));
            }
        } else if (condition instanceof QOM.IsNotNull isNotNull) {
            ColumnSpec column = column(isNotNull.$field(), columns);
            if (column != null) {
                into.add(new ArrowScan.Predicate(column.name(), ArrowScan.Op.IS_NOT_NULL,
                        List.of()));
            }
        } else if (condition instanceof QOM.Eq<?> eq) {
            compare(eq.$arg1(), eq.$arg2(), ArrowScan.Op.EQ, columns, into);
        } else if (condition instanceof QOM.Ne<?> ne) {
            compare(ne.$arg1(), ne.$arg2(), ArrowScan.Op.NE, columns, into);
        } else if (condition instanceof QOM.Lt<?> lt) {
            compare(lt.$arg1(), lt.$arg2(), ArrowScan.Op.LT, columns, into);
        } else if (condition instanceof QOM.Le<?> le) {
            compare(le.$arg1(), le.$arg2(), ArrowScan.Op.LE, columns, into);
        } else if (condition instanceof QOM.Gt<?> gt) {
            compare(gt.$arg1(), gt.$arg2(), ArrowScan.Op.GT, columns, into);
        } else if (condition instanceof QOM.Ge<?> ge) {
            compare(ge.$arg1(), ge.$arg2(), ArrowScan.Op.GE, columns, into);
        } else if (condition instanceof QOM.Between<?> between && !between.$symmetric()) {
            compare(between.$arg1(), between.$arg2(), ArrowScan.Op.GE, columns, into);
            compare(between.$arg1(), between.$arg3(), ArrowScan.Op.LE, columns, into);
        } else if (condition instanceof QOM.InList<?> in) {
            ColumnSpec column = column(in.$arg1(), columns);
            if (column == null) {
                return;
            }
            List<Object> values = new ArrayList<>();
            for (Field<?> element : in.$arg2()) {
                Object value = element instanceof Param<?> param
                        ? value(column, param.getValue()) : null;
                if (value == null) {
                    return;
                }
                values.add(value);
            }
            if (!values.isEmpty()) {
                into.add(new ArrowScan.Predicate(column.name(), ArrowScan.Op.IN, values));
            }
        }
    }

    /**
     * Adds a comparison of a declared column with a literal (either side).
     *
     * @param left left operand
     * @param right right operand
     * @param op comparison with the column on the left
     * @param columns declared columns by lower-case name
     * @param into receives the predicate
     */
    private static void compare(Field<?> left, Field<?> right, ArrowScan.Op op,
            Map<String, ColumnSpec> columns, List<ArrowScan.Predicate> into) {
        ColumnSpec column = column(left, columns);
        Field<?> literal = right;
        ArrowScan.Op actual = op;
        if (column == null) {
            column = column(right, columns);
            literal = left;
            actual = flip(op);
        }
        if (column == null || !(literal instanceof Param<?> param)) {
            return;
        }
        Object value = value(column, param.getValue());
        if (value != null) {
            into.add(new ArrowScan.Predicate(column.name(), actual, List.of(value)));
        }
    }

    /**
     * Turns a comparison around, for a literal on the left.
     *
     * @param op comparison
     * @return the comparison with its operands swapped
     */
    private static ArrowScan.Op flip(ArrowScan.Op op) {
        return switch (op) {
            case LT -> ArrowScan.Op.GT;
            case LE -> ArrowScan.Op.GE;
            case GT -> ArrowScan.Op.LT;
            case GE -> ArrowScan.Op.LE;
            default -> op;
        };
    }

    /**
     * Reads a plain reference to a declared column (no cast, no expression).
     *
     * @param field expression
     * @param columns declared columns by lower-case name
     * @return the column, or null
     */
    private static ColumnSpec column(Field<?> field, Map<String, ColumnSpec> columns) {
        if (field instanceof Param<?>) {
            return null;
        }
        Matcher m = COLUMN.matcher(RENDER.render(field).trim());
        return m.matches() ? columns.get(m.group(1).toLowerCase(Locale.ROOT)) : null;
    }

    /**
     * Converts a literal to a value of a column's declared type, when DuckDB would compare the
     * column with exactly that value.
     *
     * @param column declared column
     * @param literal literal value as the parser read it
     * @return value of the type {@link ArrowScan.Predicate} names, or null when not handed over
     */
    static Object value(ColumnSpec column, Object literal) {
        if (literal == null) {
            return null;
        }
        String type = column.duckDbType().trim().toUpperCase(Locale.ROOT);
        if (INTEGERS.contains(type)) {
            return integral(literal);
        }
        if (type.startsWith("DECIMAL")) {
            if (literal instanceof BigDecimal decimal) {
                return decimal;
            }
            Long whole = integral(literal);
            return whole == null ? null : BigDecimal.valueOf(whole);
        }
        if ("DOUBLE".equals(type)) {
            return literal instanceof Number number && !(literal instanceof Float)
                    ? number.doubleValue() : null;
        }
        if ("BOOLEAN".equals(type)) {
            return literal instanceof Boolean ? literal : null;
        }
        if ("VARCHAR".equals(type)) {
            return literal instanceof String ? literal : null;
        }
        if ("DATE".equals(type)) {
            return date(literal);
        }
        if ("TIMESTAMP".equals(type)) {
            return timestamp(literal);
        }
        return null;
    }

    /**
     * Reads an integer literal that fits a long.
     *
     * @param literal literal
     * @return the value, or null
     */
    private static Long integral(Object literal) {
        if (literal instanceof Long || literal instanceof Integer || literal instanceof Short
                || literal instanceof Byte) {
            return ((Number) literal).longValue();
        }
        if (literal instanceof BigInteger big && big.bitLength() < Long.SIZE) {
            return big.longValue();
        }
        return null;
    }

    /**
     * Reads a date literal ({@code DATE '...'} or a text DuckDB casts to a date).
     *
     * @param literal literal
     * @return the date, or null
     */
    private static LocalDate date(Object literal) {
        if (literal instanceof LocalDate date) {
            return date;
        }
        if (literal instanceof java.sql.Date date) {
            return date.toLocalDate();
        }
        if (literal instanceof String text && text.trim().matches("\\d{4}-\\d{2}-\\d{2}")) {
            try {
                return LocalDate.parse(text.trim());
            } catch (DateTimeParseException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Reads a timestamp literal ({@code TIMESTAMP '...'} or a text DuckDB casts to one).
     *
     * @param literal literal
     * @return the timestamp, or null
     */
    private static LocalDateTime timestamp(Object literal) {
        if (literal instanceof LocalDateTime time) {
            return time;
        }
        if (literal instanceof java.sql.Timestamp time) {
            return time.toLocalDateTime();
        }
        if (literal instanceof String text
                && text.trim().matches("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?")) {
            try {
                return LocalDateTime.parse(text.trim().replace(' ', 'T'));
            } catch (DateTimeParseException e) {
                return null;
            }
        }
        return null;
    }
}
