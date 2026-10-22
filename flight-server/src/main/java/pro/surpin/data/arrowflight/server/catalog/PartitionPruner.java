package pro.surpin.data.arrowflight.server.catalog;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Leaves out the files of partitions a query's WHERE clause rules out, before a partial plan
 * spreads them over the nodes. Only simple predicates on partition columns are evaluated
 * ({@code =, <>, <, <=, >, >=, IN, NOT IN, BETWEEN, IS [NOT] NULL} against literals, combined
 * with AND, OR, NOT), in SQL's three-valued logic; anything else counts as "may be true", so a
 * partition is only left out when the clause can never select its rows. Values are compared as
 * their column's type (text, integers, decimals, dates, booleans); other types are not pruned.
 */
public final class PartitionPruner {

    /** A column reference as jOOQ renders it with quoted names: "c" or "t"."c". */
    private static final Pattern COLUMN = Pattern.compile("^(?:\"[^\"]*\"\\.)*\"([^\"]*)\"$");

    /**
     * The outcomes a predicate may have for one partition: a set of TRUE, FALSE and NULL (SQL's
     * three values). A predicate on data columns or an unsupported expression may have all three.
     *
     * @param bits 1 TRUE, 2 FALSE, 4 NULL
     */
    record Truth(int bits) {
        /** Selects the rows. */
        static final Truth TRUE = new Truth(1);
        /** Rules them out. */
        static final Truth FALSE = new Truth(2);
        /** SQL NULL: rules them out too, but not under NOT. */
        static final Truth NULL = new Truth(4);
        /** Not known here (depends on data columns or an unsupported expression). */
        static final Truth MAYBE = new Truth(7);

        /**
         * Tells whether the predicate may select rows.
         *
         * @return true when TRUE is a possible outcome
         */
        boolean maySelect() {
            return (bits & 1) != 0;
        }
    }

    private final Map<String, String> types = new HashMap<>();
    private final Map<String, String> names = new HashMap<>();
    private final DSLContext render = DSL.using(SQLDialect.DEFAULT,
            new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));

    /**
     * Creates a pruner for a snapshot's partition columns.
     *
     * @param snapshot snapshot with its declared columns
     */
    private PartitionPruner(TableSnapshot snapshot) {
        for (ColumnSpec column : snapshot.columns()) {
            if (snapshot.partitionColumns().contains(column.name())) {
                String key = column.name().toLowerCase(Locale.ROOT);
                types.put(key, column.duckDbType().toUpperCase(Locale.ROOT));
                names.put(key, column.name());
            }
        }
    }

    /**
     * Returns the files of the partitions a WHERE clause may select.
     *
     * @param snapshot snapshot with partition columns and files
     * @param filter WHERE clause as the query parser rendered it, or null
     * @return files kept, in their order; all files when nothing can be ruled out
     */
    public static List<TableFile> prune(TableSnapshot snapshot, String filter) {
        if (filter == null || filter.isBlank() || snapshot.partitionColumns().isEmpty()) {
            return snapshot.files();
        }
        Condition condition;
        try {
            condition = DSL.using(SQLDialect.DEFAULT,
                    new Settings().withParseNameCase(ParseNameCase.AS_IS)).parser()
                    .parseCondition(filter);
        } catch (RuntimeException e) {
            return snapshot.files();
        }
        PartitionPruner pruner = new PartitionPruner(snapshot);
        Map<Map<String, String>, Boolean> decided = new HashMap<>();
        List<TableFile> kept = new ArrayList<>();
        for (TableFile file : snapshot.files()) {
            boolean keep = decided.computeIfAbsent(file.partitionValues(), values -> {
                return pruner.eval(condition, values).maySelect();
            });
            if (keep) {
                kept.add(file);
            }
        }
        return kept;
    }

    /**
     * Evaluates a condition for one partition.
     *
     * @param condition condition
     * @param values partition values
     * @return outcome
     */
    Truth eval(Condition condition, Map<String, String> values) {
        if (condition instanceof QOM.And and) {
            return and(eval(and.$arg1(), values), eval(and.$arg2(), values));
        }
        if (condition instanceof QOM.Or or) {
            return or(eval(or.$arg1(), values), eval(or.$arg2(), values));
        }
        if (condition instanceof QOM.Not not) {
            return not(eval(not.$condition(), values));
        }
        if (condition instanceof QOM.IsNull isNull) {
            Operand o = operand(isNull.$field(), values);
            return o == null ? Truth.MAYBE : o.value() == null ? Truth.TRUE : Truth.FALSE;
        }
        if (condition instanceof QOM.IsNotNull isNotNull) {
            Operand o = operand(isNotNull.$field(), values);
            return o == null ? Truth.MAYBE : o.value() == null ? Truth.FALSE : Truth.TRUE;
        }
        if (condition instanceof QOM.Eq<?> eq) {
            return compare(eq.$arg1(), eq.$arg2(), values, c -> c == 0);
        }
        if (condition instanceof QOM.Ne<?> ne) {
            return compare(ne.$arg1(), ne.$arg2(), values, c -> c != 0);
        }
        if (condition instanceof QOM.Lt<?> lt) {
            return compare(lt.$arg1(), lt.$arg2(), values, c -> c < 0);
        }
        if (condition instanceof QOM.Le<?> le) {
            return compare(le.$arg1(), le.$arg2(), values, c -> c <= 0);
        }
        if (condition instanceof QOM.Gt<?> gt) {
            return compare(gt.$arg1(), gt.$arg2(), values, c -> c > 0);
        }
        if (condition instanceof QOM.Ge<?> ge) {
            return compare(ge.$arg1(), ge.$arg2(), values, c -> c >= 0);
        }
        if (condition instanceof QOM.Between<?> between && !between.$symmetric()) {
            return and(compare(between.$arg1(), between.$arg2(), values, c -> c >= 0),
                    compare(between.$arg1(), between.$arg3(), values, c -> c <= 0));
        }
        if (condition instanceof QOM.InList<?> in) {
            Truth result = Truth.FALSE;
            for (Field<?> element : in.$arg2()) {
                result = or(result, compare(in.$arg1(), element, values, c -> c == 0));
            }
            return result;
        }
        if (condition instanceof QOM.NotInList<?> notIn) {
            Truth result = Truth.TRUE;
            for (Field<?> element : notIn.$arg2()) {
                result = and(result, compare(notIn.$arg1(), element, values, c -> c != 0));
            }
            return result;
        }
        return Truth.MAYBE;
    }

    /** Tells whether a comparison result satisfies an operator. */
    @FunctionalInterface
    private interface Test {
        /**
         * Applies the operator.
         *
         * @param comparison sign of left minus right
         * @return whether the operator holds
         */
        boolean holds(int comparison);
    }

    /**
     * A partition column's value, or a literal, with the type it is compared as.
     *
     * @param value value as text, null for NULL
     * @param type DuckDB type of the partition column, null for a literal
     * @param text whether a literal is a plain text literal (not a number or a cast)
     */
    private record Operand(String value, String type, boolean text) {
    }

    /**
     * Compares a partition column with a literal (either side).
     *
     * @param left left operand
     * @param right right operand
     * @param values partition values
     * @param test operator
     * @return outcome
     */
    private Truth compare(Field<?> left, Field<?> right, Map<String, String> values, Test test) {
        Operand a = operand(left, values);
        Operand b = operand(right, values);
        if (a == null || b == null || (a.type() == null) == (b.type() == null)) {
            return Truth.MAYBE;   // not "partition column op literal"
        }
        if (a.value() == null || b.value() == null) {
            return Truth.NULL;
        }
        String type = a.type() != null ? a.type() : b.type();
        Operand literal = a.type() != null ? b : a;
        if ("VARCHAR".equals(type) && !literal.text()) {
            return Truth.MAYBE;   // DuckDB would convert the text column to the literal's type
        }
        Integer sign = compareAs(type, a.value(), b.value());
        if (sign == null) {
            return Truth.MAYBE;
        }
        return test.holds(sign) ? Truth.TRUE : Truth.FALSE;
    }

    /**
     * Reads an operand: a partition column (with its value) or a literal.
     *
     * @param field expression
     * @param values partition values
     * @return operand, or null when it is neither
     */
    private Operand operand(Field<?> field, Map<String, String> values) {
        Field<?> inner = field instanceof QOM.Cast<?> cast ? cast.$field() : field;
        if (inner instanceof Param<?> param) {
            Object value = param.getValue();
            return new Operand(value == null ? null : literalText(value), null,
                    inner == field && value instanceof String);
        }
        if (field instanceof QOM.Cast<?>) {
            return null;   // a cast of a column changes what is compared
        }
        Matcher m = COLUMN.matcher(render.render(field).trim());
        if (!m.matches()) {
            return null;
        }
        String key = m.group(1).toLowerCase(Locale.ROOT);
        String type = types.get(key);
        if (type == null) {
            return null;   // a data column
        }
        String value = values.get(names.get(key));
        if (value != null && compareAs(type, value, value) == null) {
            value = null;   // TRY_CAST reads it as NULL
        }
        return new Operand(value, type, false);
    }

    /**
     * Writes a literal's value as text.
     *
     * @param value literal value
     * @return text
     */
    private static String literalText(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        return value.toString();
    }

    /**
     * Compares two texts as values of a DuckDB type.
     *
     * @param type DuckDB type, upper case
     * @param a left text
     * @param b right text
     * @return sign of a minus b, or null when the type is not supported or a text does not
     *         convert
     */
    static Integer compareAs(String type, String a, String b) {
        try {
            if ("VARCHAR".equals(type)) {
                return Integer.signum(compareCodePoints(a, b));
            }
            if (List.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT").contains(type)
                    || type.startsWith("DECIMAL") || "FLOAT".equals(type) || "DOUBLE".equals(type)) {
                BigDecimal x = new BigDecimal(a.trim());
                BigDecimal y = new BigDecimal(b.trim());
                if (!type.startsWith("DECIMAL") && !"FLOAT".equals(type) && !"DOUBLE".equals(type)
                        && (x.stripTrailingZeros().scale() > 0)) {
                    return null;   // "1.5" is no integer partition value
                }
                return x.compareTo(y);
            }
            if ("DATE".equals(type)) {
                return Integer.signum(LocalDate.parse(a.trim()).compareTo(LocalDate.parse(b.trim())));
            }
            if ("BOOLEAN".equals(type)) {
                return Boolean.compare(bool(a), bool(b));
            }
        } catch (RuntimeException e) {
            return null;
        }
        return null;
    }

    /**
     * Reads a boolean the way DuckDB casts text.
     *
     * @param text text
     * @return value
     * @throws IllegalArgumentException when it is no boolean
     */
    private static boolean bool(String text) {
        String t = text.trim().toLowerCase(Locale.ROOT);
        if (List.of("true", "t", "1").contains(t)) {
            return true;
        }
        if (List.of("false", "f", "0").contains(t)) {
            return false;
        }
        throw new IllegalArgumentException("not a boolean: " + text);
    }

    /**
     * Compares texts by code point, the order of DuckDB's (UTF-8 byte) comparison.
     *
     * @param a left text
     * @param b right text
     * @return comparison
     */
    private static int compareCodePoints(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int x = a.codePointAt(i);
            int y = b.codePointAt(j);
            if (x != y) {
                return Integer.compare(x, y);
            }
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    /**
     * Combines with AND, every outcome with every outcome.
     *
     * @param a left
     * @param b right
     * @return possible outcomes
     */
    static Truth and(Truth a, Truth b) {
        return combine(a, b, true);
    }

    /**
     * Combines with OR, every outcome with every outcome.
     *
     * @param a left
     * @param b right
     * @return possible outcomes
     */
    static Truth or(Truth a, Truth b) {
        return combine(a, b, false);
    }

    /**
     * Negates every outcome.
     *
     * @param a possible outcomes
     * @return negated outcomes
     */
    static Truth not(Truth a) {
        int bits = a.bits() & 4;
        bits |= (a.bits() & 1) != 0 ? 2 : 0;
        bits |= (a.bits() & 2) != 0 ? 1 : 0;
        return new Truth(bits);
    }

    /**
     * Combines two sets of outcomes with SQL's AND or OR.
     *
     * @param a left outcomes
     * @param b right outcomes
     * @param and AND, else OR
     * @return possible outcomes
     */
    private static Truth combine(Truth a, Truth b, boolean and) {
        int result = 0;
        for (int x = 1; x <= 4; x <<= 1) {
            for (int y = 1; y <= 4; y <<= 1) {
                if ((a.bits() & x) != 0 && (b.bits() & y) != 0) {
                    result |= and ? and3(x, y) : or3(x, y);
                }
            }
        }
        return new Truth(result);
    }

    /**
     * SQL AND of two single outcomes (1 TRUE, 2 FALSE, 4 NULL).
     *
     * @param x left
     * @param y right
     * @return outcome
     */
    private static int and3(int x, int y) {
        if (x == 2 || y == 2) {
            return 2;
        }
        return x == 4 || y == 4 ? 4 : 1;
    }

    /**
     * SQL OR of two single outcomes (1 TRUE, 2 FALSE, 4 NULL).
     *
     * @param x left
     * @param y right
     * @return outcome
     */
    private static int or3(int x, int y) {
        if (x == 1 || y == 1) {
            return 1;
        }
        return x == 4 || y == 4 ? 4 : 2;
    }
}
