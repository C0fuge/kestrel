package pro.surpin.data.arrowflight.plugin.hive;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;

/**
 * Turns the server's filter hints into an ORC search argument, which skips stripes and row
 * groups whose statistics rule out every row. Only predicates on data columns of a primitive
 * type with a literal of the matching kind are used: integers, DOUBLE, DECIMAL, STRING and
 * VARCHAR, BOOLEAN and DATE. CHAR (padding), FLOAT, TIMESTAMP and nested columns are left to the
 * server, as are comparisons with NULL; leaving a predicate out only reads more.
 */
final class OrcSearchArguments {

    private OrcSearchArguments() {
    }

    /**
     * Builds the conjunction of the usable predicates.
     *
     * @param predicates the server's hints
     * @param types data column name (as declared) to its ORC type
     * @return search argument, null when no predicate is usable
     */
    static SearchArgument of(List<ArrowScan.Predicate> predicates, Map<String, TypeDescription> types) {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder().startAnd();
        int used = 0;
        for (ArrowScan.Predicate predicate : predicates) {
            TypeDescription type = types.get(predicate.column());
            PredicateLeaf.Type leaf = type == null ? null : leafType(type);
            if (leaf == null) {
                continue;
            }
            List<Object> literals = new ArrayList<>();
            boolean usable = true;
            for (Object value : predicate.values()) {
                if (value == null) {
                    // x = NULL matches nothing; a NULL in an IN list matches nothing either.
                    usable &= predicate.op() == ArrowScan.Op.IN;
                    continue;
                }
                Object literal = literal(leaf, value);
                if (literal == null) {
                    usable = false;
                } else {
                    literals.add(literal);
                }
            }
            if (!usable || (predicate.op() == ArrowScan.Op.IN && literals.isEmpty())) {
                continue;
            }
            add(builder, predicate.op(), predicate.column(), leaf, literals);
            used++;
        }
        return used == 0 ? null : builder.end().build();
    }

    /**
     * Adds one predicate.
     *
     * @param builder builder inside the conjunction
     * @param op comparison
     * @param column column name
     * @param leaf ORC literal type
     * @param literals converted literals
     */
    private static void add(SearchArgument.Builder builder, ArrowScan.Op op, String column,
            PredicateLeaf.Type leaf, List<Object> literals) {
        switch (op) {
            case EQ -> builder.equals(column, leaf, literals.get(0));
            case NE -> builder.startNot().equals(column, leaf, literals.get(0)).end();
            case LT -> builder.lessThan(column, leaf, literals.get(0));
            case LE -> builder.lessThanEquals(column, leaf, literals.get(0));
            case GT -> builder.startNot().lessThanEquals(column, leaf, literals.get(0)).end();
            case GE -> builder.startNot().lessThan(column, leaf, literals.get(0)).end();
            case IN -> builder.in(column, leaf, literals.toArray());
            case IS_NULL -> builder.isNull(column, leaf);
            case IS_NOT_NULL -> builder.startNot().isNull(column, leaf).end();
            default -> throw new IllegalArgumentException("Unknown comparison " + op);
        }
    }

    /**
     * Returns the ORC literal type of a column type, null when its predicates are not used.
     *
     * @param type ORC type
     * @return literal type or null
     */
    static PredicateLeaf.Type leafType(TypeDescription type) {
        return switch (type.getCategory()) {
            case BYTE, SHORT, INT, LONG -> PredicateLeaf.Type.LONG;
            case DOUBLE -> PredicateLeaf.Type.FLOAT;
            case DECIMAL -> PredicateLeaf.Type.DECIMAL;
            case STRING, VARCHAR -> PredicateLeaf.Type.STRING;
            case BOOLEAN -> PredicateLeaf.Type.BOOLEAN;
            case DATE -> PredicateLeaf.Type.DATE;
            default -> null;
        };
    }

    /**
     * Converts a literal of the server to ORC's class for a literal type.
     *
     * @param leaf literal type
     * @param value non-null literal
     * @return ORC literal, null when the value is not of the type's kind
     */
    private static Object literal(PredicateLeaf.Type leaf, Object value) {
        return switch (leaf) {
            case LONG -> value instanceof Long l ? l : null;
            case FLOAT -> value instanceof Double d && !d.isNaN() ? d : null;
            case DECIMAL -> value instanceof BigDecimal d && HiveDecimal.create(d) != null
                    ? new HiveDecimalWritable(HiveDecimal.create(d)) : null;
            case STRING -> value instanceof String s ? s : null;
            case BOOLEAN -> value instanceof Boolean b ? b : null;
            // java.sql.Date of the same local date, whatever the JVM's zone: ORC compares it as
            // its local date.
            case DATE -> value instanceof LocalDate d ? java.sql.Date.valueOf(d) : null;
            default -> null;
        };
    }
}
