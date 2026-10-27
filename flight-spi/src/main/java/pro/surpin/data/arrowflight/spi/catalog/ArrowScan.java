package pro.surpin.data.arrowflight.spi.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * What the server asks of one Arrow stream of a snapshot read as {@link ReadSpec.ArrowStream}:
 * the columns to read and filters the catalog may use to skip data. The server applies every
 * filter of the query itself on top of the stream, so the predicates are hints: a catalog may
 * drop rows, stripes or files only when no row of them can match, and may ignore any predicate.
 *
 * @param columns names of the columns the stream carries, in this order: declared columns of
 *                the snapshot, each at most once, at least one (DuckDB reads no stream without
 *                columns; for {@code count(*)} the server asks for one column)
 * @param predicates conjunction of simple column filters
 * @since SPI 1.4
 */
public record ArrowScan(List<String> columns, List<Predicate> predicates) {

    /**
     * Checks the parts and copies the lists.
     *
     * @param columns columns to read, at least one
     * @param predicates filters, all of which a returned row would have to pass
     */
    public ArrowScan {
        columns = columns == null ? List.of() : List.copyOf(columns);
        predicates = predicates == null ? List.of() : List.copyOf(predicates);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("An Arrow scan reads at least one column");
        }
        if (columns.stream().distinct().count() != columns.size()) {
            throw new IllegalArgumentException("A column is named twice: " + columns);
        }
    }

    /** Comparison of a predicate. */
    public enum Op {
        /** Column equals the one value. */
        EQ,
        /** Column differs from the one value. */
        NE,
        /** Column is less than the one value. */
        LT,
        /** Column is less than or equal to the one value. */
        LE,
        /** Column is greater than the one value. */
        GT,
        /** Column is greater than or equal to the one value. */
        GE,
        /** Column equals one of the values. */
        IN,
        /** Column is NULL; no value. */
        IS_NULL,
        /** Column is not NULL; no value. */
        IS_NOT_NULL
    }

    /**
     * A filter on one column against literal values. A comparison with a NULL value matches no
     * row; values are {@code Boolean}, {@code Long}, {@code Double}, {@code java.math.BigDecimal},
     * {@code String}, {@code java.time.LocalDate} or {@code java.time.LocalDateTime} (UTC wall
     * time) and fit the column's declared type.
     *
     * @param column declared column name (a partition column too)
     * @param op comparison
     * @param values literals: none for the NULL tests, one or more for {@link Op#IN}, else one
     */
    public record Predicate(String column, Op op, List<Object> values) {

        /**
         * Checks the parts and copies the values, which may hold NULL.
         *
         * @param column column name
         * @param op comparison
         * @param values literals
         */
        public Predicate {
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(op, "op");
            values = values == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(values));
            int expected = switch (op) {
                case IS_NULL, IS_NOT_NULL -> 0;
                case IN -> -1;
                default -> 1;
            };
            if (expected >= 0 ? values.size() != expected : values.isEmpty()) {
                throw new IllegalArgumentException(op + " on " + column + " takes "
                        + (expected < 0 ? "one or more values" : expected + " value(s)")
                        + ", got " + values.size());
            }
        }
    }
}
