package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan.Op;

/**
 * The server's filter hints as ORC search arguments: every comparison, and the hints that must
 * be left out (types and literals ORC would compare differently, NULL literals).
 */
@Tag("unit")
class OrcSearchArgumentsTest {

    private static final Map<String, TypeDescription> TYPES = new HashMap<>();

    static {
        for (String column : List.of("i:int", "b:bigint", "d:double", "f:float", "dec:decimal(12,2)",
                "s:string", "vc:varchar(5)", "ch:char(4)", "flag:boolean", "dt:date", "ts:timestamp",
                "arr:array<int>")) {
            String[] parts = column.split(":", 2);
            TYPES.put(parts[0], TypeDescription.fromString(parts[1]));
        }
    }

    @Test
    void everyComparisonHasItsSearchArgument() {
        assertEquals("(and leaf-(EQUALS i 1) (not leaf-(EQUALS b 2)) leaf-(LESS_THAN d 1.5)"
                + " leaf-(LESS_THAN_EQUALS dec 2.5) (not leaf-(LESS_THAN_EQUALS s x))"
                + " (not leaf-(LESS_THAN vc y)) leaf-(IN i 1 2) leaf-(IS_NULL flag)"
                + " (not leaf-(IS_NULL dt)))",
                text(p("i", Op.EQ, 1L), p("b", Op.NE, 2L), p("d", Op.LT, 1.5),
                        p("dec", Op.LE, new BigDecimal("2.50")), p("s", Op.GT, "x"),
                        p("vc", Op.GE, "y"), p("i", Op.IN, 1L, null, 2L), p("flag", Op.IS_NULL),
                        p("dt", Op.IS_NOT_NULL)));
        assertEquals("leaf-(EQUALS dt 2024-02-29)", text(p("dt", Op.EQ, LocalDate.of(2024, 2, 29))));
        assertEquals("leaf-(EQUALS flag true)", text(p("flag", Op.EQ, true)));
    }

    @Test
    void unusableHintsAreLeftOut() {
        // CHAR pads, FLOAT and TIMESTAMP compare differently, nested and unknown columns, a
        // literal of another kind, NULL literals, NaN.
        assertNull(OrcSearchArguments.of(List.of(p("ch", Op.EQ, "a"), p("f", Op.LT, 1.0),
                p("ts", Op.LT, LocalDateTime.of(2020, 1, 1, 0, 0)), p("arr", Op.IS_NULL),
                p("nope", Op.EQ, 1L), p("i", Op.EQ, "1"), p("i", Op.EQ, (Object) null),
                p("i", Op.IN, null, null), p("d", Op.EQ, Double.NaN)), TYPES));
        assertEquals("leaf-(EQUALS i 7)", text(p("ch", Op.EQ, "a"), p("i", Op.EQ, 7L)));
    }

    private static ArrowScan.Predicate p(String column, Op op, Object... values) {
        return new ArrowScan.Predicate(column, op, Arrays.asList(values));
    }

    private static String text(ArrowScan.Predicate... predicates) {
        return OrcSearchArguments.of(List.of(predicates), TYPES).toString();
    }
}
