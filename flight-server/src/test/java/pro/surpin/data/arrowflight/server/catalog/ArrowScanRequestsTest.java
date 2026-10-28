package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan.Op;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan.Predicate;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * The scan requests of queries over tables read as Arrow streams ({@link ArrowScanRequests}):
 * the columns a query names (all for a star, one for {@code count(*)}) and the predicate hints
 * of the top-level AND of its WHERE clause, with values of the columns' declared types.
 */
@Tag("unit")
class ArrowScanRequestsTest {

    private static final TableSnapshot SNAPSHOT = new TableSnapshot(new TableName("s", "t"), "1",
            "f", List.of(new ColumnSpec("id", "BIGINT", true, null),
                    new ColumnSpec("Name", "VARCHAR", true, null),
                    new ColumnSpec("price", "DECIMAL(12,2)", true, null),
                    new ColumnSpec("ratio", "DOUBLE", true, null),
                    new ColumnSpec("day", "DATE", true, null),
                    new ColumnSpec("ts", "TIMESTAMP", true, null),
                    new ColumnSpec("ok", "BOOLEAN", true, null),
                    new ColumnSpec("small", "FLOAT", true, null),
                    new ColumnSpec("p", "INTEGER", true, null)),
            List.of("p"), List.of(), new ReadSpec.ArrowStream("orc"));

    private static final String SOURCE = "__kestrel_arrow_source";

    @Test
    void columnsAreTheDeclaredOnesTheQueryNamesInDeclaredOrder() {
        assertEquals(List.of("id", "Name", "p"), ArrowScanRequests.columns(SNAPSHOT,
                "SELECT \"name\", \"id\" FROM " + SOURCE + " WHERE \"P\" = 1 ORDER BY \"id\""));
        assertEquals(List.of("id", "price"), ArrowScanRequests.columns(SNAPSHOT,
                "SELECT \"id\", sum(price) AS \"s\" FROM " + SOURCE + " GROUP BY \"id\""));
        // Names inside string literals and comments are no column references.
        assertEquals(List.of("id"), ArrowScanRequests.columns(SNAPSHOT,
                "SELECT id FROM " + SOURCE + " WHERE id <> 'ratio' -- day\n /* ts */"));
    }

    @Test
    void countStarReadsTheFirstColumnAndAStarReadsAll() {
        assertEquals(List.of("id"), ArrowScanRequests.columns(SNAPSHOT,
                "SELECT count(*) FROM " + SOURCE));
        assertEquals(List.of("p"), ArrowScanRequests.columns(SNAPSHOT,
                "SELECT count( * ) AS \"n\" FROM " + SOURCE + " WHERE \"p\" > 2"));
        List<String> all = SNAPSHOT.columns().stream().map(ColumnSpec::name).toList();
        assertEquals(all, ArrowScanRequests.columns(SNAPSHOT, "SELECT * FROM " + SOURCE));
        // A product names no column by its star: reading every column is merely more work.
        assertEquals(all, ArrowScanRequests.columns(SNAPSHOT,
                "SELECT id * 2 FROM " + SOURCE));
    }

    @Test
    void simpleConditionsOfTheTopLevelAndBecomePredicates() {
        assertEquals(List.of(
                new Predicate("id", Op.GE, List.of(10L)),
                new Predicate("Name", Op.EQ, List.of("a'b")),
                new Predicate("p", Op.GT, List.of(3L)),
                new Predicate("day", Op.GE, List.of(LocalDate.of(2024, 1, 1))),
                new Predicate("day", Op.LE, List.of(LocalDate.of(2024, 1, 31))),
                new Predicate("price", Op.IN, List.of(new BigDecimal("1.50"), BigDecimal.valueOf(2))),
                new Predicate("ok", Op.IS_NOT_NULL, List.of()),
                new Predicate("ratio", Op.LT, List.of(0.5)),
                new Predicate("ts", Op.NE, List.of(LocalDateTime.of(2024, 1, 2, 3, 4, 5)))),
                ArrowScanRequests.predicates(SNAPSHOT, "\"id\" >= 10 AND \"NAME\" = 'a''b'"
                        + " AND 3 < \"p\" AND \"day\" BETWEEN '2024-01-01' AND DATE '2024-01-31'"
                        + " AND \"price\" IN (1.50, 2) AND \"ok\" IS NOT NULL"
                        + " AND \"ratio\" < 0.5 AND \"ts\" <> '2024-01-02 03:04:05'"));
    }

    @Test
    void conditionsThatAreNotImpliedOrNotPlainAreLeftToDuckDb() {
        // OR, NOT, functions, casts, column against column, unknown columns.
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT,
                "\"id\" = 1 OR \"p\" = 2"));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, "NOT (\"id\" = 1)"));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, "lower(\"Name\") = 'a'"));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT,
                "CAST(\"id\" AS VARCHAR) = '1'"));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, "\"id\" = \"p\""));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, "\"nope\" = 1"));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, "not a condition (("));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, null));
        // Only the plain part of a conjunction is handed over.
        assertEquals(List.of(new Predicate("id", Op.EQ, List.of(1L))),
                ArrowScanRequests.predicates(SNAPSHOT, "\"id\" = 1 AND (\"p\" = 1 OR \"p\" = 2)"));
        // A NULL in an IN list (or anything not a literal) drops the whole IN.
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, "\"id\" IN (1, NULL)"));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, "\"id\" IN (1, \"p\")"));
        assertEquals(List.of(new Predicate("id", Op.IS_NULL, List.of())),
                ArrowScanRequests.predicates(SNAPSHOT, "\"id\" IS NULL"));
    }

    @Test
    void literalsOfAnotherTypeAreNotHandedOver() {
        ColumnSpec bigint = SNAPSHOT.columns().get(0);
        assertNull(ArrowScanRequests.value(bigint, new BigDecimal("1.5")));
        assertNull(ArrowScanRequests.value(bigint, "1"));
        assertEquals(1L, ArrowScanRequests.value(bigint, 1));
        assertNull(ArrowScanRequests.value(SNAPSHOT.columns().get(1), 1L));
        assertNull(ArrowScanRequests.value(SNAPSHOT.columns().get(4), "2024-1-1"));
        assertNull(ArrowScanRequests.value(SNAPSHOT.columns().get(5), "2024-01-01"));
        assertNull(ArrowScanRequests.value(SNAPSHOT.columns().get(6), "true"));
        assertNull(ArrowScanRequests.value(SNAPSHOT.columns().get(7), 0.5));
        assertNull(ArrowScanRequests.value(bigint, null));
        assertEquals(List.of(), ArrowScanRequests.predicates(SNAPSHOT, "\"id\" = 1.5"));
    }

    @Test
    void ofBuildsTheScan() {
        ArrowScan scan = ArrowScanRequests.of(SNAPSHOT, "SELECT \"id\" FROM " + SOURCE
                + " WHERE \"p\" = 7", "\"p\" = 7");
        assertEquals(List.of("id", "p"), scan.columns());
        assertEquals(List.of(new Predicate("p", Op.EQ, List.of(7L))), scan.predicates());
    }
}
