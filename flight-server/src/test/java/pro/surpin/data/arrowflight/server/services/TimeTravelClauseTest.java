package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.spi.catalog.AsOf;

/**
 * Tests finding and replacing time-travel clauses: names, points, aliases, literals and
 * comments that only look like a clause, and the errors for malformed clauses.
 */
@Tag("unit")
class TimeTravelClauseTest {

    private static String replaced(String sql) {
        List<TimeTravelClause.Reference> refs = TimeTravelClause.find(sql);
        return TimeTravelClause.rewrite(sql, refs, r -> "<" + String.join("/", r.nameParts())
                + (r.aliased() ? "" : " AS t") + ">");
    }

    @Test
    void findsVersionAndTimestampClausesAfterTableNames() {
        List<TimeTravelClause.Reference> refs = TimeTravelClause.find(
                "SELECT * FROM ice.evolved FOR VERSION AS OF 8213491011553364004 WHERE id > 1");
        assertEquals(1, refs.size());
        assertEquals(List.of("ice", "evolved"), refs.get(0).nameParts());
        assertEquals(new AsOf.SnapshotId(8213491011553364004L), refs.get(0).point());
        assertFalse(refs.get(0).aliased());
        assertEquals("SELECT * FROM <ice/evolved AS t> WHERE id > 1",
                replaced("SELECT * FROM ice.evolved FOR VERSION AS OF 8213491011553364004 WHERE id > 1"));

        TimeTravelClause.Reference ts = TimeTravelClause.find("select 1 from \"Ice\".\"a\"\"b\""
                + " for timestamp as of timestamp '2026-10-01 00:00:00 UTC'").get(0);
        assertEquals(List.of("Ice", "a\"b"), ts.nameParts());
        assertEquals(new AsOf.Timestamp(Instant.parse("2026-10-01T00:00:00Z")), ts.point());

        assertEquals(new AsOf.Ref("v1'x"), TimeTravelClause.find(
                "SELECT * FROM s.t FOR VERSION AS OF 'v1''x'").get(0).point());
        assertEquals(new AsOf.SnapshotId(-5L), TimeTravelClause.find(
                "SELECT * FROM s.t FOR VERSION AS OF -5").get(0).point());
        assertEquals(new AsOf.Timestamp(Instant.parse("2026-10-01T06:30:00Z")), TimeTravelClause.find(
                "SELECT * FROM s.t FOR TIMESTAMP AS OF TIMESTAMP WITH TIME ZONE"
                        + " '2026-10-01 09:30:00+03:00'").get(0).point());
        assertEquals(new AsOf.Timestamp(Instant.parse("2026-10-01T00:00:00.5Z")), TimeTravelClause.find(
                "SELECT * FROM s.t FOR TIMESTAMP AS OF '2026-10-01T02:00:00.5 Europe/Berlin'").get(0)
                .point());
    }

    @Test
    void keepsTheClientsAliasAndReplacesEveryReference() {
        assertEquals("SELECT x.id FROM <s/t> x JOIN <s/t> AS y ON x.id = y.id",
                replaced("SELECT x.id FROM s.t FOR VERSION AS OF 1 x JOIN s.t FOR VERSION AS OF 2"
                        + " AS y ON x.id = y.id"));
        assertEquals("SELECT * FROM <s/t AS t> JOIN s.u USING (id)",
                replaced("SELECT * FROM s.t FOR VERSION AS OF 1 JOIN s.u USING (id)"));
        assertEquals("SELECT * FROM <s/t AS t>, <s/u AS t>",
                replaced("SELECT * FROM s.t FOR VERSION AS OF 1, s.u FOR VERSION AS OF 2"));
        assertEquals("SELECT * FROM (SELECT * FROM <s/t AS t>) q",
                replaced("SELECT * FROM (SELECT * FROM s.t FOR VERSION AS OF 1) q"));
        assertEquals("SELECT * FROM <s/t AS t>\nORDER BY 1",
                replaced("SELECT * FROM s.t FOR VERSION AS OF 1\nORDER BY 1"));
        assertEquals("SELECT * FROM <s/t> \"q\"", replaced("SELECT * FROM s.t FOR VERSION AS OF 1 \"q\""));
    }

    @Test
    void literalsCommentsAndOtherSqlAreNotClauses() {
        for (String sql : List.of(
                "SELECT 'FROM s.t FOR VERSION AS OF 1' FROM s.t",
                "SELECT \"for version as of\" FROM s.t",
                "SELECT 1 -- FROM s.t FOR VERSION AS OF 1\nFROM s.t",
                "SELECT /* s.t FOR VERSION AS OF 1 */ 1",
                "SELECT $$ s.t FOR VERSION AS OF 1 $$",
                "SELECT * FROM a ASOF JOIN b ON a.t >= b.t",
                "SELECT * FROM s.t AS OF",
                "SELECT version AS of FROM s.t",
                "SELECT * FROM s.t")) {
            assertFalse(TimeTravelClause.present(sql), sql);
            assertEquals(List.of(), TimeTravelClause.find(sql), sql);
        }
        assertTrue(TimeTravelClause.present("SELECT * FROM s.t FOR VERSION AS OF 'x'"));
        assertFalse(TimeTravelClause.present(null));
    }

    @Test
    void malformedClausesAreRefused() {
        for (String[] c : new String[][] {
                {"SELECT * FROM (SELECT 1) FOR VERSION AS OF 1", "must follow a table name"},
                {"SELECT * FROM s.t FOR VERSION AS OF", "needs a snapshot ID"},
                {"SELECT * FROM s.t FOR VERSION AS OF 1.5", "64-bit integer"},
                {"SELECT * FROM s.t FOR VERSION AS OF 99999999999999999999", "64-bit integer"},
                {"SELECT * FROM s.t FOR TIMESTAMP AS OF now()", "timestamp literal"},
                {"SELECT * FROM s.t FOR TIMESTAMP AS OF TIMESTAMP '2026-10-01 00:00:00'",
                        "needs a time zone"},
                {"SELECT * FROM s.t FOR TIMESTAMP AS OF TIMESTAMP '2026-10-01'", "Not a timestamp"},
                {"SELECT * FROM s.t FOR TIMESTAMP AS OF TIMESTAMP '2026-10-01 00:00 Mars/Base'",
                        "Not a timestamp with a time zone"},
                {"SELECT * FROM s.t FOR TIMESTAMP AS OF TIMESTAMP '2026-13-01 00:00 UTC'",
                        "Not a timestamp with a time zone"}}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> TimeTravelClause.find(c[0]), c[0]);
            assertTrue(e.getMessage().contains(c[1]), c[0] + ": " + e.getMessage());
        }
    }
}
