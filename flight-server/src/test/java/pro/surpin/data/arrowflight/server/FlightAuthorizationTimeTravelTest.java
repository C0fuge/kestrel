package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests that a table read at an earlier snapshot ({@code FOR VERSION AS OF}) is checked like the
 * table itself, alone and in joins.
 */
@Tag("unit")
class FlightAuthorizationTimeTravelTest {

    /** Reads only table s.a. */
    private static final FlightAuthorization ONLY_A = new FlightAuthorization() {
        @Override
        public boolean canSeeSchema(String principal, String schema) {
            return true;
        }

        @Override
        public boolean canReadTable(String principal, String schema, String table) {
            return "s".equals(schema) && "a".equals(table);
        }

        @Override
        public void close() {
            // Nothing to release.
        }
    };

    @Test
    void timeTravelTablesAreChecked() {
        assertDoesNotThrow(() -> ONLY_A.requireQuery("u", "SELECT * FROM s.a FOR VERSION AS OF 1"));
        assertDoesNotThrow(() -> ONLY_A.requireQuery("u", "SELECT * FROM \"s\".\"a\" FOR TIMESTAMP"
                + " AS OF TIMESTAMP '2026-10-01 00:00:00 UTC' WHERE id > 1"));
        assertThrows(FlightRuntimeException.class,
                () -> ONLY_A.requireQuery("u", "SELECT * FROM s.b FOR VERSION AS OF 1"));
        assertThrows(FlightRuntimeException.class, () -> ONLY_A.requireQuery("u",
                "SELECT x.id FROM s.a x JOIN s.b FOR VERSION AS OF 1 y ON x.id = y.id"));
        assertThrows(RuntimeException.class,
                () -> ONLY_A.requireQuery("u", "SELECT * FROM s.b FOR VERSION AS OF now()"));
    }
}
