package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests how plain Flight descriptors are read as queries. */
@Tag("unit")
class PlainFlightCommandsTest {

    /** Verifies SQL text commands are read as they are, trimmed, and Flight SQL commands are left alone. */
    @Test
    void commandsAreSqlTextUnlessFlightSql() {
        assertEquals("SELECT * FROM sales.orders", PlainFlightCommands.query(command(" SELECT * FROM sales.orders\n")));
        assertEquals("SELECT 'ä' AS \"x\"", PlainFlightCommands.query(command("SELECT 'ä' AS \"x\"")));
        assertNull(PlainFlightCommands.query(FlightDescriptor.command(Any.pack(
                FlightSql.CommandStatementQuery.newBuilder().setQuery("SELECT 1").build()).toByteArray())));
        assertNull(PlainFlightCommands.query(FlightDescriptor.command(Any.pack(
                FlightSql.CommandGetTables.getDefaultInstance()).toByteArray())));
        // Another protobuf message is not a Flight SQL command: read as text, refused by the SQL path.
        byte[] other = Any.pack(StringValue.of("SELECT 1")).toByteArray();
        assertEquals(new String(other, StandardCharsets.UTF_8).strip(),
                PlainFlightCommands.query(FlightDescriptor.command(other)));
    }

    /** Verifies empty and non-UTF-8 commands are refused. */
    @Test
    void emptyAndBinaryCommandsAreRefused() {
        assertThrows(IllegalArgumentException.class, () -> PlainFlightCommands.query(command("")));
        assertThrows(IllegalArgumentException.class, () -> PlainFlightCommands.query(command(" \n\t")));
        assertThrows(IllegalArgumentException.class, () -> PlainFlightCommands.query(
                FlightDescriptor.command(new byte[] {'S', (byte) 0xC3, (byte) 0x28})));
    }

    /** Verifies paths name one table of plain identifiers, dotted or as elements. */
    @Test
    void pathsNameOneTable() {
        assertEquals("SELECT * FROM sales.orders", PlainFlightCommands.query(FlightDescriptor.path("sales.orders")));
        assertEquals("SELECT * FROM sales.orders", PlainFlightCommands.query(FlightDescriptor.path("sales", "orders")));
        assertEquals("SELECT * FROM c.s.t_1", PlainFlightCommands.query(FlightDescriptor.path("c", "s.t_1")));
        assertEquals("SELECT * FROM orders", PlainFlightCommands.query(FlightDescriptor.path("orders")));
        for (String bad : new String[] {"", "sales.", ".orders", "sales..orders", "a.b.c.d", "sales.orders;",
            "sales.orders where 1=1", "sales.\"orders\"", "1abc", "read_csv('x')", "sales.orders--"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> PlainFlightCommands.query(FlightDescriptor.path(bad)), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> PlainFlightCommands.query(FlightDescriptor.path()));
        assertThrows(IllegalArgumentException.class,
                () -> PlainFlightCommands.query(FlightDescriptor.path("a", "b", "c", "d")));
    }

    /**
     * Builds a command descriptor of UTF-8 text.
     *
     * @param text command text
     * @return descriptor
     */
    private static FlightDescriptor command(String text) {
        return FlightDescriptor.command(text.getBytes(StandardCharsets.UTF_8));
    }
}
