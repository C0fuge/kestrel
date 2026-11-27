package pro.surpin.data.arrowflight.server;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Checks restart-load endpoint reads use the shared canonical checksum for Arrow scalar values. */
@Tag("unit")
class RollingRestartChecksumTest {
    /** Verifies q12 integer and decimal endpoint answers retain the fixed shared checksum. */
    @Test
    void endpointReaderUsesCanonicalTextAndNumericValues() {
        for (boolean decimals : new boolean[] {false, true}) {
            FlightSqlClient client = mock(FlightSqlClient.class);
            FlightEndpoint endpoint = mock(FlightEndpoint.class);
            FlightStream stream = mock(FlightStream.class);
            VectorSchemaRoot root = mock(VectorSchemaRoot.class);
            FieldVector mode = mock(FieldVector.class);
            FieldVector high = mock(FieldVector.class);
            FieldVector low = mock(FieldVector.class);
            when(endpoint.getTicket()).thenReturn(new Ticket(new byte[] {1}));
            when(client.getStream(any(Ticket.class), any(CallOption.class), any(CallOption.class)))
                    .thenReturn(stream);
            when(stream.next()).thenReturn(true, false);
            when(stream.getRoot()).thenReturn(root);
            when(root.getRowCount()).thenReturn(2);
            when(root.getFieldVectors()).thenReturn(List.of(mode, high, low));
            when(mode.getObject(0)).thenReturn(new Text("MAIL"));
            when(mode.getObject(1)).thenReturn(new Text("SHIP"));
            when(high.getObject(0)).thenReturn(decimals ? new BigDecimal("6202") : 6202L);
            when(high.getObject(1)).thenReturn(decimals ? new BigDecimal("6200") : 6200L);
            when(low.getObject(0)).thenReturn(decimals ? new BigDecimal("9324") : 9324L);
            when(low.getObject(1)).thenReturn(decimals ? new BigDecimal("9262") : 9262L);
            CallOption header = new HeaderCallOption(new FlightCallHeaders());
            CallOption timeout = CallOptions.timeout(1, TimeUnit.SECONDS);
            long[] answer = RollingRestartLoadTest.Reader.read(client, endpoint, header, timeout);
            assertEquals(2, answer[0]);
            assertEquals("66191bf0fe853f3d", String.format(Locale.ROOT, "%016x", answer[1]));
        }
    }
    /** Verifies restart-load reads preserve logical Date32 values beside physical integer cells. */
    @Test
    void endpointReaderUsesLogicalDateValues() {
        try (RootAllocator allocator = new RootAllocator();
                DateDayVector dates = new DateDayVector("date", allocator);
                IntVector integers = new IntVector("integer", allocator)) {
            int day = Math.toIntExact(LocalDate.of(1994, 1, 1).toEpochDay());
            dates.allocateNew(1);
            dates.set(0, day);
            dates.setValueCount(1);
            integers.allocateNew(1);
            integers.set(0, day);
            integers.setValueCount(1);
            FlightSqlClient client = mock(FlightSqlClient.class);
            FlightEndpoint endpoint = mock(FlightEndpoint.class);
            FlightStream stream = mock(FlightStream.class);
            VectorSchemaRoot root = mock(VectorSchemaRoot.class);
            when(endpoint.getTicket()).thenReturn(new Ticket(new byte[] {1}));
            when(client.getStream(any(Ticket.class), any(CallOption.class), any(CallOption.class)))
                    .thenReturn(stream);
            when(stream.next()).thenReturn(true, false);
            when(stream.getRoot()).thenReturn(root);
            when(root.getRowCount()).thenReturn(1);
            when(root.getFieldVectors()).thenReturn(List.of(dates, integers));
            CallOption header = new HeaderCallOption(new FlightCallHeaders());
            CallOption timeout = CallOptions.timeout(1, TimeUnit.SECONDS);
            long[] answer = RollingRestartLoadTest.Reader.read(client, endpoint, header, timeout);
            assertEquals(1, answer[0]);
            assertEquals("1e967ed8f5775dac", String.format(Locale.ROOT, "%016x", answer[1]));
        }
    }

}
