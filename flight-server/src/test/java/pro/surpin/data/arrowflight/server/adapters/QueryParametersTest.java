package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests parameter handles and the conversion of parameter values. */
@Tag("unit")
class QueryParametersTest {

    /** Verifies a plain handle is the SQL and a handle with values keeps both apart. */
    @Test
    void handlesRoundTrip() {
        QueryParameters.Handle plain = QueryParameters.parseHandle(ByteString.copyFromUtf8("SELECT ?"));
        assertEquals("SELECT ?", plain.sql());
        assertNull(plain.parameters());

        byte[] values = {1, 2, 3};
        QueryParameters.Handle bound = QueryParameters.parseHandle(
                QueryParameters.boundHandle("SELECT 'é', ?", values));
        assertEquals("SELECT 'é', ?", bound.sql());
        assertArrayEquals(values, bound.parameters());
    }

    /** Verifies values come back as the Java types DuckDB binds. */
    @Test
    void valuesConvertToJdbcTypes() throws Exception {
        Schema schema = new Schema(List.of(
                Field.nullable("i", new ArrowType.Int(32, true)),
                Field.nullable("s", ArrowType.Utf8.INSTANCE),
                Field.nullable("d", new ArrowType.Date(DateUnit.DAY)),
                Field.nullable("n", new ArrowType.Decimal(10, 2, 128)),
                Field.nullable("t", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                Field.nullable("z", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                Field.nullable("x", new ArrowType.Int(32, true))));
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ((IntVector) root.getVector("i")).setSafe(0, 42);
            ((VarCharVector) root.getVector("s")).setSafe(0, "O'Brien".getBytes(StandardCharsets.UTF_8));
            ((DateDayVector) root.getVector("d")).setSafe(0, (int) LocalDate.of(2020, 1, 2).toEpochDay());
            ((DecimalVector) root.getVector("n")).setSafe(0, new BigDecimal("1.25"));
            ((TimeMicroVector) root.getVector("t")).setSafe(0, 3_723_000_001L);
            ((TimeStampMicroTZVector) root.getVector("z")).setSafe(0, 1_000_000L);
            root.getVector("x").setNull(0);
            root.setRowCount(1);

            List<Object> values = QueryParameters.values(allocator, QueryParameters.serialize(root));

            assertEquals(Arrays.asList(42, "O'Brien", LocalDate.of(2020, 1, 2), new BigDecimal("1.25"),
                    LocalTime.of(1, 2, 3, 1000), OffsetDateTime.of(1970, 1, 1, 0, 0, 1, 0, ZoneOffset.UTC), null),
                    values);
        }
    }

    /** Verifies parameter types map to the Arrow types a client sends values in. */
    @Test
    void parameterTypesMapToArrow() {
        assertEquals(new ArrowType.Int(32, true), QueryParameters.arrowType(Types.INTEGER, "INTEGER"));
        assertEquals(new ArrowType.Decimal(18, 3, 128), QueryParameters.arrowType(Types.DECIMAL, "DECIMAL(18,3)"));
        assertEquals(new ArrowType.Date(DateUnit.DAY), QueryParameters.arrowType(Types.DATE, "DATE"));
        assertEquals(ArrowType.Utf8.INSTANCE, QueryParameters.arrowType(Types.OTHER, "INVALID"));
    }

    @Test
    void damagedValuesAreAClientError() {
        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
            byte[] garbage = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f, 1, 2, 3, 4, 5, 6, 7};
            IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalArgumentException.class, () -> QueryParameters.values(allocator, garbage));
            assertEquals("Malformed prepared statement handle", e.getMessage());
        }
    }

    @Test
    void changedHandleBytesAreRefused() {
        byte[] handle = QueryParameters.boundHandle("SELECT ?", new byte[] {1, 2, 3, 4}).toByteArray();
        for (int i = 5; i < handle.length; i++) {
            byte[] damaged = handle.clone();
            damaged[i] ^= 0x5a;
            IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                    () -> QueryParameters.parseHandle(ByteString.copyFrom(damaged)));
            assertEquals("Malformed prepared statement handle", e.getMessage());
        }
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> QueryParameters.parseHandle(ByteString.copyFrom(Arrays.copyOf(handle, handle.length - 1))));
    }
}
