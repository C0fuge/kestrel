package pro.surpin.data.arrowflight.server;

import java.time.LocalDate;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.IntVector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Checks real Arrow date vectors retain logical date semantics at checksum boundaries. */
@Tag("unit")
class TpchArrowValuesTest {
    /** Verifies Date32 storage integers become dates while ordinary integer cells stay numeric. */
    @Test
    void dateDayStorageIsDistinctFromIntegerValues() {
        try (RootAllocator allocator = new RootAllocator();
                DateDayVector dates = new DateDayVector("date", allocator);
                IntVector integers = new IntVector("integer", allocator)) {
            dates.allocateNew(3);
            integers.allocateNew(3);
            int day = Math.toIntExact(LocalDate.of(1994, 1, 1).toEpochDay());
            dates.set(0, day);
            dates.set(1, -1);
            dates.setNull(2);
            dates.setValueCount(3);
            integers.set(0, day);
            integers.set(1, -1);
            integers.setNull(2);
            integers.setValueCount(3);
            assertInstanceOf(Integer.class, dates.getObject(0));
            assertEquals(LocalDate.of(1994, 1, 1), TpchArrowValues.cell(dates, 0));
            assertEquals(LocalDate.of(1969, 12, 31), TpchArrowValues.cell(dates, 1));
            assertEquals(day, TpchArrowValues.cell(integers, 0));
            assertEquals(-1, TpchArrowValues.cell(integers, 1));
            assertNull(TpchArrowValues.cell(dates, 2));
            assertNull(TpchArrowValues.cell(integers, 2));
        }
    }

    /** Verifies Date64 uses UTC date boundaries, including negative milliseconds and null cells. */
    @Test
    void dateMillisStorageUsesLogicalDates() {
        try (RootAllocator allocator = new RootAllocator();
                DateMilliVector dates = new DateMilliVector("date", allocator)) {
            dates.allocateNew(3);
            dates.set(0, LocalDate.of(1994, 1, 1).toEpochDay() * 86_400_000L);
            dates.set(1, -1L);
            dates.setNull(2);
            dates.setValueCount(3);
            assertEquals(LocalDate.of(1994, 1, 1), TpchArrowValues.cell(dates, 0));
            assertEquals(LocalDate.of(1969, 12, 31), TpchArrowValues.cell(dates, 1));
            assertNull(TpchArrowValues.cell(dates, 2));
        }
    }
}
