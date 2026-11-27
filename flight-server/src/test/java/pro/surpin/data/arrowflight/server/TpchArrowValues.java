package pro.surpin.data.arrowflight.server;

import java.time.LocalDate;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.util.Text;

/** Converts Arrow physical scalar representations to canonical TPC-H logical values. */
final class TpchArrowValues {
    /** Prevents instantiation of the scalar conversion helper. */
    private TpchArrowValues() {
    }

    /**
     * Reads logical dates and text without treating Arrow date storage integers as numeric cells.
     *
     * @param vector typed Arrow column
     * @param row row index
     * @return canonical scalar, or null
     */
    static Object cell(FieldVector vector, int row) {
        if (vector instanceof DateDayVector dates) {
            return dates.isNull(row) ? null : LocalDate.ofEpochDay(dates.get(row));
        }
        if (vector instanceof DateMilliVector dates) {
            return dates.isNull(row) ? null : LocalDate.ofEpochDay(Math.floorDiv(dates.get(row), 86_400_000L));
        }
        Object value = vector.getObject(row);
        return value instanceof Text text ? text.toString() : value;
    }
}
