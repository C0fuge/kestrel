package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Fields of Hive text files read as Hive's lazy objects read them: what does not read as the
 * type is NULL.
 */
@Tag("unit")
class TextValuesTest {

    @Test
    void integersAllowBlanksSignsAndAFractionAndAreNullOutOfRange() {
        assertEquals(42L, integer("42", Integer.MIN_VALUE, Integer.MAX_VALUE));
        assertEquals(-42L, integer(" -42 ", Integer.MIN_VALUE, Integer.MAX_VALUE));
        assertEquals(42L, integer("+42", Integer.MIN_VALUE, Integer.MAX_VALUE));
        assertEquals(3L, integer("3.99", Integer.MIN_VALUE, Integer.MAX_VALUE));
        assertEquals(3L, integer("3.", Integer.MIN_VALUE, Integer.MAX_VALUE));
        assertEquals(Long.MIN_VALUE, integer("-9223372036854775808", Long.MIN_VALUE, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, integer("9223372036854775807", Long.MIN_VALUE, Long.MAX_VALUE));
        assertNull(integer("9223372036854775808", Long.MIN_VALUE, Long.MAX_VALUE));
        assertNull(integer("-9223372036854775809", Long.MIN_VALUE, Long.MAX_VALUE));
        assertNull(integer("128", Byte.MIN_VALUE, Byte.MAX_VALUE));
        assertEquals(-128L, integer("-128", Byte.MIN_VALUE, Byte.MAX_VALUE));
        for (String bad : new String[] {"", " ", "-", "+", "1e3", "0x10", "1 2", "3.a", ".5", "abc"}) {
            assertNull(integer(bad, Long.MIN_VALUE, Long.MAX_VALUE), bad);
        }
    }

    @Test
    void booleansFloatsAndDecimals() {
        assertEquals(1, TextValues.bool(b("TRUE"), 0, 4));
        assertEquals(0, TextValues.bool(b("false"), 0, 5));
        assertEquals(-1, TextValues.bool(b("1"), 0, 1));
        assertEquals(-1, TextValues.bool(b("yes"), 0, 3));
        assertEquals(1.5, TextValues.floating(b(" 1.5 "), 0, 5));
        assertEquals(Double.NaN, TextValues.floating(b("NaN"), 0, 3));
        assertEquals(-1e300, TextValues.floating(b("-1e300"), 0, 6));
        assertNull(TextValues.floating(b("1,5"), 0, 3));
        assertEquals(Float.POSITIVE_INFINITY, TextValues.single(b("1e300"), 0, 5));
        assertEquals(new BigDecimal("12.35"), decimal("12.345", 4, 2));
        assertEquals(new BigDecimal("-12.35"), decimal("-12.345", 4, 2));
        assertEquals(new BigDecimal("1000.00"), decimal("1e3", 6, 2));
        assertEquals(new BigDecimal("12.00"), decimal(" 12 ", 4, 2));
        assertNull(decimal("123.4", 4, 2));
        assertNull(decimal("99.995", 4, 2));
        assertNull(decimal("1e100000", 38, 0));
        assertNull(decimal("abc", 10, 2));
        assertNull(decimal("", 10, 2));
    }

    @Test
    void datesAndTimestampsKeepTheirWallClockAndImpossibleOnesAreNull() {
        assertEquals((int) LocalDate.of(2024, 2, 29).toEpochDay(), date("2024-02-29"));
        assertEquals((int) LocalDate.of(2024, 3, 5).toEpochDay(), date("2024-3-5"));
        assertEquals((int) LocalDate.of(2024, 3, 5).toEpochDay(), date(" 2024-03-05 10:00:00"));
        assertNull(date("2023-02-29"));
        assertNull(date("2024-13-01"));
        assertNull(date("20240101"));
        assertNull(date("2024-01-01T10:00"));
        assertEquals(micros(LocalDateTime.of(2024, 1, 2, 3, 4, 5), 123456),
                TextValues.timestamp(b("2024-01-02 03:04:05.123456789"), 0, 29));
        assertEquals(micros(LocalDateTime.of(2024, 1, 2, 3, 4, 5), 100000), timestamp("2024-01-02T03:04:05.1"));
        assertEquals(micros(LocalDateTime.of(2024, 1, 2, 3, 4, 0), 0), timestamp("2024-01-02 03:04"));
        assertEquals(micros(LocalDateTime.of(2024, 1, 2, 0, 0, 0), 0), timestamp("2024-01-02"));
        assertEquals(micros(LocalDateTime.of(1969, 12, 31, 23, 59, 59), 500000),
                timestamp("1969-12-31 23:59:59.5"));
        assertNull(timestamp("2024-01-02 25:00:00"));
        assertNull(timestamp("NULL"));
        assertNull(timestamp("1704164645"));
    }

    @Test
    void binariesAreDecodedWhenBase64AndTextIsUtf8() {
        assertArrayEquals(b("hello"), binary("aGVsbG8="));
        assertArrayEquals(b("hello"), binary("aGVsbG8"));
        assertArrayEquals(b("hi?"), binary("aGk_"));
        assertArrayEquals(b("hello!"), binary("hello!"));
        assertArrayEquals(new byte[0], binary(""));
        byte[] bad = {'a', (byte) 0xff, 'b'};
        assertArrayEquals("a�b".getBytes(StandardCharsets.UTF_8), TextValues.utf8(bad, 0, 3));
        assertArrayEquals(b("été"), TextValues.utf8(b("été"), 0, 5));
        assertArrayEquals(b("ét"), TextValues.truncate(b("été"), 2));
        assertArrayEquals(b("ab"), TextValues.truncate(b("ab"), 5));
    }

    private static Long integer(String text, long min, long max) {
        return TextValues.integer(b(text), 0, b(text).length, min, max);
    }

    private static BigDecimal decimal(String text, int precision, int scale) {
        return TextValues.decimal(b(text), 0, b(text).length, precision, scale);
    }

    private static Integer date(String text) {
        return TextValues.date(b(text), 0, b(text).length);
    }

    private static Long timestamp(String text) {
        return TextValues.timestamp(b(text), 0, b(text).length);
    }

    private static byte[] binary(String text) {
        return TextValues.binary(b(text), 0, b(text).length);
    }

    private static long micros(LocalDateTime time, long fraction) {
        return time.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + fraction;
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
