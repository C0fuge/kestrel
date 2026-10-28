package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    @Test
    void plainDecimalsEqualTheGeneralReader() {
        List<String> texts = new ArrayList<>(List.of("0", "-0", "+1", "1.", ".5", "-.5", ".", "-", "",
                "1.234", "12.30", "999.99", "1000.00", "-999.995", "00012.5", "1e3", " 1.5", "1.5 ",
                "1,5", "1.2.3", "123456789012345678", "1234567890123456789", "0.000000000000000001"));
        Random random = new Random(7);
        for (int i = 0; i < 20_000; i++) {
            StringBuilder text = new StringBuilder(random.nextInt(4) == 0 ? "-" : "");
            int whole = random.nextInt(12);
            for (int d = 0; d < whole; d++) {
                text.append((char) ('0' + random.nextInt(10)));
            }
            if (random.nextBoolean()) {
                text.append('.');
                for (int d = random.nextInt(5); d > 0; d--) {
                    text.append((char) ('0' + random.nextInt(10)));
                }
            }
            texts.add(text.toString());
        }
        int plain = 0;
        for (String text : texts) {
            byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
            for (int[] type : new int[][] {{15, 2}, {6, 2}, {18, 0}, {18, 18}, {4, 1}, {1, 0}}) {
                long fast = TextValues.plainDecimal(bytes, 0, bytes.length, type[0], type[1]);
                if (fast == TextValues.NOT_PLAIN) {
                    continue;
                }
                plain++;
                BigDecimal general = TextValues.decimal(bytes, 0, bytes.length, type[0], type[1]);
                assertEquals(general, BigDecimal.valueOf(fast, type[1]),
                        text + " as decimal(" + type[0] + "," + type[1] + ")");
            }
        }
        assertTrue(plain > 30_000, "plain reads: " + plain);
    }

    @Test
    void datesOfTenBytesEqualThePatternReader() {
        List<String> texts = new ArrayList<>(List.of("2024-02-29", "2023-02-29", "2024-13-01",
                "2024-00-10", "2024-01-00", "2024-01-32", "0000-01-01", "9999-12-31", "2024-1-011",
                "20240-1-01", "2024/01/01", "2024-01-0a", "+024-01-01", "2024-04-31"));
        Random random = new Random(11);
        for (int i = 0; i < 20_000; i++) {
            texts.add(String.format("%04d-%02d-%02d", random.nextInt(3000), random.nextInt(14),
                    random.nextInt(33)));
        }
        for (String text : texts) {
            byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
            assertEquals(patternDate(text), TextValues.date(bytes, 0, bytes.length), text);
        }
    }

    /** The date reader before its fast path, as reference. */
    private static Integer patternDate(String field) {
        String text = field.strip();
        int blank = text.indexOf(' ');
        Matcher m = Pattern.compile("(\\d{4})-(\\d{1,2})-(\\d{1,2})")
                .matcher(blank < 0 ? text : text.substring(0, blank));
        if (!m.matches()) {
            return null;
        }
        try {
            return (int) LocalDate.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)),
                    Integer.parseInt(m.group(3))).toEpochDay();
        } catch (DateTimeException e) {
            return null;
        }
    }
}
