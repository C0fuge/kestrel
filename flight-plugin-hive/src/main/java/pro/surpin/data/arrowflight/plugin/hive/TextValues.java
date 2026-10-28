package pro.surpin.data.arrowflight.plugin.hive;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the fields of Hive text files as Hive's lazy objects ({@code LazyInteger},
 * {@code LazyHiveDecimal}, ...) do: a field that does not read as its type is NULL, never an
 * error. Integers allow surrounding blanks, a sign and a fraction (dropped); out of range is
 * NULL. Booleans are {@code true} or {@code false} in any case. Floats and doubles read as Java
 * reads them. Decimals are rounded half up to the column's scale and NULL when they then have
 * more digits than its precision. Dates are {@code yyyy-M-d}, timestamps
 * {@code yyyy-M-d[( |T)H:m[:s[.fraction]]]} (microseconds kept), anything after a date's first
 * blank is ignored; impossible days are NULL. Binaries are decoded from Base64 when every byte is
 * Base64, else kept. Text is UTF-8 (malformed bytes become U+FFFD, as Hive's text reads them).
 */
final class TextValues {

    private static final Pattern DATE = Pattern.compile("(\\d{4})-(\\d{1,2})-(\\d{1,2})");
    private static final Pattern TIMESTAMP = Pattern.compile("(\\d{4})-(\\d{1,2})-(\\d{1,2})"
            + "(?:[ T](\\d{1,2}):(\\d{1,2})(?::(\\d{1,2})(?:\\.(\\d{1,9}))?)?)?");
    private static final long MICROS_PER_SECOND = 1_000_000L;
    /** Returned by {@link #plainDecimal} for text it leaves to {@link #decimal}. */
    static final long NOT_PLAIN = Long.MIN_VALUE;
    private static final long[] POWERS_OF_TEN = new long[19];

    static {
        POWERS_OF_TEN[0] = 1L;
        for (int i = 1; i < POWERS_OF_TEN.length; i++) {
            POWERS_OF_TEN[i] = POWERS_OF_TEN[i - 1] * 10;
        }
    }

    private TextValues() {
    }

    /**
     * Reads a whole number.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @param min smallest value of the type
     * @param max largest value of the type
     * @return the value, or null
     */
    static Long integer(byte[] bytes, int start, int end, long min, long max) {
        while (start < end && blank(bytes[start])) {
            start++;
        }
        while (end > start && blank(bytes[end - 1])) {
            end--;
        }
        if (start == end) {
            return null;
        }
        boolean negative = bytes[start] == '-';
        if (negative || bytes[start] == '+') {
            start++;
        }
        int digitsEnd = start;
        while (digitsEnd < end && bytes[digitsEnd] >= '0' && bytes[digitsEnd] <= '9') {
            digitsEnd++;
        }
        if (digitsEnd == start) {
            return null;
        }
        if (digitsEnd < end) {
            if (bytes[digitsEnd] != '.') {
                return null;
            }
            for (int i = digitsEnd + 1; i < end; i++) {
                if (bytes[i] < '0' || bytes[i] > '9') {
                    return null;
                }
            }
        }
        // Accumulated negative, which holds Long.MIN_VALUE.
        long value = 0;
        for (int i = start; i < digitsEnd; i++) {
            int digit = bytes[i] - '0';
            if (value < (Long.MIN_VALUE + digit) / 10) {
                return null;
            }
            value = value * 10 - digit;
        }
        if (!negative) {
            if (value == Long.MIN_VALUE) {
                return null;
            }
            value = -value;
        }
        return value < min || value > max ? null : value;
    }

    /**
     * Reads a boolean.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @return 1 for true, 0 for false, -1 when it is neither (NULL)
     */
    static int bool(byte[] bytes, int start, int end) {
        String text = new String(bytes, start, end - start, StandardCharsets.ISO_8859_1);
        if ("true".equalsIgnoreCase(text)) {
            return 1;
        }
        return "false".equalsIgnoreCase(text) ? 0 : -1;
    }

    /**
     * Reads a double.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @return the value, or null
     */
    static Double floating(byte[] bytes, int start, int end) {
        try {
            return Double.parseDouble(new String(bytes, start, end - start, StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Reads a float.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @return the value, or null
     */
    static Float single(byte[] bytes, int start, int end) {
        try {
            return Float.parseFloat(new String(bytes, start, end - start, StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Reads a decimal of a column's precision and scale.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @param precision column precision
     * @param scale column scale
     * @return the value at the column's scale, or null
     */
    static BigDecimal decimal(byte[] bytes, int start, int end, int precision, int scale) {
        String text = new String(bytes, start, end - start, StandardCharsets.ISO_8859_1).strip();
        if (text.isEmpty() || text.length() > 400) {
            return null;
        }
        try {
            BigDecimal value = new BigDecimal(text);
            if (value.precision() - value.scale() > 400) {
                return null;
            }
            value = value.setScale(scale, RoundingMode.HALF_UP);
            return value.precision() > precision ? null : value;
        } catch (NumberFormatException | ArithmeticException e) {
            return null;
        }
    }

    /**
     * Reads a plain decimal ({@code [-+]digits[.digits]}, at most 18 digits, no more fraction
     * digits than the scale) as its unscaled value at a column's scale, without objects; any
     * other text is left to {@link #decimal}.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @param precision column precision, at most 18
     * @param scale column scale
     * @return the unscaled value, or {@link #NOT_PLAIN} when {@link #decimal} must read it
     */
    static long plainDecimal(byte[] bytes, int start, int end, int precision, int scale) {
        int i = start;
        boolean negative = false;
        if (i < end && (bytes[i] == '-' || bytes[i] == '+')) {
            negative = bytes[i] == '-';
            i++;
        }
        long unscaled = 0;
        int digits = 0;
        int fraction = -1;
        for (; i < end; i++) {
            byte b = bytes[i];
            if (b >= '0' && b <= '9') {
                if (++digits > 18) {
                    return NOT_PLAIN;
                }
                unscaled = unscaled * 10 + (b - '0');
                if (fraction >= 0) {
                    fraction++;
                }
            } else if (b == '.' && fraction < 0) {
                fraction = 0;
            } else {
                return NOT_PLAIN;
            }
        }
        int fractionDigits = Math.max(fraction, 0);
        if (digits == 0 || fractionDigits > scale || digits - fractionDigits + scale > 18) {
            return NOT_PLAIN;
        }
        for (int k = fractionDigits; k < scale; k++) {
            unscaled *= 10;
        }
        if (unscaled >= POWERS_OF_TEN[precision]) {
            return NOT_PLAIN;
        }
        return negative ? -unscaled : unscaled;
    }

    /**
     * Reads ASCII digits.
     *
     * @param bytes buffer
     * @param start first byte
     * @param count digits
     * @return their value, or -1 when a byte is not a digit
     */
    private static int digits(byte[] bytes, int start, int count) {
        int value = 0;
        for (int i = start; i < start + count; i++) {
            if (bytes[i] < '0' || bytes[i] > '9') {
                return -1;
            }
            value = value * 10 + (bytes[i] - '0');
        }
        return value;
    }

    /**
     * Reads a date.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @return days since 1970-01-01, or null
     */
    static Integer date(byte[] bytes, int start, int end) {
        if (end - start == 10 && bytes[start + 4] == '-' && bytes[start + 7] == '-') {
            int year = digits(bytes, start, 4);
            int month = digits(bytes, start + 5, 2);
            int day = digits(bytes, start + 8, 2);
            if (year >= 0 && month >= 0 && day >= 0) {
                try {
                    return (int) LocalDate.of(year, month, day).toEpochDay();
                } catch (DateTimeException e) {
                    return null;
                }
            }
        }
        String text = new String(bytes, start, end - start, StandardCharsets.ISO_8859_1).strip();
        int blank = text.indexOf(' ');
        Matcher m = DATE.matcher(blank < 0 ? text : text.substring(0, blank));
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

    /**
     * Reads a timestamp as its wall clock.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @return microseconds since 1970-01-01 00:00 of the wall clock, or null
     */
    static Long timestamp(byte[] bytes, int start, int end) {
        Matcher m = TIMESTAMP.matcher(
                new String(bytes, start, end - start, StandardCharsets.ISO_8859_1).strip());
        if (!m.matches()) {
            return null;
        }
        try {
            LocalDateTime time = LocalDateTime.of(Integer.parseInt(m.group(1)),
                    Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)),
                    m.group(4) == null ? 0 : Integer.parseInt(m.group(4)),
                    m.group(5) == null ? 0 : Integer.parseInt(m.group(5)),
                    m.group(6) == null ? 0 : Integer.parseInt(m.group(6)));
            long micros = 0;
            if (m.group(7) != null) {
                String fraction = (m.group(7) + "000000").substring(0, 6);
                micros = Long.parseLong(fraction);
            }
            return time.toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND + micros;
        } catch (DateTimeException e) {
            return null;
        }
    }

    /**
     * Reads a binary: Base64 decoded when every byte is Base64 (or a blank), else as is.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @return the bytes
     */
    static byte[] binary(byte[] bytes, int start, int end) {
        StringBuilder base64 = new StringBuilder(end - start);
        boolean padded = false;
        for (int i = start; i < end; i++) {
            byte b = bytes[i];
            if (b == '=') {
                padded = true;
            } else if (blank(b) || b == '\n' || b == '\r') {
                continue;
            } else if ((b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
                    || b == '+' || b == '/' || b == '-' || b == '_') {
                if (!padded) {
                    base64.append((char) (b == '-' ? '+' : b == '_' ? '/' : b));
                }
            } else {
                byte[] raw = new byte[end - start];
                System.arraycopy(bytes, start, raw, 0, raw.length);
                return raw;
            }
        }
        // Leftover bits of an incomplete last group are dropped, as lenient decoders do.
        int length = base64.length() % 4 == 1 ? base64.length() - 1 : base64.length();
        return Base64.getDecoder().decode(base64.substring(0, length));
    }

    /**
     * Decodes UTF-8 text, replacing malformed bytes.
     *
     * @param bytes buffer
     * @param start first byte
     * @param end end of the field
     * @return the bytes when they are valid UTF-8, else the text re-encoded with U+FFFD
     */
    static byte[] utf8(byte[] bytes, int start, int end) {
        int i = start;
        while (i < end && bytes[i] >= 0) {
            i++;
        }
        if (i == end) {
            // ASCII is valid UTF-8 as it is.
            return Arrays.copyOfRange(bytes, start, end);
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            decoder.decode(ByteBuffer.wrap(bytes, start, end - start));
            byte[] copy = new byte[end - start];
            System.arraycopy(bytes, start, copy, 0, copy.length);
            return copy;
        } catch (CharacterCodingException e) {
            return new String(bytes, start, end - start, StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Cuts UTF-8 text to at most a number of characters (code points).
     *
     * @param utf8 valid UTF-8
     * @param characters most characters
     * @return the text, cut
     */
    static byte[] truncate(byte[] utf8, int characters) {
        int count = 0;
        for (int i = 0; i < utf8.length; i++) {
            if ((utf8[i] & 0xC0) != 0x80) {
                if (count == characters) {
                    byte[] cut = new byte[i];
                    System.arraycopy(utf8, 0, cut, 0, i);
                    return cut;
                }
                count++;
            }
        }
        return utf8;
    }

    private static boolean blank(byte b) {
        return b == ' ' || b == '\t';
    }
}
