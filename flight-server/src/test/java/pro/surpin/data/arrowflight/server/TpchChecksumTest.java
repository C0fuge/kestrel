package pro.surpin.data.arrowflight.server;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks fixed Python/Java canonical checksum vectors and representation boundaries. */
@Tag("unit")
class TpchChecksumTest {
    /** Verifies q12 integer and Arrow decimal answers share the fixed cross-language checksum. */
    @Test
    void q12IntegersAndDecimalsHaveIdenticalChecksums() {
        TpchChecksum integers = new TpchChecksum();
        integers.addRow("MAIL", 6202, 9324);
        integers.addRow("SHIP", 6200, 9262);
        TpchChecksum decimals = new TpchChecksum();
        decimals.addRow("MAIL", new BigDecimal("6202"), new BigDecimal("9324"));
        decimals.addRow("SHIP", new BigDecimal("6200"), new BigDecimal("9262"));
        assertEquals("66191bf0fe853f3d", hex(integers.value()));
        assertEquals(integers.value(), decimals.value());
        assertEquals("num:9007199254740993.00", TpchChecksum.token(9007199254740993L));
    }

    /** Verifies exact 128-bit boundaries and binary floating-point half-even rounding. */
    @Test
    void hugeIntegersAndRoundingMatchTheFixedVector() {
        BigInteger boundary = BigInteger.ONE.shiftLeft(127);
        TpchChecksum checksum = new TpchChecksum();
        checksum.addRow(boundary, boundary.negate(), 9007199254740993L,
                new BigDecimal("1.005"), new BigDecimal("1.015"), 1.005d, 1.015d,
                new BigDecimal("123456789012345678901234567890.125"));
        assertEquals("ce8996ef5eccd4b9", hex(checksum.value()));
        assertEquals("num:170141183460469231731687303715884105728.00", TpchChecksum.token(boundary));
        assertEquals("num:-170141183460469231731687303715884105728.00", TpchChecksum.token(boundary.negate()));
        assertEquals("num:1.02", TpchChecksum.token(new BigDecimal("1.015")));
        assertEquals("num:1.01", TpchChecksum.token(1.015d));
        assertEquals("num:2.67", TpchChecksum.token(2.675d));
        assertEquals("num:2.68", TpchChecksum.token(new BigDecimal("2.675")));
        assertEquals("num:-1.02", TpchChecksum.token(new BigDecimal("-1.015")));
        assertEquals("num:-1.01", TpchChecksum.token(-1.015d));
        assertEquals(TpchChecksum.token((double) 1.015f), TpchChecksum.token(1.015f));
    }

    /** Verifies null, booleans, non-finite values and signed zeros have deterministic distinct tokens. */
    @Test
    void specialValuesMatchTheFixedVector() {
        TpchChecksum checksum = new TpchChecksum();
        checksum.addRow(null, "None", true, false, 0, -0.0d, new BigDecimal("-0.00"),
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        assertEquals("93494177e701e149", hex(checksum.value()));
        for (Object zero : List.of(0, 0L, BigInteger.ZERO, -0.0d, -0.0f,
                Double.MIN_VALUE, -Double.MIN_VALUE, new BigDecimal("-0.0049"))) {
            assertEquals("num:0.00", TpchChecksum.token(zero));
        }
        assertEquals("num:nan", TpchChecksum.token(Double.longBitsToDouble(0xfff8000000000001L)));
        assertEquals("num:nan", TpchChecksum.token(Float.NaN));
        assertEquals("num:+inf", TpchChecksum.token(Float.POSITIVE_INFINITY));
        assertEquals("num:-inf", TpchChecksum.token(Float.NEGATIVE_INFINITY));
        assertNotEquals(TpchChecksum.token(null), TpchChecksum.token("null"));
        assertNotEquals(TpchChecksum.token(true), TpchChecksum.token(1));
    }

    /** Verifies Unicode, delimiters, dates, empty strings and null use the agreed byte framing. */
    @Test
    void unicodeAndFramingMatchTheFixedVectors() {
        TpchChecksum unicode = new TpchChecksum();
        unicode.addRow("a|b", "a:b", "é", "🙂", LocalDate.of(1994, 1, 1), "", null);
        assertEquals("a2dbafa4baf6ca42", hex(unicode.value()));
        Object[] row = {1, "a|b", null, "é"};
        assertEquals("383a6e756d3a312e3030383a746578743a617c62343a6e756c6c373a746578743ac3a9",
                HexFormat.of().formatHex(TpchChecksum.encodedRow(row)));
        TpchChecksum framed = new TpchChecksum();
        framed.addRow(row);
        assertEquals("c7055ca8781d8861", hex(framed.value()));
        assertNotEquals(HexFormat.of().formatHex(TpchChecksum.encodedRow("a|b", "c")),
                HexFormat.of().formatHex(TpchChecksum.encodedRow("a", "b|c")));
    }

    /** Verifies null and numeric sentinels cannot collide with strings containing the same text. */
    @Test
    void sentinelTypesMatchTheFixedVector() {
        Object[] row = {null, "null", "<null>", 1, "1.00"};
        TpchChecksum checksum = new TpchChecksum();
        checksum.addRow(row);
        assertEquals("70f6a9588ea18f67", hex(checksum.value()));
        assertEquals("343a6e756c6c393a746578743a6e756c6c31313a746578743a3c6e756c6c3e"
                        + "383a6e756d3a312e3030393a746578743a312e3030",
                HexFormat.of().formatHex(TpchChecksum.encodedRow(row)));
        assertNotEquals(TpchChecksum.token(null), TpchChecksum.token("<null>"));
        assertNotEquals(TpchChecksum.token(1), TpchChecksum.token("1.00"));
        assertEquals("tpch-v2-sha256-64-numeric2", TpchChecksum.VERSION);
    }

    /** Verifies empty answers, row order independence and modulo-64-bit duplicate-row addition. */
    @Test
    void answerAccumulationPreservesOrderIndependenceAndOverflow() {
        assertEquals(0, new TpchChecksum().value());
        TpchChecksum reversed = new TpchChecksum();
        reversed.addRow("SHIP", 6200, 9262);
        reversed.addRow("MAIL", 6202, 9324);
        assertEquals("66191bf0fe853f3d", hex(reversed.value()));
        TpchChecksum duplicate = new TpchChecksum();
        duplicate.addRow(1, "a|b", null, "é");
        duplicate.addRow(1, "a|b", null, "é");
        assertEquals("8e0ab950f03b10c2", hex(duplicate.value()));
    }

    /** Verifies unsupported scalar types and malformed Unicode cannot silently acquire guessed encodings. */
    @Test
    void unsupportedScalarsAndMalformedUnicodeAreRejected() {
        for (Object value : List.of(new byte[] {1}, Instant.EPOCH, LocalDateTime.of(1994, 1, 1, 0, 0), new Object())) {
            assertThrows(IllegalArgumentException.class, () -> TpchChecksum.token(value));
        }
        assertThrows(IllegalArgumentException.class, () -> TpchChecksum.encodedRow("\ud800"));
        assertThrows(IllegalArgumentException.class, () -> TpchChecksum.encodedRow("\udc00"));
    }

    /**
     * Formats an unsigned wrapped checksum in the stable printed representation.
     *
     * @param checksum checksum bits
     * @return sixteen lowercase hexadecimal digits
     */
    private static String hex(long checksum) {
        return String.format(Locale.ROOT, "%016x", checksum);
    }
}
