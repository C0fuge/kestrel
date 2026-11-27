package pro.surpin.data.arrowflight.server;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;

/** Computes versioned, order-independent TPC-H checksums from exact canonical scalar tokens. */
final class TpchChecksum {
    static final String VERSION = "tpch-v2-sha256-64-numeric2";
    private final MessageDigest digest;
    private long sum;

    /** Creates an empty answer checksum using the mandatory SHA-256 implementation. */
    TpchChecksum() {
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Adds the first eight big-endian SHA-256 row bytes with unsigned 64-bit wraparound.
     *
     * @param values row cells in column order
     * @throws IllegalArgumentException when a scalar or text encoding is unsupported
     */
    void addRow(Object... values) {
        sum += ByteBuffer.wrap(digest.digest(encodedRow(values))).getLong();
    }

    /**
     * Returns the accumulated answer, interpreted as an unsigned 64-bit value when formatted.
     *
     * @return wrapped checksum, zero for an empty answer
     */
    long value() {
        return sum;
    }

    /**
     * Encodes each token as its ASCII UTF-8 byte length, a colon and the exact token bytes.
     *
     * @param values row cells in column order
     * @return concatenated row bytes
     * @throws IllegalArgumentException when a scalar or text encoding is unsupported
     */
    static byte[] encodedRow(Object... values) {
        ByteArrayOutputStream row = new ByteArrayOutputStream();
        for (Object value : values) {
            byte[] bytes = utf8(token(value));
            row.writeBytes(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
            row.write(':');
            row.writeBytes(bytes);
        }
        return row.toByteArray();
    }

    /**
     * Normalizes supported scalar values without converting integers or decimals through double.
     *
     * @param value scalar cell, or null
     * @return versioned checksum token
     * @throws IllegalArgumentException when the scalar type is unsupported
     */
    static String token(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Boolean bool) {
            return "bool:" + bool;
        }
        BigDecimal number;
        if (value instanceof BigDecimal decimal) {
            number = decimal;
        } else if (value instanceof BigInteger integer) {
            number = new BigDecimal(integer);
        } else if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            number = BigDecimal.valueOf(((Number) value).longValue());
        } else if (value instanceof Float || value instanceof Double) {
            double binary = ((Number) value).doubleValue();
            if (Double.isNaN(binary)) {
                return "num:nan";
            }
            if (Double.isInfinite(binary)) {
                return binary > 0 ? "num:+inf" : "num:-inf";
            }
            number = new BigDecimal(binary);
        } else if (value instanceof CharSequence || value instanceof LocalDate) {
            return "text:" + value;
        } else {
            throw new IllegalArgumentException("Unsupported checksum scalar: " + value.getClass().getName());
        }
        BigDecimal rounded = number.setScale(2, RoundingMode.HALF_EVEN);
        return "num:" + (rounded.signum() == 0 ? "0.00" : rounded.toPlainString());
    }

    /**
     * Encodes valid Unicode without silently replacing malformed UTF-16 surrogate sequences.
     *
     * @param token canonical cell token
     * @return exact UTF-8 bytes
     * @throws IllegalArgumentException when the text is malformed
     */
    private static byte[] utf8(String token) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(token));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Checksum text must be valid Unicode", e);
        }
    }
}
