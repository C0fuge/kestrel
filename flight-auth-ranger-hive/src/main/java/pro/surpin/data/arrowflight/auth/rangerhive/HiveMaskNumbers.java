package pro.surpin.data.arrowflight.auth.rangerhive;

/** Hive 3.1.3's integer masks in Java, with the overflow of each integer width. */
final class HiveMaskNumbers {
    private HiveMaskNumbers() {
    }

    /**
     * Masks one integer as Hive's MaskTransformer, MaskShowFirstNTransformer or MaskShowLastNTransformer does.
     *
     * @param value the value (within the width)
     * @param bits 8, 16, 32 or 64
     * @param charCount shown digits, or -1 for a plain mask
     * @param first true to show the first digits, false the last
     * @param number replacement digit
     * @return the masked value
     */
    static long mask(long value, int bits, int charCount, boolean first, int number) {
        long val = value < 0 ? narrow(-value, bits) : value;
        if (first && charCount >= 0) {
            int digitCount = 0;
            for (long v = val; v != 0; v /= 10) {
                digitCount++;
            }
            if (digitCount - charCount <= 0) {
                return value;
            }
            charCount = digitCount - charCount;
        }
        long ret = 0;
        long pos = 1;
        for (int i = 0; val != 0; i++) {
            boolean masked = charCount < 0 || (first ? i < charCount : i >= charCount);
            ret = narrow(ret + (masked ? number * pos : (val % 10) * pos), bits);
            val /= 10;
            pos = narrow(pos * 10, bits == 64 ? 64 : 32);
        }
        return value < 0 ? narrow(-ret, bits) : ret;
    }

    /**
     * Wraps a value to a signed integer width, as a Java narrowing conversion does.
     *
     * @param value value
     * @param bits width
     * @return wrapped value
     */
    private static long narrow(long value, int bits) {
        return switch (bits) {
            case 8 -> (byte) value;
            case 16 -> (short) value;
            case 32 -> (int) value;
            default -> value;
        };
    }
}
