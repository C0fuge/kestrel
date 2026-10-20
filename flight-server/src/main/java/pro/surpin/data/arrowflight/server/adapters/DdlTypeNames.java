package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Names Arrow column types for the informational DDL the server logs at startup. The names are
 * the ones Calcite's Arrow adapter gave before (a golden test keeps them): a SQL type name for
 * the types that have one, the Arrow type itself for the others (binary, UUID, interval, list,
 * struct, map), so that no column stops the server from starting.
 */
final class DdlTypeNames {

    /** Largest DECIMAL precision of the names; a larger precision is cut to it. */
    static final int MAX_DECIMAL_PRECISION = 19;

    /** Not instantiable. */
    private DdlTypeNames() {
    }

    /**
     * Names a column type.
     *
     * @param type Arrow type of the column
     * @return SQL type name, or the Arrow type name when there is none
     */
    static String of(ArrowType type) {
        String name = switch (type.getTypeID()) {
            case Int -> intName(((ArrowType.Int) type).getBitWidth());
            case Bool -> "BOOLEAN";
            case Utf8 -> "VARCHAR";
            case FloatingPoint -> floatName(((ArrowType.FloatingPoint) type).getPrecision());
            case Date -> "DATE";
            case Decimal -> decimalName((ArrowType.Decimal) type);
            case Time -> "TIME(0)";
            // Timestamps are named as their 64-bit integer values.
            case Timestamp -> "BIGINT";
            default -> null;
        };
        return name != null ? name : type.toString();
    }

    /**
     * Names an integer type by its width; signed and unsigned get the same name.
     *
     * @param bitWidth width in bits
     * @return type name, or null for an unusual width
     */
    private static String intName(int bitWidth) {
        return switch (bitWidth) {
            case 8 -> "TINYINT";
            case 16 -> "SMALLINT";
            case 32 -> "INTEGER";
            case 64 -> "BIGINT";
            default -> null;
        };
    }

    /**
     * Names a floating-point type.
     *
     * @param precision floating-point precision
     * @return type name, or null for half precision
     */
    private static String floatName(FloatingPointPrecision precision) {
        return switch (precision) {
            case SINGLE -> "REAL";
            case DOUBLE -> "DOUBLE";
            default -> null;
        };
    }

    /**
     * Names a decimal type, its precision cut to {@link #MAX_DECIMAL_PRECISION}.
     *
     * @param decimal decimal type
     * @return type name, or null for a precision below one or a negative scale
     */
    private static String decimalName(ArrowType.Decimal decimal) {
        if (decimal.getPrecision() < 1 || decimal.getScale() < 0) {
            return null;
        }
        return "DECIMAL(" + Math.min(decimal.getPrecision(), MAX_DECIMAL_PRECISION) + ", "
                + decimal.getScale() + ")";
    }
}
