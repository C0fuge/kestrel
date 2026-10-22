package pro.surpin.data.arrowflight.plugin.iceberg;

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Names Iceberg types as DuckDB types, the way DuckDB's iceberg extension shows them (checked
 * against {@code DESCRIBE SELECT * FROM iceberg_scan(...)} of DuckDB 1.4.5).
 */
final class DuckDbTypes {

    private DuckDbTypes() {
    }

    /**
     * Names the DuckDB type of an Iceberg type.
     *
     * @param type Iceberg type
     * @return DuckDB type name, or null when DuckDB's Parquet reader has no matching type
     */
    static String of(Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> "BOOLEAN";
            case INTEGER -> "INTEGER";
            case LONG -> "BIGINT";
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case DECIMAL -> {
                Types.DecimalType decimal = (Types.DecimalType) type;
                yield "DECIMAL(" + decimal.precision() + "," + decimal.scale() + ")";
            }
            case DATE -> "DATE";
            case TIME -> "TIME";
            case TIMESTAMP -> ((Types.TimestampType) type).shouldAdjustToUTC()
                    ? "TIMESTAMP WITH TIME ZONE" : "TIMESTAMP";
            case STRING -> "VARCHAR";
            case UUID -> "UUID";
            case FIXED, BINARY -> "BLOB";
            case STRUCT -> struct((Types.StructType) type);
            case LIST -> {
                String element = of(((Types.ListType) type).elementType());
                yield element == null ? null : element + "[]";
            }
            case MAP -> {
                Types.MapType map = (Types.MapType) type;
                String key = of(map.keyType());
                String value = of(map.valueType());
                yield key == null || value == null ? null : "MAP(" + key + ", " + value + ")";
            }
            default -> null;
        };
    }

    /**
     * Names a struct type with quoted field names.
     *
     * @param struct struct type
     * @return DuckDB struct type, or null when a field has no DuckDB type
     */
    private static String struct(Types.StructType struct) {
        List<String> fields = new ArrayList<>();
        for (Types.NestedField field : struct.fields()) {
            String type = of(field.type());
            if (type == null) {
                return null;
            }
            fields.add("\"" + field.name().replace("\"", "\"\"") + "\" " + type);
        }
        return "STRUCT(" + String.join(", ", fields) + ")";
    }
}
