package pro.surpin.data.arrowflight.plugin.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.orc.TypeDescription;

/**
 * The reader schema of a Hive Avro table ({@code AvroSerDe}), found as Hive 3.1.3 finds it: the
 * {@code avro.schema.literal} property (table properties override serde properties), else a
 * schema made from the declared columns as Hive's {@code TypeInfoToSchema} makes it (every field
 * a union with null, default null). An {@code avro.schema.url} (when no literal is set) is read
 * through a {@link SchemaReader}: the catalog reads files under its roots only. Hive takes a
 * table's columns from that schema ({@code SchemaToTypeInfo}); the catalog shows a table only
 * when they are the metastore's columns, so a stale metastore never misnames or mistypes values.
 *
 * @param schemaJson the reader schema as JSON
 * @param tableLevel whether a table property names the schema (partitions then read with it)
 */
record AvroFormat(String schemaJson, boolean tableLevel) {

    /** Property of the schema text. */
    static final String LITERAL = "avro.schema.literal";
    /** Property of the schema location. */
    static final String URL = "avro.schema.url";
    /** Hive's value for an unset schema property. */
    private static final String NONE = "none";
    private static final int MAX_PRECISION = 38;

    /** Refuses every schema location (no store to read them from). */
    static final SchemaReader NO_URLS = url -> {
        throw new IllegalArgumentException(URL + " is not read here (set " + LITERAL + ")");
    };

    /** Reads the schema text an {@code avro.schema.url} names. */
    @FunctionalInterface
    interface SchemaReader {

        /**
         * Reads a schema file.
         *
         * @param url the property's value
         * @return the schema text
         * @throws IllegalArgumentException when it cannot or may not be read; the message says
         *     why
         */
        String read(String url);
    }

    /**
     * Finds a table's reader schema and checks it against the declared columns.
     *
     * @param serde serde properties
     * @param table table properties
     * @param columns declared data columns
     * @return the format
     * @throws IllegalArgumentException when Hive would not read the schema, the plugin does not
     *     read it yet or its columns are not the declared ones
     */
    static AvroFormat of(Map<String, String> serde, Map<String, String> table,
            List<FieldSchema> columns) {
        return of(serde, table, columns, NO_URLS);
    }

    /**
     * Finds a table's reader schema, reading an {@code avro.schema.url} with a reader, and
     * checks it against the declared columns.
     *
     * @param serde serde properties
     * @param table table properties
     * @param columns declared data columns
     * @param urls reads schema locations
     * @return the format
     * @throws IllegalArgumentException when Hive would not read the schema, the reader cannot
     *     read it or its columns are not the declared ones
     */
    static AvroFormat of(Map<String, String> serde, Map<String, String> table,
            List<FieldSchema> columns, SchemaReader urls) {
        List<FieldSchema> declared = columns == null ? List.of() : columns;
        Map<String, String> props = new HashMap<>(serde == null ? Map.of() : serde);
        if (table != null) {
            props.putAll(table);
        }
        boolean tableLevel = table != null && (set(table.get(LITERAL)) || table.containsKey(URL));
        Schema schema;
        if (set(props.get(LITERAL))) {
            schema = parse(props.get(LITERAL));
        } else if (props.containsKey(URL)) {
            String url = props.get(URL);
            if (NONE.equals(url)) {
                throw new IllegalArgumentException("Avro table without a schema (" + URL + "=none)");
            }
            schema = parse(urls.read(url), URL + " " + url);
        } else {
            schema = fromColumns(declared);
        }
        check(schema, declared);
        return new AvroFormat(schema.toString(), tableLevel);
    }

    /**
     * Finds the reader schema of a partition with its own serde properties and columns.
     *
     * @param serde the partition's serde properties
     * @param columns the partition's data columns
     * @param urls reads schema locations
     * @return the partition's format; this format when a table property names the schema
     * @throws IllegalArgumentException when the partition's schema is not read
     */
    AvroFormat forPartition(Map<String, String> serde, List<FieldSchema> columns,
            SchemaReader urls) {
        if (tableLevel) {
            return this;
        }
        AvroFormat partition = of(serde, Map.of(), columns, urls);
        return new AvroFormat(partition.schemaJson(), false);
    }

    /**
     * Parses the reader schema.
     *
     * @return the schema
     */
    Schema schema() {
        return parse(schemaJson);
    }

    /**
     * Parses a schema as Hive does (defaults not validated).
     *
     * @param json schema text
     * @return the schema
     * @throws IllegalArgumentException when it does not parse
     */
    static Schema parse(String json) {
        return parse(json, LITERAL);
    }

    private static Schema parse(String json, String source) {
        try {
            return new Schema.Parser().setValidateDefaults(false).parse(json);
        } catch (AvroRuntimeException e) {
            throw new IllegalArgumentException("unreadable " + source + ": " + e.getMessage(), e);
        }
    }

    private static boolean set(String value) {
        return value != null && !NONE.equals(value);
    }

    /**
     * Checks that a schema's columns are the declared ones, by position, name (any case) and
     * Hive type.
     *
     * @param schema reader schema
     * @param columns declared data columns
     * @throws IllegalArgumentException when they differ
     */
    static void check(Schema schema, List<FieldSchema> columns) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Avro schema is a " + schema.getType()
                    + ", not a record");
        }
        List<Schema.Field> fields = schema.getFields();
        if (fields.size() != columns.size()) {
            throw new IllegalArgumentException("Avro schema has " + fields.size()
                    + " fields, the metastore " + columns.size() + " columns");
        }
        for (int i = 0; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);
            FieldSchema column = columns.get(i);
            String type = hiveType(field.schema());
            if (!field.name().equalsIgnoreCase(column.getName())
                    || !normalized(type).equals(normalized(column.getType()))) {
                throw new IllegalArgumentException("Avro schema field " + (i + 1) + " is "
                        + field.name() + " " + type + ", the metastore column " + column.getName()
                        + " " + column.getType());
            }
        }
    }

    private static String normalized(String hiveType) {
        try {
            return TypeDescription.fromString(hiveType).toString().toLowerCase(Locale.ROOT);
        } catch (IllegalArgumentException e) {
            return hiveType.toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Returns the Hive type of an Avro schema, as Hive's {@code SchemaToTypeInfo}: a union with
     * null is its other type, records structs, enums strings, fixed binary, bytes binary or
     * {@code decimal} (logical type), int {@code date}, long {@code timestamp-millis}, string
     * {@code char} / {@code varchar} (logical types with maxLength), maps
     * {@code map<string,...>}.
     *
     * @param schema Avro schema
     * @return Hive type text
     * @throws IllegalArgumentException for other unions, null, recursive records and bad
     *     decimal or length properties
     */
    static String hiveType(Schema schema) {
        return hiveType(schema, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static String hiveType(Schema schema, Set<Schema> seen) {
        String logical = schema.getProp("logicalType");
        switch (schema.getType()) {
            case BYTES -> {
                if ("decimal".equalsIgnoreCase(logical)) {
                    int precision = schema.getObjectProp("precision") instanceof Integer p ? p : 0;
                    int scale = schema.getObjectProp("scale") instanceof Integer s ? s : 0;
                    if (precision < 1 || precision > MAX_PRECISION || scale < 0 || scale > precision) {
                        throw new IllegalArgumentException("Invalid precision or scale for decimal"
                                + " type: " + schema);
                    }
                    return "decimal(" + precision + "," + scale + ")";
                }
                return "binary";
            }
            case STRING -> {
                if ("char".equalsIgnoreCase(logical) || "varchar".equalsIgnoreCase(logical)) {
                    return logical.toLowerCase(Locale.ROOT) + "(" + maxLength(schema) + ")";
                }
                return "string";
            }
            case INT -> {
                return "date".equals(logical) ? "date" : "int";
            }
            case LONG -> {
                return "timestamp-millis".equals(logical) ? "timestamp" : "bigint";
            }
            case BOOLEAN -> {
                return "boolean";
            }
            case FLOAT -> {
                return "float";
            }
            case DOUBLE -> {
                return "double";
            }
            case FIXED -> {
                return "binary";
            }
            case ENUM -> {
                return "string";
            }
            case ARRAY -> {
                return "array<" + hiveType(schema.getElementType(), seen) + ">";
            }
            case MAP -> {
                return "map<string," + hiveType(schema.getValueType(), seen) + ">";
            }
            case RECORD -> {
                if (!seen.add(schema)) {
                    throw new IllegalArgumentException("Recursive schemas are not supported: "
                            + schema.getFullName());
                }
                List<String> fields = new ArrayList<>();
                for (Schema.Field field : schema.getFields()) {
                    fields.add(field.name() + ":" + hiveType(field.schema(), seen));
                }
                // A record used again outside itself is no recursion (Hive caches its type).
                seen.remove(schema);
                return "struct<" + String.join(",", fields) + ">";
            }
            case UNION -> {
                Schema other = nullable(schema);
                if (other == null) {
                    throw new IllegalArgumentException("Avro union " + schema
                            + " (Hive uniontype) is not read yet");
                }
                return hiveType(other, seen);
            }
            default -> throw new IllegalArgumentException("Avro type " + schema.getType()
                    + " has no Hive column type");
        }
    }

    /**
     * Returns the other type of a union of null and one type.
     *
     * @param schema a schema
     * @return the other type, or null when the schema is not such a union
     */
    static Schema nullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION || schema.getTypes().size() != 2) {
            return null;
        }
        Schema first = schema.getTypes().get(0);
        Schema second = schema.getTypes().get(1);
        if (first.getType() == Schema.Type.NULL && second.getType() != Schema.Type.NULL) {
            return second;
        }
        return second.getType() == Schema.Type.NULL && first.getType() != Schema.Type.NULL
                ? first : null;
    }

    /**
     * Returns a schema without its union with null.
     *
     * @param schema a schema, or null
     * @return its other type when it is a union with null, else the schema
     */
    static Schema unwrapped(Schema schema) {
        Schema other = schema == null ? null : nullable(schema);
        return other == null ? schema : other;
    }

    /**
     * Reads an integer property as Hive's {@code AvroSerdeUtils.getIntFromSchema}.
     *
     * @param schema schema
     * @param name property
     * @return its value
     * @throws IllegalArgumentException when it is missing or not a number
     */
    static int intProp(Schema schema, String name) {
        Object value = schema.getObjectProp(name);
        if (value instanceof Integer i) {
            return i;
        }
        if (value instanceof String s) {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("property " + name + " of " + schema
                        + " is not a number", e);
            }
        }
        throw new IllegalArgumentException("property " + name + " of " + schema
                + " is missing or not a number");
    }

    private static int maxLength(Schema schema) {
        return intProp(schema, "maxLength");
    }

    /**
     * Makes the schema Hive makes from declared columns ({@code TypeInfoToSchema}).
     *
     * @param columns declared data columns
     * @return record schema
     * @throws IllegalArgumentException for types Hive cannot write to Avro
     */
    static Schema fromColumns(List<FieldSchema> columns) {
        int[] records = {0};
        List<Schema.Field> fields = new ArrayList<>();
        for (FieldSchema column : columns) {
            TypeDescription type;
            try {
                type = TypeDescription.fromString(column.getType());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("column " + column.getName() + " has type "
                        + column.getType() + ": " + e.getMessage(), e);
            }
            fields.add(new Schema.Field(column.getName(), avro(type, records), column.getComment(),
                    JsonProperties.NULL_VALUE));
        }
        Schema schema = Schema.createRecord("baseRecord", null, null, false);
        schema.setFields(fields);
        return schema;
    }

    private static Schema avro(TypeDescription type, int[] records) {
        Schema schema = switch (type.getCategory()) {
            case STRING -> Schema.create(Schema.Type.STRING);
            case CHAR, VARCHAR -> parse("{\"type\":\"string\",\"logicalType\":\""
                    + type.getCategory().getName() + "\",\"maxLength\":" + type.getMaxLength() + "}");
            case BINARY -> Schema.create(Schema.Type.BYTES);
            case BYTE, SHORT, INT -> Schema.create(Schema.Type.INT);
            case LONG -> Schema.create(Schema.Type.LONG);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case DECIMAL -> parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":"
                    + type.getPrecision() + ",\"scale\":" + type.getScale() + "}");
            case DATE -> parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
            case TIMESTAMP -> parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
            case LIST -> Schema.createArray(avro(type.getChildren().get(0), records));
            case MAP -> {
                if (type.getChildren().get(0).getCategory() != TypeDescription.Category.STRING) {
                    throw new IllegalArgumentException("Key of Map can only be a String: " + type);
                }
                yield Schema.createMap(avro(type.getChildren().get(1), records));
            }
            case STRUCT -> {
                List<Schema.Field> fields = new ArrayList<>();
                for (int i = 0; i < type.getChildren().size(); i++) {
                    TypeDescription child = type.getChildren().get(i);
                    fields.add(new Schema.Field(type.getFieldNames().get(i), avro(child, records),
                            child.toString(), JsonProperties.NULL_VALUE));
                }
                Schema record = Schema.createRecord("record_" + records[0]++, type.toString(),
                        null, false);
                record.setFields(fields);
                yield record;
            }
            default -> throw new IllegalArgumentException(type + " is not supported in Avro tables");
        };
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }
}
