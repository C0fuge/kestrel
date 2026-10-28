package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Reader schemas of Avro tables as Hive 3.1.3 finds them: the literal (table properties over
 * serde properties) or the columns' schema, Avro types as Hive column types, and refusals.
 */
@Tag("unit")
class AvroFormatTest {

    /** A schema with every type Hive reads, fields named in mixed case. */
    static final String ALL_TYPES = "{\"type\":\"record\",\"name\":\"row\",\"fields\":["
            + "{\"name\":\"I\",\"type\":[\"null\",\"int\"]},"
            + "{\"name\":\"s\",\"type\":[\"null\",\"string\"]},"
            + "{\"name\":\"e\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"kind\","
            + "\"symbols\":[\"A\",\"B\"]}]},"
            + "{\"name\":\"d\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":10,\"scale\":2}]},"
            + "{\"name\":\"dt\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]},"
            + "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\","
            + "\"logicalType\":\"timestamp-millis\"}]},"
            + "{\"name\":\"b\",\"type\":[\"null\",\"boolean\"]},"
            + "{\"name\":\"c\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"char\","
            + "\"maxLength\":3}]},"
            + "{\"name\":\"v\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"varchar\","
            + "\"maxLength\":5}]},"
            + "{\"name\":\"bin\",\"type\":[\"null\",\"bytes\"]},"
            + "{\"name\":\"fx\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"two\",\"size\":2}]},"
            + "{\"name\":\"l\",\"type\":[\"null\",\"long\"]},"
            + "{\"name\":\"f\",\"type\":[\"null\",\"float\"]},"
            + "{\"name\":\"db\",\"type\":[\"null\",\"double\"]},"
            + "{\"name\":\"a\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"int\"]}]},"
            + "{\"name\":\"m\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"int\"]}]},"
            + "{\"name\":\"st\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"pair\",\"fields\":["
            + "{\"name\":\"x\",\"type\":\"int\"},{\"name\":\"y\",\"type\":[\"null\",\"string\"]}]}]}"
            + "]}";
    /** The metastore columns of {@link #ALL_TYPES}. */
    static final String ALL_COLUMNS = "i int;s string;e string;d decimal(10,2);dt date;ts timestamp;"
            + "b boolean;c char(3);v varchar(5);bin binary;fx binary;l bigint;f float;db double;"
            + "a array<int>;m map<string,int>;st struct<x:int,y:string>";

    @Test
    void literalColumnsAreHiveTypes() {
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, ALL_TYPES),
                columns(ALL_COLUMNS));
        assertTrue(format.tableLevel());
        assertEquals(AvroFormat.parse(ALL_TYPES), format.schema());
        assertEquals("map<string,array<struct<x:int>>>", AvroFormat.hiveType(AvroFormat.parse(
                "{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":{\"type\":\"record\","
                        + "\"name\":\"r\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}}")));
        // Decimal is a bytes logical type for Hive; fixed decimals are binary.
        assertEquals("binary", AvroFormat.hiveType(AvroFormat.parse("{\"type\":\"fixed\","
                + "\"name\":\"f\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":5}")));
        // A record used twice is no recursion.
        assertEquals("struct<p:struct<x:int>,q:struct<x:int>>", AvroFormat.hiveType(AvroFormat.parse(
                "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"p\",\"type\":{\"type\":"
                        + "\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}},"
                        + "{\"name\":\"q\",\"type\":\"r\"}]}")));
    }

    @Test
    void tablePropertiesOverrideSerdePropertiesAndNoneIsUnset() {
        AvroFormat format = AvroFormat.of(Map.of(AvroFormat.LITERAL, "not json"),
                Map.of(AvroFormat.LITERAL, ALL_TYPES), columns(ALL_COLUMNS));
        assertEquals(AvroFormat.parse(ALL_TYPES), format.schema());
        AvroFormat serde = AvroFormat.of(Map.of(AvroFormat.LITERAL, ALL_TYPES), Map.of(),
                columns(ALL_COLUMNS));
        assertFalse(serde.tableLevel());
        assertEquals(AvroFormat.parse(ALL_TYPES), serde.schema());
        // "none" in the table properties hides the serde's literal: the columns' schema.
        AvroFormat none = AvroFormat.of(Map.of(AvroFormat.LITERAL, ALL_TYPES),
                Map.of(AvroFormat.LITERAL, "none"), columns(ALL_COLUMNS));
        assertFalse(none.tableLevel());
        assertEquals(AvroFormat.fromColumns(columns(ALL_COLUMNS)), none.schema());
        assertThrows(IllegalArgumentException.class, () -> AvroFormat.of(
                Map.of(AvroFormat.LITERAL, "not json"), Map.of(), columns(ALL_COLUMNS)));
    }

    @Test
    void columnsGiveHiveTypeInfoToSchema() {
        String columns = "i int;s string;d decimal(10,2);dt date;ts timestamp;b boolean;c char(3);"
                + "v varchar(5);bin binary;l bigint;f float;db double;a array<int>;"
                + "m map<string,int>;st struct<x:int,y:struct<z:string>>";
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(), columns(columns));
        assertFalse(format.tableLevel());
        Schema schema = format.schema();
        assertEquals("baseRecord", schema.getName());
        for (Schema.Field field : schema.getFields()) {
            assertEquals(Schema.Type.UNION, field.schema().getType(), field.name());
            assertEquals(Schema.Type.NULL, field.schema().getTypes().get(0).getType());
            assertTrue(field.hasDefaultValue(), field.name());
        }
        assertEquals("decimal(10,2)", AvroFormat.hiveType(schema.getField("d").schema()));
        assertEquals(Schema.Type.BYTES,
                AvroFormat.unwrapped(schema.getField("d").schema()).getType());
        // Inner records are named first, as Hive numbers them.
        Schema st = AvroFormat.unwrapped(schema.getField("st").schema());
        assertEquals("record_1", st.getName());
        assertEquals("record_0", AvroFormat.unwrapped(st.getField("y").schema()).getName());
        // Avro has no tinyint: Hive's schema of the column gives int, not the metastore's type.
        assertThrows(IllegalArgumentException.class,
                () -> AvroFormat.of(Map.of(), Map.of(), columns("t tinyint")));
        assertThrows(IllegalArgumentException.class,
                () -> AvroFormat.of(Map.of(), Map.of(), columns("m map<int,int>")));
        assertNotEquals(format, AvroFormat.of(Map.of(), Map.of(), columns("i int")));
    }

    @Test
    void schemasHiveDoesNotReadOrThatDifferFromTheColumnsAreRefused() {
        assertRefused(Map.of(AvroFormat.URL, "hdfs://nn/schemas/a.avsc"), ALL_COLUMNS, "not read");
        assertRefused(Map.of(AvroFormat.URL, "none"), ALL_COLUMNS, "without a schema");
        assertRefused(Map.of(AvroFormat.LITERAL, ALL_TYPES), "i int", "fields");
        assertRefused(Map.of(AvroFormat.LITERAL, ALL_TYPES), ALL_COLUMNS.replace("i int", "j int"),
                "metastore column");
        assertRefused(Map.of(AvroFormat.LITERAL, ALL_TYPES), ALL_COLUMNS.replace("i int", "i bigint"),
                "metastore column");
        assertRefused(Map.of(AvroFormat.LITERAL, record("[\"int\",\"string\"]")), "u int", "union");
        assertRefused(Map.of(AvroFormat.LITERAL, record("\"null\"")), "u int", "NULL");
        assertRefused(Map.of(AvroFormat.LITERAL, record("{\"type\":\"bytes\","
                + "\"logicalType\":\"decimal\",\"scale\":2}")), "u decimal(10,2)", "precision");
        assertRefused(Map.of(AvroFormat.LITERAL, record("{\"type\":\"string\","
                + "\"logicalType\":\"varchar\"}")), "u varchar(3)", "maxLength");
        assertRefused(Map.of(AvroFormat.LITERAL, "{\"type\":\"record\",\"name\":\"n\",\"fields\":["
                + "{\"name\":\"next\",\"type\":[\"null\",\"n\"]}]}"), "next struct<next:int>",
                "Recursive");
        assertRefused(Map.of(AvroFormat.LITERAL, "\"int\""), "u int", "not a record");
    }

    private static String record(String type) {
        return "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"u\",\"type\":" + type
                + "}]}";
    }

    private static void assertRefused(Map<String, String> table, String columns, String message) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> AvroFormat.of(Map.of(), table, columns(columns)));
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    /**
     * Parses {@code name type;name type}.
     *
     * @param spec columns
     * @return metastore columns
     */
    static List<FieldSchema> columns(String spec) {
        List<FieldSchema> result = new ArrayList<>();
        for (String part : spec.split(";")) {
            int space = part.indexOf(' ');
            result.add(new FieldSchema(part.substring(0, space), part.substring(space + 1), null));
        }
        return result;
    }
}
