package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.orc.TypeDescription;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Parses the lines of a Hive JSON table ({@link JsonLayout}) as Hive 3.1.3's {@code JsonSerDe}
 * does, with the same parser (Jackson 1.9) walking the same tokens: a line must start with an
 * object; its keys are matched to the columns ignoring case (unknown keys are skipped, a later
 * key of a column wins; HCatalog's serde also takes {@code _col<n>} for column n); JSON
 * {@code null} is NULL. Numbers go through Jackson's accessors (an integer column takes a
 * fraction cut towards zero, a tinyint up to 255 wraps, a quoted number fails), booleans are
 * true only for the text {@code true} in any case, strings take any scalar's text, binary the
 * text's UTF-8 bytes, dates and timestamps Hive's lenient {@code Date.valueOf} /
 * {@code Timestamp.valueOf} patterns, decimals {@code HiveDecimal} (unreadable or too wide
 * NULL), varchar and char cut to their length (char without trailing spaces). Map keys come from
 * the JSON keys. Where Hive's walk meets a token it does not expect (an object for a string
 * column takes the text <code>{</code> and its keys then count for the enclosing object) the walk
 * here does the same. Every row Hive's serde fails on fails here: a line that is not one JSON
 * object, a value of the wrong kind for a number column, an unreadable date or timestamp, a map
 * key that does not read as its type. Rows Hive reads but Arrow cannot hold fail too: NULL or
 * duplicate map keys (after cutting values to Arrow's microseconds), out of range dates and
 * timestamps.
 */
final class JsonRows {

    private static final JsonFactory FACTORY = new JsonFactory();
    private static final Pattern INTERNAL_NAME = Pattern.compile("_col([0-9]+)");
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final int NANOS_PER_MICRO = 1_000;
    /** Hive's {@code Date.valueOf} pattern, resolved leniently (month 13 is next January). */
    private static final DateTimeFormatter DATE = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 1, 10, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .toFormatter().withResolverStyle(ResolverStyle.LENIENT);
    /** Hive's {@code Timestamp.valueOf} pattern, resolved leniently. */
    private static final DateTimeFormatter TIMESTAMP = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 1, 10, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .optionalStart().appendLiteral(" ")
            .appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NORMAL).appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL).appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .optionalEnd()
            .toFormatter().withResolverStyle(ResolverStyle.LENIENT);

    private final JsonLayout layout;
    private final TypeDescription row;
    /** Column position by key, per struct type (the row and nested structs). */
    private final Map<TypeDescription, Map<String, Integer>> positions = new IdentityHashMap<>();

    /**
     * Prepares the parser of a table.
     *
     * @param layout which serde reads the table
     * @param columns the table's data columns, in table order
     * @throws IllegalArgumentException when a type is not read (see {@link #checkType})
     */
    JsonRows(JsonLayout layout, List<OrcArrowReader.Column> columns) {
        this.layout = layout;
        this.row = TypeDescription.createStruct();
        for (OrcArrowReader.Column column : columns) {
            row.addField(column.name(), TypeDescription.fromString(column.hiveType()));
        }
        index(row);
    }

    /**
     * Checks that a column of a JSON table is read.
     *
     * @param name column name
     * @param type its type
     * @param layout the table's serde
     * @throws IllegalArgumentException for unions, time zone timestamps, map keys that are not
     *     primitives, and (HCatalog's serde, which refuses them) struct fields whose names are
     *     equal ignoring case
     */
    static void checkType(String name, TypeDescription type, JsonLayout layout) {
        switch (type.getCategory()) {
            case BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, VARCHAR, CHAR, BINARY,
                    DECIMAL, DATE, TIMESTAMP -> {
            }
            case LIST -> checkType(name, type.getChildren().get(0), layout);
            case MAP -> {
                if (!type.getChildren().get(0).getCategory().isPrimitive()) {
                    throw new IllegalArgumentException("JSON column " + name
                            + " has a map key that is not a primitive");
                }
                checkType(name, type.getChildren().get(0), layout);
                checkType(name, type.getChildren().get(1), layout);
            }
            case STRUCT -> {
                Set<String> names = new HashSet<>();
                for (String field : type.getFieldNames()) {
                    if (layout.hcatalog() && !names.add(field.toLowerCase(Locale.ROOT))) {
                        throw new IllegalArgumentException("JSON column " + name
                                + " has struct fields named " + field + " ignoring case");
                    }
                }
                for (TypeDescription child : type.getChildren()) {
                    checkType(name, child, layout);
                }
            }
            default -> throw new IllegalArgumentException("JSON column " + name + " has type "
                    + type + " that the JSON reader does not read");
        }
    }

    private void index(TypeDescription type) {
        if (type.getCategory() == TypeDescription.Category.STRUCT) {
            Map<String, Integer> byKey = new HashMap<>();
            List<String> names = type.getFieldNames();
            for (int i = 0; i < names.size(); i++) {
                // HCatalog looks keys up lower case among lower case names, serde2 lower case
                // keys among the names as declared (the first of equal names).
                byKey.putIfAbsent(layout.hcatalog() ? names.get(i).toLowerCase(Locale.ROOT)
                        : names.get(i), i);
            }
            positions.put(type, byKey);
        }
        if (type.getChildren() != null) {
            for (TypeDescription child : type.getChildren()) {
                index(child);
            }
        }
    }

    /**
     * Parses one line.
     *
     * @param bytes buffer holding the line
     * @param length the line's length, from the buffer's start
     * @return the values of the data columns in table order (see {@link #write} for their
     *     classes)
     * @throws IOException when Hive's serde fails on the line, or it is not JSON
     * @throws RuntimeException where Hive's serde fails with one (a number format, an index)
     */
    Object[] parse(byte[] bytes, int length) throws IOException {
        try (JsonParser parser = FACTORY.createJsonParser(bytes, 0, length)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("Start token not found where expected");
            }
            Object[] values = new Object[row.getChildren().size()];
            JsonToken token = parser.nextToken();
            while (token != JsonToken.END_OBJECT && token != null) {
                populate(values, token, parser, row);
                token = parser.nextToken();
            }
            return values;
        }
    }

    /**
     * Reads one key and its value into a row or struct, as Hive's {@code populateRecord}.
     *
     * @param values the row's or struct's values
     * @param token the current token, a key
     * @param parser parser
     * @param struct the row's or struct's type
     * @throws IOException when the token is not a key or a value fails
     */
    private void populate(Object[] values, JsonToken token, JsonParser parser,
            TypeDescription struct) throws IOException {
        if (token != JsonToken.FIELD_NAME) {
            throw new IOException("Field name expected");
        }
        String name = parser.getText();
        Integer position = positions.get(struct).get(name.toLowerCase(Locale.ROOT));
        if (position == null) {
            String internal = layout.hcatalog() ? name : name.toLowerCase(Locale.ROOT);
            Matcher matcher = INTERNAL_NAME.matcher(internal);
            if (!matcher.matches()) {
                skipValue(parser);
                return;
            }
            int number = Integer.parseInt(matcher.group(1));
            if (!internal.equalsIgnoreCase("_col" + number)) {
                throw new IOException("Hive internal column name (" + name
                        + ") and position encoding (" + number + ") for the column name are at odds");
            }
            if (!layout.hcatalog() || number >= values.length) {
                // serde2 then looks the column up by that name, which is not there.
                throw new IOException("cannot find field " + name);
            }
            position = number;
        }
        values[position] = extract(parser, struct.getChildren().get(position), false);
    }

    private static void skipValue(JsonParser parser) throws IOException {
        JsonToken token = parser.nextToken();
        if (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT) {
            parser.skipChildren();
        }
    }

    /**
     * Reads a value as Hive's {@code extractCurrentField}: primitives leave the parser on their
     * token, arrays, maps and structs on their end.
     *
     * @param parser parser
     * @param type the value's type
     * @param current whether the parser is on the value's token already
     * @return the value, or null
     * @throws IOException when the value is not of the type
     */
    private Object extract(JsonParser parser, TypeDescription type, boolean current)
            throws IOException {
        JsonToken token = current ? parser.getCurrentToken() : parser.nextToken();
        if (token == JsonToken.VALUE_NULL) {
            return null;
        }
        if (token == null) {
            throw new IOException("Unexpected end of the line");
        }
        switch (type.getCategory()) {
            case INT -> {
                return parser.getIntValue();
            }
            case BYTE -> {
                return parser.getByteValue();
            }
            case SHORT -> {
                return parser.getShortValue();
            }
            case LONG -> {
                return parser.getLongValue();
            }
            case BOOLEAN -> {
                return Boolean.valueOf(parser.getText());
            }
            case FLOAT -> {
                return parser.getFloatValue();
            }
            case DOUBLE -> {
                return parser.getDoubleValue();
            }
            case LIST -> {
                if (token != JsonToken.START_ARRAY) {
                    throw new IOException("Start of Array expected");
                }
                List<Object> list = new ArrayList<>();
                TypeDescription element = type.getChildren().get(0);
                while (next(parser) != JsonToken.END_ARRAY) {
                    list.add(extract(parser, element, true));
                }
                return list;
            }
            case MAP -> {
                if (token != JsonToken.START_OBJECT) {
                    throw new IOException("Start of Object expected");
                }
                Map<Object, Object> map = new LinkedHashMap<>();
                while (next(parser) != JsonToken.END_OBJECT) {
                    Object key = scalar(parser.getCurrentName(), type.getChildren().get(0));
                    map.put(key, extract(parser, type.getChildren().get(1), false));
                }
                return map;
            }
            case STRUCT -> {
                if (token != JsonToken.START_OBJECT) {
                    throw new IOException("Start of Object expected");
                }
                Object[] fields = new Object[type.getChildren().size()];
                JsonToken field = next(parser);
                while (field != JsonToken.END_OBJECT) {
                    populate(fields, field, parser, type);
                    field = next(parser);
                }
                return fields;
            }
            default -> {
                return scalar(parser.getText(), type);
            }
        }
    }

    private static JsonToken next(JsonParser parser) throws IOException {
        JsonToken token = parser.nextToken();
        if (token == null) {
            throw new IOException("Unexpected end of the line");
        }
        return token;
    }

    /**
     * Reads a text as a value of a type, as Hive reads a scalar's text or a map key (numbers
     * and booleans of values never come here: Jackson reads them).
     *
     * @param text the text
     * @param type a primitive type
     * @return the value
     * @throws IOException for a type no text is read as
     * @throws RuntimeException where Hive's conversion fails with one
     */
    private static Object scalar(String text, TypeDescription type) throws IOException {
        if (text == null) {
            throw new IOException("a map key is missing");
        }
        return switch (type.getCategory()) {
            case INT -> Integer.valueOf(text);
            case BYTE -> Byte.valueOf(text);
            case SHORT -> Short.valueOf(text);
            case LONG -> Long.valueOf(text);
            // Hive's equalsIgnoreCase("true") for keys, Boolean.valueOf for values: the same.
            case BOOLEAN -> Boolean.parseBoolean(text);
            case FLOAT -> Float.valueOf(text);
            case DOUBLE -> Double.valueOf(text);
            case STRING -> text;
            // Hive keeps binary map keys as byte arrays, equal only to themselves.
            case BINARY -> text.getBytes(StandardCharsets.UTF_8);
            case DATE -> date(text);
            case TIMESTAMP -> timestamp(text);
            case DECIMAL -> HiveDecimal.create(text);
            case VARCHAR -> cut(text, type.getMaxLength());
            case CHAR -> stripEnd(cut(text, type.getMaxLength()));
            default -> throw new IOException("type " + type + " is not read from JSON");
        };
    }

    /**
     * Reads a date as Hive's {@code Date.valueOf}.
     *
     * @param text the text
     * @return the date
     * @throws IllegalArgumentException when it does not read
     */
    static LocalDate date(String text) {
        String s = text.trim();
        int blank = s.indexOf(' ');
        if (blank != -1) {
            s = s.substring(0, blank);
        }
        try {
            return LocalDate.parse(s, DATE);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Cannot create date, parsing error", e);
        }
    }

    /**
     * Reads a timestamp as Hive's {@code Timestamp.valueOf}: its pattern, else ISO-8601.
     *
     * @param text the text
     * @return the timestamp
     * @throws IllegalArgumentException when it does not read
     */
    static LocalDateTime timestamp(String text) {
        String s = text.trim();
        try {
            return LocalDateTime.parse(s, TIMESTAMP);
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(s);
            } catch (DateTimeParseException e2) {
                throw new IllegalArgumentException("Cannot create timestamp, parsing error", e2);
            }
        }
    }

    /**
     * Cuts a text to a number of code points, as Hive's {@code HiveBaseChar.enforceMaxLength}.
     *
     * @param text the text
     * @param length most code points
     * @return the text, cut
     */
    private static String cut(String text, int length) {
        if (length > 0 && text.codePointCount(0, text.length()) > length) {
            return text.substring(0, text.offsetByCodePoints(0, length));
        }
        return text;
    }

    private static String stripEnd(String text) {
        int end = text.length();
        while (end > 0 && text.charAt(end - 1) == ' ') {
            end--;
        }
        return text.substring(0, end);
    }

    /**
     * Writes a parsed value into a vector of {@link OrcArrowReader#field}'s type.
     *
     * @param vector target vector
     * @param type the value's type
     * @param index target index
     * @param value the value from {@link #parse}: a boxed number or Boolean, a String, a
     *     byte array, a HiveDecimal, a LocalDate, a LocalDateTime, a List, a Map or an
     *     {@code Object[]} of struct fields; or null
     * @throws IOException when Arrow cannot hold it (a NULL or duplicate map key, a date or
     *     timestamp out of range)
     */
    static void write(FieldVector vector, TypeDescription type, int index, Object value)
            throws IOException {
        Object arrow = value == null ? null : arrowValue(type, value);
        if (arrow == null) {
            OrcArrowReader.setNull(vector, index);
            return;
        }
        switch (type.getCategory()) {
            case BOOLEAN -> ((BitVector) vector).setSafe(index, (Boolean) arrow ? 1 : 0);
            case BYTE -> ((TinyIntVector) vector).setSafe(index, (Byte) arrow);
            case SHORT -> ((SmallIntVector) vector).setSafe(index, (Short) arrow);
            case INT -> ((IntVector) vector).setSafe(index, (Integer) arrow);
            case LONG -> ((BigIntVector) vector).setSafe(index, (Long) arrow);
            case FLOAT -> ((Float4Vector) vector).setSafe(index, (Float) arrow);
            case DOUBLE -> ((Float8Vector) vector).setSafe(index, (Double) arrow);
            case STRING, VARCHAR, CHAR -> ((VarCharVector) vector).setSafe(index,
                    ((String) arrow).getBytes(StandardCharsets.UTF_8));
            case BINARY -> {
                ByteBuffer bytes = (ByteBuffer) arrow;
                ((VarBinaryVector) vector).setSafe(index, bytes, 0, bytes.remaining());
            }
            case DECIMAL -> ((DecimalVector) vector).setSafe(index, (BigDecimal) arrow);
            case DATE -> ((DateDayVector) vector).setSafe(index, (Integer) arrow);
            case TIMESTAMP -> ((TimeStampVector) vector).setSafe(index, (Long) arrow);
            case LIST -> {
                ListVector list = (ListVector) vector;
                List<?> elements = (List<?>) arrow;
                int offset = list.startNewValue(index);
                for (int i = 0; i < elements.size(); i++) {
                    write(list.getDataVector(), type.getChildren().get(0), offset + i,
                            elements.get(i));
                }
                list.endValue(index, elements.size());
            }
            case MAP -> writeMap((MapVector) vector, type, index, (Map<?, ?>) arrow);
            case STRUCT -> {
                StructVector struct = (StructVector) vector;
                Object[] fields = (Object[]) arrow;
                struct.setIndexDefined(index);
                for (int f = 0; f < fields.length; f++) {
                    write((FieldVector) struct.getChildByOrdinal(f), type.getChildren().get(f),
                            index, fields[f]);
                }
            }
            default -> throw new IOException("type " + type + " is not read from JSON");
        }
    }

    private static void writeMap(MapVector map, TypeDescription type, int index, Map<?, ?> entries)
            throws IOException {
        StructVector structs = (StructVector) map.getDataVector();
        FieldVector keys = (FieldVector) structs.getChildByOrdinal(0);
        FieldVector values = (FieldVector) structs.getChildByOrdinal(1);
        TypeDescription keyType = type.getChildren().get(0);
        int offset = map.startNewValue(index);
        Set<Object> seen = new HashSet<>();
        int count = 0;
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            Object key = entry.getKey() == null ? null : arrowValue(keyType, entry.getKey());
            if (key == null) {
                throw new IOException("map key " + entry.getKey() + " reads as NULL");
            }
            if (!seen.add(key)) {
                throw new IOException("map has key " + entry.getKey() + " twice as "
                        + keyType);
            }
            write(keys, keyType, offset + count, entry.getKey());
            write(values, type.getChildren().get(1), offset + count, entry.getValue());
            structs.setIndexDefined(offset + count);
            count++;
        }
        map.endValue(index, count);
    }

    /**
     * Converts a parsed primitive to the value Arrow holds; nested values are returned as they
     * are.
     *
     * @param type its type
     * @param value non-null parsed value
     * @return the value to write, or null for a decimal that does not fit (NULL as in Hive)
     * @throws IOException for a date or timestamp out of Arrow's range
     */
    private static Object arrowValue(TypeDescription type, Object value) throws IOException {
        try {
            return switch (type.getCategory()) {
                case DECIMAL -> {
                    HiveDecimal decimal = HiveDecimal.enforcePrecisionScale((HiveDecimal) value,
                            type.getPrecision(), type.getScale());
                    yield decimal == null ? null : decimal.bigDecimalValue().setScale(type.getScale());
                }
                case DATE -> Math.toIntExact(((LocalDate) value).toEpochDay());
                case TIMESTAMP -> {
                    LocalDateTime time = (LocalDateTime) value;
                    yield Math.addExact(Math.multiplyExact(time.toEpochSecond(ZoneOffset.UTC),
                            MICROS_PER_SECOND), time.getNano() / NANOS_PER_MICRO);
                }
                case BINARY -> ByteBuffer.wrap((byte[]) value);
                default -> value;
            };
        } catch (ArithmeticException e) {
            throw new IOException(type + " value " + value + " is out of range", e);
        }
    }
}
