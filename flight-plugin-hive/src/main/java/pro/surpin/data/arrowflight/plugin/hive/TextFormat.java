package pro.surpin.data.arrowflight.plugin.hive;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * How the files of a Hive text table ({@code LazySimpleSerDe}, {@code STORED AS TEXTFILE}) are
 * laid out, from its serde and table properties as Hive reads them (table properties win over
 * serde properties of the same key): the field delimiter ({@code field.delim}, else
 * {@code serialization.format}, default byte 1), the escape byte ({@code escape.delim}, none by
 * default) with {@code serialization.escape.crlf}, the text that stands for NULL
 * ({@code serialization.null.format}, default {@code \N}), the header lines of every file
 * ({@code skip.header.line.count}) and {@code serialization.last.column.takes.rest}. A delimiter
 * property is a byte number when it reads as one ({@code 9} is a tab), else its first character.
 * Rows end at a line feed, a carriage return or both, as Hadoop's text input format splits them.
 *
 * @param fieldDelimiter byte between fields
 * @param escape escape byte, or -1 for none
 * @param escapeCrlf whether an escaped {@code r} or {@code n} stands for a carriage return or a
 *     line feed
 * @param nullSequence bytes of a field that is NULL
 * @param headerLines lines skipped at the start of every file
 * @param lastColumnTakesRest whether the last column holds the rest of a row with more fields
 * @param tableOverrides the table's parameters among {@link #KEYS}, which also apply to its
 *     partitions (not part of equality)
 */
record TextFormat(byte fieldDelimiter, int escape, boolean escapeCrlf, byte[] nullSequence,
        int headerLines, boolean lastColumnTakesRest, Map<String, String> tableOverrides) {

    /** Properties that change how files are read. */
    static final List<String> KEYS = List.of("field.delim", "serialization.format", "escape.delim",
            "serialization.escape.crlf", "serialization.null.format", "skip.header.line.count",
            "skip.footer.line.count", "serialization.last.column.takes.rest", "line.delim",
            "serialization.encoding", "timestamp.formats");

    /** Hive's default field delimiter, byte 1 (Ctrl-A). */
    static final byte DEFAULT_FIELD_DELIMITER = 1;

    /**
     * Reads the format of a table.
     *
     * @param serdeParameters the serde's parameters, or null
     * @param tableParameters the table's parameters, or null
     * @return the format
     * @throws IllegalArgumentException when the table uses something the reader does not read
     *     (another line delimiter or encoding, footer lines, timestamp formats, a delimiter
     *     outside ASCII)
     */
    static TextFormat of(Map<String, String> serdeParameters, Map<String, String> tableParameters) {
        Map<String, String> properties = new HashMap<>();
        Map<String, String> overrides = new HashMap<>();
        if (serdeParameters != null) {
            properties.putAll(serdeParameters);
        }
        if (tableParameters != null) {
            tableParameters.forEach((key, value) -> {
                if (value != null && KEYS.contains(key)) {
                    properties.put(key, value);
                    overrides.put(key, value);
                }
            });
        }
        String line = properties.get("line.delim");
        if (line != null && !line.isEmpty() && byteOf(line, (byte) '\n') != '\n') {
            throw new IllegalArgumentException("text table with line delimiter "
                    + describe(line) + ": only line feeds end rows");
        }
        String encoding = properties.get("serialization.encoding");
        if (encoding != null && !encoding.isBlank()
                && !"UTF-8".equalsIgnoreCase(encoding.strip()) && !"UTF8".equalsIgnoreCase(encoding.strip())) {
            throw new IllegalArgumentException("text table with serialization.encoding " + encoding
                    + ": only UTF-8 is read");
        }
        if (count(properties, "skip.footer.line.count") > 0) {
            throw new IllegalArgumentException("text table with skip.footer.line.count: not read yet");
        }
        String formats = properties.get("timestamp.formats");
        if (formats != null && !formats.isBlank()) {
            throw new IllegalArgumentException("text table with timestamp.formats: not read yet");
        }
        String field = properties.getOrDefault("field.delim", properties.get("serialization.format"));
        byte delimiter = field == null || field.isEmpty() ? DEFAULT_FIELD_DELIMITER
                : byteOf(field, DEFAULT_FIELD_DELIMITER);
        String escapeText = properties.get("escape.delim");
        int escape = escapeText == null || escapeText.isEmpty() ? -1 : byteOf(escapeText, (byte) 0);
        if (delimiter < 0 || escape < -1 || delimiter == '\n' || delimiter == '\r'
                || (escape >= 0 && (escape == delimiter || escape == '\n' || escape == '\r'))) {
            throw new IllegalArgumentException("text table with field delimiter " + describe(field)
                    + " and escape " + describe(escapeText) + ": not read");
        }
        String nullFormat = properties.getOrDefault("serialization.null.format", "\\N");
        return new TextFormat(delimiter, escape,
                "true".equalsIgnoreCase(properties.getOrDefault("serialization.escape.crlf", "false")),
                nullFormat.getBytes(StandardCharsets.UTF_8), count(properties, "skip.header.line.count"),
                "true".equalsIgnoreCase(properties.getOrDefault(
                        "serialization.last.column.takes.rest", "false")), Map.copyOf(overrides));
    }

    /**
     * Reads the format of a partition: its own serde parameters, overridden by the table's.
     *
     * @param serdeParameters the partition's serde parameters, or null
     * @return the format
     * @throws IllegalArgumentException as {@link #of}
     */
    TextFormat forPartition(Map<String, String> serdeParameters) {
        return of(serdeParameters, tableOverrides);
    }

    /**
     * Describes the format for the table's schema ID: every node must split rows the same way.
     *
     * @return canonical text
     */
    String canonical() {
        return "d" + fieldDelimiter + ",e" + escape + ",c" + escapeCrlf + ",n"
                + HexFormat.of().formatHex(nullSequence) + ",h" + headerLines + ",r"
                + lastColumnTakesRest;
    }

    /**
     * Reads a delimiter property as Hive does: a byte number, else the first character.
     *
     * @param text property value, not empty
     * @param fallback value for an empty text
     * @return the byte; negative for a character outside ASCII
     */
    private static byte byteOf(String text, byte fallback) {
        if (text.isEmpty()) {
            return fallback;
        }
        try {
            return Byte.parseByte(text);
        } catch (NumberFormatException e) {
            char c = text.charAt(0);
            return c < 0x80 ? (byte) c : (byte) -1;
        }
    }

    /**
     * Reads a line count property.
     *
     * @param properties properties
     * @param key key
     * @return the count, 0 when absent
     * @throws IllegalArgumentException when it is not a non-negative number
     */
    private static int count(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return 0;
        }
        try {
            int count = Integer.parseInt(value.strip());
            if (count >= 0) {
                return count;
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        throw new IllegalArgumentException("text table with " + key + " " + value);
    }

    private static String describe(String value) {
        return value == null ? "none" : "'" + value.replace("\t", "\\t").replace("\n", "\\n")
                .replace("\r", "\\r") + "'" + (value.length() == 1
                        ? " (" + (int) value.charAt(0) + ")" : "");
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TextFormat format && canonical().equals(format.canonical());
    }

    @Override
    public int hashCode() {
        return canonical().hashCode();
    }

    @Override
    public String toString() {
        return "TextFormat{" + canonical() + "}";
    }
}
