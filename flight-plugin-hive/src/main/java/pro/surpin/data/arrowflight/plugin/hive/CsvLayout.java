package pro.surpin.data.arrowflight.plugin.hive;

import java.io.CharArrayReader;
import java.io.IOException;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

/**
 * How the rows of an {@code OpenCSVSerde} text table are parsed, as Hive 3.1.3's serde parses
 * them: each line on its own with opencsv 2.3's {@code CSVReader} ({@code separatorChar}, default
 * comma; {@code quoteChar}, default double quote; {@code escapeChar}, whose default, the double
 * quote, makes the serde leave opencsv's own escape, the backslash). A property's first character
 * counts. Every column is text, a field the line does not have is NULL, an empty line is all NULL.
 *
 * @param separator field separator
 * @param quote quote character
 * @param escape escape character as the table sets it
 */
record CsvLayout(char separator, char quote, char escape) {

    /** Table properties of the serde. */
    static final String SEPARATOR = "separatorChar";
    /** Quote property. */
    static final String QUOTE = "quoteChar";
    /** Escape property. */
    static final String ESCAPE = "escapeChar";

    /**
     * Reads the layout from a table's merged properties.
     *
     * @param properties serde and table properties
     * @return the layout
     * @throws IllegalArgumentException when a property is empty or opencsv refuses the characters
     */
    static CsvLayout of(Map<String, String> properties) {
        CsvLayout layout = new CsvLayout(character(properties, SEPARATOR, CSVWriter.DEFAULT_SEPARATOR),
                character(properties, QUOTE, CSVWriter.DEFAULT_QUOTE_CHARACTER),
                character(properties, ESCAPE, CSVWriter.DEFAULT_ESCAPE_CHARACTER));
        try {
            layout.read("x");
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("OpenCSVSerde table with separator, quote and escape "
                    + layout.canonical() + ": " + e.getMessage(), e);
        }
        return layout;
    }

    /**
     * Parses one line as the serde does.
     *
     * @param row the line, without its end
     * @return its fields, or null for an empty line
     * @throws IOException never for a line in memory (opencsv's signature)
     */
    String[] read(String row) throws IOException {
        CharArrayReader in = new CharArrayReader(row.toCharArray());
        try (CSVReader reader = escape == CSVWriter.DEFAULT_ESCAPE_CHARACTER
                ? new CSVReader(in, separator, quote) : new CSVReader(in, separator, quote, escape)) {
            return reader.readNext();
        }
    }

    /**
     * Describes the layout for the table's schema ID.
     *
     * @return the three characters as numbers
     */
    String canonical() {
        return (int) separator + "." + (int) quote + "." + (int) escape;
    }

    private static char character(Map<String, String> properties, String key, char fallback) {
        String value = properties.get(key);
        if (value == null) {
            return fallback;
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("OpenCSVSerde table with an empty " + key);
        }
        return value.charAt(0);
    }
}
