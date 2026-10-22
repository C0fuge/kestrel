package pro.surpin.data.arrowflight.plugin.hive;

import java.util.Locale;

/**
 * Maps Hive column types, as the metastore writes them ({@code decimal(12,2)},
 * {@code array<struct<a:int,b:string>>}, ...), to DuckDB type names for reading Parquet files.
 */
final class HiveTypes {

    private final String text;
    private int pos;

    private HiveTypes(String text) {
        this.text = text;
    }

    /**
     * Maps a Hive type.
     *
     * @param hiveType type as the metastore writes it
     * @return DuckDB type name
     * @throws IllegalArgumentException when the type has no DuckDB counterpart (uniontype,
     *         interval, void) or cannot be read
     */
    static String duckDb(String hiveType) {
        if (hiveType == null || hiveType.isBlank()) {
            throw new IllegalArgumentException("no type");
        }
        HiveTypes parser = new HiveTypes(hiveType.strip());
        String result = parser.type();
        parser.skipSpaces();
        if (parser.pos != parser.text.length()) {
            throw new IllegalArgumentException("cannot read type " + hiveType);
        }
        return result;
    }

    /**
     * Reads one type at the current position.
     *
     * @return DuckDB type
     */
    private String type() {
        skipSpaces();
        String name = word().toLowerCase(Locale.ROOT);
        switch (name) {
            case "tinyint":
                return "TINYINT";
            case "smallint":
                return "SMALLINT";
            case "int":
            case "integer":
                return "INTEGER";
            case "bigint":
                return "BIGINT";
            case "boolean":
                return "BOOLEAN";
            case "float":
                return "FLOAT";
            case "double":
                skipSpaces();
                if (text.regionMatches(true, pos, "precision", 0, "precision".length())) {
                    pos += "precision".length();
                }
                return "DOUBLE";
            case "string":
                return "VARCHAR";
            case "varchar":
            case "char":
                arguments();
                return "VARCHAR";
            case "date":
                return "DATE";
            case "timestamp":
                skipSpaces();
                if (text.regionMatches(true, pos, "with local time zone", 0, 20)) {
                    pos += 20;
                    return "TIMESTAMP WITH TIME ZONE";
                }
                return "TIMESTAMP";
            case "binary":
                return "BLOB";
            case "decimal":
            case "numeric":
                int[] args = arguments();
                int precision = args.length > 0 ? args[0] : 10;
                int scale = args.length > 1 ? args[1] : 0;
                if (precision < 1 || precision > 38 || scale < 0 || scale > precision) {
                    throw new IllegalArgumentException("decimal(" + precision + "," + scale
                            + ") is out of range");
                }
                return "DECIMAL(" + precision + "," + scale + ")";
            case "array":
                expect('<');
                String element = type();
                expect('>');
                return element + "[]";
            case "map":
                expect('<');
                String key = type();
                expect(',');
                String value = type();
                expect('>');
                return "MAP(" + key + ", " + value + ")";
            case "struct":
                return struct();
            default:
                throw new IllegalArgumentException("type " + name + " has no DuckDB counterpart");
        }
    }

    /**
     * Reads the fields of a struct after the word "struct".
     *
     * @return DuckDB STRUCT type
     */
    private String struct() {
        expect('<');
        StringBuilder result = new StringBuilder("STRUCT(");
        skipSpaces();
        boolean first = true;
        while (pos < text.length() && text.charAt(pos) != '>') {
            if (!first) {
                expect(',');
            }
            skipSpaces();
            String field;
            if (pos < text.length() && text.charAt(pos) == '`') {
                int end = text.indexOf('`', pos + 1);
                if (end < 0) {
                    throw new IllegalArgumentException("cannot read type " + text);
                }
                field = text.substring(pos + 1, end);
                pos = end + 1;
            } else {
                int start = pos;
                while (pos < text.length() && text.charAt(pos) != ':') {
                    pos++;
                }
                field = text.substring(start, pos).strip();
            }
            expect(':');
            String type = type();
            result.append(first ? "" : ", ").append('"').append(field.replace("\"", "\"\""))
                    .append("\" ").append(type);
            first = false;
            skipSpaces();
        }
        expect('>');
        if (first) {
            throw new IllegalArgumentException("struct without fields");
        }
        return result.append(')').toString();
    }

    /**
     * Reads optional numeric arguments in parentheses.
     *
     * @return arguments, empty without parentheses
     */
    private int[] arguments() {
        skipSpaces();
        if (pos >= text.length() || text.charAt(pos) != '(') {
            return new int[0];
        }
        int end = text.indexOf(')', pos);
        if (end < 0) {
            throw new IllegalArgumentException("cannot read type " + text);
        }
        String[] parts = text.substring(pos + 1, end).split(",");
        pos = end + 1;
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                result[i] = Integer.parseInt(parts[i].strip());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("cannot read type " + text, e);
            }
        }
        return result;
    }

    /**
     * Reads a word of letters, digits and underscores.
     *
     * @return word
     */
    private String word() {
        int start = pos;
        while (pos < text.length()
                && (Character.isLetterOrDigit(text.charAt(pos)) || text.charAt(pos) == '_')) {
            pos++;
        }
        if (start == pos) {
            throw new IllegalArgumentException("cannot read type " + text);
        }
        return text.substring(start, pos);
    }

    /**
     * Skips spaces and then one expected character.
     *
     * @param c character
     */
    private void expect(char c) {
        skipSpaces();
        if (pos >= text.length() || text.charAt(pos) != c) {
            throw new IllegalArgumentException("cannot read type " + text);
        }
        pos++;
    }

    /** Skips spaces. */
    private void skipSpaces() {
        while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) {
            pos++;
        }
    }
}
