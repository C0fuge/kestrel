package pro.surpin.data.arrowflight.auth.rangerhive;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * One Ranger column mask of the Hive service, written as a DuckDB expression with the results of Hive 3.1.3's
 * mask UDFs (mask, mask_show_first_n, mask_show_last_n, mask_hash).
 */
public final class HiveMask {
    private static final int UNMASKED = -1;
    private static final String OUTSIDE_BMP = "'[\\x{10000}-\\x{10FFFF}]'";
    private static final Object COLUMN = new Object();

    /** What the mask does. */
    private enum Kind { MASK, SHOW_FIRST, SHOW_LAST, HASH, NULL, NONE }

    private final Kind kind;
    private final int charCount;
    private final int upper;
    private final int lower;
    private final int digit;
    private final int other;
    private final int number;
    private final int day;
    private final int month;
    private final int year;

    /**
     * Creates a mask from parsed arguments.
     *
     * @param kind what the mask does
     * @param charCount characters or digits shown (show-first / show-last)
     * @param chars upper, lower, digit and other replacement code points (-1 keeps)
     * @param values number digit, day, month and year values
     */
    private HiveMask(Kind kind, int charCount, int[] chars, int[] values) {
        this.kind = kind;
        this.charCount = charCount;
        this.upper = chars[0];
        this.lower = chars[1];
        this.digit = chars[2];
        this.other = chars[3];
        this.number = values[0];
        this.day = values[1];
        this.month = values[2];
        this.year = values[3];
    }

    /**
     * Reads a Ranger data mask result of the Hive service.
     *
     * @param maskType mask type name (MASK, MASK_SHOW_LAST_4, MASK_NULL, ...)
     * @param transformer the mask type's transformer from the service definition, may be null
     * @return the mask
     * @throws IllegalArgumentException when the mask cannot be written exactly (custom expressions, unknown
     *     functions, arguments Hive would refuse)
     */
    public static HiveMask parse(String maskType, String transformer) {
        if ("MASK_NULL".equals(maskType)) {
            return new HiveMask(Kind.NULL, 0, new int[4], new int[4]);
        }
        if ("MASK_NONE".equals(maskType)) {
            return new HiveMask(Kind.NONE, 0, new int[4], new int[4]);
        }
        if (transformer == null || transformer.isBlank()) {
            throw new IllegalArgumentException("mask type " + maskType + " has no transformer");
        }
        return parseTransformer(transformer);
    }

    /**
     * Reads a transformer call such as {@code mask_show_last_n({col}, 4, 'x', 'x', 'x', -1, '1')}.
     *
     * @param transformer transformer text
     * @return the mask
     * @throws IllegalArgumentException when it is not one supported call with literal arguments
     */
    private static HiveMask parseTransformer(String transformer) {
        String text = transformer.trim();
        int open = text.indexOf('(');
        if (open <= 0 || !text.endsWith(")")) {
            throw new IllegalArgumentException("unsupported mask transformer: " + transformer);
        }
        String name = text.substring(0, open).trim().toLowerCase(Locale.ROOT);
        List<Object> args = arguments(text.substring(open + 1, text.length() - 1), transformer);
        if (args.isEmpty() || args.get(0) != COLUMN) {
            throw new IllegalArgumentException("mask transformer must start with {col}: " + transformer);
        }
        List<Object> rest = args.subList(1, args.size());
        return switch (name) {
            case "mask" -> masking(Kind.MASK, 0, rest, 8, transformer);
            case "mask_show_first_n" -> masking(Kind.SHOW_FIRST, countArg(rest, transformer),
                    rest.isEmpty() ? rest : rest.subList(1, rest.size()), 8, transformer);
            case "mask_show_last_n" -> masking(Kind.SHOW_LAST, countArg(rest, transformer),
                    rest.isEmpty() ? rest : rest.subList(1, rest.size()), 8, transformer);
            case "mask_hash" -> masking(Kind.HASH, 0, rest, 0, transformer);
            default -> throw new IllegalArgumentException("unsupported mask function: " + transformer);
        };
    }

    /**
     * Reads the shown character count as Hive does (default 4, negative counts as 0).
     *
     * @param rest arguments after the column
     * @param transformer transformer text for messages
     * @return the count
     */
    private static int countArg(List<Object> rest, String transformer) {
        int count = rest.isEmpty() ? 4 : intArg(rest.get(0), 4, transformer);
        return Math.max(count, 0);
    }

    /**
     * Builds a mask from Hive's MaskTransformer arguments with its defaults and corrections.
     *
     * @param kind what the mask does
     * @param charCount shown count
     * @param args upper, lower, digit, other, number, day, month, year (any prefix)
     * @param maxArgs most arguments the function reads
     * @param transformer transformer text for messages
     * @return the mask
     */
    private static HiveMask masking(Kind kind, int charCount, List<Object> args, int maxArgs,
            String transformer) {
        if (args.size() > maxArgs) {
            throw new IllegalArgumentException("too many mask arguments: " + transformer);
        }
        int[] chars = {'X', 'x', 'n', UNMASKED};
        for (int i = 0; i < chars.length && i < args.size(); i++) {
            chars[i] = charArg(args.get(i), chars[i], transformer);
        }
        int[] values = {1, 1, 0, 0};
        for (int i = 0; i < values.length && i + 4 < args.size(); i++) {
            values[i] = intArg(args.get(i + 4), values[i], transformer);
        }
        if (values[0] < 0 || values[0] > 9) {
            values[0] = 1;
        }
        if (values[1] != UNMASKED && (values[1] < 1 || values[1] > 31)) {
            values[1] = 1;
        }
        if (values[2] != UNMASKED && (values[2] < 0 || values[2] > 11)) {
            values[2] = 0;
        }
        return new HiveMask(kind, charCount, chars, values);
    }

    /**
     * Reads a character argument as Hive's getCharArg does: an integer is a code point, a string its first char.
     *
     * @param arg literal
     * @param fallback default when the string is empty
     * @param transformer transformer text for messages
     * @return code point, or -1 to keep the character
     */
    private static int charArg(Object arg, int fallback, String transformer) {
        int value;
        if (arg instanceof Long number) {
            if (number < Integer.MIN_VALUE || number > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("mask argument out of range: " + transformer);
            }
            value = number.intValue();
        } else if (arg instanceof String text) {
            value = text.isEmpty() ? fallback : text.charAt(0);
        } else {
            throw new IllegalArgumentException("unsupported mask argument: " + transformer);
        }
        // Hive appends the value as a code point; anything that is not a usable character fails there.
        if (value != UNMASKED && (value <= 0 || value > Character.MAX_CODE_POINT
                || (value >= Character.MIN_SURROGATE && value <= Character.MAX_SURROGATE))) {
            throw new IllegalArgumentException("unusable mask character in " + transformer);
        }
        return value;
    }

    /**
     * Reads an integer argument as Hive's getIntArg does (a string literal is parsed).
     *
     * @param arg literal
     * @param fallback default when the string is empty
     * @param transformer transformer text for messages
     * @return the value
     */
    private static int intArg(Object arg, int fallback, String transformer) {
        if (arg instanceof Long number) {
            return number.intValue();
        }
        if (arg instanceof String text) {
            if (text.isEmpty()) {
                return fallback;
            }
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("mask argument is not a number: " + transformer, e);
            }
        }
        throw new IllegalArgumentException("unsupported mask argument: " + transformer);
    }

    /**
     * Splits literal arguments: {@code {col}}, integers and quoted strings without quotes inside.
     *
     * @param text text between the parentheses
     * @param transformer transformer text for messages
     * @return arguments as String, Long or the column marker
     */
    private static List<Object> arguments(String text, String transformer) {
        List<Object> args = new ArrayList<>();
        int i = 0;
        while (i < text.length()) {
            while (i < text.length() && Character.isWhitespace(text.charAt(i))) {
                i++;
            }
            int start = i;
            char c = i < text.length() ? text.charAt(i) : ',';
            if (c == '\'' || c == '"') {
                int end = text.indexOf(c, i + 1);
                if (end < 0 || text.substring(i + 1, end).indexOf('\\') >= 0) {
                    throw new IllegalArgumentException("unsupported mask literal: " + transformer);
                }
                args.add(text.substring(i + 1, end));
                i = end + 1;
            } else if (text.startsWith("{col}", i)) {
                args.add(COLUMN);
                i += 5;
            } else {
                if (c == '-') {
                    i++;
                }
                while (i < text.length() && text.charAt(i) >= '0' && text.charAt(i) <= '9') {
                    i++;
                }
                try {
                    args.add(Long.parseLong(text.substring(start, i)));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unsupported mask argument: " + transformer, e);
                }
            }
            while (i < text.length() && Character.isWhitespace(text.charAt(i))) {
                i++;
            }
            if (i < text.length()) {
                if (text.charAt(i) != ',') {
                    throw new IllegalArgumentException("unsupported mask argument: " + transformer);
                }
                i++;
                if (i >= text.length()) {
                    throw new IllegalArgumentException("unsupported mask argument: " + transformer);
                }
            }
        }
        return args;
    }

    /**
     * Writes the mask of one column as a DuckDB expression of the column's own type.
     *
     * @param column SQL reference to the column (already quoted)
     * @param hiveType the column's Hive type as the metastore stores it ({@code string}, {@code char(10)}, ...)
     * @param duckDbType the column's DuckDB type as the server shows it
     * @return DuckDB expression
     * @throws IllegalArgumentException when Hive would refuse the mask for this type (non-primitive columns)
     */
    public String sql(String column, String hiveType, String duckDbType) {
        String type = hiveType.trim().toLowerCase(Locale.ROOT).replace(" ", "");
        String nullValue = "CAST(NULL AS " + duckDbType + ")";
        if (kind == Kind.NONE) {
            return column;
        }
        if (kind == Kind.NULL) {
            return nullValue;
        }
        if (type.startsWith("array<") || type.startsWith("map<") || type.startsWith("struct<")
                || type.startsWith("uniontype<")) {
            throw new IllegalArgumentException("Hive masks only primitive columns, not " + hiveType);
        }
        int bits = switch (type) {
            case "tinyint" -> 8;
            case "smallint" -> 16;
            case "int", "integer" -> 32;
            case "bigint" -> 64;
            default -> 0;
        };
        if (bits > 0) {
            return kind == Kind.HASH ? nullValue : numberSql(column, bits, duckDbType);
        }
        if ("date".equals(type)) {
            return kind == Kind.HASH ? nullValue : dateSql(column);
        }
        boolean isChar = type.startsWith("char(");
        if (!"string".equals(type) && !type.startsWith("varchar(") && !isChar) {
            // Hive's mask UDFs answer NULL for every other primitive type.
            return nullValue;
        }
        String source = column;
        if (isChar) {
            int length = Integer.parseInt(type.substring(5, type.length() - 1));
            // Hive masks the padded CHAR value; the server shows CHAR values without trailing blanks.
            source = "(CASE WHEN length(" + column + ") < " + length + " THEN rpad(" + column + ", " + length
                    + ", ' ') ELSE " + column + " END)";
        }
        String text = kind == Kind.HASH ? "sha256(" + source + ")" : stringSql(source);
        if (isChar) {
            text = "rtrim(" + text + ", ' ')";
        }
        return "CASE WHEN " + column + " IS NULL THEN NULL ELSE CAST(" + text + " AS " + duckDbType + ") END";
    }

    /**
     * Writes the character masks of a string as Hive applies them per UTF-16 char.
     *
     * @param source string expression
     * @return masked string expression
     */
    private String stringSql(String source) {
        String chars = "string_split(" + source + ", '')";
        if (kind == Kind.MASK) {
            return "array_to_string(list_transform(" + chars + ", c -> " + charSql() + "), '')";
        }
        String shown = kind == Kind.SHOW_FIRST ? "i <= " + charCount
                : "i > length(" + source + ") - " + charCount;
        // Hive counts UTF-16 chars; a value with characters outside the BMP that it masks in part fails.
        String utf16Length = "(length(" + source + ") + length(regexp_replace(" + source + ", "
                + OUTSIDE_BMP.replace("[", "[^") + ", '', 'g')))";
        return "(CASE WHEN " + utf16Length + " <= " + charCount + " THEN " + source + " WHEN regexp_matches("
                + source + ", " + OUTSIDE_BMP + ") THEN error('Ranger mask of a value with characters outside"
                + " the Basic Multilingual Plane is not supported') ELSE array_to_string(list_transform("
                + chars + ", (c, i) -> CASE WHEN " + shown + " THEN c ELSE " + charSql() + " END), '') END)";
    }

    /**
     * Writes Hive's transformChar for one code point {@code c}.
     *
     * @return expression of the replaced character
     */
    private String charSql() {
        // A character outside the BMP is two surrogate chars in Java, whose type is neither letter nor digit.
        String outside = other == UNMASKED ? "c" : literal(other) + " || " + literal(other);
        // string_split of an empty value gives one empty element.
        return "CASE WHEN c = '' THEN c WHEN unicode(c) > 65535 THEN " + outside
                + " WHEN regexp_full_match(c, '\\p{Lu}') THEN " + replacement(upper)
                + " WHEN regexp_full_match(c, '\\p{Ll}') THEN " + replacement(lower)
                + " WHEN regexp_full_match(c, '\\p{Nd}') THEN " + replacement(digit)
                + " ELSE " + replacement(other) + " END";
    }

    /**
     * Writes one replacement character, or the original when unmasked.
     *
     * @param codePoint code point or -1
     * @return expression
     */
    private static String replacement(int codePoint) {
        return codePoint == UNMASKED ? "c" : literal(codePoint);
    }

    /**
     * Writes one code point as a SQL string literal.
     *
     * @param codePoint code point
     * @return literal
     */
    private static String literal(int codePoint) {
        return "'" + new String(Character.toChars(codePoint)).replace("'", "''") + "'";
    }

    /**
     * Writes a number mask with Java's integer overflow of the column's width.
     *
     * @param column column reference
     * @param bits 8, 16, 32 or 64
     * @param duckDbType result type
     * @return expression
     */
    private String numberSql(String column, int bits, String duckDbType) {
        long min = -(1L << (bits - 1));
        String abs = "abs(CAST(" + column + " AS HUGEINT))";
        String digits = "(CASE WHEN " + column + " = 0 THEN 0 ELSE length(CAST(" + abs + " AS VARCHAR)) END)";
        String masked = switch (kind) {
            case MASK -> number + " * " + ones(digits);
            case SHOW_LAST -> charCount >= 20 ? abs : "(CASE WHEN " + digits + " <= " + charCount + " THEN "
                    + abs + " ELSE " + abs + " % " + BigInteger.TEN.pow(charCount) + " + " + number
                    + " * CAST(repeat('1', " + digits + " - " + charCount + ") || repeat('0', " + charCount
                    + ") AS HUGEINT) END)";
            default -> "(" + abs + " // CAST('1' || repeat('0', " + digits + " - " + charCount + ") AS HUGEINT)"
                    + " * CAST('1' || repeat('0', " + digits + " - " + charCount + ") AS HUGEINT) + " + number
                    + " * " + ones(digits + " - " + charCount) + ")";
        };
        String signed = "(CASE WHEN " + column + " < 0 THEN -" + masked + " ELSE " + masked + " END)";
        BigInteger modulus = BigInteger.ONE.shiftLeft(bits);
        String wrapped = "(((" + signed + " % " + modulus + ") + " + modulus + ") % " + modulus + ")";
        String value = "(CASE WHEN " + wrapped + " >= " + modulus.shiftRight(1) + " THEN " + wrapped + " - "
                + modulus + " ELSE " + wrapped + " END)";
        if (kind == Kind.SHOW_FIRST) {
            // Hive returns the value itself when no digit is masked.
            value = "(CASE WHEN " + digits + " <= " + charCount + " THEN CAST(" + column + " AS HUGEINT) ELSE "
                    + value + " END)";
        }
        return "CASE WHEN " + column + " IS NULL THEN NULL WHEN " + column + " = " + min + " THEN CAST("
                + HiveMaskNumbers.mask(min, bits, kind == Kind.MASK ? -1 : charCount, kind == Kind.SHOW_FIRST,
                        number)
                + " AS " + duckDbType + ") ELSE CAST(" + value + " AS " + duckDbType + ") END";
    }

    /**
     * Writes the number with {@code count} ones as digits (0 for none).
     *
     * @param count digit count expression
     * @return HUGEINT expression
     */
    private static String ones(String count) {
        return "CAST(COALESCE(NULLIF(repeat('1', " + count + "), ''), '0') AS HUGEINT)";
    }

    /**
     * Writes Hive's date mask: each of year, month and day fixed or kept.
     *
     * @param column column reference
     * @return expression
     */
    private String dateSql(String column) {
        if (year != UNMASKED && month != UNMASKED && day != UNMASKED) {
            // Hive's Date.of fails on an impossible day such as 31 February; so does the constant here.
            long epochDay;
            try {
                epochDay = LocalDate.of(year, month + 1, day).toEpochDay();
            } catch (DateTimeException e) {
                throw new IllegalArgumentException("mask gives an impossible date", e);
            }
            return "CASE WHEN " + column + " IS NULL THEN NULL ELSE CAST(DATE '1970-01-01' + " + epochDay
                    + " AS DATE) END";
        }
        String y = year == UNMASKED ? "year(" + column + ")" : Integer.toString(year);
        String m = month == UNMASKED ? "month(" + column + ")" : Integer.toString(month + 1);
        String d = day == UNMASKED ? "day(" + column + ")" : Integer.toString(day);
        return "CASE WHEN " + column + " IS NULL THEN NULL ELSE make_date(" + y + ", " + m + ", " + d + ") END";
    }
}
