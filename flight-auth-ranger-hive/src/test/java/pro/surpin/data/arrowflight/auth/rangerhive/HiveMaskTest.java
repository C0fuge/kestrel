package pro.surpin.data.arrowflight.auth.rangerhive;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks that DuckDB computes Ranger's Hive masks with the values of Hive 3.1.3's mask UDFs. */
@Tag("unit")
class HiveMaskTest {
    private static final String[] STRINGS = {
        "", "a", "Ab1", "4111-1111-1111-1234", "John Smith", "x'y\"z", "Ünïcödé ÀÉ ß", "Ωμέγα 123",
        "٣٤ arabic-indic digits", "tab\tand\nnewline", "   ", "12", "1234", "12345", "émoji 😀 x",
        "𝐀 bold A"};
    private static Connection duckdb;

    /** Opens an in-memory DuckDB. */
    @BeforeAll
    static void open() throws SQLException {
        duckdb = DriverManager.getConnection("jdbc:duckdb:");
    }

    /** Closes DuckDB. */
    @AfterAll
    static void close() throws SQLException {
        duckdb.close();
    }

    /** Every mask type of Ranger's own Hive service definition is understood. */
    @Test
    void readsEveryMaskTypeOfRangersHiveServiceDefinition() throws Exception {
        Map<String, String> transformers = serviceDefinitionMasks();
        assertEquals(List.of("MASK", "MASK_SHOW_LAST_4", "MASK_SHOW_FIRST_4", "MASK_HASH", "MASK_NULL",
                "MASK_NONE", "MASK_DATE_SHOW_YEAR", "CUSTOM"), List.copyOf(transformers.keySet()));
        for (Map.Entry<String, String> type : transformers.entrySet()) {
            if ("CUSTOM".equals(type.getKey())) {
                assertThrows(IllegalArgumentException.class, () -> HiveMask.parse("CUSTOM", type.getValue()));
            } else {
                HiveMask mask = HiveMask.parse(type.getKey(), type.getValue());
                assertEquals("\"c\"", HiveMask.parse("MASK_NONE", null).sql("\"c\"", "string", "VARCHAR"));
                assertTrue(mask.sql("\"c\"", "string", "VARCHAR").contains("\"c\"")
                        || "MASK_NULL".equals(type.getKey()));
            }
        }
    }

    /** String, VARCHAR and CHAR masks equal Hive's per-char transformation. */
    @Test
    void stringMasksEqualHive() throws Exception {
        Map<String, String> transformers = serviceDefinitionMasks();
        List<String> calls = new ArrayList<>(List.of(transformers.get("MASK"), transformers.get("MASK_SHOW_LAST_4"),
                transformers.get("MASK_SHOW_FIRST_4"), transformers.get("MASK_DATE_SHOW_YEAR"),
                "mask({col}, 'Q', -1, '#', '*')", "mask({col}, \"\", 'l', 48, 95)", "mask_show_first_n({col}, 0)",
                "mask_show_last_n({col}, -3, -1, -1, 'd')", "mask_show_first_n({col}, '2', 'U', 'L', 'D', '.')",
                "MASK_SHOW_LAST_N( {col} , 20 )"));
        for (String call : calls) {
            Oracle oracle = Oracle.of(call);
            HiveMask mask = HiveMask.parse("X", call);
            for (String value : STRINGS) {
                boolean outsideBmp = value.codePoints().anyMatch(c -> c > 0xFFFF);
                if (outsideBmp && oracle.count >= 0 && value.length() > oracle.count) {
                    String sql = mask.sql("v", "string", "VARCHAR");
                    assertThrows(SQLException.class, () -> evaluate(sql, value), call + " " + value);
                    continue;
                }
                assertEquals(oracle.string(value), evaluate(mask.sql("v", "string", "VARCHAR"), value),
                        call + " of " + value);
                assertEquals(oracle.string(value), evaluate(mask.sql("v", "varchar(40)", "VARCHAR"), value));
            }
            // CHAR(n): Hive masks the padded value; the server shows it without trailing blanks.
            for (String value : new String[] {"ab", "abcdefgh", "Ab 1"}) {
                String padded = String.format("%-8s", value);
                assertEquals(oracle.string(padded).replaceAll(" +$", ""),
                        evaluate(mask.sql("v", "char(8)", "VARCHAR"), value), call + " of char " + value);
            }
        }
        assertNull(evaluate(HiveMask.parse("MASK", "mask({col})").sql("v", "string", "VARCHAR"), null));
    }

    /** mask_hash is SHA-256 of the text (of the padded text for CHAR) and NULL for other types. */
    @Test
    void hashMaskIsSha256() throws Exception {
        HiveMask hash = HiveMask.parse("MASK_HASH", "mask_hash({col})");
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                evaluate(hash.sql("v", "string", "VARCHAR"), "abc"));
        assertEquals(evaluate(hash.sql("v", "string", "VARCHAR"), "abc  "),
                evaluate(hash.sql("v", "char(5)", "VARCHAR"), "abc"));
        assertNull(query("SELECT " + hash.sql("v", "int", "INTEGER") + " FROM (VALUES (5)) t(v)"));
        assertNull(query("SELECT " + hash.sql("v", "date", "DATE") + " FROM (VALUES (DATE '2020-05-06')) t(v)"));
    }

    /** Integer masks equal Hive's digit arithmetic with Java overflow, for every width. */
    @Test
    void integerMasksEqualHive() throws Exception {
        assertEquals(11111L, HiveMaskNumbers.mask(12345, 32, -1, false, 1));
        assertEquals(-113456L, HiveMaskNumbers.mask(-123456, 32, 4, false, 1));
        assertEquals(123411L, HiveMaskNumbers.mask(123456, 32, 4, true, 1));
        assertEquals(-25L, HiveMaskNumbers.mask(127, 8, -1, false, 9));
        assertEquals(12L, HiveMaskNumbers.mask(12, 32, 4, true, 1));
        String[] types = {"tinyint", "smallint", "int", "bigint"};
        String[] duck = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT"};
        String[] calls = {"mask({col})", "mask({col}, 'x', 'x', 'x', -1, '9')", "mask_show_last_n({col}, 4, 'x', "
                + "'x', 'x', -1, '1')", "mask_show_first_n({col}, 4, 'x', 'x', 'x', -1, '1')",
            "mask_show_first_n({col}, 2, 'x', 'x', 'x', -1, '9')", "mask_show_last_n({col}, 0, 'x', 'x', 'x', -1, 7)",
            "mask_show_last_n({col}, 25)", "mask({col}, 'x', 'x', 'x', -1, 12)"};
        Random random = new Random(7);
        for (int t = 0; t < types.length; t++) {
            int bits = 8 << t;
            long min = -(1L << (bits - 1));
            long max = (1L << (bits - 1)) - 1;
            List<Long> values = new ArrayList<>(List.of(0L, 1L, -1L, 9L, 10L, -10L, 99L, max, min, min + 1));
            for (int i = 0; i < 40; i++) {
                values.add(bits == 64 ? random.nextLong() : min + (long) (random.nextDouble() * (max - min)));
                values.add((long) random.nextInt(100_000) % (max + 1));
            }
            for (String call : calls) {
                Oracle oracle = Oracle.of(call);
                String sql = HiveMask.parse("X", call).sql("v", types[t], duck[t]);
                for (long value : values) {
                    long expected = HiveMaskNumbers.mask(value, bits, oracle.count, oracle.first, oracle.number);
                    Object actual = query("SELECT " + sql + " FROM (VALUES (CAST(" + value + " AS " + duck[t]
                            + "))) t(v)");
                    assertEquals(expected, ((Number) actual).longValue(), call + " of " + types[t] + " " + value);
                }
            }
        }
    }

    /** Date masks set the masked parts as Hive does (month 0 = January); show masks use the date defaults. */
    @Test
    void dateMasksEqualHive() throws Exception {
        String[] calls = {"mask({col})", "mask({col}, 'x', 'x', 'x', -1, '1', 1, 0, -1)",
            "mask({col}, 'x', 'x', 'x', -1, '1', -1, 5, 1999)", "mask({col}, 'x', 'x', 'x', -1, '1', 40, 40, -1)",
            "mask_show_last_n({col}, 4)"};
        String[][] expected = {
            {"0000-01-01", "0000-01-01"}, {"2024-01-01", "1969-01-01"}, {"1999-06-29", "1999-06-31"},
            {"2024-01-01", "1969-01-01"}, {"0000-01-01", "0000-01-01"}};
        for (int i = 0; i < calls.length; i++) {
            String sql = HiveMask.parse("X", calls[i]).sql("v", "date", "DATE");
            assertEquals(LocalDate.parse(expected[i][0]), toDate(query("SELECT " + sql
                    + " FROM (VALUES (DATE '2024-02-29')) t(v)")), calls[i]);
            if (i == 2) {
                assertThrows(SQLException.class, () -> query("SELECT " + sql
                        + " FROM (VALUES (DATE '1969-12-31')) t(v)"));
            } else {
                assertEquals(LocalDate.parse(expected[i][1]), toDate(query("SELECT " + sql
                        + " FROM (VALUES (DATE '1969-12-31')) t(v)")), calls[i]);
            }
        }
        assertThrows(IllegalArgumentException.class,
                () -> HiveMask.parse("X", "mask({col}, 'x', 'x', 'x', -1, '1', 31, 1, 2000)").sql("v", "date", "DATE"));
    }

    /** Other primitive types become NULL of their type; complex types and unsupported transformers are refused. */
    @Test
    void otherTypesAndRefusals() throws Exception {
        HiveMask mask = HiveMask.parse("MASK", "mask({col})");
        assertNull(query("SELECT " + mask.sql("v", "decimal(10,2)", "DECIMAL(10,2)") + " FROM (VALUES (1.5)) t(v)"));
        assertNull(query("SELECT " + mask.sql("v", "timestamp", "TIMESTAMP") + " FROM (VALUES (now())) t(v)"));
        assertEquals("DECIMAL(10,2)", query("SELECT typeof(" + mask.sql("v", "decimal(10,2)", "DECIMAL(10,2)")
                + ") FROM (VALUES (1.5)) t(v)"));
        assertNull(query("SELECT " + HiveMask.parse("MASK_NULL", null).sql("v", "array<int>", "INTEGER[]")
                + " FROM (VALUES ([1])) t(v)"));
        assertThrows(IllegalArgumentException.class, () -> mask.sql("v", "array<string>", "VARCHAR[]"));
        assertThrows(IllegalArgumentException.class, () -> mask.sql("v", "struct<a:int>", "STRUCT(a INTEGER)"));
        for (String bad : new String[] {"upper({col})", "mask_first_n({col}, 4)", "mask('{col}')", "mask({col}",
            "mask({col}, 'x' || 'y')", "mask({col}, '\\'')", "mask({col}, 0)", "mask({col}, 55296)",
            "mask({col}, 'x', 'x', 'x', -1, 'one')", "mask({col}, 1, 2, 3, 4, 5, 6, 7, 8, 9)", "mask({col},)",
            "mask_hash({col}, 1)", "concat({col}, 'x')", "{col}", ""}) {
            assertThrows(IllegalArgumentException.class, () -> HiveMask.parse("X", bad), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> HiveMask.parse("MASK", null));
    }

    /**
     * Reads the mask types of Ranger's Hive service definition.
     *
     * @return transformer per mask type, in order
     */
    private static Map<String, String> serviceDefinitionMasks() throws Exception {
        try (InputStream in = HiveMaskTest.class.getResourceAsStream("/service-defs/ranger-servicedef-hive.json")) {
            JsonNode def = new ObjectMapper().readTree(Objects.requireNonNull(in));
            Map<String, String> types = new LinkedHashMap<>();
            for (JsonNode type : def.path("dataMaskDef").path("maskTypes")) {
                types.put(type.path("name").asText(), type.hasNonNull("transformer")
                        ? type.path("transformer").asText() : null);
            }
            return types;
        }
    }

    /**
     * Evaluates a mask over one string value.
     *
     * @param sql mask expression over column v
     * @param value input
     * @return result
     */
    private static String evaluate(String sql, String value) throws SQLException {
        try (PreparedStatement statement = duckdb.prepareStatement("SELECT " + sql
                + " FROM (SELECT CAST(? AS VARCHAR) AS v) t")) {
            statement.setString(1, value);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getString(1);
            }
        }
    }

    /**
     * Runs a one-value query.
     *
     * @param sql query
     * @return the value
     */
    private static Object query(String sql) throws SQLException {
        try (PreparedStatement statement = duckdb.prepareStatement(sql); ResultSet rows = statement.executeQuery()) {
            rows.next();
            return rows.getObject(1);
        }
    }

    /**
     * Reads a DuckDB date result.
     *
     * @param value JDBC value
     * @return the date
     */
    private static LocalDate toDate(Object value) {
        return value instanceof java.sql.Date date ? date.toLocalDate() : (LocalDate) value;
    }

    /**
     * Hive 3.1.3's string masks (MaskTransformer and its show-first / show-last subclasses), written from its
     * sources for the test.
     */
    private record Oracle(int count, boolean first, int[] chars, int number) {
        /**
         * Parses a transformer the simple way (no validation) for the test.
         *
         * @param call transformer
         * @return oracle
         */
        static Oracle of(String call) {
            String name = call.substring(0, call.indexOf('(')).trim().toLowerCase(java.util.Locale.ROOT);
            String inner = call.substring(call.indexOf('(') + 1, call.lastIndexOf(')'));
            List<String> args = new ArrayList<>();
            for (String part : inner.split(",")) {
                args.add(part.trim());
            }
            args.remove(0);
            int count = -1;
            if (!"mask".equals(name)) {
                count = args.isEmpty() ? 4 : Math.max(0, Integer.parseInt(unquote(args.remove(0))));
            }
            int[] chars = {'X', 'x', 'n', -1};
            for (int i = 0; i < 4 && i < args.size(); i++) {
                String a = args.get(i);
                if (a.startsWith("'") || a.startsWith("\"")) {
                    String text = unquote(a);
                    chars[i] = text.isEmpty() ? chars[i] : text.charAt(0);
                } else {
                    chars[i] = Integer.parseInt(a);
                }
            }
            int number = args.size() > 4 ? Integer.parseInt(unquote(args.get(4))) : 1;
            if (number < 0 || number > 9) {
                number = 1;
            }
            return new Oracle(count, name.contains("first"), chars, number);
        }

        /**
         * Removes quotes.
         *
         * @param text literal
         * @return content
         */
        private static String unquote(String text) {
            return text.startsWith("'") || text.startsWith("\"") ? text.substring(1, text.length() - 1) : text;
        }

        /**
         * Masks a string as Hive does, char by char.
         *
         * @param value input
         * @return masked
         */
        String string(String value) {
            if (count >= 0 && value.length() <= count) {
                return value;
            }
            StringBuilder ret = new StringBuilder(value.length());
            for (int i = 0; i < value.length(); i++) {
                boolean shown = count >= 0 && (first ? i < count : i >= value.length() - count);
                ret.appendCodePoint(shown ? value.charAt(i) : transformChar(value.charAt(i)));
            }
            return ret.toString();
        }

        /**
         * Hive's transformChar.
         *
         * @param c char
         * @return replacement
         */
        private int transformChar(int c) {
            int replaced = switch (Character.getType(c)) {
                case Character.UPPERCASE_LETTER -> chars[0];
                case Character.LOWERCASE_LETTER -> chars[1];
                case Character.DECIMAL_DIGIT_NUMBER -> chars[2];
                default -> chars[3];
            };
            return replaced == -1 ? c : replaced;
        }
    }
}
