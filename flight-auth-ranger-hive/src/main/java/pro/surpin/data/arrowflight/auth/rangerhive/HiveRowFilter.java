package pro.surpin.data.arrowflight.auth.rangerhive;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Translates the Hive expression of a Ranger row filter into a DuckDB condition that keeps the same rows. Only a
 * small typed subset is translated: column names of the table, string and integer literals, TRUE / FALSE / NULL,
 * comparisons of operands of one kind (text, number, date, boolean), IS [NOT] NULL, [NOT] IN, [NOT] BETWEEN, [NOT]
 * LIKE with a literal pattern, AND / OR / NOT, parentheses, {@code lower} / {@code upper}, and
 * {@code current_user()} (the caller's short name, as HiveServer2 gives it). Everything else is refused with the
 * reason, so the table is denied rather than filtered differently: other functions, arithmetic, casts, subqueries,
 * qualified names, comparisons of different kinds (Hive and DuckDB convert them differently), {@code ||}.
 */
final class HiveRowFilter {

    /** Kind of an operand, for the comparisons. */
    private enum Kind { TEXT, NUMBER, DATE, BOOLEAN, NULL, OTHER }

    /** A translated piece of the expression. */
    private record Expr(String sql, Kind kind) {
    }

    /** Comparison operators, Hive spelling to DuckDB spelling. */
    private static final Map<String, String> COMPARISONS = Map.of("=", "=", "==", "=", "<>", "<>", "!=", "<>",
            "<", "<", ">", ">", "<=", "<=", ">=", ">=");
    /** DuckDB types of number columns. */
    private static final Set<String> NUMBERS = Set.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
            "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "FLOAT", "REAL", "DOUBLE");

    private final List<String> tokens;
    private final Map<String, Map.Entry<String, String>> columns;
    private final String user;
    private int at;

    /**
     * Creates a translator over the tokens of one expression.
     *
     * @param tokens tokens
     * @param columns lower-case column name to the column's name and DuckDB type
     * @param user the caller's short name
     */
    private HiveRowFilter(List<String> tokens, Map<String, Map.Entry<String, String>> columns, String user) {
        this.tokens = tokens;
        this.columns = columns;
        this.user = user;
    }

    /**
     * Translates a row filter.
     *
     * @param expression Hive expression of the filter
     * @param tableColumns the table's columns, name to DuckDB type
     * @param user the caller's short name, for {@code current_user()}
     * @return DuckDB condition
     * @throws IllegalArgumentException with the reason when the filter cannot be translated exactly
     */
    static String sql(String expression, Map<String, String> tableColumns, String user) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("empty row filter");
        }
        Map<String, Map.Entry<String, String>> byName = new LinkedHashMap<>();
        tableColumns.forEach((name, type) -> byName.put(name.toLowerCase(Locale.ROOT), Map.entry(name, type)));
        HiveRowFilter filter = new HiveRowFilter(tokenize(expression), byName, user);
        Expr result = filter.or();
        if (filter.at != filter.tokens.size()) {
            throw refused("'" + filter.tokens.get(filter.at) + "'");
        }
        requireBoolean(result);
        return result.sql();
    }

    /**
     * Reads OR terms.
     *
     * @return the expression
     */
    private Expr or() {
        Expr left = and();
        while (keyword("or")) {
            left = logical(left, "OR", and());
        }
        return left;
    }

    /**
     * Reads AND terms.
     *
     * @return the expression
     */
    private Expr and() {
        Expr left = not();
        while (keyword("and")) {
            left = logical(left, "AND", not());
        }
        return left;
    }

    /**
     * Reads a negation or a predicate.
     *
     * @return the expression
     */
    private Expr not() {
        if (keyword("not")) {
            Expr operand = not();
            requireBoolean(operand);
            return new Expr("(NOT " + operand.sql() + ")", Kind.BOOLEAN);
        }
        return predicate();
    }

    /**
     * Reads an operand and the comparison that may follow it.
     *
     * @return the expression
     */
    private Expr predicate() {
        Expr left = operand();
        String next = peek();
        if (next != null && COMPARISONS.containsKey(next)) {
            at++;
            Expr right = operand();
            sameKind(left, right);
            return new Expr("(" + left.sql() + " " + COMPARISONS.get(next) + " " + right.sql() + ")", Kind.BOOLEAN);
        }
        if (keyword("is")) {
            boolean negated = keyword("not");
            expectKeyword("null");
            return new Expr("(" + left.sql() + (negated ? " IS NOT NULL)" : " IS NULL)"), Kind.BOOLEAN);
        }
        boolean negated = keyword("not");
        if (keyword("in")) {
            expect("(");
            List<String> items = new ArrayList<>();
            do {
                Expr item = operand();
                sameKind(left, item);
                items.add(item.sql());
            } while (accept(","));
            expect(")");
            return new Expr("(" + left.sql() + (negated ? " NOT IN (" : " IN (") + String.join(", ", items) + "))",
                    Kind.BOOLEAN);
        }
        if (keyword("between")) {
            Expr low = operand();
            expectKeyword("and");
            Expr high = operand();
            sameKind(left, low);
            sameKind(left, high);
            return new Expr("(" + left.sql() + (negated ? " NOT BETWEEN " : " BETWEEN ") + low.sql() + " AND "
                    + high.sql() + ")", Kind.BOOLEAN);
        }
        if (keyword("like")) {
            String pattern = next();
            if (pattern == null || !pattern.startsWith("'")) {
                throw refused("LIKE with a pattern that is not a literal");
            }
            if (left.kind() != Kind.TEXT) {
                throw refused("LIKE on a value that is not text");
            }
            // Hive's LIKE escapes % and _ with a backslash; DuckDB's has no escape by default.
            return new Expr("(" + left.sql() + (negated ? " NOT LIKE " : " LIKE ") + pattern + " ESCAPE '\\')",
                    Kind.BOOLEAN);
        }
        if (negated) {
            throw refused("NOT after a value");
        }
        return left;
    }

    /**
     * Reads an operand: a column, a literal, a parenthesised condition or an allowed function call.
     *
     * @return the operand
     */
    private Expr operand() {
        String token = next();
        if (token == null) {
            throw refused("an incomplete expression");
        }
        if (token.equals("(")) {
            Expr inner = or();
            expect(")");
            return new Expr("(" + inner.sql() + ")", inner.kind());
        }
        if (token.equals("-")) {
            String number = next();
            if (number == null || !Character.isDigit(number.charAt(0))) {
                throw refused("operator -");
            }
            return new Expr("(-" + number + ")", Kind.NUMBER);
        }
        if (token.startsWith("'")) {
            return new Expr(token, Kind.TEXT);
        }
        if (Character.isDigit(token.charAt(0))) {
            return new Expr(token, Kind.NUMBER);
        }
        if (token.startsWith("`")) {
            return column(token.substring(1));
        }
        if (!Character.isLetter(token.charAt(0)) && token.charAt(0) != '_') {
            throw refused("operator " + token);
        }
        String word = token.toLowerCase(Locale.ROOT);
        if (accept("(")) {
            return call(word);
        }
        switch (word) {
            case "true", "false" -> {
                return new Expr(word.toUpperCase(Locale.ROOT), Kind.BOOLEAN);
            }
            case "null" -> {
                return new Expr("NULL", Kind.NULL);
            }
            default -> {
                if (".".equals(peek())) {
                    throw refused("qualified name " + token);
                }
                return column(token);
            }
        }
    }

    /**
     * Reads the arguments of a function call after its opening parenthesis.
     *
     * @param function lower-case function name
     * @return the call
     */
    private Expr call(String function) {
        switch (function) {
            case "current_user" -> {
                expect(")");
                return new Expr("'" + user.replace("'", "''") + "'", Kind.TEXT);
            }
            case "lower", "lcase", "upper", "ucase" -> {
                Expr argument = operand();
                expect(")");
                if (argument.kind() != Kind.TEXT && argument.kind() != Kind.NULL) {
                    throw refused(function + " of a value that is not text");
                }
                String name = function.startsWith("l") ? "lower" : "upper";
                return new Expr(name + "(" + argument.sql() + ")", Kind.TEXT);
            }
            default -> throw refused("function " + function);
        }
    }

    /**
     * Resolves a column of the table.
     *
     * @param name column name as written
     * @return the column
     */
    private Expr column(String name) {
        if (".".equals(peek())) {
            throw refused("qualified name " + name);
        }
        Map.Entry<String, String> column = columns.get(name.toLowerCase(Locale.ROOT));
        if (column == null) {
            throw refused("unknown column or keyword " + name);
        }
        return new Expr("\"" + column.getKey().replace("\"", "\"\"") + "\"", kind(column.getValue()));
    }

    /**
     * Names the kind of a DuckDB column type.
     *
     * @param duckDbType DuckDB type
     * @return its kind
     */
    private static Kind kind(String duckDbType) {
        String type = duckDbType.trim().toUpperCase(Locale.ROOT);
        if (type.equals("VARCHAR")) {
            return Kind.TEXT;
        }
        if (NUMBERS.contains(type) || type.startsWith("DECIMAL")) {
            return Kind.NUMBER;
        }
        if (type.equals("DATE")) {
            return Kind.DATE;
        }
        if (type.equals("BOOLEAN")) {
            return Kind.BOOLEAN;
        }
        return Kind.OTHER;
    }

    /**
     * Joins two conditions.
     *
     * @param left left condition
     * @param operator AND or OR
     * @param right right condition
     * @return the joined condition
     */
    private static Expr logical(Expr left, String operator, Expr right) {
        requireBoolean(left);
        requireBoolean(right);
        return new Expr("(" + left.sql() + " " + operator + " " + right.sql() + ")", Kind.BOOLEAN);
    }

    /**
     * Requires a condition (or NULL, which keeps no row in both engines).
     *
     * @param expr operand of a logical operator
     */
    private static void requireBoolean(Expr expr) {
        if (expr.kind() != Kind.BOOLEAN && expr.kind() != Kind.NULL) {
            throw refused("a logical operator on a value that is not a condition");
        }
    }

    /**
     * Requires two compared operands of one kind; NULL compares with anything.
     *
     * @param left left operand
     * @param right right operand
     */
    private static void sameKind(Expr left, Expr right) {
        if (left.kind() == Kind.NULL || right.kind() == Kind.NULL) {
            return;
        }
        if (left.kind() == Kind.OTHER || right.kind() == Kind.OTHER || left.kind() != right.kind()) {
            throw refused("a comparison of " + left.kind() + " with " + right.kind());
        }
    }

    /**
     * Returns the next token without reading it.
     *
     * @return the token, null at the end
     */
    private String peek() {
        return at < tokens.size() ? tokens.get(at) : null;
    }

    /**
     * Reads the next token.
     *
     * @return the token, null at the end
     */
    private String next() {
        return at < tokens.size() ? tokens.get(at++) : null;
    }

    /**
     * Reads a token when it is the given one.
     *
     * @param token token text
     * @return whether it was read
     */
    private boolean accept(String token) {
        if (token.equals(peek())) {
            at++;
            return true;
        }
        return false;
    }

    /**
     * Reads a keyword (a bare word, any case) when it is next.
     *
     * @param keyword lower-case keyword
     * @return whether it was read
     */
    private boolean keyword(String keyword) {
        String token = peek();
        if (token != null && !token.startsWith("`") && token.equalsIgnoreCase(keyword)) {
            at++;
            return true;
        }
        return false;
    }

    /**
     * Requires a token.
     *
     * @param token token text
     */
    private void expect(String token) {
        if (!accept(token)) {
            throw refused("missing " + token);
        }
    }

    /**
     * Requires a keyword.
     *
     * @param keyword lower-case keyword
     */
    private void expectKeyword(String keyword) {
        if (!keyword(keyword)) {
            throw refused("missing " + keyword.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * Splits an expression into tokens: words, {@code `quoted`} names (kept with their opening backtick), string
     * literals (as DuckDB literals), integer literals, and operators.
     *
     * @param text Hive expression
     * @return tokens
     */
    static List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        int pos = 0;
        while (pos < text.length()) {
            char c = text.charAt(pos);
            if (Character.isWhitespace(c)) {
                pos++;
            } else if (text.startsWith("--", pos) || text.startsWith("/*", pos)) {
                throw refused("a comment");
            } else if (c == '`') {
                int end = text.indexOf('`', pos + 1);
                if (end < 0 || (end + 1 < text.length() && text.charAt(end + 1) == '`')) {
                    throw refused("a quoted name");
                }
                tokens.add("`" + text.substring(pos + 1, end));
                pos = end + 1;
            } else if (c == '\'' || c == '"') {
                StringBuilder value = new StringBuilder();
                pos = literal(text, pos, value);
                tokens.add("'" + value.toString().replace("'", "''") + "'");
            } else if (Character.isDigit(c)) {
                int start = pos;
                while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
                    pos++;
                }
                if (pos < text.length() && (Character.isLetter(text.charAt(pos)) || text.charAt(pos) == '.'
                        || text.charAt(pos) == '_')) {
                    throw refused("a number that is not an integer literal");
                }
                String number = text.substring(start, pos);
                if (number.length() > 9) {
                    // Hive types a longer literal as BIGINT or fails; keep to INT literals.
                    throw refused("a long integer literal");
                }
                tokens.add(number);
            } else if (Character.isLetter(c) || c == '_') {
                int start = pos;
                while (pos < text.length() && (Character.isLetterOrDigit(text.charAt(pos))
                        || text.charAt(pos) == '_')) {
                    pos++;
                }
                tokens.add(text.substring(start, pos));
            } else {
                String two = pos + 2 <= text.length() ? text.substring(pos, pos + 2) : "";
                if (Set.of("<>", "<=", ">=", "!=", "==").contains(two)) {
                    tokens.add(two);
                    pos += 2;
                } else if ("=<>(),-.".indexOf(c) >= 0) {
                    tokens.add(String.valueOf(c));
                    pos++;
                } else {
                    throw refused("operator " + c);
                }
            }
        }
        return tokens;
    }

    /**
     * Reads a string literal with Hive's escapes.
     *
     * @param text expression
     * @param start offset of the opening quote
     * @param value receives the value
     * @return offset after the closing quote
     */
    private static int literal(String text, int start, StringBuilder value) {
        char quote = text.charAt(start);
        int pos = start + 1;
        while (true) {
            if (pos >= text.length()) {
                throw refused("an unclosed string literal");
            }
            char c = text.charAt(pos++);
            if (c == quote) {
                return pos;
            }
            if (c == '\\') {
                if (pos >= text.length()) {
                    throw refused("an unclosed string literal");
                }
                char e = text.charAt(pos++);
                switch (e) {
                    case '\'', '"', '\\' -> value.append(e);
                    case 'n' -> value.append('\n');
                    case 't' -> value.append('\t');
                    case 'r' -> value.append('\r');
                    // Hive keeps the backslash of these, for LIKE patterns.
                    case '%', '_' -> value.append('\\').append(e);
                    default -> throw refused("string escape \\" + e);
                }
                continue;
            }
            value.append(c);
        }
    }

    /**
     * Makes the refusal for a construct.
     *
     * @param what the construct
     * @return exception naming it
     */
    private static IllegalArgumentException refused(String what) {
        return new IllegalArgumentException("row filter uses " + what + " (not translated)");
    }
}
