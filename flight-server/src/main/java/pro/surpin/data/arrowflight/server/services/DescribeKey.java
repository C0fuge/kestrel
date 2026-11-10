package pro.surpin.data.arrowflight.server.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.server.services.TimeTravelClause.Kind;
import pro.surpin.data.arrowflight.server.services.TimeTravelClause.Token;

/**
 * Key of the describe cache that point queries differing only in their key share (item 40 (b)):
 * a client's {@code SELECT ... FROM s.t WHERE k = 5} and {@code ... WHERE k = 6} get one
 * description instead of one DuckDB describe per new key.
 *
 * <p>Only integer literals of at most nine digits (always DuckDB {@code INTEGER} literals) are
 * left out of the key, and only in the WHERE clause of a text with one SELECT and one FROM (no
 * subquery, CTE or set operation), where each is the whole right-hand side of a comparison
 * ({@code =, ==, <, >, <=, >=, <>, !=}, optionally after a unary minus) and is followed by the
 * end, {@code )}, {@code AND}, {@code OR} or the next clause. The answer's columns come from the
 * select list alone, so such a literal cannot change them, and swapping one {@code INTEGER}
 * literal for another leaves every type in the clause as it was: whether DuckDB binds the
 * comparison does not depend on the value (checked for every column type, see the test).
 * Every other text is its own key.
 */
public final class DescribeKey {

    /** An integer literal that is always an INTEGER in DuckDB (below 2^31). */
    private static final Pattern SMALL_INTEGER = Pattern.compile("[0-9]{1,9}");
    /** An integer literal of a pruning shape: any that fits a {@code long}. */
    private static final Pattern LONG_INTEGER = Pattern.compile("[0-9]{1,18}");
    /** A decimal literal of a pruning shape (no exponent). */
    private static final Pattern DECIMAL = Pattern.compile("[0-9]{1,18}\\.[0-9]{1,18}");
    /** The string of a {@code DATE '...'} literal of a pruning shape. */
    private static final Pattern DATE_STRING = Pattern.compile("'[0-9]{4}-[0-9]{2}-[0-9]{2}'");
    /** Comparison operators whose right-hand literal may be left out. */
    private static final Set<String> COMPARISONS = Set.of("=", "==", "<", ">", "<=", ">=", "<>", "!=");
    /** Words that may follow such a literal. */
    private static final Set<String> FOLLOWERS = Set.of("AND", "OR", "GROUP", "ORDER", "LIMIT",
            "HAVING", "QUALIFY", "WINDOW", "OFFSET", "FETCH");
    /** Words that end the WHERE clause at the top level. */
    private static final Set<String> WHERE_ENDS = Set.of("GROUP", "ORDER", "LIMIT", "HAVING",
            "QUALIFY", "WINDOW", "OFFSET", "FETCH");
    /** Prefix of a key with literals left out; a client text never starts with it. */
    private static final String SHAPE = "\u0000L";
    /** Prefix of a client text that holds a NUL itself, so it cannot look like a shape key. */
    private static final String RAW = "\u0000R";
    /** Prefix of a pruning shape key; never a describe key or a client text. */
    private static final String PRUNING_SHAPE = "\u0000P";
    /** Stands for a literal left out. */
    private static final char LITERAL = '\u0001';
    /** Stands for a decimal literal left out of a pruning shape. */
    private static final char DECIMAL_LITERAL = '\u0002';
    /** Stands for the string of a date literal left out of a pruning shape. */
    private static final char DATE_LITERAL = '\u0003';

    /** Kind of a literal left out of a shape. */
    public enum LiteralKind {
        /** An integer literal (digits only). */
        INTEGER,
        /** A decimal literal (digits, a point, digits). */
        DECIMAL,
        /** The quoted string of a {@code DATE 'yyyy-mm-dd'} literal. */
        DATE
    }

    private DescribeKey() {
    }

    /**
     * Returns the describe cache key of a client text.
     *
     * @param sql client SQL
     * @return the text with the literals described above left out (prefixed so that it differs
     *         from every client text), or the text itself when none is left out
     */
    public static String of(String sql) {
        if (sql == null) {
            return null;
        }
        if (sql.indexOf('\u0000') >= 0) {
            return RAW + sql;
        }
        Shape shape = shape(sql);
        return shape == null ? sql : shape.key();
    }

    /**
     * Returns the key of a client text with literals left out and where each one was.
     *
     * @param sql client SQL
     * @return the key and the literals' spans in the text, in order, or null when no literal is
     *         left out (the text is then its own key)
     */
    public static Shape shape(String sql) {
        return shape(sql, false);
    }

    /**
     * Returns the shape of a client text for file pruning (item 40 (b)): as {@link #shape(String)},
     * and also integer literals of up to 18 digits, decimal literals ({@code 1234.5}) and the
     * strings of {@code DATE 'yyyy-mm-dd'} literals in the same places are left out. Only the
     * values such literals compare with matter for pruning, not the types DuckDB gives them, so
     * this is no describe key.
     *
     * @param sql client SQL
     * @return the shape (its key differs from every describe key and client text), or null when
     *         no literal is left out
     */
    public static Shape pruningShape(String sql) {
        return shape(sql, true);
    }

    private static Shape shape(String sql, boolean pruning) {
        if (sql == null || sql.indexOf('\u0000') >= 0) {
            return null;
        }
        List<Token> tokens = TimeTravelClause.tokens(sql);
        int where = whereIndex(tokens);
        if (where < 0) {
            return null;
        }
        StringBuilder key = null;
        List<Span> literals = new ArrayList<>();
        int copied = 0;
        int depth = 0;
        for (int i = where + 1; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (isOther(token, "(")) {
                depth++;
            } else if (isOther(token, ")")) {
                depth--;
            } else if (depth == 0 && token.kind() == Kind.WORD
                    && WHERE_ENDS.contains(token.text().toUpperCase(Locale.ROOT))) {
                break;
            } else {
                LiteralKind kind = literalOfComparison(tokens, i, pruning);
                if (kind == null) {
                    continue;
                }
                if (key == null) {
                    key = new StringBuilder(sql.length() + SHAPE.length())
                            .append(pruning ? PRUNING_SHAPE : SHAPE);
                }
                char stand = switch (kind) {
                    case INTEGER -> LITERAL;
                    case DECIMAL -> DECIMAL_LITERAL;
                    case DATE -> DATE_LITERAL;
                };
                key.append(sql, copied, token.start()).append(stand);
                literals.add(new Span(token.start(), token.end(), kind));
                copied = token.end();
            }
        }
        return key == null ? null
                : new Shape(key.append(sql, copied, sql.length()).toString(), List.copyOf(literals));
    }

    /**
     * A text's describe cache key with literals left out, and where they were.
     *
     * @param key      the key ({@link #of(String)})
     * @param literals spans of the left-out literals in the text, in order
     */
    public record Shape(String key, List<Span> literals) {
    }

    /**
     * Where a left-out literal is in a text.
     *
     * @param start index of its first character
     * @param end   index after its last character
     * @param kind  what kind of literal it is (a date literal's span is its quoted string)
     */
    public record Span(int start, int end, LiteralKind kind) {
    }

    /**
     * Finds the WHERE of a text with one SELECT (its first word) and one FROM before the WHERE,
     * outside parentheses, and no other query block.
     *
     * @param tokens tokens of the text
     * @return index of the WHERE token, or -1
     */
    private static int whereIndex(List<Token> tokens) {
        int selects = 0;
        int froms = 0;
        int where = -1;
        int depth = 0;
        boolean first = true;
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (isOther(token, "(")) {
                depth++;
            } else if (isOther(token, ")")) {
                depth--;
            } else if (isOther(token, ";")) {
                return -1;   // more than one statement: its own key
            }
            if (token.kind() != Kind.WORD) {
                first = false;
                continue;
            }
            String word = token.text().toUpperCase(Locale.ROOT);
            if (first && !"SELECT".equals(word)) {
                return -1;
            }
            first = false;
            switch (word) {
                case "SELECT" -> selects++;
                case "FROM" -> froms++;
                case "WHERE" -> {
                    if (where >= 0 || depth != 0 || froms != 1) {
                        return -1;
                    }
                    where = i;
                }
                case "UNION", "EXCEPT", "INTERSECT", "WITH" -> {
                    return -1;
                }
                default -> {
                    // Other words do not open a query block.
                }
            }
        }
        return selects == 1 && froms == 1 ? where : -1;
    }

    /**
     * Tells whether a token is a literal that is the whole right-hand side of a comparison and
     * may be left out of a shape.
     *
     * @param tokens tokens of the text
     * @param i index of the token
     * @param pruning whether the shape is a pruning shape (more literals left out)
     * @return the literal's kind, or null when it is kept in the key
     */
    private static LiteralKind literalOfComparison(List<Token> tokens, int i, boolean pruning) {
        Token token = tokens.get(i);
        LiteralKind kind;
        int before = i - 1;
        if (token.kind() == Kind.NUMBER) {
            if ((pruning ? LONG_INTEGER : SMALL_INTEGER).matcher(token.text()).matches()) {
                kind = LiteralKind.INTEGER;
            } else if (pruning && DECIMAL.matcher(token.text()).matches()) {
                kind = LiteralKind.DECIMAL;
            } else {
                return null;
            }
            if (before >= 0 && tokens.get(before).kind() == Kind.MINUS) {
                before--;   // unary minus: -15
            }
        } else if (pruning && token.kind() == Kind.STRING && DATE_STRING.matcher(token.text()).matches()
                && before >= 0 && tokens.get(before).kind() == Kind.WORD
                && "DATE".equalsIgnoreCase(tokens.get(before).text())) {
            kind = LiteralKind.DATE;
            before--;
        } else {
            return null;
        }
        if (!COMPARISONS.contains(operatorEndingAt(tokens, before))) {
            return null;
        }
        if (i + 1 == tokens.size()) {
            return kind;
        }
        Token next = tokens.get(i + 1);
        return isOther(next, ")")
                || next.kind() == Kind.WORD && FOLLOWERS.contains(next.text().toUpperCase(Locale.ROOT))
                ? kind : null;
    }

    /**
     * Returns the operator whose last character is a token, made of the adjacent one-character
     * tokens before it ({@code <} {@code =} is {@code <=}).
     *
     * @param tokens tokens of the text
     * @param last index of its last token
     * @return the operator, or an empty string when the token is not an operator character
     */
    private static String operatorEndingAt(List<Token> tokens, int last) {
        StringBuilder op = new StringBuilder();
        int end = -1;
        for (int j = last; j >= 0; j--) {
            Token t = tokens.get(j);
            boolean operatorChar = (t.kind() == Kind.OTHER || t.kind() == Kind.MINUS)
                    && t.text().length() == 1 && "=<>!-:".contains(t.text());
            if (!operatorChar || end >= 0 && t.end() != end) {
                break;
            }
            op.insert(0, t.text());
            end = t.start();
        }
        return op.toString();
    }

    private static boolean isOther(Token token, String text) {
        return token.kind() == Kind.OTHER && token.text().equals(text);
    }
}
