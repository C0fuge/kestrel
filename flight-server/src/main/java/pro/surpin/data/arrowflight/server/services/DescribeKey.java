package pro.surpin.data.arrowflight.server.services;

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
    /** Stands for a literal left out. */
    private static final char LITERAL = '\u0001';

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
        List<Token> tokens = TimeTravelClause.tokens(sql);
        int where = whereIndex(tokens);
        if (where < 0) {
            return sql;
        }
        StringBuilder key = null;
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
            } else if (literalOfComparison(tokens, i)) {
                if (key == null) {
                    key = new StringBuilder(sql.length() + SHAPE.length()).append(SHAPE);
                }
                key.append(sql, copied, token.start()).append(LITERAL);
                copied = token.end();
            }
        }
        return key == null ? sql : key.append(sql, copied, sql.length()).toString();
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
     * Tells whether a token is a small integer literal that is the whole right-hand side of a
     * comparison.
     *
     * @param tokens tokens of the text
     * @param i index of the token
     * @return true when it may be left out of the key
     */
    private static boolean literalOfComparison(List<Token> tokens, int i) {
        Token token = tokens.get(i);
        if (token.kind() != Kind.NUMBER || !SMALL_INTEGER.matcher(token.text()).matches()) {
            return false;
        }
        int before = i - 1;
        if (before >= 0 && tokens.get(before).kind() == Kind.MINUS) {
            before--;   // unary minus: -15
        }
        if (!COMPARISONS.contains(operatorEndingAt(tokens, before))) {
            return false;
        }
        if (i + 1 == tokens.size()) {
            return true;
        }
        Token next = tokens.get(i + 1);
        return isOther(next, ")")
                || next.kind() == Kind.WORD && FOLLOWERS.contains(next.text().toUpperCase(Locale.ROOT));
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
