package pro.surpin.data.arrowflight.server.services;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.spi.catalog.AsOf;

/**
 * Finds the time-travel clauses of a client's SQL, the form Trino and Spark use after a table
 * name: {@code FROM ns.t FOR VERSION AS OF 8213491011553364004} (a snapshot ID, or a quoted
 * branch or tag name) and {@code FOR TIMESTAMP AS OF TIMESTAMP '2026-10-01 00:00:00 UTC'} (the
 * latest snapshot committed at or before; the time zone is required). DuckDB has no such clause:
 * the server replaces each table reference with the clause by a read of that snapshot. String
 * literals, quoted identifiers and comments are skipped.
 */
public final class TimeTravelClause {

    /** Cheap test before tokenizing: no clause without these words. */
    private static final Pattern AS_OF = Pattern.compile("(?i)\\bas\\s+of\\b");
    /** Words after a table reference that start the next clause instead of naming an alias. */
    static final Set<String> NOT_ALIAS = Set.of("WHERE", "JOIN", "INNER", "LEFT", "RIGHT",
            "FULL", "OUTER", "CROSS", "NATURAL", "POSITIONAL", "ASOF", "ANTI", "SEMI", "LATERAL",
            "ON", "USING", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION", "EXCEPT",
            "INTERSECT", "WINDOW", "QUALIFY", "FETCH", "TABLESAMPLE", "PIVOT", "UNPIVOT",
            "RETURNING", "FOR", "WITH", "SELECT", "FROM", "AS", "VALUES");
    private static final Pattern PLAIN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern TIMESTAMP = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2})[ T](\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?)\\s*(\\S.*)?");

    /**
     * One table reference with a time-travel clause.
     *
     * @param start offset of the table name in the SQL
     * @param end offset just after the clause
     * @param nameParts parts of the table name, unquoted
     * @param point the point the clause names
     * @param aliased whether the client gave the reference an alias after the clause
     */
    public record Reference(int start, int end, List<String> nameParts, AsOf point,
            boolean aliased) {

        /**
         * Copies the name parts.
         *
         * @param start name offset
         * @param end clause end
         * @param nameParts name parts
         * @param point time-travel point
         * @param aliased whether an alias follows
         */
        public Reference {
            nameParts = List.copyOf(nameParts);
        }
    }

    /** Token kinds. */
    enum Kind { WORD, QUOTED, STRING, NUMBER, DOT, MINUS, OTHER }

    /**
     * One token of a SQL text.
     *
     * @param kind kind
     * @param start offset of its first character
     * @param end offset after its last character
     * @param text its text
     */
    record Token(Kind kind, int start, int end, String text) {
    }

    private TimeTravelClause() {
    }

    /**
     * Tells whether a query has a time-travel clause, without checking its parts.
     *
     * @param sql client SQL
     * @return true when at least one clause is present
     */
    public static boolean present(String sql) {
        if (sql == null || !AS_OF.matcher(sql).find()) {
            return false;
        }
        List<Token> tokens = tokens(sql);
        for (int i = 0; i + 3 < tokens.size(); i++) {
            if (clauseAt(tokens, i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds every table reference with a time-travel clause.
     *
     * @param sql client SQL
     * @return references in text order, empty when there is none
     * @throws IllegalArgumentException when a clause does not follow a table name or names no
     *         valid point
     */
    public static List<Reference> find(String sql) {
        if (sql == null || !AS_OF.matcher(sql).find()) {
            return List.of();
        }
        List<Token> tokens = tokens(sql);
        List<Reference> result = new ArrayList<>();
        for (int i = 0; i + 3 < tokens.size(); i++) {
            if (!clauseAt(tokens, i)) {
                continue;
            }
            boolean version = tokens.get(i + 1).text().equalsIgnoreCase("VERSION");
            String clause = version ? "FOR VERSION AS OF" : "FOR TIMESTAMP AS OF";
            // The table name: identifiers joined by dots, right before FOR.
            int first = i - 1;
            if (first < 0 || !identifier(tokens.get(first))) {
                throw new IllegalArgumentException(clause + " must follow a table name");
            }
            while (first >= 2 && tokens.get(first - 1).kind() == Kind.DOT
                    && identifier(tokens.get(first - 2))) {
                first -= 2;
            }
            List<String> parts = new ArrayList<>();
            for (int k = first; k < i; k += 2) {
                parts.add(unquote(tokens.get(k)));
            }
            int next = i + 4;
            AsOf point;
            if (version) {
                Token value = next < tokens.size() ? tokens.get(next) : null;
                boolean negative = value != null && value.kind() == Kind.MINUS
                        && next + 1 < tokens.size() && tokens.get(next + 1).kind() == Kind.NUMBER;
                if (negative) {
                    next++;
                    value = tokens.get(next);
                }
                if (value != null && value.kind() == Kind.NUMBER) {
                    try {
                        point = new AsOf.SnapshotId(Long.parseLong((negative ? "-" : "")
                                + value.text()));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(clause + " needs a snapshot ID (a"
                                + " 64-bit integer) or a quoted branch or tag name, got "
                                + value.text(), e);
                    }
                } else if (value != null && value.kind() == Kind.STRING) {
                    point = new AsOf.Ref(unquote(value));
                } else {
                    throw new IllegalArgumentException(clause + " needs a snapshot ID or a quoted"
                            + " branch or tag name");
                }
            } else {
                if (next < tokens.size() && tokens.get(next).kind() == Kind.WORD
                        && tokens.get(next).text().equalsIgnoreCase("TIMESTAMP")) {
                    next++;
                    if (next + 2 < tokens.size() && tokens.get(next).text().equalsIgnoreCase("WITH")
                            && tokens.get(next + 1).text().equalsIgnoreCase("TIME")
                            && tokens.get(next + 2).text().equalsIgnoreCase("ZONE")) {
                        next += 3;
                    }
                }
                if (next >= tokens.size() || tokens.get(next).kind() != Kind.STRING) {
                    throw new IllegalArgumentException(clause + " needs a timestamp literal with a"
                            + " time zone, e.g. TIMESTAMP '2026-10-01 00:00:00 UTC'");
                }
                point = new AsOf.Timestamp(instant(unquote(tokens.get(next))));
            }
            int end = tokens.get(next).end();
            Token after = next + 1 < tokens.size() ? tokens.get(next + 1) : null;
            boolean aliased = after != null && (after.kind() == Kind.QUOTED
                    || (after.kind() == Kind.WORD && (after.text().equalsIgnoreCase("AS")
                    || !NOT_ALIAS.contains(after.text().toUpperCase(Locale.ROOT)))));
            result.add(new Reference(tokens.get(first).start(), end, parts, point, aliased));
            i = next;
        }
        return result;
    }

    /**
     * Replaces every table reference with a time-travel clause.
     *
     * @param sql client SQL
     * @param references references {@link #find} returned for it
     * @param replacement text for each reference
     * @return the SQL with the references replaced
     */
    public static String rewrite(String sql, List<Reference> references,
            Function<Reference, String> replacement) {
        StringBuilder out = new StringBuilder(sql.length() + 256);
        int at = 0;
        for (Reference reference : references) {
            out.append(sql, at, reference.start()).append(replacement.apply(reference));
            at = reference.end();
        }
        return out.append(sql, at, sql.length()).toString();
    }

    /**
     * Removes the time-travel clauses and keeps the table names, for code that only needs to know
     * which tables a query reads (access checks, planning).
     *
     * @param sql client SQL
     * @return the SQL without clauses; unchanged when it has none
     * @throws IllegalArgumentException when a clause is malformed
     */
    public static String withoutClauses(String sql) {
        List<Reference> references = find(sql);
        if (references.isEmpty()) {
            return sql;
        }
        return rewrite(sql, references, r -> String.join(".", r.nameParts().stream()
                .map(TimeTravelClause::quote).toList()));
    }

    /**
     * Writes a name part as the client could have written it: plain identifiers unquoted, so
     * that the SQL without clauses parses exactly like the same query without time travel.
     *
     * @param name name part
     * @return identifier
     */
    private static String quote(String name) {
        return PLAIN.matcher(name).matches() && !NOT_ALIAS.contains(name.toUpperCase(Locale.ROOT))
                ? name : "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /**
     * Parses a timestamp literal that names its time zone: {@code UTC}, {@code Z}, an offset
     * such as {@code +03:00}, or a region such as {@code Europe/Vienna}.
     *
     * @param text literal text
     * @return the instant
     * @throws IllegalArgumentException when the text is no timestamp or has no time zone
     */
    static Instant instant(String text) {
        Matcher m = TIMESTAMP.matcher(text.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException("Not a timestamp: '" + text + "' (expected e.g."
                    + " '2026-10-01 00:00:00 UTC')");
        }
        if (m.group(3) == null) {
            throw new IllegalArgumentException("FOR TIMESTAMP AS OF needs a time zone in '" + text
                    + "' (e.g. UTC or +03:00): the server's local time is never assumed");
        }
        try {
            String zone = m.group(3).trim();
            ZoneId id = zone.equalsIgnoreCase("UTC") || zone.equalsIgnoreCase("Z")
                    ? ZoneOffset.UTC : ZoneId.of(zone);
            return LocalDate.parse(m.group(1)).atTime(LocalTime.parse(m.group(2))).atZone(id)
                    .toInstant();
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("Not a timestamp with a time zone: '" + text + "' ("
                    + e.getMessage() + ")", e);
        }
    }

    private static boolean clauseAt(List<Token> tokens, int i) {
        return word(tokens.get(i), "FOR")
                && (word(tokens.get(i + 1), "VERSION") || word(tokens.get(i + 1), "TIMESTAMP"))
                && word(tokens.get(i + 2), "AS") && word(tokens.get(i + 3), "OF");
    }

    /**
     * Tells whether a token is a given keyword.
     *
     * @param token token
     * @param text keyword
     * @return true when the token is that word, in any case
     */
    static boolean word(Token token, String text) {
        return token.kind() == Kind.WORD && token.text().equalsIgnoreCase(text);
    }

    /**
     * Tells whether a token can name a table or an alias.
     *
     * @param token token
     * @return true for a quoted identifier or a word that starts no clause
     */
    static boolean identifier(Token token) {
        return (token.kind() == Kind.WORD
                && !NOT_ALIAS.contains(token.text().toUpperCase(Locale.ROOT)))
                || token.kind() == Kind.QUOTED;
    }

    /**
     * Returns the text of an identifier or string literal without its quotes.
     *
     * @param token token
     * @return unquoted text
     */
    static String unquote(Token token) {
        if (token.kind() == Kind.QUOTED) {
            return token.text().substring(1, token.text().length() - 1).replace("\"\"", "\"");
        }
        if (token.kind() == Kind.STRING) {
            return token.text().substring(1, token.text().length() - 1).replace("''", "'");
        }
        return token.text();
    }

    /**
     * Splits SQL into the tokens the clause is made of; comments and whitespace are dropped,
     * literals and quoted identifiers are kept whole.
     *
     * @param sql SQL text
     * @return tokens
     */
    static List<Token> tokens(String sql) {
        List<Token> tokens = new ArrayList<>();
        int n = sql.length();
        int i = 0;
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i);
                i = eol < 0 ? n : eol + 1;
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int close = sql.indexOf("*/", i + 2);
                i = close < 0 ? n : close + 2;
            } else if (c == '\'' || c == '"') {
                int j = i + 1;
                while (j < n) {
                    if (sql.charAt(j) == c) {
                        if (j + 1 < n && sql.charAt(j + 1) == c) {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j++;
                }
                int end = Math.min(j + 1, n);
                tokens.add(new Token(c == '\'' ? Kind.STRING : Kind.QUOTED, i, end,
                        sql.substring(i, end)));
                i = end;
            } else if (c == '$' && dollarTag(sql, i) != null) {
                String tag = dollarTag(sql, i);
                int close = sql.indexOf(tag, i + tag.length());
                int end = close < 0 ? n : close + tag.length();
                tokens.add(new Token(Kind.OTHER, i, end, sql.substring(i, end)));
                i = end;
            } else if (Character.isLetter(c) || c == '_') {
                int j = i + 1;
                while (j < n && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_'
                        || sql.charAt(j) == '$')) {
                    j++;
                }
                tokens.add(new Token(Kind.WORD, i, j, sql.substring(i, j)));
                i = j;
            } else if (Character.isDigit(c)) {
                int j = i + 1;
                while (j < n && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_'
                        || sql.charAt(j) == '.')) {
                    j++;
                }
                tokens.add(new Token(Kind.NUMBER, i, j, sql.substring(i, j)));
                i = j;
            } else {
                Kind kind = c == '.' ? Kind.DOT : c == '-' ? Kind.MINUS : Kind.OTHER;
                tokens.add(new Token(kind, i, i + 1, String.valueOf(c)));
                i++;
            }
        }
        return tokens;
    }

    /**
     * Returns the dollar-quote tag starting at an offset ({@code $$} or {@code $name$}).
     *
     * @param sql SQL text
     * @param i offset of a {@code $}
     * @return the tag, or null when none starts there (for example a parameter {@code $1})
     */
    private static String dollarTag(String sql, int i) {
        int j = i + 1;
        while (j < sql.length() && (Character.isLetter(sql.charAt(j)) || sql.charAt(j) == '_')) {
            j++;
        }
        return j < sql.length() && sql.charAt(j) == '$' ? sql.substring(i, j + 1) : null;
    }
}
