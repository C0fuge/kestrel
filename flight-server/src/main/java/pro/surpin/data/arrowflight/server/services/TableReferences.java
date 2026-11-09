package pro.surpin.data.arrowflight.server.services;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.services.TimeTravelClause.Kind;
import pro.surpin.data.arrowflight.server.services.TimeTravelClause.Token;

/**
 * Finds the references of a client's SQL to some tables named with their schema
 * ({@code FROM hive.orders o}, {@code JOIN "hive"."orders"}, after a comma of a FROM list), so
 * that each can be replaced by a source of its own. Only a name right after FROM, JOIN or a comma
 * of a FROM list counts; a reference the finder misses keeps its name and fails as a missing
 * table, never reads other data. String literals, quoted identifiers and comments are skipped
 * (the tokens of {@link TimeTravelClause}).
 */
final class TableReferences {

    /** Words that end the table list of a FROM clause at their parenthesis depth. */
    private static final Set<String> END_FROM = Set.of("WHERE", "GROUP", "ORDER", "HAVING",
            "LIMIT", "OFFSET", "QUALIFY", "WINDOW", "UNION", "EXCEPT", "INTERSECT", "ON", "USING",
            "SELECT", "RETURNING", "FETCH", "VALUES", "SET");

    /**
     * One table reference.
     *
     * @param start offset of the schema name
     * @param end offset just after the table name
     * @param schema schema name, unquoted
     * @param table table name, unquoted
     * @param aliased whether the client gave the reference an alias
     * @param wholeRow whether the query may name the table's whole row (its alias, or its name
     *        without an alias, used as a value and not as a qualifier)
     */
    record Reference(int start, int end, String schema, String table, boolean aliased,
            boolean wholeRow) {
    }

    private TableReferences() {
    }

    /**
     * Finds the references to the tables a test accepts.
     *
     * @param sql client SQL
     * @param wanted takes a schema and a table name as written (unquoted) and tells whether its
     *        references are wanted
     * @return references in text order, empty when there is none
     */
    static List<Reference> find(String sql, BiPredicate<String, String> wanted) {
        if (sql == null) {
            return List.of();
        }
        List<Token> tokens = TimeTravelClause.tokens(sql);
        List<Reference> result = new ArrayList<>();
        List<int[]> spans = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Deque<Boolean> outer = new ArrayDeque<>();
        boolean from = false;
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.kind() == Kind.OTHER && token.text().equals("(")) {
                outer.push(from);
                from = false;
                continue;
            }
            if (token.kind() == Kind.OTHER && token.text().equals(")")) {
                from = !outer.isEmpty() && outer.pop();
                continue;
            }
            if (token.kind() == Kind.WORD) {
                String upper = token.text().toUpperCase(Locale.ROOT);
                if (upper.equals("FROM") || upper.equals("JOIN")) {
                    from = true;
                } else if (END_FROM.contains(upper)) {
                    from = false;
                }
            }
            boolean starts = i > 0 && (TimeTravelClause.word(tokens.get(i - 1), "FROM")
                    || TimeTravelClause.word(tokens.get(i - 1), "JOIN")
                    || (from && tokens.get(i - 1).kind() == Kind.OTHER
                    && tokens.get(i - 1).text().equals(",")));
            if (!starts || i + 2 >= tokens.size() || !TimeTravelClause.identifier(token)
                    || tokens.get(i + 1).kind() != Kind.DOT
                    || !TimeTravelClause.identifier(tokens.get(i + 2))) {
                continue;
            }
            Token after = i + 3 < tokens.size() ? tokens.get(i + 3) : null;
            if (after != null && (after.kind() == Kind.DOT
                    || (after.kind() == Kind.OTHER && after.text().equals("(")))) {
                continue;   // a three-part name or a table function
            }
            String schema = TimeTravelClause.unquote(token);
            String table = TimeTravelClause.unquote(tokens.get(i + 2));
            if (!wanted.test(schema, table)) {
                continue;
            }
            int last = i + 2;
            String name = table;
            boolean aliased = false;
            if (after != null && TimeTravelClause.word(after, "AS") && i + 4 < tokens.size()
                    && TimeTravelClause.identifier(tokens.get(i + 4))) {
                aliased = true;
                last = i + 4;
            } else if (after != null && TimeTravelClause.identifier(after)) {
                aliased = true;
                last = i + 3;
            }
            if (aliased) {
                name = TimeTravelClause.unquote(tokens.get(last));
            }
            result.add(new Reference(token.start(), tokens.get(i + 2).end(), schema, table,
                    aliased, false));
            spans.add(new int[] {i, last});
            names.add(name);
            i = last;
        }
        if (result.isEmpty()) {
            return List.of();
        }
        List<Reference> withRows = new ArrayList<>(result.size());
        for (int r = 0; r < result.size(); r++) {
            Reference reference = result.get(r);
            boolean whole = namedAsValue(tokens, spans, names.get(r));
            withRows.add(new Reference(reference.start(), reference.end(), reference.schema(),
                    reference.table(), reference.aliased(), whole));
        }
        return withRows;
    }

    /**
     * Prefixes the table names a query writes without a schema ({@code FROM lineitem},
     * {@code JOIN orders o}, after a comma of a FROM list) with the schema a resolver names, so
     * that every later step (planning, locality checks, local copies) sees the table it reads.
     * DuckDB binds {@code tpch.lineitem} under the name {@code lineitem}, so column qualifiers
     * keep working. A name defined by the query itself (a CTE or a named window) is never
     * prefixed; a reference the finder misses keeps its plain name, so callers must not assume
     * every plain name is gone.
     *
     * @param sql client SQL
     * @param schemaOf takes a plain table name as written (unquoted) and returns the schema to
     *        prefix it with, or null to leave it alone
     * @return the SQL with the found names prefixed, the same instance when nothing changed
     */
    static String qualifyPlainNames(String sql, Function<String, String> schemaOf) {
        if (sql == null) {
            return null;
        }
        List<Token> tokens = TimeTravelClause.tokens(sql);
        Set<String> defined = definedNames(tokens);
        StringBuilder out = null;
        int at = 0;
        Deque<Boolean> outer = new ArrayDeque<>();
        boolean from = false;
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.kind() == Kind.OTHER && token.text().equals("(")) {
                outer.push(from);
                from = false;
                continue;
            }
            if (token.kind() == Kind.OTHER && token.text().equals(")")) {
                from = !outer.isEmpty() && outer.pop();
                continue;
            }
            if (token.kind() == Kind.WORD) {
                String upper = token.text().toUpperCase(Locale.ROOT);
                if (upper.equals("FROM") || upper.equals("JOIN")) {
                    from = true;
                    continue;
                } else if (END_FROM.contains(upper)) {
                    from = false;
                }
            }
            boolean starts = i > 0 && (TimeTravelClause.word(tokens.get(i - 1), "FROM")
                    || TimeTravelClause.word(tokens.get(i - 1), "JOIN")
                    || (from && tokens.get(i - 1).kind() == Kind.OTHER
                    && tokens.get(i - 1).text().equals(",")));
            if (!starts || !TimeTravelClause.identifier(token)) {
                continue;
            }
            Token after = i + 1 < tokens.size() ? tokens.get(i + 1) : null;
            if (after != null && (after.kind() == Kind.DOT
                    || (after.kind() == Kind.OTHER && after.text().equals("(")))) {
                continue;   // a qualified name or a table function
            }
            String name = TimeTravelClause.unquote(token);
            if (defined.contains(name.toLowerCase(Locale.ROOT))) {
                continue;
            }
            String schema = schemaOf.apply(name);
            if (schema == null) {
                continue;
            }
            if (out == null) {
                out = new StringBuilder(sql.length() + 64);
            }
            out.append(sql, at, token.start()).append(DuckDbAdapter.quoteIdentifier(schema)).append('.');
            at = token.start();
        }
        return out == null ? sql : out.append(sql, at, sql.length()).toString();
    }

    /**
     * Finds an identifier of a query that a predicate accepts and that is neither qualified nor
     * a qualifier ({@code x} in {@code x.y}) nor a function name: a conservative check for names
     * a reference finder may have missed.
     *
     * @param sql SQL text
     * @param accepts takes an unquoted identifier and tells whether it counts
     * @return the first such identifier unquoted, or null
     */
    static String unqualifiedName(String sql, java.util.function.Predicate<String> accepts) {
        List<Token> tokens = TimeTravelClause.tokens(sql);
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (!TimeTravelClause.identifier(token)) {
                continue;
            }
            Token before = i > 0 ? tokens.get(i - 1) : null;
            Token after = i + 1 < tokens.size() ? tokens.get(i + 1) : null;
            if (before != null && before.kind() == Kind.DOT
                    || after != null && (after.kind() == Kind.DOT
                    || after.kind() == Kind.OTHER && after.text().equals("("))) {
                continue;
            }
            String name = TimeTravelClause.unquote(token);
            if (accepts.test(name)) {
                return name;
            }
        }
        return null;
    }

    /**
     * Names the query defines itself: CTEs ({@code name AS (}, {@code name AS [NOT]
     * MATERIALIZED (}, {@code name(columns) AS (}) and named windows, in lower case. Any
     * identifier written that way counts, which may leave a real table unqualified but never
     * qualifies a name the query defines.
     *
     * @param tokens tokens of the SQL
     * @return names in lower case
     */
    private static Set<String> definedNames(List<Token> tokens) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i + 1 < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (!TimeTravelClause.identifier(token)) {
                continue;
            }
            int next = i + 1;
            if (tokens.get(next).kind() == Kind.OTHER && tokens.get(next).text().equals("(")) {
                int depth = 0;
                for (; next < tokens.size(); next++) {
                    Token t = tokens.get(next);
                    if (t.kind() == Kind.OTHER && t.text().equals("(")) {
                        depth++;
                    } else if (t.kind() == Kind.OTHER && t.text().equals(")") && --depth == 0) {
                        break;
                    }
                }
                next++;
            }
            if (next >= tokens.size() || !TimeTravelClause.word(tokens.get(next), "AS")) {
                continue;
            }
            int body = next + 1;
            if (body < tokens.size() && TimeTravelClause.word(tokens.get(body), "NOT")) {
                body++;
            }
            if (body < tokens.size() && TimeTravelClause.word(tokens.get(body), "MATERIALIZED")) {
                body++;
            }
            if (body < tokens.size() && tokens.get(body).kind() == Kind.OTHER
                    && tokens.get(body).text().equals("(")) {
                names.add(TimeTravelClause.unquote(token).toLowerCase(Locale.ROOT));
            }
        }
        return names;
    }

    /** Words that start a query right after an opening parenthesis (a subquery). */
    private static final Set<String> QUERY_STARTS = Set.of("SELECT", "FROM", "TABLE", "VALUES",
            "WITH", "PIVOT", "UNPIVOT", "DESCRIBE", "SHOW", "SUMMARIZE");

    /**
     * Tells whether a SQL text holds a nested query: a second SELECT, or a query started right
     * after an opening parenthesis ({@code IN (FROM t)}). String literals, quoted identifiers and
     * comments do not count.
     *
     * @param sql SQL text
     * @return true when the text holds a subquery
     */
    static boolean nestedQuery(String sql) {
        List<Token> tokens = TimeTravelClause.tokens(sql);
        int selects = 0;
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.kind() == Kind.WORD && token.text().equalsIgnoreCase("SELECT") && ++selects > 1) {
                return true;
            }
            if (token.kind() == Kind.OTHER && token.text().equals("(") && i + 1 < tokens.size()
                    && tokens.get(i + 1).kind() == Kind.WORD
                    && QUERY_STARTS.contains(tokens.get(i + 1).text().toUpperCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Tells whether a SQL text is one plain SELECT: one SELECT and one FROM, no set operation, no
     * WITH clause (so no subquery and no second query block whose filters could be mistaken for
     * the table's own).
     *
     * @param sql SQL text
     * @return true for a single query block
     */
    static boolean singleSelect(String sql) {
        int selects = 0;
        int froms = 0;
        for (Token token : TimeTravelClause.tokens(sql)) {
            if (token.kind() != Kind.WORD) {
                continue;
            }
            switch (token.text().toUpperCase(Locale.ROOT)) {
                case "SELECT" -> selects++;
                case "FROM" -> froms++;
                case "UNION", "EXCEPT", "INTERSECT", "WITH" -> {
                    return false;
                }
                default -> {
                    // Other words do not open a query block.
                }
            }
        }
        return selects == 1 && froms == 1;
    }

    /**
     * Replaces table references by other text.
     *
     * @param sql client SQL
     * @param references references {@link #find} returned for it
     * @param replacement text for each reference, in the same order
     * @return the SQL with the references replaced
     */
    static String rewrite(String sql, List<Reference> references, List<String> replacement) {
        StringBuilder out = new StringBuilder(sql.length() + 256);
        int at = 0;
        for (int i = 0; i < references.size(); i++) {
            Reference reference = references.get(i);
            out.append(sql, at, reference.start()).append(replacement.get(i));
            at = reference.end();
        }
        return out.append(sql, at, sql.length()).toString();
    }

    /**
     * Tells whether a name appears outside the references as a value: neither qualifying a
     * column ({@code o.id}) nor qualified itself ({@code x.o}). DuckDB reads such a name as the
     * whole row of the table it names, so the source must have all its columns.
     *
     * @param tokens tokens of the SQL
     * @param spans token ranges of the references, with their aliases
     * @param name alias or table name, unquoted
     * @return true when the name may stand for a row
     */
    private static boolean namedAsValue(List<Token> tokens, List<int[]> spans, String name) {
        for (int i = 0; i < tokens.size(); i++) {
            int index = i;
            if (spans.stream().anyMatch(span -> index >= span[0] && index <= span[1])) {
                continue;
            }
            Token token = tokens.get(i);
            if ((token.kind() != Kind.WORD && token.kind() != Kind.QUOTED)
                    || !TimeTravelClause.unquote(token).equalsIgnoreCase(name)) {
                continue;
            }
            boolean qualifier = i + 1 < tokens.size() && tokens.get(i + 1).kind() == Kind.DOT;
            boolean qualified = i > 0 && tokens.get(i - 1).kind() == Kind.DOT;
            if (!qualifier && !qualified) {
                return true;
            }
        }
        return false;
    }
}
