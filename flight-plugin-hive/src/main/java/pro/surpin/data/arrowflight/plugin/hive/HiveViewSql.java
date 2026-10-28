package pro.surpin.data.arrowflight.plugin.hive;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Translates the expanded text of a simple Hive view ({@code viewExpandedText}, where Hive names
 * every table with its database) into DuckDB SQL that gives the same rows: projections, filters,
 * joins, grouping and unions over columns, with operators and functions whose results are the
 * same in both engines. Everything else is refused with the reason, never translated
 * approximately: casts, division, array and map subscripts, decimal literals without a suffix,
 * ORDER BY and LIMIT (Hive orders NULLs first, DuckDB last), window functions, lateral views,
 * subqueries named with WITH, and every function not listed here. The rows come out with the
 * view's declared column names and types.
 */
final class HiveViewSql {

    /**
     * A table the view reads.
     *
     * @param database Hive database
     * @param table Hive table
     */
    record Source(String database, String table) {
    }

    /**
     * A translated view.
     *
     * @param sql DuckDB {@code SELECT} with the view's columns, in their order
     * @param sources tables the view reads, in order of first use
     */
    record Translated(String sql, List<Source> sources) {
    }

    /** Keywords copied as they are. */
    private static final Set<String> KEYWORDS = Set.of("select", "distinct", "all", "from",
            "where", "group", "by", "having", "as", "and", "or", "not", "is", "null", "true",
            "false", "in", "between", "case", "when", "then", "else", "end", "join", "inner",
            "left", "right", "full", "outer", "cross", "on", "union", "like");
    /** Keywords after which a table name follows. */
    private static final Set<String> TABLE_FOLLOWS = Set.of("from", "join");
    /** Keywords that end a FROM clause. */
    private static final Set<String> FROM_ENDS = Set.of("where", "group", "having", "on",
            "union", "select");
    /** Hive functions DuckDB has with the same results, Hive name to DuckDB name. */
    private static final Map<String, String> FUNCTIONS = Map.ofEntries(
            Map.entry("count", "count"), Map.entry("sum", "sum"), Map.entry("min", "min"),
            Map.entry("max", "max"), Map.entry("coalesce", "coalesce"),
            Map.entry("nvl", "coalesce"), Map.entry("lower", "lower"),
            Map.entry("lcase", "lower"), Map.entry("upper", "upper"),
            Map.entry("ucase", "upper"), Map.entry("abs", "abs"), Map.entry("year", "year"),
            Map.entry("month", "month"), Map.entry("day", "day"),
            Map.entry("dayofmonth", "day"));
    /** Words of HiveQL or DuckDB that are never read as column names in a view. */
    private static final Set<String> REFUSED_WORDS = Set.of("rlike", "regexp", "div", "over",
            "window", "lateral", "view", "explode", "tablesample", "sort", "cluster",
            "distribute", "with", "values", "transform", "map", "reduce", "using", "semi",
            "anti", "exists", "interval", "partition", "rows", "range", "unique", "except",
            "intersect", "minus", "limit", "order", "nulls", "first", "last", "cast",
            "current_date", "current_timestamp", "grouping", "sets", "cube", "rollup",
            "qualify", "pivot", "unpivot", "asof", "positional", "natural", "lambda",
            "filter", "within", "escape", "similar", "glob", "ilike", "collate", "array",
            "struct", "named_struct", "if", "date", "timestamp", "time", "current_user",
            "row", "localtimestamp", "localtime", "default", "returning", "into", "sample");
    /** Operators copied as they are. */
    private static final Set<String> OPERATORS = Set.of("=", "<>", "<", ">", "<=", ">=", "+",
            "-", "||", ",", "(", ")", ".");

    private final String text;
    private final Function<String, String> schemas;
    private int pos;
    private final StringBuilder out = new StringBuilder();
    private final Set<Source> sources = new LinkedHashSet<>();
    /** Per parenthesis depth: whether the current clause is a FROM clause. */
    private final Deque<Boolean> inFrom = new ArrayDeque<>();
    private boolean tableNext;
    /** Whether a parenthesis opened where a table belongs, so a SELECT must follow. */
    private boolean subqueryNext;
    /** Whether the pattern of a LIKE is being written; its escape follows it. */
    private boolean likePending;
    private String previous = "";

    private HiveViewSql(String text, Function<String, String> schemas) {
        this.text = text;
        this.schemas = schemas;
    }

    /**
     * Translates a view.
     *
     * @param expandedText the view's {@code viewExpandedText}
     * @param columns the view's declared columns, name to Hive type, in order
     * @param schemas the server's schema name of a Hive database, null when it is not shown
     * @return the DuckDB query and the tables it reads
     * @throws IllegalArgumentException with the reason when the view cannot be translated
     */
    static Translated translate(String expandedText, List<Map.Entry<String, String>> columns,
            Function<String, String> schemas) {
        if (expandedText == null || expandedText.isBlank()) {
            throw new IllegalArgumentException("view without expanded text");
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("view without columns");
        }
        HiveViewSql translator = new HiveViewSql(expandedText, schemas);
        translator.inFrom.push(false);
        translator.run();
        if (translator.inFrom.size() != 1) {
            throw new IllegalArgumentException("unbalanced parentheses");
        }
        if (translator.sources.isEmpty()) {
            throw new IllegalArgumentException("view reads no table");
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        StringBuilder inner = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Map.Entry<String, String> column = columns.get(i);
            String type = HiveTypes.duckDb(column.getValue());
            if (i > 0) {
                sql.append(", ");
                inner.append(", ");
            }
            sql.append("CAST(").append(quote("_v" + i)).append(" AS ").append(type).append(") AS ")
                    .append(quote(column.getKey()));
            inner.append(quote("_v" + i));
        }
        sql.append(" FROM (").append(translator.out.toString().strip()).append(") AS ")
                .append(quote("_view")).append('(').append(inner).append(')');
        return new Translated(sql.toString(), List.copyOf(translator.sources));
    }

    /** Reads every token. */
    private void run() {
        while (true) {
            skipBlanksAndComments();
            if (pos >= text.length()) {
                return;
            }
            char c = text.charAt(pos);
            if (c == '`') {
                identifier(backtick());
            } else if (c == '\'' || c == '"') {
                if (previous.equals("string")) {
                    throw refused("adjacent string literals");
                }
                emit(stringLiteral(c), "string");
            } else if (Character.isDigit(c)) {
                number();
            } else if (Character.isLetter(c) || c == '_') {
                word();
            } else {
                operator();
            }
        }
    }

    /** Skips blanks, {@code --} and block comments. */
    private void skipBlanksAndComments() {
        while (pos < text.length()) {
            char c = text.charAt(pos);
            if (Character.isWhitespace(c)) {
                pos++;
            } else if (text.startsWith("--", pos)) {
                int end = text.indexOf('\n', pos);
                pos = end < 0 ? text.length() : end + 1;
            } else if (text.startsWith("/*", pos)) {
                int end = text.indexOf("*/", pos + 2);
                if (end < 0) {
                    throw refused("unclosed comment");
                }
                pos = end + 2;
            } else {
                return;
            }
        }
    }

    /**
     * Reads a backtick-quoted identifier.
     *
     * @return the name
     */
    private String backtick() {
        StringBuilder name = new StringBuilder();
        pos++;
        while (true) {
            if (pos >= text.length()) {
                throw refused("unclosed identifier");
            }
            char c = text.charAt(pos++);
            if (c == '`') {
                if (pos < text.length() && text.charAt(pos) == '`') {
                    name.append('`');
                    pos++;
                    continue;
                }
                return name.toString();
            }
            name.append(c);
        }
    }

    /**
     * Reads a string literal with Hive's escapes and writes it as a DuckDB literal.
     *
     * @param quote the opening quote
     * @return the DuckDB literal
     */
    private String stringLiteral(char quote) {
        StringBuilder value = new StringBuilder();
        pos++;
        while (true) {
            if (pos >= text.length()) {
                throw refused("unclosed string literal");
            }
            char c = text.charAt(pos++);
            if (c == quote) {
                break;
            }
            if (c == '\\') {
                if (pos >= text.length()) {
                    throw refused("unclosed string literal");
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
        return "'" + value.toString().replace("'", "''") + "'";
    }

    /** Reads an integer literal; other numbers are refused. */
    private void number() {
        int start = pos;
        while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
            pos++;
        }
        if (pos < text.length() && (Character.isLetter(text.charAt(pos)) || text.charAt(pos) == '.'
                || text.charAt(pos) == '_')) {
            throw refused("number " + text.substring(start, Math.min(text.length(), pos + 3))
                    + " (only integer literals)");
        }
        emit(text.substring(start, pos), "number");
    }

    /** Reads a bare word: keyword, function name or identifier. */
    private void word() {
        int start = pos;
        while (pos < text.length() && (Character.isLetterOrDigit(text.charAt(pos))
                || text.charAt(pos) == '_')) {
            pos++;
        }
        String word = text.substring(start, pos);
        String lower = word.toLowerCase(Locale.ROOT);
        int after = pos;
        skipBlanksAndComments();
        boolean call = pos < text.length() && text.charAt(pos) == '(';
        pos = after;
        if (call && !KEYWORDS.contains(lower)) {
            String function = FUNCTIONS.get(lower);
            if (function == null) {
                throw refused("function " + lower);
            }
            emit(function, "function");
            return;
        }
        if (KEYWORDS.contains(lower)) {
            keyword(lower);
            return;
        }
        if (REFUSED_WORDS.contains(lower)) {
            throw refused("keyword " + lower);
        }
        identifier(word);
    }

    /**
     * Writes a keyword and follows the clauses.
     *
     * @param keyword the keyword in lower case
     */
    private void keyword(String keyword) {
        emit(keyword.toUpperCase(Locale.ROOT), "keyword");
        if (TABLE_FOLLOWS.contains(keyword)) {
            setFrom(true);
            tableNext = true;
        } else if (FROM_ENDS.contains(keyword)) {
            setFrom(false);
        } else if (keyword.equals("like")) {
            likePending = true;
        }
    }

    /**
     * Writes an identifier, or a table name when one is expected.
     *
     * @param name the identifier
     */
    private void identifier(String name) {
        if (!tableNext) {
            int after = pos;
            skipBlanksAndComments();
            boolean call = pos < text.length() && text.charAt(pos) == '(';
            pos = after;
            if (call) {
                throw refused("function " + name);
            }
            emit(quote(name), "identifier");
            return;
        }
        skipBlanksAndComments();
        if (pos >= text.length() || text.charAt(pos) != '.') {
            throw refused("table " + name + " without database");
        }
        pos++;
        skipBlanksAndComments();
        String table;
        if (pos < text.length() && text.charAt(pos) == '`') {
            table = backtick();
        } else {
            int start = pos;
            while (pos < text.length() && (Character.isLetterOrDigit(text.charAt(pos))
                    || text.charAt(pos) == '_')) {
                pos++;
            }
            table = text.substring(start, pos);
            if (table.isEmpty()) {
                throw refused("table name after " + name + ".");
            }
        }
        String database = name.toLowerCase(Locale.ROOT);
        table = table.toLowerCase(Locale.ROOT);
        String schema = schemas.apply(database);
        if (schema == null) {
            throw refused("database " + database + " is not shown");
        }
        sources.add(new Source(database, table));
        tableNext = false;
        emit(quote(schema) + "." + quote(table), "table");
    }

    /** Reads an operator or punctuation. */
    private void operator() {
        String two = pos + 2 <= text.length() ? text.substring(pos, pos + 2) : "";
        String op;
        if (Set.of("<>", "<=", ">=", "||", "!=", "==").contains(two)) {
            pos += 2;
            op = switch (two) {
                case "!=" -> "<>";
                case "==" -> "=";
                default -> two;
            };
        } else {
            op = String.valueOf(text.charAt(pos++));
            if (op.equals("*")) {
                // Hive expands SELECT * into columns: only count(*) keeps a star.
                skipBlanksAndComments();
                if (!previous.equals("(") || pos >= text.length() || text.charAt(pos) != ')') {
                    throw refused("operator *");
                }
                emit("*", "*");
                return;
            }
            if (!OPERATORS.contains(op)) {
                throw refused("operator " + op);
            }
        }
        if (op.equals(")") || op.equals(",")) {
            endLike();
        }
        boolean subquery = tableNext && op.equals("(");
        emit(op, op);
        if (op.equals("(")) {
            tableNext = false;
            subqueryNext = subquery;
            inFrom.push(false);
        } else if (op.equals(")")) {
            if (inFrom.size() <= 1) {
                throw refused("unbalanced parentheses");
            }
            inFrom.pop();
        } else if (op.equals(",") && Boolean.TRUE.equals(inFrom.peek())) {
            tableNext = true;
        }
    }

    /**
     * Marks whether the clause at the current depth is a FROM clause.
     *
     * @param from true in a FROM clause
     */
    private void setFrom(boolean from) {
        inFrom.pop();
        inFrom.push(from);
    }

    /**
     * Appends a token.
     *
     * @param token DuckDB text
     * @param kind token kind, for the checks of the next token
     */
    private void emit(String token, String kind) {
        if (subqueryNext && !token.equals("SELECT") && !kind.equals("(")) {
            throw refused(token.strip() + " where a subquery belongs");
        }
        subqueryNext = false;
        if (tableNext && !kind.equals("table") && !kind.equals("(")) {
            // Only a database table or a subquery may follow FROM, JOIN or a comma there: DuckDB
            // reads files and table functions named in FROM.
            throw refused(token.strip() + " where a table belongs");
        }
        if (likePending && !kind.equals("keyword") && !token.equals("(") && !kind.equals("string")) {
            throw refused("LIKE with a pattern that is not a literal");
        }
        if (!token.equals(".") && !previous.equals(".")) {
            out.append(' ');
        }
        out.append(token);
        previous = kind;
        if (likePending && kind.equals("string")) {
            // Hive's LIKE escapes % and _ with a backslash; DuckDB's has no escape by default.
            out.append(" ESCAPE '\\'");
            likePending = false;
        }
    }

    /** Fails when a LIKE has no pattern literal. */
    private void endLike() {
        if (likePending) {
            throw refused("LIKE with a pattern that is not a literal");
        }
    }

    /**
     * Quotes a DuckDB identifier.
     *
     * @param name the name
     * @return double-quoted name
     */
    static String quote(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    /**
     * Makes the refusal for a construct.
     *
     * @param what the construct
     * @return exception naming it
     */
    private IllegalArgumentException refused(String what) {
        return new IllegalArgumentException("view uses " + what + " (not translated)");
    }
}
