/*
 * Lane 4 (catalogs and sources), item C2 step 1: can DuckDB's postgres / mysql scanner extensions serve a
 * read-only federation plugin under the server's DuckDB lockdown? Run as a single-file program:
 *
 *   java -cp duckdb_jdbc.jar tools/ci/DuckDbScannerProbe.java <extensionDir>
 *
 * The extension directory holds postgres_scanner.duckdb_extension and mysql_scanner.duckdb_extension. Each
 * engine whose PROBE_<PG|MYSQL>_HOST is set is probed (PORT, DATABASE, USER, PASSWORD alike); its database
 * holds table t(id int, name varchar) with rows (1,'a'), (2,'b'), (3,'c'). The connection is set up as the
 * server sets up a query connection (in-memory secrets, LOAD by path, ATTACH ... READ_ONLY with a temporary
 * secret, then allowed_directories + no extension autoload + enable_external_access=false +
 * lock_configuration=true), then the reads a plugin needs and the statements a client must not get through
 * are tried. Every result is printed (password masked) and appended to $GITHUB_STEP_SUMMARY when set. Exit
 * status 1 when loading, attaching or the plain read fails; the refusal checks are only recorded.
 */

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class DuckDbScannerProbe {

    private static final List<String> SUMMARY = new ArrayList<>();
    private static String password = "";
    private static boolean failed;

    private DuckDbScannerProbe() {
    }

    public static void main(String[] args) throws Exception {
        Path extDir = Path.of(args[0]);
        SUMMARY.add("| engine | check | result | detail |");
        SUMMARY.add("|---|---|---|---|");
        probe("pg", "postgres", "postgres_scanner", extDir);
        probe("mysql", "mysql", "mysql_scanner", extDir);
        String summary = System.getenv("GITHUB_STEP_SUMMARY");
        if (summary != null) {
            Files.writeString(Path.of(summary), "## DuckDB scanner probe\n\n" + String.join("\n", SUMMARY) + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        if (failed) {
            System.exit(1);
        }
    }

    private static void probe(String key, String type, String extension, Path extDir) throws Exception {
        String prefix = "PROBE_" + key.toUpperCase() + "_";
        String host = System.getenv(prefix + "HOST");
        if (host == null || host.isEmpty()) {
            return;
        }
        String port = System.getenv(prefix + "PORT");
        String database = System.getenv(prefix + "DATABASE");
        String user = System.getenv(prefix + "USER");
        password = System.getenv(prefix + "PASSWORD");
        String dsn = "postgres".equals(type)
                ? "host=" + host + " port=" + port + " dbname=" + database + " user=" + user + " password=" + password
                : "host=" + host + " port=" + port + " database=" + database + " user=" + user
                        + " password=" + password;
        String schema = "postgres".equals(type) ? "public" : database;
        String fn = "postgres".equals(type) ? "postgres" : "mysql";
        Path tmp = Files.createTempDirectory("probe-" + key);

        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        props.setProperty("allow_persistent_secrets", "false");
        // Each check gets its own statement: the driver closes a statement whose query failed.
        try (Connection s = DriverManager.getConnection("jdbc:duckdb:", props)) {
            String ext = extDir.resolve(extension + ".duckdb_extension").toString();
            String secret = "CREATE TEMPORARY SECRET src_secret (TYPE " + type + ", HOST " + lit(host) + ", PORT "
                    + port + ", DATABASE " + lit(database) + ", USER " + lit(user) + ", PASSWORD " + lit(password)
                    + ")";
            must(key, "functions before", s, "CREATE TEMPORARY TABLE fn_before AS SELECT DISTINCT function_name"
                    + " FROM duckdb_functions()");
            boolean ready = must(key, "load", s, "LOAD " + lit(ext))
                    && must(key, "functions added", s, "SELECT string_agg(function_name || ':' || function_type, ', '"
                            + " ORDER BY function_name) FROM (SELECT DISTINCT function_name, function_type FROM"
                            + " duckdb_functions() WHERE function_name NOT IN (SELECT function_name FROM fn_before))")
                    && must(key, "secret", s, secret)
                    && must(key, "attach", s, "ATTACH '' AS src (TYPE " + type + ", SECRET src_secret, READ_ONLY)");
            if (!ready) {
                return;
            }
            // The server's lockdown of a query connection (DuckDbAdapter.configureConnection).
            must(key, "lockdown", s, "SET allowed_directories = [" + lit(tmp + "/") + "]");
            must(key, "lockdown", s, "SET autoinstall_known_extensions = false");
            must(key, "lockdown", s, "SET autoload_known_extensions = false");
            must(key, "lockdown", s, "SET allow_community_extensions = false");
            must(key, "lockdown", s, "SET enable_external_access = false");
            must(key, "lockdown", s, "SET lock_configuration = true");

            // What the plugin needs after the lockdown.
            mustQuery(key, "read", s, "SELECT count(*), sum(id), string_agg(name, '' ORDER BY id) FROM src."
                    + schema + ".t", "3|6|abc");
            mustQuery(key, "filter+projection", s, "SELECT name FROM src." + schema + ".t WHERE id = 2", "b");
            query(key, "explain filter", s, "EXPLAIN SELECT name FROM src." + schema + ".t WHERE id = 2");
            query(key, "types", s, "SELECT * FROM src." + schema + ".types");
            query(key, "type names", s, "SELECT column_name, data_type FROM information_schema.columns"
                    + " WHERE table_catalog = 'src' AND table_name = 'types' ORDER BY ordinal_position");
            query(key, "tables", s, "SELECT table_schema, table_name FROM information_schema.tables"
                    + " WHERE table_catalog = 'src' ORDER BY 1, 2");

            // What a client must not get through (only SELECTs pass the server's parse check, but the
            // statements are recorded too).
            query(key, "query fn", s, "SELECT * FROM " + fn + "_query('src', 'SELECT 1 AS one')");
            query(key, "query fn write", s, "SELECT * FROM " + fn + "_query('src', 'CREATE TABLE z (i int)')");
            query(key, "query fn multi", s, "SELECT * FROM " + fn + "_query('src', 'SELECT 1 AS one; CREATE TABLE"
                    + " z2 (i int)')");
            query(key, "query fn multi created", s, "SELECT * FROM " + fn + "_query('src', 'SELECT count(*) AS n"
                    + " FROM information_schema.tables WHERE table_name = ''z2''')");
            if ("postgres".equals(type)) {
                query(key, "remote read-only", s, "SELECT * FROM postgres_query('src', 'SELECT"
                        + " current_setting(''transaction_read_only'') AS ro')");
                query(key, "query fn side effect", s, "SELECT * FROM postgres_query('src', 'SELECT"
                        + " set_config(''application_name'', ''probe'', false) AS v')");
            }
            query(key, "execute fn", s,"CALL " + fn + "_execute('src', 'CREATE TABLE z (i int)')");
            if ("postgres".equals(type)) {
                query(key, "scan fn dsn", s, "SELECT count(*) FROM postgres_scan(" + lit(dsn) + ", 'public', 't')");
                query(key, "scan fn attached", s, "SELECT count(*) FROM postgres_scan('src', 'public', 't')");
            }
            query(key, "secrets visible", s, "SELECT name, type, secret_string FROM duckdb_secrets()");
            query(key, "insert", s, "INSERT INTO src." + schema + ".t VALUES (9, 'z')");
            query(key, "attach new", s, "ATTACH " + lit(dsn) + " AS other (TYPE " + type + ")");
            query(key, "attach secret", s, "ATTACH '' AS other2 (TYPE " + type + ", SECRET src_secret)");
            query(key, "replace secret", s, "CREATE OR REPLACE TEMPORARY SECRET src_secret (TYPE " + type
                    + ", HOST '127.0.0.2', PASSWORD 'x')");
            query(key, "settings", s, "SELECT name, value FROM duckdb_settings() WHERE name LIKE '" + fn + "%'"
                    + " OR name LIKE 'pg_%'");
            query(key, "detach", s, "DETACH src");
            query(key, "read after detach", s, "SELECT count(*) FROM src." + schema + ".t");
        }
    }

    private static boolean must(String key, String check, Connection s, String sql) {
        if (!run(key, check, s, sql, null)) {
            failed = true;
            return false;
        }
        return true;
    }

    private static void mustQuery(String key, String check, Connection s, String sql, String expected) {
        if (!run(key, check, s, sql, expected)) {
            failed = true;
        }
    }

    private static void query(String key, String check, Connection s, String sql) {
        run(key, check, s, sql, null);
    }

    private static boolean run(String key, String check, Connection s, String sql, String expected) {
        String result;
        String detail;
        boolean ok;
        try (Statement st = s.createStatement()) {
            StringBuilder rows = new StringBuilder();
            if (st.execute(sql)) {
                try (ResultSet rs = st.getResultSet()) {
                    ResultSetMetaData md = rs.getMetaData();
                    int n = 0;
                    while (rs.next() && n++ < 40) {
                        if (rows.length() > 0) {
                            rows.append(" ; ");
                        }
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            rows.append(i > 1 ? "|" : "").append(rs.getString(i));
                        }
                    }
                }
            }
            detail = rows.toString();
            ok = expected == null || expected.equals(detail);
            result = ok ? "ok" : "WRONG (expected " + expected + ")";
        } catch (Exception e) {
            detail = String.valueOf(e.getMessage());
            ok = false;
            result = "refused";
        }
        detail = mask(detail).replace('\n', ' ');
        System.out.println("RESULT " + key + " | " + check + " | " + result + " | " + detail);
        SUMMARY.add("| " + key + " | " + check + " | " + result + " | `"
                + detail.replace("|", "\\|").replace("`", "'").substring(0, Math.min(400, detail.length())) + "` |");
        return ok;
    }

    private static String mask(String text) {
        return password.isEmpty() ? text : text.replace(password, "***");
    }

    private static String lit(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
