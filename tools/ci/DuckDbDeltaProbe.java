/*
 * Lane 4 (catalogs and sources), item C3 step 1: can DuckDB's delta extension serve Delta Lake tables under the
 * server's DuckDB lockdown, and which Delta features does it read correctly? Run as a single-file program:
 *
 *   java -cp duckdb_jdbc.jar tools/ci/DuckDbDeltaProbe.java <extensionDir> <fixtureDir>
 *
 * The extension directory holds delta.duckdb_extension and httpfs.duckdb_extension. The fixture directory is
 * the output of tools/ci/delta-fixtures.py (one Delta table per directory, expected.tsv with Spark's answers).
 * With PROBE_S3_ENDPOINT (host:port), PROBE_S3_KEY, PROBE_S3_SECRET and PROBE_S3_ROOT (s3://bucket/prefix/, the
 * same tables uploaded there) the S3 path is probed too. A copy of table t outside the allowed directories
 * (PROBE_OUTSIDE_DIR locally, PROBE_S3_OUTSIDE on S3) checks that the lockdown holds for the extension's own
 * I/O. The connection is set up as the server sets up a query connection (in-memory secrets, LOAD by path,
 * secrets, then allowed_directories + no extension autoload + enable_external_access=false +
 * lock_configuration=true). Every result is printed and appended to $GITHUB_STEP_SUMMARY when set. Exit status
 * 1 when loading or the plain read before the lockdown fails; everything else is only recorded ("GETS THROUGH"
 * marks what the lockdown should have refused).
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

public final class DuckDbDeltaProbe {

    private static final List<String> SUMMARY = new ArrayList<>();
    private static String secretValue = "";
    private static boolean failed;

    private DuckDbDeltaProbe() {
    }

    public static void main(String[] args) throws Exception {
        Path extDir = Path.of(args[0]);
        Path fixtures = Path.of(args[1]).toAbsolutePath();
        List<String[]> expected = new ArrayList<>();
        for (String line : Files.readAllLines(fixtures.resolve("expected.tsv"))) {
            if (!line.isBlank()) {
                expected.add(line.split("\t", -1));
            }
        }
        SUMMARY.add("| where | check | result | detail |");
        SUMMARY.add("|---|---|---|---|");
        probe(extDir, fixtures, expected);
        String summary = System.getenv("GITHUB_STEP_SUMMARY");
        if (summary != null) {
            Files.writeString(Path.of(summary), "## DuckDB delta probe\n\n" + String.join("\n", SUMMARY) + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        if (failed) {
            System.exit(1);
        }
    }

    private static void probe(Path extDir, Path fixtures, List<String[]> expected) throws Exception {
        String s3Endpoint = env("PROBE_S3_ENDPOINT");
        String s3Root = env("PROBE_S3_ROOT");
        boolean s3 = !s3Endpoint.isEmpty() && !s3Root.isEmpty();
        secretValue = env("PROBE_S3_SECRET");
        String local = fixtures + "/";
        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        props.setProperty("allow_persistent_secrets", "false");
        // Each check gets its own statement: the driver closes a statement whose query failed.
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:", props)) {
            must("setup", "functions before", c, "CREATE TEMPORARY TABLE fn_before AS SELECT DISTINCT function_name"
                    + " FROM duckdb_functions()");
            boolean ready = must("setup", "load httpfs", c,
                    "LOAD " + lit(extDir.resolve("httpfs.duckdb_extension").toString()))
                    && must("setup", "load delta", c,
                            "LOAD " + lit(extDir.resolve("delta.duckdb_extension").toString()));
            if (!ready) {
                return;
            }
            query("setup", "functions added", c, "SELECT string_agg(function_name || ':' || function_type, ', '"
                    + " ORDER BY function_name) FROM (SELECT DISTINCT function_name, function_type FROM"
                    + " duckdb_functions() WHERE function_name NOT IN (SELECT function_name FROM fn_before))");
            query("setup", "settings", c, "SELECT name, value FROM duckdb_settings() WHERE name LIKE '%delta%'");
            query("setup", "version", c, "SELECT extension_name, extension_version FROM duckdb_extensions()"
                    + " WHERE loaded AND extension_name IN ('delta', 'httpfs')");
            if (s3) {
                must("s3", "secret", c, "CREATE TEMPORARY SECRET s3_probe (TYPE s3, KEY_ID "
                        + lit(env("PROBE_S3_KEY")) + ", SECRET " + lit(secretValue) + ", ENDPOINT " + lit(s3Endpoint)
                        + ", URL_STYLE 'path', USE_SSL false, REGION 'us-east-1', SCOPE " + lit(s3Root) + ")");
            }
            mustQuery("local", "read before lockdown", c, answer("delta_scan(" + lit(local + "t") + ")", ""),
                    lookup(expected, "t", ""));
            query("local", "attach before lockdown", c, "ATTACH " + lit(local + "t") + " AS att_t (TYPE delta)");
            query("local", "attach pinned before lockdown", c, "ATTACH " + lit(local + "tt")
                    + " AS att_tt (TYPE delta, PIN_SNAPSHOT)");

            // The server's lockdown of a query connection (DuckDbAdapter.configureConnection).
            String allowed = lit(local) + (s3 ? ", " + lit(s3Root) : "");
            must("setup", "lockdown", c, "SET allowed_directories = [" + allowed + "]");
            must("setup", "lockdown", c, "SET autoinstall_known_extensions = false");
            must("setup", "lockdown", c, "SET autoload_known_extensions = false");
            must("setup", "lockdown", c, "SET allow_community_extensions = false");
            must("setup", "lockdown", c, "SET enable_external_access = false");
            must("setup", "lockdown", c, "SET lock_configuration = true");

            List<String> roots = new ArrayList<>(List.of(local));
            if (s3) {
                roots.add(s3Root);
            }
            for (String root : roots) {
                String where = root.startsWith("s3://") ? "s3" : "local";
                for (String[] e : expected) {
                    String table = e[0];
                    String version = "";
                    int at = table.indexOf('@');
                    if (at >= 0) {
                        version = table.substring(at + 1);
                        table = table.substring(0, at);
                    }
                    String scan = "delta_scan(" + lit(root + table) + ")";
                    if (!version.isEmpty()) {
                        // Time travel: recorded, the extension's syntax for it is part of the question.
                        query(where, e[0] + " version (delta_scan)", c, answer("delta_scan(" + lit(root + table)
                                + ", version := " + version + ")", e[1]));
                        continue;
                    }
                    check(where, e[0] + (e[1].isEmpty() ? "" : " where " + e[1]), c, answer(scan, e[1]), e[2]);
                }
                query(where, "types", c, "SELECT * EXCLUDE (id, name) FROM delta_scan(" + lit(root + "types")
                        + ") ORDER BY id");
                query(where, "type names", c, "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM"
                        + " delta_scan(" + lit(root + "types") + "))");
                query(where, "cm_name columns", c, "SELECT string_agg(column_name, ',') FROM (DESCRIBE SELECT *"
                        + " FROM delta_scan(" + lit(root + "cm_name") + "))");
                query(where, "explain filter", c, "EXPLAIN SELECT name FROM delta_scan(" + lit(root + "part")
                        + ") WHERE p = 'x' AND id > 1");
                query(where, "files", c, "SELECT count(*) FROM delta_list_files(" + lit(root + "part") + ")");
                query(where, "attach after lockdown", c, "ATTACH " + lit(root + "t") + " AS att2_" + where
                        + " (TYPE delta)");
            }
            query("local", "attached read", c, answer("att_t", ""));
            query("local", "pinned attached read", c, answer("att_tt", ""));
            query("local", "attach version option", c, "ATTACH " + lit(local + "tt")
                    + " AS att_v (TYPE delta, VERSION 1)");

            // What the lockdown must refuse: tables outside the allowed directories.
            String outside = env("PROBE_OUTSIDE_DIR");
            if (!outside.isEmpty()) {
                refuse("local", "delta_scan outside allowed", c,
                        "SELECT count(*) FROM delta_scan(" + lit(outside + "/t") + ")");
                refuse("local", "read_parquet outside allowed (control)", c,
                        "SELECT count(*) FROM read_parquet(" + lit(outside + "/t/*.parquet") + ")");
            }
            String s3Outside = env("PROBE_S3_OUTSIDE");
            if (s3 && !s3Outside.isEmpty()) {
                refuse("s3", "delta_scan outside allowed", c,
                        "SELECT count(*) FROM delta_scan(" + lit(s3Outside + "t") + ")");
                refuse("s3", "read_parquet outside allowed (control)", c,
                        "SELECT count(*) FROM read_parquet(" + lit(s3Outside + "t/*.parquet") + ")");
            }
            refuse("local", "delta_scan http", c, "SELECT count(*) FROM delta_scan('http://127.0.0.1:9/t')");

            // How far the extension's own I/O (delta-kernel) goes past the lockdown: the log of a table outside
            // the allowed directories (schema, file list), other hosts, and the functions it adds that write.
            if (!outside.isEmpty()) {
                refuse("local", "schema outside allowed", c, "SELECT string_agg(column_name || ' ' || column_type,"
                        + " ', ') FROM (DESCRIBE SELECT * FROM delta_scan(" + lit(outside + "/t") + "))");
                refuse("local", "list files outside allowed", c, "SELECT * FROM delta_list_files("
                        + lit(outside + "/t") + ")");
                refuse("local", "log-only table outside allowed", c, "SELECT count(*) FROM delta_scan("
                        + lit(outside + "/logonly") + ")");
                refuse("local", "file: URI outside allowed", c, "SELECT count(*) FROM delta_scan("
                        + lit("file://" + outside + "/t") + ")");
                refuse("local", "missing dir outside allowed", c, "SELECT count(*) FROM delta_scan('/proc/self')");
            }
            refuse("local", "delta_scan https", c, "SELECT count(*) FROM delta_scan('https://127.0.0.1:9/t')");
            refuse("local", "delta_scan az", c, "SELECT count(*) FROM delta_scan('az://c/t')");
            refuse("local", "delta_scan gs", c, "SELECT count(*) FROM delta_scan('gs://b/t')");
            query("setup", "added function signatures", c, "SELECT function_name, parameters, parameter_types"
                    + " FROM duckdb_functions() WHERE function_name IN ('write_blob', 'copy_dir',"
                    + " 'delta_set_transaction_version', 'delta_filter_pushdown_log')");
            if (!outside.isEmpty()) {
                refuse("local", "write_blob outside allowed", c, "SELECT write_blob(" + lit(outside + "/wb")
                        + ", 'abc'::BLOB)");
                refuse("local", "write_blob inside allowed", c, "SELECT write_blob(" + lit(local + "wb")
                        + ", 'abc'::BLOB)");
                refuse("local", "copy_dir to outside", c, "SELECT * FROM copy_dir(" + lit(local + "t") + ", "
                        + lit(outside + "/copied") + ")");
                query("local", "outside dir after writes", c, "SELECT 'see job log: ls outside'");
            }
            refuse("local", "set transaction version", c, "SELECT * FROM delta_set_transaction_version("
                    + lit(local + "t") + ", 'probe', 7)");
            if (s3 && !s3Outside.isEmpty()) {
                must("s3", "unscoped secret after lockdown", c, "CREATE TEMPORARY SECRET s3_any (TYPE s3, KEY_ID "
                        + lit(env("PROBE_S3_KEY")) + ", SECRET " + lit(secretValue) + ", ENDPOINT "
                        + lit(s3Endpoint) + ", URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')");
                refuse("s3", "delta_scan outside allowed, unscoped secret", c,
                        "SELECT count(*) FROM delta_scan(" + lit(s3Outside + "t") + ")");
                refuse("s3", "schema outside allowed, unscoped secret", c, "SELECT string_agg(column_name, ',')"
                        + " FROM (DESCRIBE SELECT * FROM delta_scan(" + lit(s3Outside + "t") + "))");
                refuse("s3", "list files outside allowed, unscoped secret", c, "SELECT count(*) FROM"
                        + " delta_list_files(" + lit(s3Outside + "t") + ")");
            }
            query("setup", "secrets visible", c, "SELECT name, type FROM duckdb_secrets()");
        }
    }

    private static String answer(String source, String where) {
        return "SELECT count(*), sum(id), coalesce(string_agg(name, '' ORDER BY id), '') FROM " + source
                + (where.isEmpty() ? "" : " WHERE " + where);
    }

    private static String lookup(List<String[]> expected, String table, String where) {
        for (String[] e : expected) {
            if (e[0].equals(table) && e[1].equals(where)) {
                return e[2];
            }
        }
        throw new IllegalStateException("no expected line for " + table);
    }

    private static boolean must(String where, String check, Connection c, String sql) {
        if (!run(where, check, c, sql, null, false)) {
            failed = true;
            return false;
        }
        return true;
    }

    private static void mustQuery(String where, String check, Connection c, String sql, String expected) {
        if (!run(where, check, c, sql, expected, false)) {
            failed = true;
        }
    }

    private static void check(String where, String check, Connection c, String sql, String expected) {
        run(where, check, c, sql, expected, false);
    }

    private static void query(String where, String check, Connection c, String sql) {
        run(where, check, c, sql, null, false);
    }

    private static void refuse(String where, String check, Connection c, String sql) {
        run(where, check, c, sql, null, true);
    }

    private static boolean run(String where, String check, Connection c, String sql, String expected,
            boolean mustFail) {
        String result;
        String detail;
        boolean ok;
        try (Statement st = c.createStatement()) {
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
            result = mustFail ? "GETS THROUGH" : ok ? "ok" : "WRONG (expected " + expected + ")";
        } catch (Exception e) {
            detail = String.valueOf(e.getMessage());
            ok = false;
            result = mustFail ? "refused (as it must)" : "refused";
        }
        detail = mask(detail).replace('\n', ' ');
        System.out.println("RESULT " + where + " | " + check + " | " + result + " | " + detail);
        SUMMARY.add("| " + where + " | " + check + " | " + result + " | `"
                + detail.replace("|", "\\|").replace("`", "'").substring(0, Math.min(400, detail.length())) + "` |");
        return ok;
    }

    private static String env(String name) {
        String value = System.getenv(name);
        return value == null ? "" : value;
    }

    private static String mask(String text) {
        return secretValue.isEmpty() ? text : text.replace(secretValue, "***");
    }

    private static String lit(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
