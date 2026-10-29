/*
 * Lane 4 (catalogs and sources), item C4 (b): do DuckDB's paimon and hudi extensions stay inside the server's
 * DuckDB lockdown, and do they answer correctly on a connection set up as the server sets one up? The extensions'
 * answers on a plain connection are already known (tools/ci/paimon-hudi-probe.py: equal to Spark everywhere); this
 * probe is the security half, as DuckDbDeltaProbe was for delta. Run as a single-file program:
 *
 *   java -cp duckdb_jdbc.jar tools/ci/DuckDbPaimonHudiProbe.java <extensionDir> <fixtureDir>
 *
 * The extension directory holds paimon.duckdb_extension, hudi.duckdb_extension and httpfs.duckdb_extension
 * (hadoopfs.duckdb_extension too for HDFS). The fixture directory is the output of tools/ci/paimon-hudi-fixtures.py
 * (expected.tsv holds Spark's answers). PROBE_OUTSIDE_DIR holds copies outside the allowed directories:
 * paimon_wh/db.db/app (paimon/db.db/app), hudi_cow (hudi-1.0.2/cow) and their metadata-only copies
 * paimon_wh/db.db/meta / hudi_meta (no data files). ATTACH checks run last and are detached again: a lazy attach
 * that cannot be read makes every later catalog lookup on the connection fail. With PROBE_S3_ENDPOINT
 * (host:port), PROBE_S3_KEY, PROBE_S3_SECRET, PROBE_S3_ROOT (s3://bucket/prefix/, the fixture directory uploaded
 * there) and PROBE_S3_OUTSIDE (the same copies as PROBE_OUTSIDE_DIR) the S3 path is probed too; with PROBE_HDFS_ROOT (the fixture directory on HDFS) the HDFS path.
 *
 * The connection is set up as the server sets up a query connection (in-memory secrets, LOAD by path, secrets,
 * then allowed_directories + no extension autoload + enable_external_access=false + lock_configuration=true).
 * Every result is printed and appended to $GITHUB_STEP_SUMMARY when set. Exit status 1 when loading or a plain read
 * before the lockdown fails; everything else is only recorded ("GETS THROUGH" marks what the lockdown should have
 * refused, "WRONG" an answer different from Spark's).
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

public final class DuckDbPaimonHudiProbe {

    private static final List<String> SUMMARY = new ArrayList<>();
    private static final String PAIMON_T = "paimon/db.db/app";
    private static final String HUDI_T = "hudi-1.0.2/cow";
    private static String secretValue = "";
    private static boolean failed;

    private DuckDbPaimonHudiProbe() {
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
            Files.writeString(Path.of(summary), "## DuckDB paimon / hudi lockdown probe\n\n"
                    + String.join("\n", SUMMARY) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
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
        String hdfsRoot = env("PROBE_HDFS_ROOT");
        Path hadoopfs = extDir.resolve("hadoopfs.duckdb_extension");
        boolean hdfs = !hdfsRoot.isEmpty() && Files.exists(hadoopfs);
        String local = fixtures + "/";
        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        props.setProperty("allow_persistent_secrets", "false");
        // Each check gets its own statement: the driver closes a statement whose query failed.
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:", props)) {
            must("setup", "functions before", c, "CREATE TEMPORARY TABLE fn_before AS SELECT DISTINCT function_name"
                    + " FROM duckdb_functions()");
            must("setup", "settings before", c, "CREATE TEMPORARY TABLE set_before AS SELECT name FROM"
                    + " duckdb_settings()");
            boolean ready = must("setup", "load httpfs", c,
                    "LOAD " + lit(extDir.resolve("httpfs.duckdb_extension").toString()))
                    && (!hdfs || must("hdfs", "load hadoopfs", c, "LOAD " + lit(hadoopfs.toString())))
                    && must("setup", "functions before paimon / hudi", c, "INSERT INTO fn_before SELECT DISTINCT"
                            + " function_name FROM duckdb_functions()")
                    && must("setup", "load paimon", c,
                            "LOAD " + lit(extDir.resolve("paimon.duckdb_extension").toString()))
                    && must("setup", "load hudi", c,
                            "LOAD " + lit(extDir.resolve("hudi.duckdb_extension").toString()));
            if (!ready) {
                return;
            }
            query("setup", "functions added", c, "SELECT string_agg(function_name || ':' || function_type, ', '"
                    + " ORDER BY function_name) FROM (SELECT DISTINCT function_name, function_type FROM"
                    + " duckdb_functions() WHERE function_name NOT IN (SELECT function_name FROM fn_before))");
            query("setup", "added function signatures", c, "SELECT function_name, parameters, parameter_types"
                    + " FROM duckdb_functions() WHERE function_name NOT IN (SELECT function_name FROM fn_before)");
            query("setup", "settings added", c, "SELECT name, value, description FROM duckdb_settings()"
                    + " WHERE name NOT IN (SELECT name FROM set_before)");
            query("setup", "secret types", c, "SELECT string_agg(type || '/' || coalesce(extension, ''), ', ')"
                    + " FROM duckdb_secret_types()");
            query("setup", "version", c, "SELECT extension_name, extension_version, install_mode FROM"
                    + " duckdb_extensions() WHERE loaded AND extension_name IN ('paimon', 'hudi', 'httpfs',"
                    + " 'hadoopfs')");
            if (s3) {
                must("s3", "secret", c, "CREATE TEMPORARY SECRET s3_probe (TYPE s3, KEY_ID "
                        + lit(env("PROBE_S3_KEY")) + ", SECRET " + lit(secretValue) + ", ENDPOINT " + lit(s3Endpoint)
                        + ", URL_STYLE 'path', USE_SSL false, REGION 'us-east-1', SCOPE " + lit(s3Root) + ")");
                // paimon brings its own secret type (it does not use the s3 secret): what does it take?
                query("s3", "paimon secret", c, "CREATE TEMPORARY SECRET paimon_probe (TYPE paimon, KEY_ID "
                        + lit(env("PROBE_S3_KEY")) + ", SECRET " + lit(secretValue) + ", ENDPOINT "
                        + lit("http://" + s3Endpoint) + ", REGION 'us-east-1', SCOPE " + lit(s3Root) + ")");
                query("s3", "paimon secret parameters", c, "SELECT * FROM duckdb_secret_types() WHERE type = 'paimon'");
            }
            mustQuery("local", "paimon read before lockdown", c, answer(scan(local, "paimon/app"), ""),
                    lookup(expected, "paimon/app", ""));
            mustQuery("local", "hudi read before lockdown", c, answer(scan(local, "hudi-1.0.2/cow"), ""),
                    lookup(expected, "hudi-1.0.2/cow", ""));
            query("local", "paimon attach before lockdown", c, "ATTACH " + lit(local + "paimon")
                    + " AS att_paimon (TYPE paimon)");
            query("local", "hudi attach before lockdown", c, "ATTACH " + lit(local + HUDI_T)
                    + " AS att_hudi (TYPE hudi)");
            if (hdfs) {
                // Control: hadoopfs itself lists the table; then the same tables through the extensions.
                must("hdfs", "glob (hadoopfs control)", c, "SELECT count(*) FROM glob("
                        + lit(hdfsRoot + PAIMON_T + "/snapshot/*") + ")");
                check("hdfs", "paimon read before lockdown", c, answer(scan(hdfsRoot, "paimon/app"), ""),
                        lookup(expected, "paimon/app", ""));
                check("hdfs", "hudi read before lockdown", c, answer(scan(hdfsRoot, "hudi-1.0.2/cow"), ""),
                        lookup(expected, "hudi-1.0.2/cow", ""));
            }

            // The server's lockdown of a query connection (DuckDbAdapter.configureConnection).
            String allowed = lit(local) + (s3 ? ", " + lit(s3Root) : "") + (hdfs ? ", " + lit(hdfsRoot) : "");
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
            if (hdfs) {
                roots.add(hdfsRoot);
            }
            for (String root : roots) {
                String where = root.startsWith("s3://") ? "s3" : root.startsWith("hdfs://") ? "hdfs" : "local";
                for (String[] e : expected) {
                    check(where, e[0] + (e[1].isEmpty() ? "" : " where " + e[1]), c, answer(scan(root, e[0]), e[1]),
                            e[2]);
                }
                query(where, "explain paimon filter", c, "EXPLAIN SELECT name FROM " + scan(root, "paimon/app")
                        + " WHERE p = 'x' AND id > 1");
                query(where, "explain hudi filter", c, "EXPLAIN SELECT name FROM " + scan(root, "hudi-1.0.2/cow")
                        + " WHERE p = 'x' AND id > 1");
                query(where, "paimon_snapshots", c, "SELECT * FROM paimon_snapshots(" + lit(root + PAIMON_T) + ")");
                query(where, "hudi_info", c, "SELECT hudi_info(" + lit(root + HUDI_T) + ")");
                query(where, "hudi_files", c, "SELECT hudi_files(" + lit(root + HUDI_T) + ")");
            }
            query("local", "replacement scan inside allowed", c, "SELECT count(*) FROM " + lit(local + PAIMON_T));
            if (s3) {
                // read_hudi's hudi_option: storage options from client SQL (endpoint, keys)?
                for (String format : List.of("aws_endpoint_url=http://%s,aws_access_key_id=%s,"
                        + "aws_secret_access_key=%s,aws_region=us-east-1,aws_allow_http=true",
                        "{\"aws_endpoint_url\": \"http://%s\", \"aws_access_key_id\": \"%s\","
                        + " \"aws_secret_access_key\": \"%s\", \"aws_region\": \"us-east-1\","
                        + " \"aws_allow_http\": \"true\"}")) {
                    query("s3", "read_hudi with hudi_option " + (format.startsWith("{") ? "json" : "k=v"), c,
                            answer("read_hudi(" + lit(s3Root + HUDI_T) + ", hudi_option := " + lit(String.format(
                                    format, s3Endpoint, env("PROBE_S3_KEY"), secretValue)) + ")", ""));
                }
            }

            // What the lockdown must refuse: tables outside the allowed directories, through the extensions' own
            // I/O (metadata) and DuckDB's (data files), and other hosts.
            String outside = env("PROBE_OUTSIDE_DIR");
            if (!outside.isEmpty()) {
                outside(c, "local", outside + "/");
                refuse("local", "read_parquet outside allowed (control)", c, "SELECT count(*) FROM read_parquet("
                        + lit(outside + "/hudi_cow/**/*.parquet") + ")");
                refuse("local", "paimon file: URI outside allowed", c, "SELECT count(*) FROM paimon_scan("
                        + lit("file://" + outside + "/paimon_wh/db.db/app") + ")");
                refuse("local", "hudi file: URI outside allowed", c, "SELECT count(*) FROM read_hudi("
                        + lit("file://" + outside + "/hudi_cow") + ")");
                refuse("local", "paimon /proc", c, "SELECT count(*) FROM paimon_scan('/proc/self')");
                refuse("local", "hudi /proc", c, "SELECT count(*) FROM read_hudi('/proc/self')");
                refuse("local", "replacement scan outside allowed", c, "SELECT count(*) FROM "
                        + lit(outside + "/paimon_wh/db.db/app"));
            }
            for (String scheme : List.of("http://127.0.0.1:9/t", "https://127.0.0.1:9/t", "az://c/t", "gs://b/t")) {
                refuse("remote", "paimon_scan " + scheme, c, "SELECT count(*) FROM paimon_scan(" + lit(scheme) + ")");
                refuse("remote", "read_hudi " + scheme, c, "SELECT count(*) FROM read_hudi(" + lit(scheme) + ")");
            }
            String s3Outside = env("PROBE_S3_OUTSIDE");
            if (s3 && !s3Outside.isEmpty()) {
                outside(c, "s3", s3Outside);
                refuse("s3", "read_parquet outside allowed (control)", c, "SELECT count(*) FROM read_parquet("
                        + lit(s3Outside + "hudi_cow/**/*.parquet") + ")");
                // With no secret covering the path: does the extension fall back to the instance metadata
                // service or environment credentials (DuckDB's own reader just fails)?
                refuse("s3", "paimon_scan no matching secret", c, "SELECT count(*) FROM paimon_scan("
                        + lit("s3://other-bucket/w/db.db/t") + ")");
                refuse("s3", "read_hudi no matching secret", c, "SELECT count(*) FROM read_hudi("
                        + lit("s3://other-bucket/t") + ")");
                must("s3", "unscoped secret after lockdown", c, "CREATE TEMPORARY SECRET s3_any (TYPE s3, KEY_ID "
                        + lit(env("PROBE_S3_KEY")) + ", SECRET " + lit(secretValue) + ", ENDPOINT "
                        + lit(s3Endpoint) + ", URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')");
                outside(c, "s3 unscoped secret", s3Outside);
            }
            query("setup", "secrets visible", c, "SELECT name, type FROM duckdb_secrets()");

            // Last: attaches (a failing lazy attach breaks later lookups), each read and detached again.
            query("local", "paimon attached read", c, "SELECT count(*) FROM att_paimon.db.app");
            query("local", "paimon attached tables", c, "SELECT string_agg(table_name, ',' ORDER BY table_name)"
                    + " FROM information_schema.tables WHERE table_catalog = 'att_paimon'");
            query("local", "detach", c, "DETACH att_paimon");
            for (String root : roots) {
                String where = root.startsWith("s3://") ? "s3" : root.startsWith("hdfs://") ? "hdfs" : "local";
                query(where, "paimon attach after lockdown", c, "ATTACH " + lit(root + "paimon") + " AS attp_"
                        + where + " (TYPE paimon)");
                query(where, "paimon attached read after lockdown", c, "SELECT count(*) FROM attp_" + where
                        + ".db.app");
                query(where, "detach", c, "DETACH attp_" + where);
            }
            if (!outside.isEmpty()) {
                refuse("local", "paimon attach outside allowed", c, "ATTACH " + lit(outside + "/paimon_wh")
                        + " AS attp_out (TYPE paimon)");
                refuse("local", "paimon attached read outside allowed", c, "SELECT count(*) FROM attp_out.db.app");
                query("local", "detach", c, "DETACH attp_out");
            }
        }
    }

    /** The checks that must be refused for the table copies below {@code dir}, which is not allowed. */
    private static void outside(Connection c, String where, String dir) {
        String app = dir + "paimon_wh/db.db/app";
        refuse(where, "paimon_scan outside allowed", c, answer("paimon_scan(" + lit(app) + ")", ""));
        refuse(where, "paimon schema outside allowed", c, "SELECT string_agg(column_name || ' ' || column_type,"
                + " ', ') FROM (DESCRIBE SELECT * FROM paimon_scan(" + lit(app) + "))");
        refuse(where, "paimon_snapshots outside allowed", c, "SELECT * FROM paimon_snapshots(" + lit(app) + ")");
        refuse(where, "paimon metadata-only outside allowed", c, "SELECT string_agg(column_name, ',') FROM"
                + " (DESCRIBE SELECT * FROM paimon_scan(" + lit(dir + "paimon_wh/db.db/meta") + "))");
        refuse(where, "read_hudi outside allowed", c, answer("read_hudi(" + lit(dir + "hudi_cow") + ")", ""));
        refuse(where, "hudi schema outside allowed", c, "SELECT string_agg(column_name || ' ' || column_type,"
                + " ', ') FROM (DESCRIBE SELECT * FROM read_hudi(" + lit(dir + "hudi_cow") + "))");
        refuse(where, "hudi_info outside allowed", c, "SELECT hudi_info(" + lit(dir + "hudi_cow") + ")");
        refuse(where, "hudi_files outside allowed", c, "SELECT hudi_files(" + lit(dir + "hudi_cow") + ")");
        refuse(where, "hudi metadata-only outside allowed", c, "SELECT hudi_info(" + lit(dir + "hudi_meta") + ")");
    }

    /** The extension scan of an expected.tsv table label ({@code paimon/<t>} or {@code hudi-<v>/<t>}). */
    private static String scan(String root, String label) {
        int slash = label.indexOf('/');
        String fmt = label.substring(0, slash);
        String name = label.substring(slash + 1);
        return fmt.equals("paimon") ? "paimon_scan(" + lit(root + "paimon/db.db/" + name) + ")"
                : "read_hudi(" + lit(root + fmt + "/" + name) + ")";
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
        long start = System.nanoTime();
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
        long millis = (System.nanoTime() - start) / 1_000_000;
        detail = mask(detail).replace('\n', ' ') + (millis >= 2000 ? " [" + millis + " ms]" : "");
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
