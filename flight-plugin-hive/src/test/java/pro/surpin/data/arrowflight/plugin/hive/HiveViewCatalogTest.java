package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.CatalogView;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * Views of a real metastore (hive.views): listed only when enabled, translated to DuckDB SQL over
 * the server's schema names, views over views, and views over tables the catalog does not show,
 * views it cannot translate and cycles skipped.
 */
class HiveViewCatalogTest {

    @TempDir
    static Path dir;
    static IsolatedHms hms;
    static String root;

    @BeforeAll
    static void start() throws Exception {
        hms = IsolatedHms.shared();
        root = dir.resolve("lake").toString();
        hms.createDatabase("hvw");
        hms.createDatabase("hvw2");
        Path people = Files.createDirectories(dir.resolve("lake/people"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT * FROM (VALUES (1, 'ann'), (2, 'bob'), (3, NULL)) t(id, name))"
                    + " TO '" + people.resolve("000000_0") + "' (FORMAT parquet)");
        }
        hms.createTable("hvw", "people", "file:" + people, "id int;name string", "", "parquet",
                null);
        hms.createTable("hvw2", "other", "file:" + people, "id int;name string", "", "parquet",
                null);
        hms.createTable("hvw", "orc", "file:" + dir.resolve("lake/orc"), "a int", "", "orc", null);
        hms.createView("hvw", "named", "SELECT `people`.`id`, upper(`people`.`name`) AS `n`"
                + " FROM `hvw`.`people` WHERE `people`.`id` > 1", "id int;n string");
        hms.createView("hvw", "counted", "SELECT count(*) AS `c` FROM `hvw`.`named`",
                "c bigint");
        hms.createView("hvw", "across", "SELECT `o`.`id` FROM `hvw2`.`other` `o`", "id int");
        hms.createView("hvw", "castsome", "SELECT CAST(`people`.`id` AS string) AS `s`"
                + " FROM `hvw`.`people`", "s string");
        hms.createView("hvw", "overorc", "SELECT `orc`.`a` FROM `hvw`.`orc`", "a int");
        hms.createView("hvw", "missing", "SELECT `x`.`a` FROM `hvw`.`gone` `x`", "a int");
        hms.createView("hvw", "overbad", "SELECT `castsome`.`s` FROM `hvw`.`castsome`",
                "s string");
        hms.createView("hvw", "loop1", "SELECT `loop2`.`a` FROM `hvw`.`loop2`", "a int");
        hms.createView("hvw", "loop2", "SELECT `loop1`.`a` FROM `hvw`.`loop1`", "a int");
        hms.createView("hvw", "union", "SELECT `people`.`id` FROM `hvw`.`people`", "id uniontype<int,string>");
    }

    @Test
    void viewsAreListedOnlyWhenEnabled() throws Exception {
        try (Catalog catalog = open(Map.of())) {
            assertEquals(List.of(), catalog.views("hvw"));
            assertFalse(catalog.tables("hvw").contains("named"));
        }
        try (Catalog catalog = open(Map.of("views", "true"))) {
            assertEquals(List.of("people"), catalog.tables("hvw"));
            assertEquals(List.of(), catalog.views("nosuch"));
        }
    }

    @Test
    void translatableViewsOverShownTablesAreListed() throws Exception {
        try (Catalog catalog = open(Map.of("views", "true", "schemaPrefix", "h_"))) {
            Map<String, CatalogView> views = new HashMap<>();
            for (CatalogView view : catalog.views("h_hvw")) {
                views.put(view.name(), view);
            }
            assertEquals(List.of("across", "counted", "named"), sorted(views.keySet()));
            CatalogView named = views.get("named");
            assertEquals(List.of(new TableName("h_hvw", "people")), named.sources());
            assertEquals(List.of(new ColumnSpec("id", "INTEGER", true, null),
                    new ColumnSpec("n", "VARCHAR", true, null)), named.columns());
            assertEquals(List.of(new TableName("h_hvw", "named")), views.get("counted").sources());
            assertEquals(List.of(new TableName("h_hvw2", "other")), views.get("across").sources());
            assertNotEquals(named.fingerprint(), views.get("counted").fingerprint());
            assertEquals(named.fingerprint(), catalog.views("h_hvw").stream()
                    .filter(v -> v.name().equals("named")).findFirst().orElseThrow().fingerprint());

            // The SQL runs in DuckDB over tables of the server's schema names; a view over a
            // view reads it as the server would after inlining it.
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement s = conn.createStatement()) {
                s.execute("CREATE SCHEMA h_hvw");
                s.execute("CREATE TABLE h_hvw.people AS SELECT * FROM (VALUES (1, 'ann'),"
                        + " (2, 'bob'), (3, NULL)) t(id, name)");
                s.execute("CREATE VIEW h_hvw.named AS " + named.sql());
                assertEquals(List.of("2|BOB", "3|null"), rows(s, named.sql() + " ORDER BY 1"));
                assertEquals(List.of("2"), rows(s, views.get("counted").sql()));
            }
        }
    }

    @Test
    void untranslatableViewsAndViewsOverHiddenTablesAreSkipped() throws Exception {
        try (Catalog catalog = open(Map.of("views", "true", "databases", "hvw"))) {
            List<String> names = new ArrayList<>();
            catalog.views("hvw").forEach(v -> names.add(v.name()));
            // across reads a database that is not shown; castsome uses a cast; overorc reads an
            // ORC table (hive.orcTables off); missing reads no table; overbad reads a skipped
            // view; the loops read each other; union has a column type DuckDB cannot hold.
            assertEquals(List.of("counted", "named"), sorted(names));
        }
        try (Catalog catalog = open(Map.of("views", "true", "orcTables", "true"))) {
            assertTrue(catalog.views("hvw").stream().anyMatch(v -> v.name().equals("overorc")));
        }
    }

    private static List<String> sorted(java.util.Collection<String> names) {
        List<String> result = new ArrayList<>(names);
        result.sort(null);
        return result;
    }

    private static List<String> rows(Statement s, String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    row.append(i > 1 ? "|" : "").append(rs.getString(i));
                }
                result.add(row.toString());
            }
        }
        return result;
    }

    private static Catalog open(Map<String, String> overrides) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("uris", hms.uri());
        values.put("roots", root);
        values.putAll(overrides);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new HiveCatalogProvider().open(new TestSupport.Context(
                new TestSupport.MapConfig("hive", values), stores, new SecretRedactor(), null));
        stores.freeze();
        return catalog;
    }
}
