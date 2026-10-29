package pro.surpin.data.arrowflight.plugin.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

class JdbcCatalogTest {

    private static Map<String, String> twoSources() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("sources", "sales, crm");
        values.put("sales.type", "postgres");
        values.put("sales.host", "pg.internal");
        values.put("sales.port", "5433");
        values.put("sales.database", "sales");
        values.put("sales.user", "reader");
        values.put("sales.tables", "public.Orders, reporting.customers");
        values.put("crm.type", "MySQL");
        values.put("crm.host", "10.0.0.7");
        values.put("crm.database", "crm");
        values.put("crm.user", "ro");
        values.put("crm.schema", "crm_db");
        values.put("crm.sslMode", "required");
        values.put("crm.tables", "crm.accounts");
        return values;
    }

    @Test
    void settingsNameSchemasAndTables() {
        JdbcSettings settings = JdbcSettings.from(new MapConfig(twoSources(),
                Map.of("sales.password", "s3cr'et")));
        JdbcSettings.Source sales = settings.sources().get(0);
        assertEquals("postgres", sales.type());
        assertEquals(5433, sales.port());
        assertEquals("sales", sales.schema());
        assertEquals(List.of("orders", "customers"), List.copyOf(sales.tables().keySet()));
        assertEquals(new JdbcSettings.RemoteTable("public", "Orders"), sales.tables().get("orders"));
        assertEquals("jdbc_attached_sales", sales.attachment());
        assertFalse(sales.toString().contains("s3cr"), sales.toString());
        JdbcSettings.Source crm = settings.sources().get(1);
        assertEquals("mysql", crm.type());
        assertEquals(null, crm.port());
        assertEquals(null, crm.password());
        assertEquals("crm_db", crm.schema());
        assertEquals("mysql_scanner", crm.extension());
    }

    @Test
    void invalidSettingsNameTheirKey() {
        for (Map.Entry<String, String> bad : List.of(Map.entry("sources", ""),
                Map.entry("sources", "Sales"), Map.entry("sources", "sales,sales"),
                Map.entry("sales.type", "oracle"), Map.entry("sales.host", "pg internal'"),
                Map.entry("sales.port", "70000"), Map.entry("sales.tables", "orders"),
                Map.entry("sales.tables", "public.orders;drop"), Map.entry("sales.tables", "a.t, b.T"),
                Map.entry("sales.sslMode", "required"), Map.entry("crm.schema", "sales"),
                Map.entry("crm.schema", "jdbc_attached_x"), Map.entry("sales.user", " "))) {
            Map<String, String> values = twoSources();
            values.put(bad.getKey(), bad.getValue());
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> JdbcSettings.from(new MapConfig(values, Map.of())), bad.toString());
            assertTrue(e.getMessage().startsWith("jdbc."), bad + ": " + e.getMessage());
        }
        assertThrows(IllegalArgumentException.class,
                () -> JdbcSettings.from(new MapConfig(Map.of(), Map.of())));
    }

    @Test
    void tablesAreReadThroughTheAttachment() {
        JdbcCatalog catalog = new JdbcCatalog(JdbcSettings.from(new MapConfig(twoSources(), Map.of())));
        assertEquals(List.of("sales", "crm_db"), catalog.schemas());
        assertEquals(List.of("orders", "customers"), catalog.tables("sales"));
        assertEquals(List.of(), catalog.tables("nope"));
        assertEquals(List.of(), catalog.readableRoots());
        assertTrue(catalog.namesAreStatic());
        TableSnapshot orders = catalog.current(new TableName("sales", "orders")).orElseThrow();
        ReadSpec.AttachedTable read = assertInstanceOf(ReadSpec.AttachedTable.class, orders.read());
        assertEquals(new ReadSpec.AttachedTable("jdbc_attached_sales", "public", "Orders"), read);
        assertEquals(List.of(), orders.files());
        assertEquals(List.of(), orders.credentials());
        assertEquals(Optional.empty(), catalog.current(new TableName("sales", "accounts")));
        assertEquals(Optional.empty(), catalog.current(new TableName("crm", "accounts")));
        assertTrue(catalog.current(new TableName("crm_db", "accounts")).isPresent());
        assertEquals(orders.fingerprint(), catalog.current(new TableName("sales", "orders"))
                .orElseThrow().fingerprint());
    }

    @Test
    void duckDbSetupRefusesTheScannersAndAttachesReadOnly() {
        JdbcCatalog catalog = new JdbcCatalog(JdbcSettings.from(new MapConfig(twoSources(),
                Map.of("sales.password", "pw"))));
        List<String> calls = new ArrayList<>();
        catalog.contributeDuckDb(new DuckDbSetup() {
            @Override
            public void loadExtension(String name) {
                calls.add("load " + name);
            }

            @Override
            public void loadExtensionRefusingFunctions(String name) {
                calls.add("loadRefusing " + name);
            }

            @Override
            public void set(String setting, String value) {
                calls.add("set " + setting);
            }

            @Override
            public void set(String setting, boolean value) {
                calls.add("set " + setting);
            }

            @Override
            public void set(String setting, long value) {
                calls.add("set " + setting);
            }

            @Override
            public void createSecret(String name, String type, String scope,
                    Map<String, SecretValue> options) {
                calls.add("secret " + name);
            }

            @Override
            public void attachDatabase(String name, String type, Map<String, SecretValue> options) {
                calls.add("attach " + name + " " + type + " " + options.keySet()
                        + (options.containsKey("PASSWORD") && options.get("PASSWORD").sensitive()
                                ? " sensitive" : ""));
            }

            @Override
            public void refuseFunctions(String... names) {
                calls.add("refuse " + String.join(",", names));
            }
        });
        assertEquals(List.of("loadRefusing postgres_scanner",
                "refuse " + String.join(",", JdbcCatalog.POSTGRES_FUNCTIONS),
                "loadRefusing mysql_scanner", "refuse " + String.join(",", JdbcCatalog.MYSQL_FUNCTIONS),
                "attach jdbc_attached_sales postgres [HOST, PORT, DATABASE, USER, PASSWORD] sensitive",
                "attach jdbc_attached_crm mysql [HOST, DATABASE, USER, SSL_MODE]"), calls);
    }
}
