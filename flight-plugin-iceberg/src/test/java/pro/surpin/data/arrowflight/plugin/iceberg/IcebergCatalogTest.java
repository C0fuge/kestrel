package pro.surpin.data.arrowflight.plugin.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

/**
 * Tests both Iceberg catalog kinds against tables written by Iceberg Java (HadoopCatalog):
 * names, snapshots with data and delete files, columns, partial-read problems, snapshot
 * pinning, and the REST catalog (prefix, paging, OAuth2) giving the same snapshots.
 */
class IcebergCatalogTest {

    @TempDir
    static Path dir;
    static String warehouse;
    static Map<String, String> metadata;

    @BeforeAll
    static void writeTables() throws IOException {
        warehouse = dir.resolve("wh").toString();
        metadata = IcebergFixtures.write(warehouse);
    }

    private static Catalog hadoop(Map<String, String> extra) throws IOException {
        Map<String, String> values = new HashMap<>(Map.of("warehouse", warehouse));
        values.putAll(extra);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.HADOOP).open(
                new TestSupport.Context(new TestSupport.MapConfig("iceberg-hadoop", values), stores,
                        new SecretRedactor()));
        stores.freeze();
        return catalog;
    }

    private static TableSnapshot current(Catalog catalog, String table) throws IOException {
        return catalog.current(new TableName("ice", table)).orElseThrow();
    }

    @Test
    void namesComeFromTheWarehouse() throws IOException {
        try (Catalog catalog = hadoop(Map.of())) {
            assertEquals(List.of("ice"), catalog.schemas());
            assertEquals(List.of("deletes", "empty", "evolved", "nested", "parted", "types"),
                    catalog.tables("ice"));
            assertEquals(List.of(), catalog.tables("nope"));
            assertEquals(Optional.empty(), catalog.current(new TableName("ice", "nope")));
            assertEquals(List.of(warehouse), catalog.readableRoots());
            assertFalse(catalog.namesAreStatic());
        }
        try (Catalog catalog = hadoop(Map.of("schemaPrefix", "lake_"))) {
            assertEquals(List.of("lake_ice"), catalog.schemas());
            assertEquals(6, catalog.tables("lake_ice").size());
            assertTrue(catalog.current(new TableName("lake_ice", "types")).isPresent());
        }
        try (Catalog catalog = hadoop(Map.of("namespaces", "other"))) {
            assertEquals(List.of(), catalog.schemas());
        }
    }

    @Test
    void columnsAreDuckDbTypesWithFieldIds() throws IOException {
        try (Catalog catalog = hadoop(Map.of())) {
            TableSnapshot types = current(catalog, "types");
            List<String> described = new ArrayList<>();
            for (ColumnSpec c : types.columns()) {
                described.add(c.name() + " " + c.duckDbType());
            }
            // As DESCRIBE SELECT * FROM iceberg_scan(...) shows them (DuckDB 1.4.5), with quoted
            // struct field names.
            assertEquals(List.of("id BIGINT", "b BOOLEAN", "i INTEGER", "f FLOAT", "d DOUBLE",
                    "dec DECIMAL(12,2)", "dec38 DECIMAL(38,6)", "dt DATE", "tm TIME", "ts TIMESTAMP",
                    "tstz TIMESTAMP WITH TIME ZONE", "s VARCHAR", "u UUID", "bin BLOB", "fx BLOB",
                    "st STRUCT(\"a\" INTEGER, \"b\" VARCHAR)", "lst INTEGER[]",
                    "mp MAP(VARCHAR, INTEGER)"), described);
            assertFalse(types.columns().get(0).nullable());
            assertEquals(1, types.columns().get(0).fieldId());
            assertEquals(2, types.files().size());
            assertEquals(5, types.files().stream().mapToLong(TableFile::recordCount).sum());
            assertEquals(new ReadSpec.IcebergMetadata(metadata.get("types")), types.read());
            assertEquals(Optional.empty(), catalog.partialReadProblem(types));
            for (TableFile f : types.files()) {
                assertTrue(f.location().startsWith(warehouse + "/ice/types/data/"), f.location());
                assertTrue(f.deletes().isEmpty());
            }
        }
    }

    @Test
    void evolvedSchemaHasCurrentNamesAndOldFieldIds() throws IOException {
        try (Catalog catalog = hadoop(Map.of())) {
            TableSnapshot evolved = current(catalog, "evolved");
            assertEquals(List.of(new ColumnSpec("id", "BIGINT", false, 1),
                    new ColumnSpec("label", "VARCHAR", true, 2),
                    new ColumnSpec("amount", "DECIMAL(12,2)", true, 3),
                    new ColumnSpec("note", "VARCHAR", true, 4)), evolved.columns());
            assertEquals(2, evolved.files().size());
            assertEquals(Optional.empty(), catalog.partialReadProblem(evolved));
            assertEquals(3, current(catalog, "parted").files().size());
            assertEquals(Optional.empty(), catalog.partialReadProblem(current(catalog, "parted")));
        }
    }

    @Test
    void deletesAndNestedChangesAreReported() throws IOException {
        try (Catalog catalog = hadoop(Map.of())) {
            TableSnapshot deletes = current(catalog, "deletes");
            assertEquals(2, deletes.files().size());
            List<DeleteFile.Kind> kinds = new ArrayList<>();
            for (TableFile f : deletes.files()) {
                for (DeleteFile d : f.deletes()) {
                    kinds.add(d.kind());
                    if (d.kind() == DeleteFile.Kind.EQUALITY) {
                        assertEquals(List.of(1), d.equalityFieldIds());
                    }
                }
            }
            // The first file has a position and an equality delete; the file written after the
            // equality delete has none (it is newer than the delete).
            assertTrue(kinds.contains(DeleteFile.Kind.POSITION), kinds.toString());
            assertTrue(kinds.contains(DeleteFile.Kind.EQUALITY), kinds.toString());

            TableSnapshot nested = current(catalog, "nested");
            String problem = catalog.partialReadProblem(nested).orElseThrow();
            assertTrue(problem.contains("nested column st"), problem);

            TableSnapshot empty = current(catalog, "empty");
            assertTrue(empty.files().isEmpty());
            assertTrue(empty.snapshotId().startsWith("none."), empty.snapshotId());

            TableSnapshot foreign = new TableSnapshot(new TableName("ice", "x"), "1.0", "unknown",
                    null, null, null, new ReadSpec.ParquetFiles(false));
            assertTrue(catalog.partialReadProblem(foreign).isPresent());
        }
    }

    @Test
    void pinnedSnapshotKeepsItsFilesAfterACommit() throws IOException {
        Path copy = dir.resolve("pin");
        String wh = copy.toString();
        IcebergFixtures.write(wh);
        Map<String, String> values = Map.of("warehouse", wh, "metadataTtlMillis", "0");
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        try (Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.HADOOP).open(
                new TestSupport.Context(new TestSupport.MapConfig("iceberg-hadoop", values), stores,
                        new SecretRedactor()))) {
            TableName name = new TableName("ice", "evolved");
            TableSnapshot before = catalog.current(name).orElseThrow();
            IcebergFixtures.appendToEvolved(wh, 100);
            TableSnapshot after = catalog.current(name).orElseThrow();
            assertNotEquals(before.snapshotId(), after.snapshotId());
            assertEquals(3, after.files().size());
            TableSnapshot pinned = catalog.pinned(name, before.snapshotId()).orElseThrow();
            assertEquals(before.files(), pinned.files());
            assertEquals(before.columns(), pinned.columns());
            assertEquals(Optional.empty(), catalog.pinned(name, "12345.0"));
            assertEquals(Optional.empty(), catalog.pinned(name, "garbage"));
        }
    }

    @Test
    void contributesAvroThenIceberg() throws IOException {
        List<String> loaded = new ArrayList<>();
        try (Catalog catalog = hadoop(Map.of())) {
            catalog.contributeDuckDb(new DuckDbSetup() {
                @Override
                public void loadExtension(String name) {
                    loaded.add(name);
                }

                @Override
                public void set(String setting, String value) {
                    throw new AssertionError(setting);
                }

                @Override
                public void set(String setting, boolean value) {
                    throw new AssertionError(setting);
                }

                @Override
                public void set(String setting, long value) {
                    throw new AssertionError(setting);
                }

                @Override
                public void createSecret(String name, String type, String scope,
                        Map<String, SecretValue> options) {
                    throw new AssertionError(name);
                }

                @Override
                public void refuseFunctions(String... names) {
                    throw new AssertionError();
                }
            });
        }
        assertEquals(List.of("avro", "iceberg"), loaded);
    }

    @Test
    void settingsAreChecked() {
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        IllegalArgumentException noWarehouse = assertThrows(IllegalArgumentException.class,
                () -> new IcebergCatalogProvider(IcebergSettings.Kind.HADOOP).open(
                        new TestSupport.Context(new TestSupport.MapConfig("iceberg-hadoop", Map.of()),
                                stores, new SecretRedactor())));
        assertTrue(noWarehouse.getMessage().contains("iceberg-hadoop.warehouse"), noWarehouse.getMessage());
        IllegalArgumentException noRoots = assertThrows(IllegalArgumentException.class,
                () -> new IcebergCatalogProvider(IcebergSettings.Kind.REST).open(
                        new TestSupport.Context(new TestSupport.MapConfig("iceberg-rest",
                                Map.of("uri", "http://127.0.0.1:9")), stores, new SecretRedactor())));
        assertTrue(noRoots.getMessage().contains("iceberg-rest.roots"), noRoots.getMessage());
    }

    @Test
    void restCatalogGivesTheSameSnapshots() throws IOException {
        try (FakeRestCatalog rest = new FakeRestCatalog(warehouse); Catalog local = hadoop(Map.of())) {
            SecretRedactor redactor = new SecretRedactor();
            TestSupport.LocalStores stores = new TestSupport.LocalStores();
            Map<String, String> values = Map.of("uri", rest.uri() + "/", "roots", warehouse,
                    "credential", FakeRestCatalog.CREDENTIAL, "warehouse", "wh");
            try (Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.REST).open(
                    new TestSupport.Context(new TestSupport.MapConfig("iceberg-rest", values), stores,
                            redactor))) {
                assertEquals(List.of(warehouse), stores.located);
                assertEquals(List.of("ice"), catalog.schemas());
                assertEquals(List.of("deletes", "empty", "evolved", "nested", "parted", "types"),
                        catalog.tables("ice"));
                for (String table : List.of("types", "evolved", "deletes", "empty", "nested")) {
                    TableSnapshot viaRest = current(catalog, table);
                    TableSnapshot viaDir = current(local, table);
                    assertEquals(viaDir.snapshotId(), viaRest.snapshotId(), table);
                    assertEquals(viaDir.files(), viaRest.files(), table);
                    assertEquals(viaDir.columns(), viaRest.columns(), table);
                    assertEquals(viaDir.read(), viaRest.read(), table);
                    assertEquals(local.partialReadProblem(viaDir), catalog.partialReadProblem(viaRest));
                }
                assertEquals(Optional.empty(), catalog.current(new TableName("ice", "nope")));
                assertEquals(1, rest.tokens(), "one OAuth2 token for all requests");
                assertTrue(rest.requests().contains("GET /v1/config?warehouse=wh"), rest.requests().toString());
                assertTrue(rest.requests().stream().anyMatch(r -> r.startsWith("GET /v1/wh1/namespaces/ice/tables?pageToken=p2")),
                        rest.requests().toString());
                assertEquals("***", redactor.redact("client-1:secret-1"));
            }
            Map<String, String> bad = Map.of("uri", rest.uri(), "roots", warehouse,
                    "credential", "client-1:wrong");
            IOException refused = assertThrows(IOException.class,
                    () -> new IcebergCatalogProvider(IcebergSettings.Kind.REST).open(
                            new TestSupport.Context(new TestSupport.MapConfig("iceberg-rest", bad),
                                    new TestSupport.LocalStores(), new SecretRedactor())));
            assertTrue(refused.getMessage().contains("401"), refused.getMessage());
            assertFalse(refused.getMessage().contains("wrong"), refused.getMessage());
        }
    }

    /**
     * A REST catalog that goes away after the start: the error names the catalog and the
     * transport failure (the JDK client's ConnectException has no message; clients saw
     * "cannot be read: null" on the Gravitino stand).
     */
    @Test
    void restCatalogGoneIsAClearError() throws IOException {
        FakeRestCatalog rest = new FakeRestCatalog(warehouse);
        String uri = rest.uri();
        Map<String, String> values = Map.of("uri", uri, "roots", warehouse,
                "credential", FakeRestCatalog.CREDENTIAL, "namesTtlSeconds", "0",
                "metadataTtlMillis", "0");
        try (Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.REST).open(
                new TestSupport.Context(new TestSupport.MapConfig("iceberg-rest", values),
                        new TestSupport.LocalStores(), new SecretRedactor()))) {
            assertEquals(List.of("ice"), catalog.schemas());
            rest.close();
            rest = null;
            IOException gone = assertThrows(IOException.class, () -> catalog.tables("ice"));
            assertTrue(gone.getMessage().startsWith("Iceberg REST catalog unavailable: " + uri
                    + " cannot be reached ("),
                    gone.getMessage());
            assertTrue(gone.getMessage().contains("Exception"), gone.getMessage());
            assertFalse(gone.getMessage().contains("secret-1"), gone.getMessage());
        } finally {
            if (rest != null) {
                rest.close();
            }
        }
    }

    /**
     * A REST catalog may name metadata anywhere: a table whose metadata file lies outside the
     * roots is absent (Gravitino stand: Spark's CREATE TABLE ... LOCATION outside the warehouse
     * failed later with "No open store holds ...").
     */
    @Test
    void restTableWithMetadataOutsideTheRootsIsHidden() throws IOException {
        try (FakeRestCatalog rest = new FakeRestCatalog(warehouse)) {
            Map<String, String> values = Map.of("uri", rest.uri(), "roots",
                    warehouse + "/ice/evolved", "credential", FakeRestCatalog.CREDENTIAL);
            try (Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.REST).open(
                    new TestSupport.Context(new TestSupport.MapConfig("iceberg-rest", values),
                            new TestSupport.LocalStores(), new SecretRedactor()))) {
                assertTrue(catalog.current(new TableName("ice", "evolved")).isPresent());
                assertEquals(Optional.empty(), catalog.current(new TableName("ice", "types")));
                assertEquals(Optional.empty(), catalog.pinned(new TableName("ice", "types"), "1.0"));
            }
        }
    }

    /**
     * A warehouse written by another catalog (Gravitino's JDBC backend, any REST or Hive catalog)
     * names its metadata 00001-<uuid>.metadata.json without a version hint: iceberg-hadoop does
     * not list such tables (it listed them, and every query said "does not exist").
     */
    @Test
    void hadoopCatalogListsOnlyTablesInTheHadoopLayout() throws IOException {
        Path other = dir.resolve("wh-other");
        Path foreign = java.nio.file.Files.createDirectories(other.resolve("x/foreign/metadata"));
        Path hadoopTable = java.nio.file.Files.createDirectories(other.resolve("x/hadoop/metadata"));
        Path source = Path.of(metadata.get("evolved"));
        java.nio.file.Files.copy(source, foreign.resolve("00001-0a1b2c3d.metadata.json"));
        java.nio.file.Files.copy(source, hadoopTable.resolve("v1.metadata.json"));
        java.nio.file.Files.writeString(hadoopTable.resolve("version-hint.text"), "1");
        Map<String, String> values = Map.of("warehouse", other.toString());
        try (Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.HADOOP).open(
                new TestSupport.Context(new TestSupport.MapConfig("iceberg-hadoop", values),
                        new TestSupport.LocalStores(), new SecretRedactor()))) {
            assertEquals(List.of("hadoop"), catalog.tables("x"));
            assertEquals(Optional.empty(), catalog.current(new TableName("x", "foreign")));
        }
    }
}
