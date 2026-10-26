package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Tests the core side of catalogs without a plugin: names next to the data directory, the roots
 * check, the reasons for complete answers, ticket keys, and partial planning of catalog tables.
 */
class CatalogTablesTest {

    private static final String ROOT = "s3://lake/wh";
    private static final String S1 = "grpc://127.0.0.1:32010";
    private static final String S2 = "grpc://127.0.0.2:32010";

    /** An in-memory catalog. */
    private static final class FakeCatalog implements Catalog {
        final Map<String, List<String>> tables = new HashMap<>();
        final Map<TableName, TableSnapshot> snapshots = new HashMap<>();
        String problem;

        @Override
        public List<String> schemas() {
            return new ArrayList<>(new TreeSet<>(tables.keySet()));
        }

        @Override
        public List<String> tables(String schema) {
            return tables.getOrDefault(schema, List.of());
        }

        @Override
        public Optional<TableSnapshot> current(TableName table) {
            return Optional.ofNullable(snapshots.get(table));
        }

        @Override
        public Optional<String> partialReadProblem(TableSnapshot snapshot) {
            return Optional.ofNullable(problem);
        }

        @Override
        public List<String> readableRoots() {
            return List.of(ROOT);
        }
    }

    private static TableSnapshot snapshot(String schema, String table, String metadata,
            List<TableFile> files) {
        return new TableSnapshot(new TableName(schema, table), "7.1", metadata + "#7.1",
                List.of(new ColumnSpec("id", "BIGINT", false, 1), new ColumnSpec("x", "VARCHAR", true, 2)),
                List.of(), files, new ReadSpec.IcebergMetadata(metadata));
    }

    private static TableFile file(String location, long size, DeleteFile... deletes) {
        return new TableFile(location, size, 0L, null, null, 10L, List.of(deletes));
    }

    private static CatalogTables tables(FakeCatalog catalog, boolean shared) {
        return new CatalogTables(List.of(new CatalogTables.Entry(
                new SpiCatalog(catalog, null, "fake"), List.of(ROOT), shared)));
    }

    @Test
    void schemasComeAfterTheDataDirectoryAndValidNamesOnly() throws IOException {
        FakeCatalog catalog = new FakeCatalog();
        catalog.tables.put("ice", List.of("b", "a", "bad-name"));
        catalog.tables.put("Dirs", List.of("t"));
        catalog.tables.put("bad schema", List.of("t"));
        CatalogTables tables = tables(catalog, true);
        Map<String, String> schemas = tables.schemas(Set.of("dirs"));
        assertEquals(Map.of("ice", "catalog:fake"), schemas);
        assertTrue(tables.owns("ice"));
        assertTrue(tables.owns("ICE"));
        assertEquals("ice", tables.spelling("ICE"));
        assertFalse(tables.owns("dirs"));
        assertFalse(tables.owns(null));
        assertEquals(List.of("a", "b"), tables.tables("ice"));
        assertFalse(CatalogTables.none().owns("ice"));
        assertTrue(CatalogTables.none().schemas(Set.of()).isEmpty());
    }

    @Test
    void tablesOutsideTheRootsAreHidden() throws IOException {
        FakeCatalog catalog = new FakeCatalog();
        catalog.tables.put("ice", List.of("in", "out", "meta", "del", "dots"));
        catalog.snapshots.put(new TableName("ice", "in"), snapshot("ice", "in",
                ROOT + "/ice/in/metadata/v1.metadata.json", List.of(file(ROOT + "/ice/in/f1.parquet", 1))));
        catalog.snapshots.put(new TableName("ice", "out"), snapshot("ice", "out",
                ROOT + "/ice/out/metadata/v1.metadata.json", List.of(file("s3://lake/other/f.parquet", 1))));
        catalog.snapshots.put(new TableName("ice", "meta"), snapshot("ice", "meta",
                "s3://lake/whx/v1.metadata.json", List.of(file(ROOT + "/ice/m/f.parquet", 1))));
        catalog.snapshots.put(new TableName("ice", "del"), snapshot("ice", "del",
                ROOT + "/ice/del/v1.metadata.json", List.of(file(ROOT + "/ice/del/f.parquet", 1,
                        new DeleteFile("/etc/passwd", 1, DeleteFile.Kind.POSITION, null)))));
        catalog.snapshots.put(new TableName("ice", "dots"), snapshot("ice", "dots",
                ROOT + "/ice/dots/v1.metadata.json", List.of(file(ROOT + "/../etc/f.parquet", 1))));
        CatalogTables tables = tables(catalog, true);
        tables.schemas(Set.of());
        assertTrue(tables.current("ice", "in").isPresent());
        assertTrue(tables.current("ice", "IN").isPresent());
        for (String hidden : List.of("out", "meta", "del", "dots")) {
            assertEquals(Optional.empty(), tables.current("ice", hidden), hidden);
        }
        assertEquals(Optional.empty(), tables.current("nope", "in"));
    }

    /** Regression (Next steps 12): files a writer named through S3A lie under an s3:// root. */
    @Test
    void s3aAndS3nFilesLieUnderAnS3Root() throws IOException {
        FakeCatalog catalog = new FakeCatalog();
        catalog.tables.put("ice", List.of("spark", "elsewhere"));
        TableSnapshot spark = snapshot("ice", "spark", "s3a://lake/wh/ice/spark/v1.metadata.json",
                List.of(file("s3a://lake/wh/ice/spark/f1.parquet", 1, new DeleteFile(
                        "S3N://lake/wh/ice/spark/d1.parquet", 1, DeleteFile.Kind.POSITION, null))));
        catalog.snapshots.put(new TableName("ice", "spark"), spark);
        catalog.snapshots.put(new TableName("ice", "elsewhere"), snapshot("ice", "elsewhere",
                "s3a://lake/wh/ice/e/v1.metadata.json", List.of(file("s3a://lake/whx/f.parquet", 1))));
        CatalogTables tables = tables(catalog, true);
        tables.schemas(Set.of());
        assertTrue(tables.current("ice", "spark").isPresent());
        assertEquals(Optional.empty(), tables.current("ice", "elsewhere"));
        String source = CatalogSources.deletesAppliedSource(spark, spark.files());
        assertTrue(source.contains("'s3://lake/wh/ice/spark/f1.parquet'"), source);
        assertTrue(source.contains("'s3://lake/wh/ice/spark/d1.parquet'"), source);
        assertFalse(source.contains("s3a://"), source);
    }

    @Test
    void completeAnswerReasons() {
        FakeCatalog catalog = new FakeCatalog();
        CatalogTables.Entry entry = new CatalogTables.Entry(new SpiCatalog(catalog, null, "fake"),
                List.of(ROOT), true);
        String meta = ROOT + "/t/v1.metadata.json";
        assertNull(CatalogTables.completeReason(new CatalogTables.Resolved(entry,
                snapshot("s", "t", meta, List.of(file(ROOT + "/t/f", 1))))));
        assertTrue(CatalogTables.completeReason(new CatalogTables.Resolved(entry,
                snapshot("s", "t", meta, List.of()))).contains("no data files"));
        // Deletes the server applies per file (position, equality on known columns) are read in
        // parts; deletion vectors and equality columns the schema dropped are not.
        assertNull(CatalogTables.completeReason(new CatalogTables.Resolved(entry,
                snapshot("s", "t", meta, List.of(file(ROOT + "/t/f", 1, new DeleteFile(ROOT + "/t/d", 1,
                        DeleteFile.Kind.EQUALITY, List.of(1)), new DeleteFile(ROOT + "/t/p", 1,
                        DeleteFile.Kind.POSITION, List.of())), file(ROOT + "/t/g", 1))))));
        assertTrue(CatalogTables.completeReason(new CatalogTables.Resolved(entry,
                snapshot("s", "t", meta, List.of(file(ROOT + "/t/f", 1, new DeleteFile(ROOT + "/t/d", 1,
                        DeleteFile.Kind.EQUALITY, List.of(9)))))))
                .contains("1 delete file(s) the server cannot apply"));
        assertTrue(CatalogTables.completeReason(new CatalogTables.Resolved(entry,
                snapshot("s", "t", meta, List.of(file(ROOT + "/t/g", 1), file(ROOT + "/t/f", 1,
                        new DeleteFile(ROOT + "/t/v.puffin", 1, DeleteFile.Kind.DELETION_VECTOR,
                                List.of())))))).contains("1 delete file(s) the server cannot apply"));
        TableSnapshot noIds = new TableSnapshot(new TableName("s", "t"), "1", "f",
                List.of(new ColumnSpec("id", "BIGINT", true, null)), null,
                List.of(file(ROOT + "/t/f", 1)), new ReadSpec.IcebergMetadata(meta));
        assertTrue(CatalogTables.completeReason(new CatalogTables.Resolved(entry, noIds))
                .contains("no field ID"));
        catalog.problem = "nested column st changed";
        assertEquals("nested column st changed", CatalogTables.completeReason(new CatalogTables.Resolved(
                entry, snapshot("s", "t", meta, List.of(file(ROOT + "/t/f", 1))))));
    }

    @Test
    void ticketKeysCarryTheSnapshot() {
        CatalogFileKey key = new CatalogFileKey("12@3.4/x", "s3://lake/wh/a@b/f.parquet");
        String encoded = key.encode();
        assertTrue(CatalogFileKey.isKey(encoded));
        assertFalse(CatalogFileKey.isKey("ice/t/f.parquet"));
        assertEquals(key, CatalogFileKey.decode(encoded));
        assertThrows(IllegalArgumentException.class, () -> CatalogFileKey.decode("x"));
        assertThrows(IllegalArgumentException.class, () -> CatalogFileKey.decode("@snapshot:1@"));
    }

    @Test
    void locationsAreComparedNormalised() {
        assertEquals("/tmp/wh/a", CatalogLocations.normalize("file:///tmp/wh/a/"));
        assertEquals("/tmp/wh/a", CatalogLocations.normalize("file:/tmp/wh/a"));
        assertTrue(CatalogLocations.underAny("file:/tmp/wh/a", List.of("/tmp/wh")));
        assertTrue(CatalogLocations.underAny("/tmp/wh", List.of("/tmp/wh/")));
        assertFalse(CatalogLocations.underAny("/tmp/whx/a", List.of("/tmp/wh")));
        assertFalse(CatalogLocations.underAny("/tmp/wh/../etc", List.of("/tmp/wh")));
        // s3a:// and s3n:// name the same objects as s3:// (Next steps 12).
        assertEquals("s3://lake/wh/f", CatalogLocations.normalize("S3A://lake/wh/f/"));
        assertTrue(CatalogLocations.underAny("s3a://lake/wh/f", List.of(ROOT)));
        assertTrue(CatalogLocations.underAny("s3://lake/wh/f", List.of("s3n://lake/wh/")));
        assertFalse(CatalogLocations.underAny("s3a://lake/whx/f", List.of(ROOT)));
        assertFalse(CatalogLocations.underAny("s3a://other/wh/f", List.of(ROOT)));
    }

    @Test
    void catalogSettingIsAList() {
        assertEquals(List.of(), new PluginSettings(null, null, "directory", false).catalogs());
        assertEquals(List.of("iceberg-hadoop", "iceberg-rest"), new PluginSettings(null, null,
                " Directory, iceberg-hadoop,iceberg-rest ,iceberg-hadoop", false).catalogs());
    }

    @Test
    void partialPlansSpreadTheSnapshotsFilesWithTheSnapshotId() throws Exception {
        FakeCatalog catalog = new FakeCatalog();
        catalog.tables.put("ice", List.of("t", "d", "p"));
        List<TableFile> files = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            files.add(file(ROOT + "/ice/t/f" + i + ".parquet", 1000));
        }
        catalog.snapshots.put(new TableName("ice", "t"), snapshot("ice", "t",
                ROOT + "/ice/t/v1.metadata.json", files));
        catalog.snapshots.put(new TableName("ice", "d"), snapshot("ice", "d",
                ROOT + "/ice/d/v1.metadata.json", List.of(file(ROOT + "/ice/d/f.parquet", 1,
                        new DeleteFile(ROOT + "/ice/d/del.puffin", 1, DeleteFile.Kind.DELETION_VECTOR,
                                null)))));
        catalog.snapshots.put(new TableName("ice", "p"), snapshot("ice", "p",
                ROOT + "/ice/p/v1.metadata.json", List.of(file(ROOT + "/ice/p/f.parquet", 1,
                        new DeleteFile(ROOT + "/ice/p/del.parquet", 1, DeleteFile.Kind.POSITION, null)),
                        file(ROOT + "/ice/p/g.parquet", 1))));
        for (boolean shared : new boolean[] {true, false}) {
            CatalogTables tables = tables(catalog, shared);
            tables.schemas(Set.of());
            ParquetAdapter parquet = mock(ParquetAdapter.class);
            ClusterService cluster = mock(ClusterService.class);
            when(parquet.localFileInventory()).thenReturn(Map.of());
            when(parquet.catalogTables()).thenReturn(tables);
            when(parquet.isCatalogSchema("ice")).thenReturn(true);
            when(cluster.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 0L));
            when(cluster.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
            when(cluster.hasFileInventory(any())).thenReturn(true);
            when(cluster.serverUri()).thenReturn(S1);
            ArgumentCaptor<HandleState> states = ArgumentCaptor.forClass(HandleState.class);
            when(cluster.createEndpointHandle(states.capture())).thenReturn(new byte[] {1});
            QueryPlanner planner = new QueryPlanner(parquet, cluster);

            QueryPlan plan = planner.plan("SELECT id FROM ice.t");
            assertEquals(4000L, plan.totalBytes());
            assertEquals(40L, plan.totalRecords());
            assertEquals(shared ? 2 : 1, plan.endpoints().size(), "shared=" + shared);
            List<String> located = new ArrayList<>();
            for (HandleState state : states.getAllValues()) {
                if (!shared) {
                    assertEquals(S1, state.serverUri());
                }
                for (String entry : state.filePaths()) {
                    CatalogFileKey key = CatalogFileKey.decode(entry);
                    assertEquals("7.1", key.snapshotId());
                    located.add(key.location());
                }
            }
            located.sort(null);
            assertEquals(files.stream().map(TableFile::location).sorted().toList(), located);
            assertNull(planner.catalogCompleteReason("SELECT id FROM ice.t"));

            assertTrue(planner.catalogCompleteReason("SELECT id FROM ice.d").contains("delete file"));
            // Deletes the nodes apply file by file: planned in parts, the row count unknown.
            assertNull(planner.catalogCompleteReason("SELECT id FROM ice.p"));
            QueryPlan deletes = planner.plan("SELECT id FROM ice.p");
            assertEquals(-1L, deletes.totalRecords());
            assertFalse(deletes.endpoints().get(0).getLocations().isEmpty());
            assertFalse(states.getValue().complete());
            assertTrue(planner.catalogCompleteReason("SELECT a.id FROM ice.t a JOIN ice.d b ON a.id = b.id")
                    .contains("join"));
            QueryPlan complete = planner.plan("SELECT id FROM ice.d");
            assertEquals(1, complete.endpoints().size());
            assertTrue(complete.endpoints().get(0).getLocations().isEmpty());
            assertTrue(states.getValue().complete());
        }
    }
}
