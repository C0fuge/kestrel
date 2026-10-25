package pro.surpin.data.arrowflight.plugin.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.AsOf;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * Tests time travel ({@link Catalog#asOf}) on table evolved (two snapshots; the second renamed
 * and widened columns and added one): by snapshot ID, by tag and branch, by instant, with the
 * columns of the snapshot read, and the errors for points without a snapshot.
 */
class IcebergTimeTravelTest {

    @TempDir
    Path dir;

    private Catalog open(String warehouse) throws IOException {
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.HADOOP).open(
                new TestSupport.Context(new TestSupport.MapConfig("iceberg-hadoop",
                        Map.of("warehouse", warehouse)), stores, new SecretRedactor()));
        stores.freeze();
        return catalog;
    }

    private static List<String> names(TableSnapshot snapshot) {
        return snapshot.columns().stream().map(ColumnSpec::name).toList();
    }

    @Test
    void snapshotsByIdRefAndInstantHaveTheirOwnFilesAndColumns() throws IOException {
        String wh = dir.resolve("wh").toString();
        IcebergFixtures.write(wh);
        HadoopCatalog iceberg = IcebergFixtures.catalog(wh);
        Table table = iceberg.loadTable(TableIdentifier.of("ice", "evolved"));
        List<Snapshot> snapshots = new java.util.ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        snapshots.sort(Comparator.comparingLong(Snapshot::sequenceNumber));
        assertEquals(2, snapshots.size());
        Snapshot first = snapshots.get(0);
        Snapshot second = snapshots.get(1);
        table.manageSnapshots().createTag("v1", first.snapshotId()).commit();
        table.manageSnapshots().createBranch("old", first.snapshotId()).commit();
        TableName name = new TableName("ice", "evolved");
        try (Catalog catalog = open(wh)) {
            TableSnapshot current = catalog.current(name).orElseThrow();
            TableSnapshot old = catalog.asOf(name, new AsOf.SnapshotId(first.snapshotId()))
                    .orElseThrow();
            assertNotEquals(current.snapshotId(), old.snapshotId());
            assertTrue(old.snapshotId().startsWith(first.snapshotId() + "."), old.snapshotId());
            // The first snapshot's own columns: before the rename and the added column.
            assertEquals(List.of("id", "name", "amount"), names(old));
            assertEquals(List.of("id", "label", "amount", "note"), names(current));
            assertEquals(1, old.files().size());
            assertEquals(2, current.files().size());
            // Another node pins it like a current snapshot.
            assertEquals(old.files(), catalog.pinned(name, old.snapshotId()).orElseThrow().files());
            assertEquals(old.snapshotId(), catalog.asOf(name, new AsOf.Ref("v1")).orElseThrow()
                    .snapshotId());
            assertEquals(old.snapshotId(), catalog.asOf(name, new AsOf.Ref("old")).orElseThrow()
                    .snapshotId());
            assertEquals(current.snapshotId(), catalog.asOf(name, new AsOf.Ref("main"))
                    .orElseThrow().snapshotId());
            // Instants: the latest commit at or before.
            Instant firstAt = Instant.ofEpochMilli(first.timestampMillis());
            Instant secondAt = Instant.ofEpochMilli(second.timestampMillis());
            assertEquals(old.snapshotId(), catalog.asOf(name, new AsOf.Timestamp(firstAt))
                    .orElseThrow().snapshotId());
            if (secondAt.isAfter(firstAt)) {
                assertEquals(old.snapshotId(), catalog.asOf(name,
                        new AsOf.Timestamp(secondAt.minusMillis(1))).orElseThrow().snapshotId());
            }
            assertEquals(current.snapshotId(), catalog.asOf(name, new AsOf.Timestamp(secondAt))
                    .orElseThrow().snapshotId());
            assertEquals(current.snapshotId(), catalog.asOf(name,
                    new AsOf.Timestamp(Instant.now().plusSeconds(3600))).orElseThrow().snapshotId());
            // No snapshot for the point: an error naming it, never the current snapshot.
            IllegalArgumentException before = assertThrows(IllegalArgumentException.class,
                    () -> catalog.asOf(name, new AsOf.Timestamp(firstAt.minusMillis(1))));
            assertTrue(before.getMessage().contains("no snapshot committed at or before"),
                    before.getMessage());
            IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
                    () -> catalog.asOf(name, new AsOf.SnapshotId(12345L)));
            assertTrue(unknown.getMessage().contains("has no snapshot 12345"), unknown.getMessage());
            IllegalArgumentException ref = assertThrows(IllegalArgumentException.class,
                    () -> catalog.asOf(name, new AsOf.Ref("nope")));
            assertTrue(ref.getMessage().contains("no branch or tag 'nope'"), ref.getMessage());
            assertEquals(Optional.empty(), catalog.asOf(new TableName("ice", "missing"),
                    new AsOf.SnapshotId(first.snapshotId())));
            // A table without snapshots has none to travel to.
            IllegalArgumentException empty = assertThrows(IllegalArgumentException.class,
                    () -> catalog.asOf(new TableName("ice", "empty"), new AsOf.Timestamp(secondAt)));
            assertTrue(empty.getMessage().contains("it has no snapshots"), empty.getMessage());
        }
    }

    @Test
    void aSnapshotCommittedAfterTheCachedMetadataIsFound() throws IOException {
        String wh = dir.resolve("later").toString();
        IcebergFixtures.write(wh);
        TableName name = new TableName("ice", "evolved");
        // The default metadata TTL keeps the metadata read here cached.
        try (Catalog catalog = open(wh)) {
            TableSnapshot before = catalog.current(name).orElseThrow();
            IcebergFixtures.appendToEvolved(wh, 100);
            Table table = IcebergFixtures.catalog(wh).loadTable(TableIdentifier.of("ice", "evolved"));
            long latest = table.currentSnapshot().snapshotId();
            TableSnapshot travelled = catalog.asOf(name, new AsOf.SnapshotId(latest)).orElseThrow();
            assertNotEquals(before.snapshotId(), travelled.snapshotId());
            assertEquals(3, travelled.files().size());
            assertEquals(latest + "." + table.currentSnapshot().schemaId(), travelled.snapshotId());
            assertEquals(3, catalog.asOf(name, new AsOf.Timestamp(Instant.now())).orElseThrow()
                    .files().size());
        }
    }
}
