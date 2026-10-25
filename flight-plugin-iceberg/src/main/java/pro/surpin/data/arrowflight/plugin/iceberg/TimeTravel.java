package pro.surpin.data.arrowflight.plugin.iceberg;

import java.util.List;
import java.util.Optional;

import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;

import pro.surpin.data.arrowflight.spi.catalog.AsOf;
import pro.surpin.data.arrowflight.spi.catalog.TableName;

/**
 * Finds the snapshot of a table's metadata that a time-travel point names, with the rules of
 * Iceberg's Spark and Trino readers: a snapshot ID must be one of the metadata's snapshots, a
 * reference a branch or tag, and an instant picks the latest entry of the snapshot log (the
 * main branch's history) committed at or before it.
 */
final class TimeTravel {

    private TimeTravel() {
    }

    /**
     * Finds the snapshot a point names.
     *
     * @param metadata table metadata
     * @param point time-travel point
     * @return snapshot, empty when the metadata has none for the point
     */
    static Optional<Snapshot> find(TableMetadata metadata, AsOf point) {
        return switch (point) {
            case AsOf.SnapshotId id -> Optional.ofNullable(metadata.snapshot(id.id()));
            case AsOf.Ref ref -> {
                SnapshotRef found = metadata.ref(ref.name());
                yield found == null ? Optional.empty()
                        : Optional.ofNullable(metadata.snapshot(found.snapshotId()));
            }
            case AsOf.Timestamp at -> {
                long millis = at.at().toEpochMilli();
                HistoryEntry latest = null;
                for (HistoryEntry entry : metadata.snapshotLog()) {
                    if (entry.timestampMillis() <= millis
                            && (latest == null || entry.timestampMillis() >= latest.timestampMillis())) {
                        latest = entry;
                    }
                }
                yield latest == null ? Optional.empty()
                        : Optional.ofNullable(metadata.snapshot(latest.snapshotId()));
            }
        };
    }

    /**
     * Returns the schema a snapshot was written with, or the current schema when the snapshot
     * does not name one (tables written before schema IDs).
     *
     * @param metadata table metadata
     * @param snapshot snapshot of the metadata
     * @return schema
     */
    static Schema schemaOf(TableMetadata metadata, Snapshot snapshot) {
        Integer id = snapshot.schemaId();
        Schema schema = id == null ? null : metadata.schemasById().get(id);
        return schema != null ? schema : metadata.schema();
    }

    /**
     * Describes why a point has no snapshot, for the client's error.
     *
     * @param table table name
     * @param metadata the table's current metadata
     * @param point time-travel point
     * @return message
     */
    static String missing(TableName table, TableMetadata metadata, AsOf point) {
        return switch (point) {
            case AsOf.SnapshotId id -> "Table " + table + " has no snapshot " + id.id()
                    + " (unknown or expired)";
            case AsOf.Ref ref -> "Table " + table + " has no branch or tag '" + ref.name() + "'";
            case AsOf.Timestamp at -> {
                List<HistoryEntry> log = metadata.snapshotLog();
                yield "Table " + table + " has no snapshot committed at or before " + at.at()
                        + (log.isEmpty() ? " (it has no snapshots)" : " (first commit "
                        + java.time.Instant.ofEpochMilli(log.stream()
                                .mapToLong(HistoryEntry::timestampMillis).min().orElseThrow())
                        + ")");
            }
        };
    }
}
