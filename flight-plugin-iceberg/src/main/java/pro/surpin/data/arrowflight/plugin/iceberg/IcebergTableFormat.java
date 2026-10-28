package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

import pro.surpin.data.arrowflight.spi.catalog.AsOf;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;

/**
 * Iceberg as a table format other catalogs use (a Hive Metastore that registers Iceberg tables
 * with their {@code metadata_location}): the metadata file is read through the server's stores
 * and planned like the iceberg catalogs plan their tables.
 */
final class IcebergTableFormat implements TableFormat {

    /** Parsed metadata files kept (they never change), the most recently used. */
    private static final int METADATA_CAPACITY = 128;

    private final StoresFileIO io;
    private final ExecutorService planPool;
    private final SnapshotPlanner planner;
    private final Map<String, TableMetadata> metadata = Collections.synchronizedMap(
            new LinkedHashMap<>(32, 0.75f, true) {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(Map.Entry<String, TableMetadata> eldest) {
                    return size() > METADATA_CAPACITY;
                }
            });

    /**
     * Creates the format.
     *
     * @param io file IO over the server's stores
     * @param planPool threads reading manifests
     */
    IcebergTableFormat(StoresFileIO io, ExecutorService planPool) {
        this.io = io;
        this.planPool = planPool;
        this.planner = new SnapshotPlanner("iceberg", io, planPool);
    }

    @Override
    public Optional<TableSnapshot> snapshot(TableName table, String metadataLocation,
            String snapshotId) throws IOException {
        TableMetadata parsed = metadata(metadataLocation);
        Snapshot snapshot;
        Schema schema;
        if (snapshotId == null) {
            snapshot = parsed.currentSnapshot();
            schema = parsed.schema();
        } else {
            Optional<SnapshotPlanner.PlanId> id = SnapshotPlanner.PlanId.parse(snapshotId);
            if (id.isEmpty()) {
                return Optional.empty();
            }
            schema = parsed.schemasById().get(id.get().schemaId());
            snapshot = id.get().snapshotId() == null ? null : parsed.snapshot(id.get().snapshotId());
            if (schema == null || (id.get().snapshotId() != null && snapshot == null)) {
                return Optional.empty();
            }
        }
        return Optional.of(planner.plan(table, metadataLocation, parsed, snapshot, schema).snapshot());
    }

    @Override
    public TableSnapshot asOf(TableName table, String metadataLocation, AsOf point)
            throws IOException {
        TableMetadata parsed = metadata(metadataLocation);
        Snapshot snapshot = TimeTravel.find(parsed, point).orElseThrow(
                () -> new IllegalArgumentException(TimeTravel.missing(table, parsed, point)));
        return planner.plan(table, metadataLocation, parsed, snapshot,
                TimeTravel.schemaOf(parsed, snapshot)).snapshot();
    }

    @Override
    public Optional<String> partialReadProblem(TableSnapshot snapshot) {
        return planner.partialReadProblem(snapshot);
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        // iceberg needs avro loaded first.
        setup.loadExtension("avro");
        setup.loadExtension("iceberg");
    }

    @Override
    public void close() {
        planPool.shutdownNow();
    }

    /**
     * Reads a metadata file once.
     *
     * @param location metadata file
     * @return parsed metadata
     * @throws IOException when it cannot be read
     */
    private TableMetadata metadata(String location) throws IOException {
        TableMetadata cached = metadata.get(location);
        if (cached != null) {
            return cached;
        }
        try {
            TableMetadata parsed = TableMetadataParser.read(io, location);
            metadata.put(location, parsed);
            return parsed;
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (RuntimeException e) {
            throw new IOException("Cannot read Iceberg metadata " + location + ": " + e.getMessage(), e);
        }
    }
}
