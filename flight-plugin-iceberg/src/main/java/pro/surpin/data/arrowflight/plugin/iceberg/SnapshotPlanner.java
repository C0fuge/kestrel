package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Plans Iceberg snapshots for the server: the data files of a snapshot with the delete files
 * that apply, the columns of a schema as DuckDB types with their field IDs, and what keeps the
 * files from being read one by one. Snapshot IDs are {@code <snapshot id>.<schema id>}, so that
 * an executing node reads the same files with the same columns as the planning node. Plans are
 * cached (the most recently used).
 */
final class SnapshotPlanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotPlanner.class);
    /** Snapshot ID of a table without a snapshot. */
    static final String NO_SNAPSHOT = "none";
    /** Planned snapshots kept, the most recently used. */
    private static final int PLANNED_CAPACITY = 256;

    private final String name;
    private final StoresFileIO io;
    private final ExecutorService planPool;
    private final Map<String, Planned> planned = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, true) {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Planned> eldest) {
                    return size() > PLANNED_CAPACITY;
                }
            });

    /**
     * A planned snapshot with what keeps its files from being read one by one.
     *
     * @param snapshot snapshot
     * @param problem reason, or null
     */
    record Planned(TableSnapshot snapshot, String problem) {
    }

    /**
     * A parsed snapshot ID.
     *
     * @param snapshotId Iceberg snapshot ID, null for a table without a snapshot
     * @param schemaId schema ID
     */
    record PlanId(Long snapshotId, int schemaId) {

        /**
         * Reads a snapshot ID this planner made.
         *
         * @param id {@code <snapshot id>.<schema id>}
         * @return parsed ID, empty when it is not one
         */
        static Optional<PlanId> parse(String id) {
            int dot = id.lastIndexOf('.');
            if (dot <= 0) {
                return Optional.empty();
            }
            String snapshotPart = id.substring(0, dot);
            try {
                return Optional.of(new PlanId(NO_SNAPSHOT.equals(snapshotPart) ? null
                        : Long.valueOf(snapshotPart), Integer.parseInt(id.substring(dot + 1))));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
    }

    /**
     * Creates a planner.
     *
     * @param name catalog or format name, for Iceberg's table names
     * @param io file IO over the server's stores
     * @param planPool threads reading manifests
     */
    SnapshotPlanner(String name, StoresFileIO io, ExecutorService planPool) {
        this.name = name;
        this.io = io;
        this.planPool = planPool;
    }

    /**
     * Tells why the files of a snapshot this planner planned cannot be read one by one.
     *
     * @param snapshot snapshot
     * @return reason, empty when they can
     */
    Optional<String> partialReadProblem(TableSnapshot snapshot) {
        Planned plan = planned.get(snapshot.fingerprint());
        if (plan == null) {
            // Never say "readable" for a snapshot this planner did not plan (or forgot).
            return Optional.of("snapshot " + snapshot.snapshotId() + " is not planned by this catalog");
        }
        return Optional.ofNullable(plan.problem());
    }

    /**
     * Plans a snapshot: its data files with their deletes, read with a schema.
     *
     * @param table table name
     * @param metadataLocation location of the metadata file
     * @param metadata parsed metadata
     * @param snapshot snapshot, null for a table without one
     * @param schema schema the columns come from
     * @return planned snapshot, cached
     * @throws IOException when a manifest cannot be read
     */
    Planned plan(TableName table, String metadataLocation, TableMetadata metadata, Snapshot snapshot,
            Schema schema) throws IOException {
        return plan(table, metadataLocation, metadata, snapshot, schema, io);
    }

    /**
     * Plans a snapshot, reading its manifests with a given FileIO (for example one with the
     * table's vended credentials).
     *
     * @param table table name
     * @param metadataLocation location of the metadata file
     * @param metadata parsed metadata
     * @param snapshot snapshot, null for a table without one
     * @param schema schema the columns come from
     * @param fileIo FileIO the manifests are read with
     * @return planned snapshot, cached
     * @throws IOException when a manifest cannot be read
     */
    Planned plan(TableName table, String metadataLocation, TableMetadata metadata, Snapshot snapshot,
            Schema schema, StoresFileIO fileIo) throws IOException {
        String snapshotId = (snapshot == null ? NO_SNAPSHOT : Long.toString(snapshot.snapshotId()))
                + "." + schema.schemaId();
        String fingerprint = metadataLocation + "#" + snapshotId;
        Planned cached = planned.get(fingerprint);
        if (cached != null) {
            return cached;
        }
        List<String> problems = new ArrayList<>();
        // Sorted by location: manifests are read in parallel, and plans must be stable.
        Map<String, TableFile> files = new TreeMap<>();
        if (snapshot != null) {
            try {
                BaseTable iceberg = new BaseTable(new StaticTableOperations(metadata, fileIo),
                        name + "." + table);
                TableScan scan = iceberg.newScan().useSnapshot(snapshot.snapshotId()).planWith(planPool);
                try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                    for (FileScanTask task : tasks) {
                        org.apache.iceberg.DataFile file = task.file();
                        String location = Locations.normalize(file.location());
                        if (file.format() != FileFormat.PARQUET && problems.stream()
                                .noneMatch(p -> p.startsWith("data files in"))) {
                            problems.add("data files in " + file.format() + " format");
                        }
                        List<DeleteFile> deletes = new ArrayList<>();
                        for (org.apache.iceberg.DeleteFile delete : task.deletes()) {
                            deletes.add(deleteFile(delete));
                        }
                        files.putIfAbsent(location, new TableFile(location, file.fileSizeInBytes(), 0L,
                                null, null, file.recordCount(), deletes));
                    }
                }
            } catch (UncheckedIOException e) {
                throw e.getCause();
            } catch (RuntimeException e) {
                throw new IOException("Cannot plan Iceberg table " + table + " snapshot "
                        + snapshot.snapshotId() + ": " + e.getMessage(), e);
            }
        }
        List<ColumnSpec> columns = new ArrayList<>();
        for (Types.NestedField field : schema.columns()) {
            String type = DuckDbTypes.of(field.type());
            if (type == null) {
                problems.add("column " + field.name() + " has type " + field.type()
                        + ", which DuckDB's Parquet reader does not map");
                type = field.type().toString();
            }
            columns.add(new ColumnSpec(field.name(), type, field.isOptional(), field.fieldId()));
            if (field.initialDefault() != null) {
                problems.add("column " + field.name() + " has an initial default value");
            }
            String nested = nestedChange(metadata, field);
            if (nested != null) {
                problems.add(nested);
            }
        }
        if (metadata.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
            problems.add("the table has a name mapping (files without field IDs)");
        }
        TableSnapshot result = new TableSnapshot(table, snapshotId, fingerprint, columns, List.of(),
                new ArrayList<>(files.values()), new ReadSpec.IcebergMetadata(
                        Locations.normalize(metadataLocation)));
        Planned plan = new Planned(result, problems.isEmpty() ? null : String.join("; ", problems));
        planned.put(fingerprint, plan);
        LOGGER.debug("iceberg=planned table={} snapshot={} files={} problem={}", table, snapshotId,
                files.size(), plan.problem());
        return plan;
    }

    /**
     * Describes a change of a nested column's fields between the table's schemas: read_parquet
     * maps nested fields by name, so files written before such a change would read wrong.
     *
     * @param metadata table metadata with every schema
     * @param field top-level column
     * @return reason, or null when the column is primitive or its fields never changed
     */
    static String nestedChange(TableMetadata metadata, Types.NestedField field) {
        if (field.type().isPrimitiveType()) {
            return null;
        }
        String shape = shape(field.type());
        for (Schema other : metadata.schemas()) {
            Types.NestedField earlier = other.findField(field.fieldId());
            if (earlier != null && !earlier.type().isPrimitiveType()
                    && !shape.equals(shape(earlier.type()))) {
                return "nested column " + field.name() + " changed its fields (schema "
                        + other.schemaId() + ")";
            }
            if (earlier != null && earlier.type().isPrimitiveType()) {
                return "column " + field.name() + " changed from primitive to nested";
            }
        }
        return null;
    }

    /**
     * Describes the nested fields of a type: IDs, names, optionality and types.
     *
     * @param type type
     * @return description
     */
    private static String shape(Type type) {
        if (type.isPrimitiveType()) {
            return type.toString();
        }
        StringBuilder result = new StringBuilder(type.typeId().name()).append('<');
        for (Types.NestedField child : type.asNestedType().fields()) {
            result.append(child.fieldId()).append(':').append(child.name()).append(':')
                    .append(child.isOptional()).append(':').append(shape(child.type())).append(',');
        }
        return result.append('>').toString();
    }

    /**
     * Converts an Iceberg delete file.
     *
     * @param delete delete file
     * @return SPI delete file
     */
    private static DeleteFile deleteFile(org.apache.iceberg.DeleteFile delete) {
        DeleteFile.Kind kind;
        if (delete.content() == FileContent.EQUALITY_DELETES) {
            kind = DeleteFile.Kind.EQUALITY;
        } else if (delete.format() == FileFormat.PUFFIN) {
            kind = DeleteFile.Kind.DELETION_VECTOR;
        } else {
            kind = DeleteFile.Kind.POSITION;
        }
        return new DeleteFile(Locations.normalize(delete.location()), delete.fileSizeInBytes(), kind,
                delete.equalityFieldIds());
    }
}
