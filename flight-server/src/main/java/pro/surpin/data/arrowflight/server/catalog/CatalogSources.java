package pro.surpin.data.arrowflight.server.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Names data files of a catalog snapshot as a DuckDB FROM source with the snapshot's declared
 * columns. Iceberg files are read by Parquet field ID; Parquet files of other catalogs (Hive) by
 * column name, with each partition column a constant of its declared type taken from the
 * catalog, never from the file paths: one {@code read_parquet} per set of partition values,
 * joined with UNION ALL (DuckDB drops the branches a filter on a partition column rules out).
 */
public final class CatalogSources {

    /** Row number column read_parquet adds for position deletes. */
    private static final String ROW_NUMBER = "file_row_number";

    private CatalogSources() {
    }

    /**
     * Names some data files of a snapshot as a FROM source.
     *
     * @param snapshot snapshot with its columns
     * @param files files of the snapshot, with their partition values
     * @return FROM source
     * @throws IllegalArgumentException when no file is given
     */
    public static String filesSource(TableSnapshot snapshot, List<TableFile> files) {
        if (files.isEmpty()) {
            throw new IllegalArgumentException("No data files of " + snapshot.name() + " to read");
        }
        if (snapshot.read() instanceof ReadSpec.IcebergMetadata) {
            List<String> locations = new ArrayList<>();
            for (TableFile file : files) {
                locations.add(file.location());
            }
            return fieldIdSource(snapshot, locations);
        }
        List<String> partitions = snapshot.partitionColumns();
        if (partitions.isEmpty()) {
            List<String> locations = new ArrayList<>();
            for (TableFile file : files) {
                locations.add(file.location());
            }
            return nameSource(snapshot, locations);
        }
        Map<String, ColumnSpec> declared = new LinkedHashMap<>();
        for (ColumnSpec column : snapshot.columns()) {
            declared.put(column.name(), column);
        }
        // Files grouped by their partition values, in the order the files come.
        Map<List<String>, List<String>> groups = new LinkedHashMap<>();
        for (TableFile file : files) {
            List<String> values = new ArrayList<>(partitions.size());
            for (String partition : partitions) {
                values.add(file.partitionValues().get(partition));
            }
            groups.computeIfAbsent(values, v -> new ArrayList<>()).add(file.location());
        }
        StringBuilder sql = new StringBuilder("(");
        boolean first = true;
        for (Map.Entry<List<String>, List<String>> group : groups.entrySet()) {
            sql.append(first ? "" : " UNION ALL ").append("SELECT *");
            for (int i = 0; i < partitions.size(); i++) {
                ColumnSpec column = declared.get(partitions.get(i));
                if (column == null) {
                    throw new IllegalArgumentException("Partition column " + partitions.get(i)
                            + " of " + snapshot.name() + " has no declared type");
                }
                sql.append(", ").append(typedConstant(group.getKey().get(i), column.duckDbType()))
                        .append(" AS ").append(DuckDbAdapter.quoteIdentifier(column.name()));
            }
            sql.append(" FROM ").append(nameSource(snapshot, group.getValue()));
            first = false;
        }
        return sql.append(')').toString();
    }

    /**
     * Names some data files of an Iceberg snapshot as a FROM source that applies their delete
     * files itself, with read_parquet only: a row is dropped when a position delete names its
     * file and row number, or when an equality delete row has the same values (null matching
     * null) in that delete's equality columns (anti hash joins). Which delete files apply to which data file
     * (sequence numbers, partitions) is the catalog's plan, as in {@link TableFile#deletes()}.
     * Files with the same delete files share one read_parquet; a file with position deletes is
     * read alone, with its row numbers. Unlike DuckDB 1.4.5's iceberg_scan this reads only the
     * columns a query uses (plus the equality columns), and a query's filters may go into the
     * scans.
     *
     * @param snapshot Iceberg snapshot with its columns
     * @param files files of the snapshot, with their delete files
     * @return FROM source with the declared columns, or null when the deletes cannot be applied
     *         this way (deletion vectors, columns without field ID, an equality column the
     *         schema no longer has, a column named {@code file_row_number})
     */
    public static String deletesAppliedSource(TableSnapshot snapshot, List<TableFile> files) {
        Map<Integer, ColumnSpec> byFieldId = applicableColumns(snapshot, files);
        if (byFieldId == null) {
            return null;
        }
        // Data files grouped by their delete files; a file with position deletes on its own.
        Map<List<DeleteFile>, List<String>> groups = new LinkedHashMap<>();
        List<TableFile> positional = new ArrayList<>();
        for (TableFile file : files) {
            boolean positions = file.deletes().stream()
                    .anyMatch(delete -> delete.kind() == DeleteFile.Kind.POSITION);
            if (positions) {
                positional.add(file);
            } else {
                groups.computeIfAbsent(file.deletes(), d -> new ArrayList<>()).add(file.location());
            }
        }
        List<String> branches = new ArrayList<>();
        for (Map.Entry<List<DeleteFile>, List<String>> group : groups.entrySet()) {
            branches.add("SELECT del_d.* FROM " + fieldIdRead(snapshot.columns(), group.getValue(), "")
                    + " AS del_d" + equalityJoins(group.getKey(), byFieldId));
        }
        for (TableFile file : positional) {
            List<String> positionFiles = new ArrayList<>();
            for (DeleteFile delete : file.deletes()) {
                if (delete.kind() == DeleteFile.Kind.POSITION) {
                    positionFiles.add(delete.location());
                }
            }
            String location = CatalogLocations.normalize(file.location());
            branches.add("SELECT del_d.* EXCLUDE (" + ROW_NUMBER + ") FROM "
                    + fieldIdRead(snapshot.columns(), List.of(location), ", " + ROW_NUMBER + " = true")
                    + " AS del_d ANTI JOIN (SELECT pos FROM " + fileList(positionFiles) + ") WHERE "
                    + normalizedLocation("file_path") + " = " + DuckDbAdapter.sqlStringLiteral(location)
                    + ") AS del_p ON del_p.pos = del_d." + ROW_NUMBER
                    + equalityJoins(file.deletes(), byFieldId));
        }
        return "(" + String.join(" UNION ALL ", branches) + ")";
    }

    /**
     * Tells whether {@link #deletesAppliedSource} applies the delete files of some data files of
     * a snapshot (it holds for every subset of files it holds for).
     *
     * @param snapshot snapshot with its columns
     * @param files files of the snapshot, with their delete files
     * @return true when the source can be written
     */
    public static boolean deletesApplicable(TableSnapshot snapshot, List<TableFile> files) {
        return applicableColumns(snapshot, files) != null;
    }

    /**
     * Checks that the delete files of some data files can be applied with read_parquet and
     * returns the snapshot's columns by field ID.
     *
     * @param snapshot snapshot with its columns
     * @param files files of the snapshot, with their delete files
     * @return columns by field ID, or null when the deletes cannot be applied this way
     */
    private static Map<Integer, ColumnSpec> applicableColumns(TableSnapshot snapshot,
            List<TableFile> files) {
        if (!(snapshot.read() instanceof ReadSpec.IcebergMetadata) || files.isEmpty()
                || snapshot.columns().isEmpty()) {
            return null;
        }
        Map<Integer, ColumnSpec> byFieldId = new LinkedHashMap<>();
        for (ColumnSpec column : snapshot.columns()) {
            if (column.fieldId() == null
                    || ROW_NUMBER.equalsIgnoreCase(column.name())) {
                return null;
            }
            byFieldId.put(column.fieldId(), column);
        }
        for (TableFile file : files) {
            for (DeleteFile delete : file.deletes()) {
                switch (delete.kind()) {
                    case POSITION -> {
                        // Applied on the file's row numbers.
                    }
                    case EQUALITY -> {
                        if (delete.equalityFieldIds().isEmpty()
                                || !byFieldId.keySet().containsAll(delete.equalityFieldIds())) {
                            return null;
                        }
                    }
                    default -> {
                        return null;
                    }
                }
            }
        }
        return byFieldId;
    }

    /**
     * Writes one ANTI JOIN per set of equality columns of a data file's equality deletes; the
     * delete files with the same columns are read together.
     *
     * @param deletes delete files of the data file (other kinds are skipped)
     * @param byFieldId the snapshot's columns by field ID
     * @return joins to append after {@code del_d}, or empty
     */
    private static String equalityJoins(List<DeleteFile> deletes,
            Map<Integer, ColumnSpec> byFieldId) {
        Map<List<Integer>, List<String>> byColumns = new LinkedHashMap<>();
        for (DeleteFile delete : deletes) {
            if (delete.kind() == DeleteFile.Kind.EQUALITY) {
                byColumns.computeIfAbsent(delete.equalityFieldIds(), k -> new ArrayList<>())
                        .add(delete.location());
            }
        }
        StringBuilder sql = new StringBuilder();
        int n = 0;
        for (Map.Entry<List<Integer>, List<String>> entry : byColumns.entrySet()) {
            String alias = "del_e" + n++;
            List<ColumnSpec> keys = new ArrayList<>();
            StringBuilder match = new StringBuilder();
            for (Integer fieldId : entry.getKey()) {
                ColumnSpec column = byFieldId.get(fieldId);
                keys.add(column);
                String name = DuckDbAdapter.quoteIdentifier(column.name());
                match.append(match.isEmpty() ? "" : " AND ").append(alias).append('.').append(name)
                        .append(" IS NOT DISTINCT FROM del_d.").append(name);
            }
            sql.append(" ANTI JOIN ").append(fieldIdRead(keys, entry.getValue(), ""))
                    .append(" AS ").append(alias).append(" ON ").append(match);
        }
        return sql.toString();
    }

    /**
     * Writes {@link CatalogLocations#normalize(String)} as a DuckDB expression, for the data file
     * paths in position delete files (Iceberg writes them as the catalog names the file, for
     * example with {@code file:}).
     *
     * @param expression SQL expression of a location
     * @return SQL expression of the normalised location
     */
    static String normalizedLocation(String expression) {
        String bare = "regexp_replace(trim(" + expression + "), '^[Ff][Ii][Ll][Ee]:(//)?', '')";
        String slashes = "CASE WHEN contains(" + bare + ", '://') THEN " + bare
                + " ELSE regexp_replace(" + bare + ", '^//+', '/') END";
        return "regexp_replace(" + slashes + ", '(.)/+$', '\\1')";
    }

    /**
     * Writes a partition value as a constant of its column's type: NULL for a missing value,
     * the text itself for text columns, else {@code TRY_CAST} (a value that does not convert
     * reads NULL, as in Hive).
     *
     * @param value partition value as the catalog stores it, or null
     * @param duckDbType declared DuckDB type
     * @return SQL constant
     */
    public static String typedConstant(String value, String duckDbType) {
        if (value == null) {
            return "CAST(NULL AS " + duckDbType + ")";
        }
        if ("VARCHAR".equals(duckDbType.toUpperCase(Locale.ROOT))) {
            return DuckDbAdapter.sqlStringLiteral(value);
        }
        return "TRY_CAST(" + DuckDbAdapter.sqlStringLiteral(value) + " AS " + duckDbType + ")";
    }

    /**
     * Reads files by Parquet field ID:
     * {@code read_parquet([...], schema = MAP {fieldId: {name, type, default_value}})}, so that
     * renamed and added columns and widened types read right.
     *
     * @param snapshot snapshot with its columns
     * @param locations data files
     * @return FROM source
     */
    static String fieldIdSource(TableSnapshot snapshot, List<String> locations) {
        return fieldIdRead(snapshot.columns(), locations, "");
    }

    /**
     * Reads some columns of files by Parquet field ID.
     *
     * @param columns columns to read, each with a field ID
     * @param locations files
     * @param options further read_parquet options, each starting with ", "; or empty
     * @return FROM source
     */
    private static String fieldIdRead(List<ColumnSpec> columns, List<String> locations,
            String options) {
        StringBuilder sql = new StringBuilder(fileList(locations)).append(options)
                .append(", schema = MAP {");
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec column = columns.get(i);
            sql.append(i > 0 ? ", " : "").append(column.fieldId().intValue())
                    .append(": {name: ").append(DuckDbAdapter.sqlStringLiteral(column.name()))
                    .append(", type: ").append(DuckDbAdapter.sqlStringLiteral(column.duckDbType()))
                    .append(", default_value: NULL}");
        }
        return sql.append("})").toString();
    }

    /**
     * Reads files by column name: the declared columns that are not partition columns, matched
     * to the file's columns by name ignoring case; a column a file lacks reads NULL.
     *
     * @param snapshot snapshot with its columns
     * @param locations data files
     * @return FROM source
     */
    static String nameSource(TableSnapshot snapshot, List<String> locations) {
        StringBuilder sql = new StringBuilder(fileList(locations)).append(", schema = MAP {");
        boolean first = true;
        for (ColumnSpec column : snapshot.columns()) {
            if (snapshot.partitionColumns().contains(column.name())) {
                continue;
            }
            String name = DuckDbAdapter.sqlStringLiteral(column.name());
            sql.append(first ? "" : ", ").append(name).append(": {name: ").append(name)
                    .append(", type: ").append(DuckDbAdapter.sqlStringLiteral(column.duckDbType()))
                    .append(", default_value: NULL}");
            first = false;
        }
        return sql.append("})").toString();
    }

    /**
     * Starts a read_parquet call over some files.
     *
     * @param locations files
     * @return {@code read_parquet([...]} without the closing parenthesis
     */
    private static String fileList(List<String> locations) {
        StringBuilder sql = new StringBuilder("read_parquet([");
        for (int i = 0; i < locations.size(); i++) {
            sql.append(i > 0 ? ", " : "")
                    .append(DuckDbAdapter.sqlStringLiteral(CatalogLocations.normalize(locations.get(i))));
        }
        return sql.append(']').toString();
    }
}
