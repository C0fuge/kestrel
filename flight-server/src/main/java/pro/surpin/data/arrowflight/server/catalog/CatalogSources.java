package pro.surpin.data.arrowflight.server.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
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
        StringBuilder sql = new StringBuilder(fileList(locations)).append(", schema = MAP {");
        List<ColumnSpec> columns = snapshot.columns();
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
