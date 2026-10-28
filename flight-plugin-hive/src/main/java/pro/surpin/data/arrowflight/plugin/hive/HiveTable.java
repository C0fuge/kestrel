package pro.surpin.data.arrowflight.plugin.hive;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.orc.TypeDescription;

import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;

/**
 * A metastore table the catalog can show: a Parquet, ORC or text table with its columns (data
 * columns, then partition columns, as DuckDB types) or an Iceberg table with its metadata file.
 *
 * @param database database name
 * @param name table name
 * @param kind how it is read
 * @param location normalised table location
 * @param columns declared columns, partition columns last (Parquet, ORC and text tables)
 * @param partitionColumns partition column names (Parquet, ORC and text tables)
 * @param schemaId hash of the columns and their types (and of a text table's format); the
 *     snapshot ID of Parquet, ORC and text tables
 * @param metadataLocation normalised current metadata file (Iceberg tables), or null
 * @param dataColumns data columns with their Hive types (ORC and text tables), else empty
 * @param textFormat how the files are laid out (text tables), else null
 */
record HiveTable(String database, String name, Kind kind, String location,
        List<ColumnSpec> columns, List<String> partitionColumns, String schemaId,
        String metadataLocation, List<OrcArrowReader.Column> dataColumns, TextFormat textFormat) {

    /** How a table is read. */
    enum Kind {
        /** Parquet files, read by the server. */
        PARQUET,
        /** Iceberg, planned by the iceberg plugin's table format. */
        ICEBERG,
        /** ORC files, read by the plugin and handed to the server as Arrow streams. */
        ORC,
        /** Text files of LazySimpleSerDe, read by the plugin and handed over as Arrow streams. */
        TEXT
    }

    /**
     * The outcome of looking at a metastore table.
     *
     * @param table the table when it can be shown
     * @param skipped why it is not shown (a format or type the catalog does not read), or null
     * @param hidden why it is hidden (a location outside the roots), or null
     */
    record Classified(HiveTable table, String skipped, String hidden) {

        /**
         * Returns the table when it can be shown.
         *
         * @return table
         */
        Optional<HiveTable> shown() {
            return Optional.ofNullable(table);
        }
    }

    /**
     * Looks at a metastore table.
     *
     * @param table metastore table
     * @param roots readable roots
     * @param icebergFormat whether Iceberg tables can be read
     * @param orcTables whether ORC tables are shown
     * @param textTables whether text tables are shown
     * @return the table, or why it is skipped or hidden
     */
    static Classified classify(Table table, List<String> roots, boolean icebergFormat,
            boolean orcTables, boolean textTables) {
        String type = table.getTableType() == null ? "" : table.getTableType();
        Map<String, String> params = table.getParameters() == null ? Map.of() : table.getParameters();
        if (type.toUpperCase(Locale.ROOT).contains("VIEW")) {
            return skipped("view (" + type + ")");
        }
        StorageDescriptor sd = table.getSd();
        String location = sd == null || sd.getLocation() == null || sd.getLocation().isBlank() ? null
                : HiveLocations.normalize(sd.getLocation());
        if ("ICEBERG".equalsIgnoreCase(params.getOrDefault("table_type", ""))) {
            String metadata = params.get("metadata_location");
            if (!icebergFormat) {
                return skipped("Iceberg table (no iceberg table format: install the iceberg plugin,"
                        + " hive.icebergTables)");
            }
            if (metadata == null || metadata.isBlank()) {
                return skipped("Iceberg table without metadata_location");
            }
            String normalized = HiveLocations.normalize(metadata);
            if (!HiveLocations.underAny(normalized, roots)
                    || (location != null && !HiveLocations.underAny(location, roots))) {
                return hidden(location + " / " + normalized);
            }
            return new Classified(new HiveTable(table.getDbName(), table.getTableName(), Kind.ICEBERG,
                    location, List.of(), List.of(), "iceberg", normalized, List.of(), null), null, null);
        }
        if (params.containsKey("storage_handler")) {
            return skipped("storage handler " + params.get("storage_handler"));
        }
        boolean orc = sd != null && orc(sd);
        boolean text = sd != null && text(sd);
        if ("true".equalsIgnoreCase(params.getOrDefault("transactional", "false"))) {
            return skipped(orc ? "transactional (ACID) ORC table: not read yet"
                    : "transactional (ACID) table");
        }
        String provider = params.get("spark.sql.sources.provider");
        if (provider != null && !"parquet".equalsIgnoreCase(provider) && !"hive".equalsIgnoreCase(provider)) {
            return skipped("Spark " + provider + " table");
        }
        if (orc && !orcTables) {
            return skipped("ORC table (hive.orcTables=false)");
        }
        if (text && !textTables) {
            return skipped("text table (hive.textTables=false)");
        }
        if (sd == null || !(orc || text || parquet(sd))) {
            String serde = sd == null || sd.getSerdeInfo() == null ? null
                    : sd.getSerdeInfo().getSerializationLib();
            return skipped("storage format " + (sd == null ? "none" : sd.getInputFormat()
                    + (serde == null ? "" : " with serde " + serde)));
        }
        if (location == null) {
            return skipped("no location");
        }
        if (!HiveLocations.underAny(location, roots)) {
            return hidden(location);
        }
        List<ColumnSpec> columns = new ArrayList<>();
        List<String> partitions = new ArrayList<>();
        List<OrcArrowReader.Column> dataColumns = new ArrayList<>();
        TextFormat format = null;
        try {
            if (text) {
                format = csv(sd) ? TextFormat.csv(sd.getSerdeInfo().getParameters(), params)
                        : TextFormat.of(sd.getSerdeInfo().getParameters(), params);
            }
            for (FieldSchema field : sd.getCols()) {
                columns.add(column(field));
                if (orc || text) {
                    dataColumns.add(orcColumn(field));
                }
                if (text) {
                    TextArrowReader.checkType(field.getName(),
                            TypeDescription.fromString(field.getType()), format);
                }
            }
            if (table.getPartitionKeys() != null) {
                for (FieldSchema field : table.getPartitionKeys()) {
                    columns.add(column(field));
                    partitions.add(field.getName());
                }
            }
        } catch (IllegalArgumentException e) {
            return skipped(e.getMessage());
        }
        if (columns.size() == partitions.size()) {
            return skipped("no data columns");
        }
        Kind kind = orc ? Kind.ORC : text ? Kind.TEXT : Kind.PARQUET;
        return new Classified(new HiveTable(table.getDbName(), table.getTableName(), kind,
                location, columns, partitions, schemaId(table, kind, columns, partitions, format),
                null, List.copyOf(dataColumns), format), null, null);
    }

    /**
     * Tells whether a storage descriptor names ORC.
     *
     * @param sd storage descriptor
     * @return true for an ORC serde or input format
     */
    private static boolean orc(StorageDescriptor sd) {
        String serde = sd.getSerdeInfo() == null || sd.getSerdeInfo().getSerializationLib() == null
                ? "" : sd.getSerdeInfo().getSerializationLib();
        String input = sd.getInputFormat() == null ? "" : sd.getInputFormat();
        return simpleName(serde).contains("orc") || simpleName(input).contains("orc");
    }

    /**
     * Tells whether a storage descriptor names a text table the plugin reads.
     *
     * @param sd storage descriptor
     * @return true for {@code TextInputFormat} with {@code LazySimpleSerDe} or {@code OpenCSVSerde}
     */
    static boolean text(StorageDescriptor sd) {
        String serde = sd.getSerdeInfo() == null || sd.getSerdeInfo().getSerializationLib() == null
                ? "" : sd.getSerdeInfo().getSerializationLib();
        String input = sd.getInputFormat() == null ? "" : sd.getInputFormat();
        return "textinputformat".equals(simpleName(input))
                && ("lazysimpleserde".equals(simpleName(serde)) || csv(sd));
    }

    /**
     * Tells whether a storage descriptor names Hive's {@code OpenCSVSerde}.
     *
     * @param sd storage descriptor
     * @return true for {@code org.apache.hadoop.hive.serde2.OpenCSVSerde}
     */
    static boolean csv(StorageDescriptor sd) {
        return sd.getSerdeInfo() != null && sd.getSerdeInfo().getSerializationLib() != null
                && "opencsvserde".equals(simpleName(sd.getSerdeInfo().getSerializationLib()));
    }

    /**
     * Returns the lower case simple name of a class name.
     *
     * @param className class name
     * @return text after the last dot
     */
    private static String simpleName(String className) {
        return className.substring(className.lastIndexOf('.') + 1).toLowerCase(Locale.ROOT);
    }

    /**
     * Checks that a data column has an Arrow type and keeps its Hive type.
     *
     * @param field metastore column
     * @return the column for the ORC reader
     * @throws IllegalArgumentException when ORC or Arrow has no counterpart of the type
     */
    private static OrcArrowReader.Column orcColumn(FieldSchema field) {
        try {
            OrcArrowReader.field(field.getName(), TypeDescription.fromString(field.getType()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("column " + field.getName() + " has type "
                    + field.getType() + " that the plugin cannot read: " + e.getMessage(), e);
        }
        return new OrcArrowReader.Column(field.getName(), field.getType());
    }

    /**
     * Tells whether a storage descriptor names Parquet.
     *
     * @param sd storage descriptor
     * @return true for the Parquet serde or input format
     */
    private static boolean parquet(StorageDescriptor sd) {
        String serde = sd.getSerdeInfo() == null || sd.getSerdeInfo().getSerializationLib() == null
                ? "" : sd.getSerdeInfo().getSerializationLib();
        String input = sd.getInputFormat() == null ? "" : sd.getInputFormat();
        return serde.toLowerCase(Locale.ROOT).contains("parquet")
                || input.toLowerCase(Locale.ROOT).contains("parquet");
    }

    /**
     * Maps a column.
     *
     * @param field metastore column
     * @return column with its DuckDB type
     * @throws IllegalArgumentException when the type has no counterpart
     */
    private static ColumnSpec column(FieldSchema field) {
        try {
            return new ColumnSpec(field.getName(), HiveTypes.duckDb(field.getType()), true, null);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("column " + field.getName() + " has type "
                    + field.getType() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Hashes what an executing node must read the same way as the planning node.
     *
     * @param table metastore table
     * @param kind how the table is read
     * @param columns declared columns
     * @param partitions partition columns
     * @param format a text table's format, else null
     * @return {@code h<16 hex>}
     */
    static String schemaId(Table table, Kind kind, List<ColumnSpec> columns,
            List<String> partitions, TextFormat format) {
        // Parquet tables keep the IDs they had before ORC tables were read.
        StringBuilder canonical = new StringBuilder(kind == Kind.ORC ? "hive-orc|"
                : kind == Kind.TEXT ? "hive-text|" + format.canonical() + "|" : "hive|")
                .append(table.getDbName()).append('.')
                .append(table.getTableName()).append('|');
        for (ColumnSpec column : columns) {
            canonical.append(column.name()).append(':').append(column.duckDbType()).append(';');
        }
        canonical.append('|').append(String.join(",", partitions));
        return "h" + hash(canonical.toString());
    }

    /**
     * Hashes a text.
     *
     * @param text text
     * @return the first 16 hex digits of its SHA-256
     */
    static String hash(String text) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is missing", e);
        }
    }

    /**
     * Names the table for logs.
     *
     * @return database.table
     */
    String qualified() {
        return database + "." + name;
    }

    private static Classified skipped(String reason) {
        return new Classified(null, reason, null);
    }

    private static Classified hidden(String location) {
        return new Classified(null, null, location);
    }
}
