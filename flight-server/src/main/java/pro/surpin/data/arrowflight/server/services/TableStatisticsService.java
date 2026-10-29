package pro.surpin.data.arrowflight.server.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.server.storage.DataStore;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

/**
 * Table statistics for a client's query optimizer, answered without planning a query: no ticket,
 * no node load. Tables of the data directory report the row count and per-column bounds, null
 * counts and sizes from their Parquet footers, read once per file set. Catalog tables report the
 * row count and size their catalog records for the current snapshot; with delete files the row
 * count is an upper bound, marked so.
 */
public final class TableStatisticsService {

    /** Name of the Flight action; the body is JSON {@code {schema, table, columns}}. */
    public static final String ACTION = "arrowflight.table_statistics";

    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ParquetAdapter parquetAdapter;
    private final ClusterService clusterService;
    private final Map<String, Footers> footers = new ConcurrentHashMap<>();

    /**
     * Statistics of one column.
     *
     * @param min smallest value as a number (decimals by value, dates as epoch days, timestamps
     *            as epoch seconds), or null when unknown (strings, missing footer statistics)
     * @param max largest value, or null when unknown
     * @param nulls number of nulls, or -1 when unknown
     * @param bytes uncompressed size of the column
     * @param ndv estimated number of distinct values; null, not estimated yet
     */
    public record ColumnStatistics(Double min, Double max, long nulls, long bytes, Long ndv) { }

    /**
     * Statistics of one table.
     *
     * @param rows row count, -1 when unknown
     * @param rowsExact false when {@code rows} is only an upper bound (delete files) or unknown
     * @param bytes size of the table's data files
     * @param files number of data files
     * @param columns statistics of the requested columns found in the footers
     */
    public record TableStatistics(long rows, boolean rowsExact, long bytes, int files,
            Map<String, ColumnStatistics> columns) { }

    /**
     * A request of the statistics action.
     *
     * @param schema schema name
     * @param table table name
     * @param columns columns to report; null or empty reports every column
     */
    public record Request(String schema, String table, List<String> columns) { }

    /** Footer aggregate of one table's file set. */
    private record Footers(String fingerprint, long rows, long bytes, int files,
            Map<String, ColumnAggregate> columns) { }

    /** Running aggregate of one column over the row groups of a file set. */
    private static final class ColumnAggregate {
        private Double min;
        private Double max;
        private boolean boundsKnown = true;
        private long nulls;
        private boolean nullsKnown = true;
        private long bytes;
    }

    /**
     * Creates the service.
     *
     * @param parquetAdapter data directory and catalog tables
     * @param clusterService live nodes, to tell whether this node sees every file
     */
    public TableStatisticsService(ParquetAdapter parquetAdapter, ClusterService clusterService) {
        this.parquetAdapter = parquetAdapter;
        this.clusterService = clusterService;
    }

    /**
     * Statistics of a table.
     *
     * @param schema schema name
     * @param table table name
     * @param columns columns to report; null or empty reports every column; unknown names are
     *                skipped
     * @return statistics
     * @throws IOException when the table cannot be listed or a footer cannot be read
     * @throws FileNotFoundException when the table does not exist or has no data files
     * @throws IllegalArgumentException when a name is invalid
     * @throws IllegalStateException when this node does not see every file of the table
     */
    public TableStatistics statistics(String schema, String table, List<String> columns)
            throws IOException {
        if (schema == null || schema.isEmpty() || table == null || table.isEmpty()) {
            throw new IllegalArgumentException("Name the schema and the table");
        }
        ParquetAdapter.validateName(schema);
        ParquetAdapter.validateName(table);
        if (parquetAdapter.isCatalogSchema(schema)) {
            return catalogStatistics(schema, table);
        }
        DataStore store = parquetAdapter.dataStore();
        try (DataStore.ReadScope scope = store.readScope()) {
            return directoryStatistics(store, schema, table, columns);
        }
    }

    /**
     * Number of file sets whose footers were read and aggregated, for tests.
     *
     * @return cached tables
     */
    int cachedTables() {
        return footers.size();
    }

    /**
     * Decodes a request of the statistics action.
     *
     * @param body JSON body
     * @return request
     * @throws IllegalArgumentException when the body is not a JSON request
     */
    public static Request parseRequest(byte[] body) {
        try {
            Request request = JSON.readValue(body, Request.class);
            if (request == null) {
                throw new IllegalArgumentException("Empty table statistics request");
            }
            return request;
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed table statistics request: "
                    + e.getMessage(), e);
        }
    }

    /**
     * Encodes statistics for the wire.
     *
     * @param statistics statistics
     * @return JSON text
     */
    public static String toJson(TableStatistics statistics) {
        try {
            return JSON.writeValueAsString(statistics);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private TableStatistics catalogStatistics(String schema, String table) throws IOException {
        CatalogTables.Resolved resolved = parquetAdapter.catalogTables().current(schema, table)
                .orElseThrow(() -> new FileNotFoundException(
                        "No table " + schema + "." + table + " in its catalog"));
        TableSnapshot snapshot = resolved.snapshot();
        if (snapshot.read() instanceof ReadSpec.AttachedTable) {
            // The rows live in the remote database: no count or size is known here.
            return new TableStatistics(-1, false, 0, 0, Map.of());
        }
        long rows = 0;
        long bytes = 0;
        boolean exact = true;
        for (TableFile file : snapshot.files()) {
            bytes += file.size();
            if (!file.deletes().isEmpty()) {
                exact = false;
            }
            rows = rows < 0 || file.recordCount() < 0 ? -1 : rows + file.recordCount();
        }
        return new TableStatistics(rows, exact && rows >= 0, bytes, snapshot.files().size(),
                Map.of());
    }

    private TableStatistics directoryStatistics(DataStore store, String schema, String table,
            List<String> columns) throws IOException {
        List<DataFile> files = parquetAdapter.tableFiles(schema, table);
        if (files.isEmpty()) {
            throw new FileNotFoundException("No Parquet files for table " + schema + "." + table);
        }
        if (!parquetAdapter.sharedStorage()) {
            int live = clusterService.filterLiveServers(
                    clusterService.allServerLoads().keySet()).size();
            if (live > 1) {
                throw new IllegalStateException("Table statistics need storage every node can"
                        + " read (HDFS, S3); this cluster of " + live
                        + " nodes keeps its files on local disks");
            }
        }
        String key = schema + "." + table;
        String fingerprint = ParquetAdapter.filesFingerprint(files) + "|" + files.hashCode();
        Footers cached = footers.get(key);
        if (cached == null || !cached.fingerprint().equals(fingerprint)) {
            long t = LogUtil.mark();
            cached = readFooters(store, files, fingerprint);
            footers.put(key, cached);
            LogUtil.logTiming(t, "statistics.footers", "table=" + key + " files=" + files.size());
        }
        Map<String, ColumnStatistics> result = new LinkedHashMap<>();
        List<String> wanted = columns == null || columns.isEmpty()
                ? List.copyOf(cached.columns().keySet()) : columns;
        for (String column : wanted) {
            String name = columnName(cached.columns(), column);
            if (name == null || result.containsKey(name)) {
                continue;
            }
            ColumnAggregate aggregate = cached.columns().get(name);
            result.put(name, new ColumnStatistics(
                    aggregate.boundsKnown ? aggregate.min : null,
                    aggregate.boundsKnown ? aggregate.max : null,
                    aggregate.nullsKnown ? aggregate.nulls : -1, aggregate.bytes, null));
        }
        return new TableStatistics(cached.rows(), true, cached.bytes(), cached.files(), result);
    }

    private static String columnName(Map<String, ColumnAggregate> columns, String requested) {
        if (requested == null) {
            return null;
        }
        if (columns.containsKey(requested)) {
            return requested;
        }
        String lower = requested.toLowerCase(Locale.ROOT);
        for (String name : columns.keySet()) {
            if (name.toLowerCase(Locale.ROOT).equals(lower)) {
                return name;
            }
        }
        return null;
    }

    private static Footers readFooters(DataStore store, List<DataFile> files, String fingerprint)
            throws IOException {
        Map<String, ColumnAggregate> columns = new LinkedHashMap<>();
        long rows = 0;
        long bytes = 0;
        for (DataFile file : files) {
            bytes += file.size();
            try (ParquetFileReader reader = ParquetFileReader.open(store.inputFile(file))) {
                for (BlockMetaData block : reader.getFooter().getBlocks()) {
                    rows += block.getRowCount();
                    for (ColumnChunkMetaData chunk : block.getColumns()) {
                        if (chunk.getPath().size() != 1) {
                            continue;   // nested fields: no bounds a client can use
                        }
                        merge(columns.computeIfAbsent(chunk.getPath().toDotString(),
                                name -> new ColumnAggregate()), chunk);
                    }
                }
            }
        }
        return new Footers(fingerprint, rows, bytes, files.size(), columns);
    }

    private static void merge(ColumnAggregate aggregate, ColumnChunkMetaData chunk) {
        aggregate.bytes += chunk.getTotalUncompressedSize();
        Statistics<?> statistics = chunk.getStatistics();
        if (statistics == null || statistics.isEmpty() || !statistics.isNumNullsSet()) {
            aggregate.nullsKnown = false;
        } else {
            aggregate.nulls += statistics.getNumNulls();
        }
        if (!aggregate.boundsKnown) {
            return;
        }
        if (statistics == null || statistics.isEmpty()) {
            aggregate.boundsKnown = false;
            return;
        }
        if (!statistics.hasNonNullValue()) {
            if (!statistics.isNumNullsSet() || statistics.getNumNulls() != chunk.getValueCount()) {
                aggregate.boundsKnown = false;   // no bounds, but not because every value is null
            }
            return;
        }
        Double low = number(statistics.genericGetMin(), chunk.getPrimitiveType());
        Double high = number(statistics.genericGetMax(), chunk.getPrimitiveType());
        if (low == null || high == null) {
            aggregate.boundsKnown = false;
            return;
        }
        aggregate.min = aggregate.min == null ? low : Math.min(aggregate.min, low);
        aggregate.max = aggregate.max == null ? high : Math.max(aggregate.max, high);
    }

    /**
     * Converts a footer bound to a number a client can compare with its predicates.
     *
     * @param value footer bound
     * @param type physical and logical type of the column
     * @return the number, or null when the type has no numeric bound
     */
    static Double number(Comparable<?> value, PrimitiveType type) {
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            BigInteger unscaled;
            if (value instanceof Integer || value instanceof Long) {
                unscaled = BigInteger.valueOf(((Number) value).longValue());
            } else if (value instanceof Binary binary) {
                unscaled = new BigInteger(binary.getBytes());
            } else {
                return null;
            }
            return new BigDecimal(unscaled, decimal.getScale()).doubleValue();
        }
        if (value instanceof Integer || value instanceof Long) {
            return integer(((Number) value).longValue(), value instanceof Integer, logical);
        }
        if ((value instanceof Float || value instanceof Double) && logical == null) {
            double number = ((Number) value).doubleValue();
            return Double.isNaN(number) || Double.isInfinite(number) ? null : number;
        }
        return null;
    }

    private static Double integer(long raw, boolean int32, LogicalTypeAnnotation logical) {
        if (logical == null || logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return (double) raw;
        }
        if (logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intType) {
            if (intType.isSigned()) {
                return (double) raw;
            }
            if (int32) {
                return (double) Integer.toUnsignedLong((int) raw);
            }
            return new BigDecimal(new BigInteger(Long.toUnsignedString(raw))).doubleValue();
        }
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
            return switch (timestamp.getUnit()) {
                case MILLIS -> raw / 1_000.0;
                case MICROS -> raw / 1_000_000.0;
                case NANOS -> raw / 1_000_000_000.0;
            };
        }
        return null;   // TIME and other annotations: no comparable number
    }
}
