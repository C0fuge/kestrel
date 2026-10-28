package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

/**
 * Compares a lineitem-like Hive table (TPC-H SF1 size by default) stored as Parquet with the same
 * rows as ORC (zlib and zstd), as uncompressed text (Hive's defaults) and as deflate Avro through
 * the server with the packaged hive plugin and hive.orcTables, textTables and avroTables on:
 * best-of-three wall time of count(*), TPC-H Q1- and Q6-style aggregations,
 * a filter on a sorted key (ORC row groups skipped by the hint) and a whole-table read, in both
 * result modes; complete answers of every table must equal the Parquet table's. Opt-in: perf tag
 * (workflow {@code hive-orc-perf.yml}). Rows: {@code -Dhive.orc.perf.rows} (default 6 000 000),
 * files per table: {@code -Dhive.orc.perf.files} (default 4); the table also goes to
 * {@code -Dhive.orc.perf.report} when set.
 */
@Tag("perf")
class HiveOrcPerfTest {

    private static final int ROUNDS = 3;
    private static final String DECIMAL = "{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":15,\"scale\":2}";
    private static final String DATE = "{\"type\":\"int\",\"logicalType\":\"date\"}";
    /** The lineitem-like table's Avro schema, fields in the columns' order. */
    private static final String AVRO_SCHEMA = "{\"type\":\"record\",\"name\":\"lineitem\","
            + "\"fields\":[{\"name\":\"l_orderkey\",\"type\":\"long\"},"
            + "{\"name\":\"l_partkey\",\"type\":\"long\"},"
            + "{\"name\":\"l_suppkey\",\"type\":\"long\"},"
            + "{\"name\":\"l_linenumber\",\"type\":\"int\"},"
            + "{\"name\":\"l_quantity\",\"type\":" + DECIMAL + "},"
            + "{\"name\":\"l_extendedprice\",\"type\":" + DECIMAL + "},"
            + "{\"name\":\"l_discount\",\"type\":" + DECIMAL + "},"
            + "{\"name\":\"l_tax\",\"type\":" + DECIMAL + "},"
            + "{\"name\":\"l_returnflag\",\"type\":\"string\"},"
            + "{\"name\":\"l_linestatus\",\"type\":\"string\"},"
            + "{\"name\":\"l_shipdate\",\"type\":" + DATE + "},"
            + "{\"name\":\"l_commitdate\",\"type\":" + DATE + "},"
            + "{\"name\":\"l_receiptdate\",\"type\":" + DATE + "},"
            + "{\"name\":\"l_shipinstruct\",\"type\":\"string\"},"
            + "{\"name\":\"l_shipmode\",\"type\":\"string\"},"
            + "{\"name\":\"l_comment\",\"type\":\"string\"}]}";
    private static final Map<String, String> QUERIES = new LinkedHashMap<>();

    static {
        QUERIES.put("count(*)", "SELECT count(*) AS n FROM hvperf.%s");
        QUERIES.put("Q1-style", "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty,"
                + " sum(l_extendedprice) AS sum_base_price,"
                + " sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,"
                + " sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,"
                + " count(*) AS count_order FROM hvperf.%s WHERE l_shipdate <= DATE '1998-09-02'"
                + " GROUP BY l_returnflag, l_linestatus");
        QUERIES.put("Q6-style", "SELECT sum(l_extendedprice * l_discount) AS revenue"
                + " FROM hvperf.%s WHERE l_shipdate >= DATE '1994-01-01'"
                + " AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07"
                + " AND l_quantity < 24");
        QUERIES.put("orderkey < 50000", "SELECT count(*) AS n, sum(l_quantity) AS q"
                + " FROM hvperf.%s WHERE l_orderkey < 50000");
        QUERIES.put("SELECT *", "SELECT * FROM hvperf.%s");
    }

    @TempDir
    static Path dir;

    /**
     * Writes the tables, checks the answers and prints the timings.
     *
     * @throws Exception on write, startup or query failure
     */
    @Test
    void orcVersusParquet() throws Exception {
        Assumptions.assumeTrue(HiveTestTables.available(), "flight-plugin-hive is not built");
        long rows = Long.getLong("hive.orc.perf.rows", 6_000_000L);
        int files = Integer.getInteger("hive.orc.perf.files", 4);
        HiveTestTables.Hms hms = HiveTestTables.hms();
        String columns = HiveTestTables.orcConstant("LINEITEM_COLUMNS");
        String lineitem = HiveTestTables.orcConstant("LINEITEM");
        Path lake = Files.createDirectories(dir.resolve("lake"));
        hms.call("createDatabase", "hvperf");
        List<String> tables = List.of("li_parquet", "li_orc_zlib", "li_orc_zstd", "li_text",
                "li_avro");
        List<String> sizes = new ArrayList<>();
        for (String table : tables) {
            Path at = Files.createDirectories(lake.resolve(table));
            long start = System.nanoTime();
            for (int f = 0; f < files; f++) {
                long from = f * rows / files;
                long to = (f + 1) * rows / files;
                Path file = at.resolve(String.format("%06d_0", f));
                if (table.equals("li_parquet")) {
                    parquet(lineitem, file, from, to);
                } else if (table.equals("li_text")) {
                    text(lineitem, columns, file, from, to);
                } else if (table.equals("li_avro")) {
                    HiveTestTables.writeAvro(file, AVRO_SCHEMA, "deflate",
                            new LineitemRows(from, to));
                } else {
                    HiveTestTables.writeLineitemOrc(file, from, to,
                            table.substring("li_orc_".length()));
                }
            }
            sizes.add(String.format("%s %.1f MB (written in %.1f s)", table, size(at) / 1e6,
                    (System.nanoTime() - start) / 1e9));
            String format = table.substring("li_".length()).replaceFirst("_.*", "");
            hms.call("createTable", "hvperf", table, "file:" + at, columns, "", format,
                    format.equals("avro") ? Map.of("avro.schema.literal", AVRO_SCHEMA) : null);
        }
        Path plugins = dir.resolve("plugins");
        HiveTestTables.installPlugin(plugins);
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.hive.uris", hms.uri());
        sys.put("arrowflight.hive.roots", lake.toString());
        sys.put("arrowflight.hive.orcTables", "true");
        sys.put("arrowflight.hive.textTables", "true");
        sys.put("arrowflight.hive.avroTables", "true");
        ConfigSource source = new ConfigSource(sys::get, new Properties(), Map.of(), null);
        PluginSettings settings = new PluginSettings(plugins, null, "directory,hive", false);
        Files.createDirectories(dir.resolve("data"));
        int threads = Runtime.getRuntime().availableProcessors();
        List<String> report = new ArrayList<>();
        report.add(String.format("%d rows, %d files per table, %d DuckDB threads; %s", rows, files,
                threads, String.join(", ", sizes)));
        report.add("");
        report.add("| query | mode | " + String.join(" s | ", tables) + " s | rows |");
        report.add("|---|---|" + "---|".repeat(tables.size()) + "---|");
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dir.resolve("data").toString()).resultMode("complete").threads(threads)
                .registry(config -> StoreRegistry.open(config, new Configuration(), "localhost",
                        settings, source, List.of()))
                .start()) {
            for (Map.Entry<String, String> query : QUERIES.entrySet()) {
                for (String mode : List.of("complete", "partial")) {
                    StringBuilder line = new StringBuilder("| " + query.getKey() + " | " + mode);
                    Result reference = null;
                    for (String table : tables) {
                        Result best = null;
                        for (int round = 0; round < ROUNDS; round++) {
                            Result result = run(helper, String.format(query.getValue(), table), mode);
                            if (best == null || result.seconds() < best.seconds()) {
                                best = result;
                            }
                        }
                        if (reference == null) {
                            reference = best;
                        } else if (mode.equals("complete")) {
                            assertEquals(reference.answer(), best.answer(), query.getKey() + " " + table);
                        } else if (query.getKey().equals("SELECT *")) {
                            // Partial aggregates are per endpoint; only the whole read compares.
                            assertEquals(rows, best.rows(), query.getKey() + " " + table);
                        }
                        line.append(String.format(" | %.2f", best.seconds()));
                    }
                    report.add(line + " | " + reference.rows() + " |");
                }
            }
        }
        String table = String.join("\n", report);
        System.out.println(table);
        String target = System.getProperty("hive.orc.perf.report");
        if (target != null && !target.isBlank()) {
            Files.writeString(Path.of(target), table + "\n", StandardCharsets.UTF_8);
        }
    }

    /**
     * One timed query.
     *
     * @param rows rows received over all endpoints
     * @param seconds wall time from planning to the last batch
     * @param answer the rows as sorted text when the answer is small, else empty
     */
    private record Result(long rows, double seconds, List<String> answer) {
    }

    private static Result run(TestFlightServerHelper helper, String sql, String mode)
            throws Exception {
        long start = System.nanoTime();
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        FlightInfo info = helper.sqlClient().execute(sql, new HeaderCallOption(headers));
        long rows = 0;
        List<String> answer = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    rows += root.getRowCount();
                    for (int i = 0; i < root.getRowCount() && answer.size() < 100; i++) {
                        List<String> values = new ArrayList<>();
                        for (FieldVector vector : root.getFieldVectors()) {
                            values.add(String.valueOf(vector.getObject(i)));
                        }
                        answer.add(String.join("|", values));
                    }
                }
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        answer.sort(null);
        return new Result(rows, seconds, rows <= 100 ? answer : List.of());
    }

    /**
     * Writes rows of the lineitem-like table as a Hive text file (\001 between fields, no NULLs).
     *
     * @param lineitem the rows in DuckDB SQL
     * @param columns the columns in metastore notation
     * @param file file to write
     * @param from first row
     * @param to row after the last
     * @throws Exception when it cannot be written
     */
    private static void text(String lineitem, String columns, Path file, long from, long to)
            throws Exception {
        List<String> names = new ArrayList<>();
        for (String column : columns.split(";")) {
            names.add(column.strip().split(" ")[0] + "::VARCHAR");
        }
        // One CSV column without quotes or escapes is a line as it is.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT concat_ws(chr(1), " + String.join(", ", names) + ") FROM ("
                    + lineitem + " FROM range(" + from + ", " + to + ") t(r))) TO '" + file
                    + "' (FORMAT csv, HEADER false, QUOTE '', ESCAPE '')");
        }
    }

    /**
     * The lineitem-like rows ({@code OrcTestFiles.LINEITEM}) as Avro values, made as they are
     * read (the Avro writer only iterates).
     */
    private static final class LineitemRows extends AbstractList<Object[]> {

        private static final String[] FLAGS = {"A", "N", "R"};
        private static final String[] STATUSES = {"O", "F"};
        private static final String[] INSTRUCTIONS = {"DELIVER IN PERSON", "COLLECT COD", "NONE",
            "TAKE BACK RETURN"};
        private static final String[] MODES = {"AIR", "MAIL", "SHIP", "TRUCK", "RAIL", "FOB",
            "REG AIR"};
        private static final String[] WORDS = {"final", "regular", "express", "pending"};
        private static final long EPOCH_1992 = LocalDate.of(1992, 1, 1).toEpochDay();

        private final long from;
        private final int size;

        LineitemRows(long from, long to) {
            this.from = from;
            this.size = Math.toIntExact(to - from);
        }

        @Override
        public Object[] get(int index) {
            long r = from + index;
            return new Object[] {r / 4, (r * 7919) % 200000 + 1, (r * 104729) % 10000 + 1,
                (int) (r % 4 + 1), BigDecimal.valueOf(((r * 31) % 50 + 1) * 100, 2),
                BigDecimal.valueOf((r * 2654435761L) % 10000000 + 90000, 2),
                BigDecimal.valueOf(r % 11, 2), BigDecimal.valueOf(r % 9, 2), FLAGS[(int) (r % 3)],
                STATUSES[(int) (r % 2)], (int) (EPOCH_1992 + (r * 7907) % 2500),
                (int) (EPOCH_1992 + (r * 7927) % 2500), (int) (EPOCH_1992 + (r * 7933) % 2500),
                INSTRUCTIONS[(int) (r % 4)], MODES[(int) (r % 7)],
                "carefully " + WORDS[(int) ((r * 13) % 4)] + " deposits " + (r * 48271) % 100000};
        }

        @Override
        public int size() {
            return size;
        }
    }

    private static void parquet(String lineitem, Path file, long from, long to) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (" + lineitem + " FROM range(" + from + ", " + to + ") t(r)) TO '" + file
                    + "' (FORMAT parquet)");
        }
    }

    private static long size(Path at) throws Exception {
        try (Stream<Path> paths = Files.list(at)) {
            long total = 0;
            for (Path p : paths.toList()) {
                total += Files.size(p);
            }
            return total;
        }
    }
}
