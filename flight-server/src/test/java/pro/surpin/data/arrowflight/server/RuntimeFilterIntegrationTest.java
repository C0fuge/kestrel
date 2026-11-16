package pro.surpin.data.arrowflight.server;

import com.google.protobuf.CodedOutputStream;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import pro.surpin.data.arrowflight.server.services.RuntimeFilters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Join runtime filters sent with a ticket (item 51): the answers of TPC-H-shaped joins are the
 * same with and without the filter block, the block narrows plain scans, and a malformed block
 * is refused.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RuntimeFilterIntegrationTest {

    private static final int ORDERS = 20_000;
    private static final int LINES_PER_ORDER = 3;

    private Path dataDir;
    private TestFlightServerHelper helper;

    @BeforeAll
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("runtime-filters");
        Path orders = Files.createDirectories(dataDir.resolve("tpch/orders"));
        Path lineitem = Files.createDirectories(dataDir.resolve("tpch/lineitem"));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = c.createStatement()) {
            s.execute("COPY (SELECT i::BIGINT AS o_orderkey, (i % 997)::INTEGER AS o_custkey,"
                    + " CASE WHEN i % 7 = 0 THEN 'F' ELSE 'O' END AS o_orderstatus"
                    + " FROM range(1, " + (ORDERS + 1) + ") t(i)) TO '"
                    + orders.resolve("part-0.parquet") + "' (FORMAT PARQUET)");
            for (int part = 0; part < 4; part++) {
                long from = 1 + (long) part * ORDERS / 4;
                long to = 1 + (long) (part + 1) * ORDERS / 4;
                s.execute("COPY (SELECT o::BIGINT AS l_orderkey, n::INTEGER AS l_linenumber,"
                        + " ((o * 31 + n) % 50)::DOUBLE AS l_quantity, ((o + n) % 100)::SMALLINT AS l_suppkey"
                        + " FROM range(" + from + ", " + to + ") a(o), range(1, " + (LINES_PER_ORDER + 1)
                        + ") b(n) ORDER BY o, n) TO '" + lineitem.resolve("part-" + part + ".parquet")
                        + "' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)");
            }
        }
        helper = TestFlightServerHelper.builder().dataDir(dataDir.toString()).resultMode("partial").start();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (helper != null) {
            helper.close();
        }
        if (dataDir != null) {
            try (Stream<Path> paths = Files.walk(dataDir)) {
                for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(p);
                }
            }
        }
    }

    @Test
    void joinAnswerIsTheSameWithAndWithoutTheBlock() throws Exception {
        long[] keys = buildSideKeys("SELECT o_orderkey FROM tpch.orders WHERE o_orderstatus = 'F'"
                + " AND o_custkey < 20");
        assertTrue(keys.length > 10 && keys.length <= 4096, "a small build side: " + keys.length);
        String probe = "SELECT l_orderkey, l_linenumber, l_quantity FROM tpch.lineitem";
        List<String> without = rows(probe, null);
        List<String> with = rows(probe, block("l_orderkey", keys));
        assertEquals(ORDERS * LINES_PER_ORDER, without.size());
        assertEquals(join(without, keys), join(with, keys));
        // A few keys go in as an exact IN list: the scan sends only the matching rows.
        assertEquals(join(without, keys), sorted(with));
    }

    @Test
    void manyKeysNarrowTheScanToTheirRangeWithTheSameJoinAnswer() throws Exception {
        long[] keys = buildSideKeys("SELECT o_orderkey FROM tpch.orders WHERE o_orderstatus = 'F'"
                + " AND o_orderkey > 5000");
        assertTrue(keys.length > 4096 / 2, "keys: " + keys.length);
        long[] many = new long[5000];
        for (int i = 0; i < many.length; i++) {
            many[i] = 12_000 + 2L * i;
        }
        String probe = "SELECT l_orderkey, l_linenumber FROM tpch.lineitem WHERE l_quantity >= 10";
        List<String> without = rows(probe, null);
        List<String> with = rows(probe, block("l_orderkey", many));
        assertEquals(join(without, many), join(with, many));
        assertTrue(with.size() < without.size(), with.size() + " of " + without.size());
        for (String row : with) {
            long key = Long.parseLong(row.substring(0, row.indexOf('|')));
            assertTrue(key >= 12_000 && key <= 12_000 + 2L * (many.length - 1), row);
        }
    }

    @Test
    void appliesToAnAliasedColumnAndSeveralFilters() throws Exception {
        String probe = "SELECT l_orderkey AS k, l_suppkey FROM tpch.lineitem";
        List<String> with = rows(probe, RuntimeFilters.encode(List.of(
                new RuntimeFilters.InFilter("K", new long[] {10, 11, 12, 900}),
                new RuntimeFilters.InFilter("l_suppkey", new long[] {11, 13, 70_000}))));
        List<String> expected = new ArrayList<>();
        for (String row : rows(probe, null)) {
            String[] parts = row.split("\\|");
            long k = Long.parseLong(parts[0]);
            long supp = Long.parseLong(parts[1]);
            if (Set.of(10L, 11L, 12L, 900L).contains(k) && (supp == 11 || supp == 13)) {
                expected.add(row);
            }
        }
        assertFalse(expected.isEmpty());
        assertEquals(sorted(expected), sorted(with));
    }

    @Test
    void otherQueriesAndColumnsIgnoreTheBlock() throws Exception {
        byte[] block = block("l_orderkey", new long[] {1, 2, 3});
        String aggregate = "SELECT l_suppkey, count(*) AS n FROM tpch.lineitem GROUP BY l_suppkey";
        assertEquals(sorted(rows(aggregate, null)), sorted(rows(aggregate, block)));
        String limited = "SELECT l_orderkey FROM tpch.lineitem ORDER BY l_orderkey DESC LIMIT 5";
        assertEquals(sorted(rows(limited, null)), sorted(rows(limited, block)));
        String text = "SELECT o_orderstatus, o_orderkey FROM tpch.orders";
        List<String> statusFiltered = rows(text, block("o_orderstatus", new long[] {1}));
        assertEquals(ORDERS, statusFiltered.size());
    }

    @Test
    void aFilterWithoutKeysKeepsNoRow() throws Exception {
        assertEquals(List.of(), rows("SELECT l_orderkey FROM tpch.lineitem", block("l_orderkey", new long[0])));
    }

    @Test
    void refusesAMalformedBlock() throws Exception {
        byte[] truncated = block("l_orderkey", new long[] {1, 2});
        byte[] bad = java.util.Arrays.copyOf(truncated, truncated.length - 3);
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                () -> rows("SELECT l_orderkey FROM tpch.lineitem", bad));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code());
        assertTrue(e.getMessage().contains("runtime filter"), e.getMessage());
    }

    private static byte[] block(String column, long[] keys) {
        return RuntimeFilters.encode(List.of(new RuntimeFilters.InFilter(column, keys)));
    }

    private long[] buildSideKeys(String sql) throws Exception {
        List<String> rows = rows(sql, null);
        long[] keys = new long[rows.size()];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = Long.parseLong(rows.get(i));
        }
        return keys;
    }

    private static List<String> join(List<String> probeRows, long[] keys) {
        Set<Long> build = new HashSet<>();
        for (long key : keys) {
            build.add(key);
        }
        List<String> matched = new ArrayList<>();
        for (String row : probeRows) {
            int bar = row.indexOf('|');
            if (build.contains(Long.parseLong(bar < 0 ? row : row.substring(0, bar)))) {
                matched.add(row);
            }
        }
        return sorted(matched);
    }

    private static List<String> sorted(List<String> rows) {
        List<String> copy = new ArrayList<>(rows);
        Collections.sort(copy);
        return copy;
    }

    private List<String> rows(String sql, byte[] block) throws Exception {
        FlightInfo info = helper.sqlClient().execute(sql);
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            Ticket ticket = block == null ? endpoint.getTicket() : withBlock(endpoint.getTicket(), block);
            try (FlightStream stream = helper.flightClient().getStream(ticket)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int r = 0; r < root.getRowCount(); r++) {
                        StringBuilder row = new StringBuilder();
                        for (FieldVector vector : root.getFieldVectors()) {
                            if (row.length() > 0) {
                                row.append('|');
                            }
                            row.append(vector.getObject(r));
                        }
                        rows.add(row.toString());
                    }
                }
            }
        }
        return rows;
    }

    /**
     * Appends a runtime filter block to a ticket as a client does: field 15 of the ticket's Any.
     */
    private static Ticket withBlock(Ticket ticket, byte[] block) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ticket.getBytes());
        CodedOutputStream coded = CodedOutputStream.newInstance(out);
        coded.writeByteArray(RuntimeFilters.TICKET_FIELD, block);
        coded.flush();
        return new Ticket(out.toByteArray());
    }
}
