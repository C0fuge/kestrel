package pro.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.model.AppConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for the TPC-H q04 wrong answer: DuckDB 1.4.1-1.4.3 skipped repartitioning in
 * the hash aggregate's Combine with two threads or fewer (duckdb PR #20357, fixed in 1.4.4). With
 * the server's settings (2 threads, 1 GB) the GROUP BY that the Trino plugin pushes down for q04
 * (~13.75M groups at SF10) then crashed in {@code TupleDataCollection::Combine}, raised INTERNAL
 * errors or silently returned duplicate groups.
 *
 * <p>No generated data set reproduced it, only the TPC-H SF10 lineitem files, so the test needs
 * them: {@code -Dq04.it.lineitem=<directory of lineitem/*.parquet>} (optionally
 * {@code -Dq04.it.runs=N}, default 20); skipped without it. Run with
 * {@code -Dgroups=perf -DexcludedGroups= -Dtest=HashAggregateRegressionTest}. On DuckDB 1.4.1
 * it fails (or the test JVM dies of SIGSEGV) within a few runs.
 */
@Tag("perf")
class HashAggregateRegressionTest {

    /** SF10 l_orderkey values that pass the filter (the q04 EXISTS subquery). */
    private static final long SF10_GROUPS = 13_753_474L;

    /** Verifies the q04 lineitem GROUP BY returns every group exactly once, run after run. */
    @Test
    void q04LineitemGroupByHasNoDuplicateGroups() throws Exception {
        String dir = System.getProperty("q04.it.lineitem");
        Assumptions.assumeTrue(dir != null && Files.isDirectory(Path.of(dir)),
                "SF10 lineitem directory not configured (-Dq04.it.lineitem)");
        int runs = Integer.getInteger("q04.it.runs", 20);
        List<String> files = new ArrayList<>();
        try (Stream<Path> list = Files.list(Path.of(dir))) {
            list.filter(p -> p.getFileName().toString().endsWith(".parquet")).sorted()
                    .forEach(p -> files.add("'" + p.toAbsolutePath() + "'"));
        }
        assertTrue(!files.isEmpty(), "no parquet files in " + dir);
        // The SQL as the server sends it for the plugin's pushed-down aggregation.
        String sql = "SELECT \"l_orderkey\" FROM read_parquet([" + String.join(", ", files)
                + "]) WHERE \"l_commitdate\" < \"l_receiptdate\" GROUP BY \"l_orderkey\"";
        // The stand's DuckDB settings: 2 threads, 1 GB per connection.
        AppConfig config = new AppConfig(
                1, 65536, 1, 131072, 0, 1, 2,
                null, false, null, null,
                null, null, "1GB", false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "partial", true,
                true);
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            for (int run = 1; run <= runs; run++) {
                long rows = 0;
                long duplicates = 0;
                BitSet seen = new BitSet();
                try (Statement s = adapter.connection().createStatement();
                        ResultSet rs = s.executeQuery(sql)) {
                    while (rs.next()) {
                        int key = Math.toIntExact(rs.getLong(1));
                        rows++;
                        if (seen.get(key)) {
                            duplicates++;
                        } else {
                            seen.set(key);
                        }
                    }
                }
                assertEquals(0, duplicates, "run " + run + ": duplicate groups");
                assertEquals(SF10_GROUPS, rows, "run " + run + ": groups");
            }
        } finally {
            ioPool.shutdownNow();
        }
    }
}
