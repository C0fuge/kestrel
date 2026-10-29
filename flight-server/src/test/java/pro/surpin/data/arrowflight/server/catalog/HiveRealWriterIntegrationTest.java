package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

/**
 * Insert-only (MM) tables written by a real Hive 4 ({@code tools/ci/hive-mm-writer.sh} in
 * {@code catalogs-ci.yml}: inserts, failed inserts, INSERT OVERWRITE, major and minor compaction,
 * gzip text output) answer through the server with the packaged hive plugin exactly what Hive
 * itself answered, in both result modes. Runs only when the workflow sets
 * {@code HIVE_MM_IT_URIS} (the metastore on the same database), {@code HIVE_MM_IT_ROOT} (the
 * warehouse directory) and {@code HIVE_MM_IT_EXPECTED} (Hive's answers, one {@code <table>.tsv}
 * per table of database {@code mmw}).
 */
class HiveRealWriterIntegrationTest {

    private static final String URIS = System.getenv("HIVE_MM_IT_URIS");
    private static final String ROOT = System.getenv("HIVE_MM_IT_ROOT");
    private static final String EXPECTED = System.getenv("HIVE_MM_IT_EXPECTED");
    private static final List<String> TABLES = List.of("tp", "tt", "tor", "tm");

    @TempDir
    static Path dir;
    static TestFlightServerHelper helper;

    @BeforeAll
    static void start() throws Exception {
        Assumptions.assumeTrue(URIS != null && ROOT != null && EXPECTED != null,
                "HIVE_MM_IT_* not set (catalogs-ci.yml job hive-writer)");
        Assumptions.assumeTrue(HiveTestTables.available(), "flight-plugin-hive is not built");
        Path plugins = dir.resolve("plugins");
        HiveTestTables.installPlugin(plugins);
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.hive.uris", URIS);
        sys.put("arrowflight.hive.roots", ROOT);
        sys.put("arrowflight.hive.orcTables", "true");
        sys.put("arrowflight.hive.textTables", "true");
        sys.put("arrowflight.hive.insertOnlyTables", "true");
        ConfigSource source = new ConfigSource(sys::get, new Properties(), Map.of(), null);
        PluginSettings settings = new PluginSettings(plugins, null, "directory,hive", false);
        Files.createDirectories(dir.resolve("data"));
        helper = TestFlightServerHelper.builder().dataDir(dir.resolve("data").toString())
                .resultMode("complete")
                .registry(config -> StoreRegistry.open(config, new Configuration(), "localhost",
                        settings, source, List.of()))
                .start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    @Test
    void everyTableAnswersWhatHiveAnswered() throws Exception {
        for (String table : TABLES) {
            List<String> expected = new ArrayList<>();
            for (String line : Files.readAllLines(Path.of(EXPECTED, table + ".tsv"),
                    StandardCharsets.UTF_8)) {
                if (!line.isEmpty()) {
                    expected.add(line.replace('\t', '|'));
                }
            }
            expected.sort(null);
            assertFalse(expected.isEmpty(), table);
            for (String mode : List.of("complete", "partial")) {
                assertEquals(expected, rows("SELECT * FROM mmw." + table, mode), mode + ": " + table);
            }
        }
    }

    @Test
    void partitionsAndColumnsAsInHive() throws Exception {
        FlightInfo info = info("SELECT * FROM mmw.tor", "partial");
        assertEquals(List.of("i", "s", "k"), info.getSchemaOptional().orElseThrow().getFields()
                .stream().map(Field::getName).toList());
        for (String mode : List.of("complete", "partial")) {
            // k=q: the INSERT OVERWRITE base replaced both earlier writes.
            assertEquals(List.of("20|o"), rows("SELECT i, s FROM mmw.tor WHERE k = 'q'", mode),
                    mode);
            // tm: a minor compaction's delta_1_3_v and its source deltas are read once.
            assertEquals(List.of("4|10"), rows("SELECT count(*), sum(i) FROM mmw.tm", mode), mode);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static List<String> rows(String sql, String mode) throws Exception {
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info(sql, mode).getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int i = 0; i < root.getRowCount(); i++) {
                        List<String> values = new ArrayList<>();
                        for (FieldVector vector : root.getFieldVectors()) {
                            values.add(String.valueOf(vector.getObject(i)));
                        }
                        rows.add(String.join("|", values));
                    }
                }
            }
        }
        rows.sort(null);
        return rows;
    }

    private static FlightInfo info(String sql, String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        return helper.sqlClient().execute(sql, new HeaderCallOption(headers));
    }
}
