package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.model.AppConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Verifies DuckDB enum dictionaries arrive over a real Flight stream. */
@Tag("unit")
class DuckDbDictionaryStreamTest {
    /**
     * Checks dictionary values and nulls survive separate Arrow batches.
     *
     * @throws Exception on native execution or Flight transfer failure
     */
    @Test
    void enumValuesSurviveAcrossBatches() throws Exception {
        for (boolean zeroCopy : new boolean[] {false, true}) {
            check("SELECT CASE i WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE NULL END"
                    + "::ENUM('a','b') AS e FROM range(3) r(i) ORDER BY i", false,
                    Arrays.asList("a", "b", null), zeroCopy);
        }
    }

    /**
     * Checks an empty enum result still transfers its dictionary-encoded schema.
     *
     * @throws Exception on native execution or Flight transfer failure
     */
    @Test
    void emptyEnumResultTransfersSchema() throws Exception {
        for (boolean zeroCopy : new boolean[] {false, true}) {
            check("SELECT 'a'::ENUM('a','b') AS e WHERE false", false, List.of(), zeroCopy);
        }
    }

    /**
     * Checks dictionary ids nested in list fields resolve through the Flight provider.
     *
     * @throws Exception on native execution or Flight transfer failure
     */
    @Test
    void nestedEnumsTransferTheirDictionary() throws Exception {
        for (boolean zeroCopy : new boolean[] {false, true}) {
            check("SELECT [CASE i WHEN 0 THEN 'a' ELSE 'b' END::ENUM('a','b'), NULL] AS es"
                    + " FROM range(2) r(i) ORDER BY i", true, List.of("[a, null]", "[b, null]"), zeroCopy);
        }
    }

    /**
     * Runs a tiny adapter query through the transport and decodes against fixed expected values.
     *
     * @param sql query to export
     * @param nested whether the enum is inside a list
     * @param expected independently specified decoded rows
     * @param zeroCopy whether Flight writes use zero copy
     * @throws Exception on setup, execution, transfer or teardown failure
     */
    private static void check(String sql, boolean nested, List<String> expected, boolean zeroCopy) throws Exception {
        var pool = Executors.newSingleThreadExecutor();
        try (var allocator = new RootAllocator(64L << 20);
                var adapter = new DuckDbAdapter(config(), pool, DuckDbSetupPlan.of(List.of(), List.of()),
                        0, 0, 1, new ProducingStreamGate(0), zeroCopy)) {
            var serverFailure = new AtomicReference<Exception>();
            var producer = new NoOpFlightProducer() {
                @Override
                public void getStream(FlightProducer.CallContext context, Ticket ticket,
                        FlightProducer.ServerStreamListener listener) {
                    try {
                        adapter.streamSql(allocator, sql, listener, true);
                        listener.completed();
                    } catch (Exception error) {
                        serverFailure.set(error);
                        listener.error(error);
                    }
                }
            };
            try (var server = FlightServer.builder(allocator,
                    Location.forGrpcInsecure("localhost", 0), producer).build().start();
                    var client = FlightClient.builder(allocator,
                            Location.forGrpcInsecure("localhost", server.getPort())).build();
                    var stream = client.getStream(new Ticket(new byte[] {1}))) {
                List<String> actual = new ArrayList<>();
                int batches = 0;
                while (assertDoesNotThrow(stream::next, () -> "Server failure: " + serverFailure.get())) {
                    var root = stream.getRoot();
                    Dictionary dictionary = dictionary(root.getSchema().getFields().get(0),
                            nested, stream.getDictionaryProvider());
                    for (int i = 0; i < root.getRowCount(); i++) {
                        Object value = root.getVector(0).getObject(i);
                        if (nested) {
                            List<String> decoded = new ArrayList<>();
                            for (Object index : (List<?>) value) {
                                decoded.add(decode(index, dictionary));
                            }
                            actual.add(decoded.toString());
                        } else {
                            actual.add(decode(value, dictionary));
                        }
                    }
                    batches++;
                }
                dictionary(stream.getRoot().getSchema().getFields().get(0), nested,
                        stream.getDictionaryProvider());
                assertEquals(expected, actual);
                assertEquals(expected.size(), batches, "one-row batches must remain distinct");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Looks up the enum field's dictionary after transport deserialization.
     *
     * @param field top-level result field
     * @param nested whether the field contains a list of enums
     * @param provider received dictionary provider
     * @return available dictionary
     */
    private static Dictionary dictionary(Field field, boolean nested,
            org.apache.arrow.vector.dictionary.DictionaryProvider provider) {
        Field encoded = nested ? field.getChildren().get(0) : field;
        assertNotNull(encoded.getDictionary());
        Dictionary dictionary = provider.lookup(encoded.getDictionary().getId());
        assertNotNull(dictionary);
        return dictionary;
    }

    /**
     * Decodes one index while preserving nulls.
     *
     * @param index received enum index or null
     * @param dictionary received values
     * @return decoded string or null
     */
    private static String decode(Object index, Dictionary dictionary) {
        return index == null ? null
                : dictionary.getVector().getObject(((Number) index).intValue()).toString();
    }

    /**
     * Builds a single-thread native configuration with one-row export batches.
     *
     * @return isolated adapter configuration
     */
    private static AppConfig config() {
        return new AppConfig(1, 1, 1, 131072, 0, 1, 1,
                null, false, null, null, null, null, "128MB", false,
                64L << 20, 1048576, 67108864, 5000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 4, System.getProperty("java.io.tmpdir"), null,
                "complete", true, true);
    }
}
