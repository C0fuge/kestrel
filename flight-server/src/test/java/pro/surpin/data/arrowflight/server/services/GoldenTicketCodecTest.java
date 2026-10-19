package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.Golden;
import pro.surpin.data.arrowflight.server.model.HandleState;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Golden test (c) of the plugin plan: the bytes of signed endpoint tickets for fixed handle
 * states. Nodes of different builds must keep reading each other's tickets, so the encoding of
 * these states must never change.
 */
@Tag("unit")
class GoldenTicketCodecTest {

    /** Verifies the encoded bytes of fixed states against the golden file, and that they decode. */
    @Test
    void encodedTicketsOfFixedStates() {
        byte[] secret = new byte[32];
        for (int i = 0; i < secret.length; i++) {
            secret[i] = (byte) (i * 7 + 1);
        }
        EndpointTicketCodec codec = new EndpointTicketCodec(secret);
        Map<String, HandleState> states = new LinkedHashMap<>();
        states.put("partial-files", HandleState.forServerFiles(
                "SELECT l_orderkey, sum(l_quantity) FROM tpch.lineitem GROUP BY 1",
                new String[] {"tpch/lineitem/part-0.parquet", "tpch/lineitem/part-1.parquet"},
                "grpc://dn:32010", 123_456_789L, true));
        states.put("partial-untracked", HandleState.forServerFiles("SELECT * FROM s.t",
                new String[] {"s/t/a.parquet"}, "grpc+tls://flight-s3:32010", 42L, false));
        states.put("partial-no-files", new HandleState("SELECT 1", new String[0],
                "grpc://h:1", 0L, false));
        states.put("query-only", HandleState.forQuery("SELECT * FROM t"));
        states.put("complete", HandleState.forCompleteAnswer(
                "SELECT n_name FROM tpch.nation WHERE n_name = 'Ünïcødé'"));
        states.put("complete-parameters", HandleState.forCompleteAnswer(
                "SELECT * FROM tpch.nation WHERE n_nationkey = ?",
                "arrow-ipc-bytes".getBytes(StandardCharsets.UTF_8)));
        states.put("partial-parameters", new HandleState("SELECT * FROM s.t WHERE id = ?",
                new String[] {"s/t/a b.parquet"}, "grpc://h:2", 7L, true, false,
                new byte[] {0, 1, 2, (byte) 0xff}));
        StringBuilder out = new StringBuilder();
        HexFormat hex = HexFormat.of();
        for (Map.Entry<String, HandleState> state : states.entrySet()) {
            byte[] encoded = codec.encode(state.getValue());
            assertEquals(state.getValue(), codec.decode(encoded), state.getKey());
            out.append(state.getKey()).append(' ').append(hex.formatHex(encoded)).append('\n');
        }
        Golden.assertMatches("endpoint-tickets.txt", out.toString());
    }
}
