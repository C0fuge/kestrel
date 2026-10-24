package pro.surpin.data.arrowflight.client;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks schema and routing headers reach every call and pooled clients are not shared across settings. */
@Tag("unit")
class ClientHeadersTest {

    private BufferAllocator allocator;
    private FlightServer server;
    private final Map<FlightMethod, CallHeaders> seen = new ConcurrentHashMap<>();

    /**
     * Starts an in-process Flight server that records the headers of each call by method.
     *
     * @throws Exception if the server cannot start
     */
    @BeforeEach
    void start() throws Exception {
        allocator = new RootAllocator();
        FlightServerMiddleware.Factory<FlightServerMiddleware> recorder =
                (CallInfo info, CallHeaders headers, RequestContext context) -> {
                    seen.put(info.method(), headers);
                    return new FlightServerMiddleware() {
                        @Override
                        public void onBeforeSendingHeaders(CallHeaders outgoing) {
                        }

                        @Override
                        public void onCallCompleted(CallStatus status) {
                        }

                        @Override
                        public void onCallErrored(Throwable err) {
                        }
                    };
                };
        server = FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", 0), new NoOpFlightProducer())
                .middleware(FlightServerMiddleware.Key.of("recorder"), recorder)
                .build()
                .start();
    }

    /**
     * Stops the server.
     *
     * @throws Exception if closing fails
     */
    @AfterEach
    void stop() throws Exception {
        server.close();
        allocator.close();
    }

    /** Regression: routing headers went only with the handshake, later calls were sent without them. */
    @Test
    void routingHeadersAreSentWithEveryCall() throws Exception {
        Configuration config = config();
        config.setDefaultSchema("sales");
        config.setRoutingTag("etl");
        config.setRoutingQueue("night");
        try (Client client = Client.getOrCreate(config)) {
            assertEquals("etl", seen.get(FlightMethod.HANDSHAKE).get("ROUTING_TAG"));
            assertThrows(FlightRuntimeException.class, () -> client.getFlightClient()
                    .getInfo(FlightDescriptor.command(new byte[0]), client.callOptions()));
            CallHeaders getInfo = seen.get(FlightMethod.GET_FLIGHT_INFO);
            assertEquals("sales", getInfo.get("SCHEMA"));
            assertEquals("etl", getInfo.get("ROUTING_TAG"));
            assertEquals("night", getInfo.get("ROUTING_QUEUE"));
            assertEquals("partial", getInfo.get("x-flight-result-mode"));
        }
    }

    /** Checks calls carry no routing headers when none are configured. */
    @Test
    void noRoutingHeadersWhenUnset() throws Exception {
        try (Client client = Client.getOrCreate(config())) {
            assertThrows(FlightRuntimeException.class, () -> client.getFlightClient()
                    .getInfo(FlightDescriptor.command(new byte[0]), client.callOptions()));
            assertNull(seen.get(FlightMethod.GET_FLIGHT_INFO).get("ROUTING_TAG"));
        }
    }

    /** Regression: a pooled client built for one routing tag served a configuration with another. */
    @Test
    void poolKeepsClientsWithDifferentSettingsApart() throws Exception {
        Configuration first = config();
        first.setRoutingTag("a");
        Configuration second = config();
        second.setRoutingTag("b");
        Configuration sameAsFirst = config();
        sameAsFirst.setRoutingTag("a");
        try (Client a = Client.getOrCreate(first); Client b = Client.getOrCreate(second)) {
            assertNotSame(a, b);
            assertSame(a, Client.getOrCreate(sameAsFirst));
        }
    }

    private Configuration config() {
        Configuration config = new Configuration("localhost", server.getPort(), "user", null, "token");
        config.setMaxRetries(0);
        return config;
    }
}
