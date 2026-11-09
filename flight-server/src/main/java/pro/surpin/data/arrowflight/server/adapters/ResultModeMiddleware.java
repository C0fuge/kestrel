package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * Carries the result mode a client asks for with the {@code x-flight-result-mode} header:
 * {@code complete} (one endpoint with the whole answer) or {@code partial} (one endpoint per node
 * with that node's part, merged by the client). Also carries whether the client tries the
 * locations of an endpoint in order ({@code x-flight-location-failover: 1}), and the schema of
 * plain table names it names with the {@code schema} header.
 */
public final class ResultModeMiddleware implements FlightServerMiddleware {

    /** Request header naming the result mode. */
    public static final String HEADER = "x-flight-result-mode";
    /** Request header by which a client says it tries an endpoint's locations in order. */
    public static final String LOCATION_FAILOVER_HEADER = "x-flight-location-failover";
    /**
     * Request header naming the schema of table names written without one (item 39 (a)); the
     * Arrow Flight JDBC driver sends a URL property such as {@code schema=tpch} as this header.
     */
    public static final String SCHEMA_HEADER = "schema";
    /** Key under which the middleware is registered. */
    public static final FlightServerMiddleware.Key<ResultModeMiddleware> KEY =
            FlightServerMiddleware.Key.of("flight-result-mode");

    private final String requested;
    private final boolean locationFailover;
    private final String schema;

    /**
     * Remembers the requested mode of one call.
     *
     * @param requested header value, or null
     * @param locationFailover whether the client tries an endpoint's locations in order
     */
    ResultModeMiddleware(String requested, boolean locationFailover) {
        this(requested, locationFailover, null);
    }

    /**
     * Remembers the requested mode and schema of one call.
     *
     * @param requested header value, or null
     * @param locationFailover whether the client tries an endpoint's locations in order
     * @param schema value of the schema header, or null
     */
    ResultModeMiddleware(String requested, boolean locationFailover, String schema) {
        this.requested = requested;
        this.locationFailover = locationFailover;
        this.schema = schema == null || schema.isBlank() ? null : schema.strip();
    }

    /**
     * Returns the schema the call names for table names written without one.
     *
     * @return schema name, or null when the call sends no {@code schema} header
     */
    public String schema() {
        return schema;
    }

    /**
     * Tells whether the client tries the locations of an endpoint in order, so that the node it
     * planned with may be listed as a backup without drawing every read to it.
     *
     * @return true when the client sent {@code x-flight-location-failover: 1} or {@code true}
     */
    public boolean locationFailover() {
        return locationFailover;
    }

    /**
     * Reads the location failover header value.
     *
     * @param value header value, or null
     * @return true for 1 or true
     */
    static boolean isEnabled(String value) {
        if (value == null) {
            return false;
        }
        String trimmed = value.trim();
        return "1".equals(trimmed) || "true".equalsIgnoreCase(trimmed);
    }

    /**
     * Returns the mode the client asked for.
     *
     * @return header value, or null when the client did not ask
     */
    public String requested() {
        return requested;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        // Nothing to add.
    }

    @Override
    public void onCallCompleted(CallStatus status) {
        // Nothing to release.
    }

    @Override
    public void onCallErrored(Throwable err) {
        // Nothing to release.
    }

    /** Creates the middleware for every call. */
    public static final class Factory implements FlightServerMiddleware.Factory<ResultModeMiddleware> {

        @Override
        public ResultModeMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders,
                RequestContext context) {
            return new ResultModeMiddleware(incomingHeaders.get(HEADER),
                    isEnabled(incomingHeaders.get(LOCATION_FAILOVER_HEADER)),
                    incomingHeaders.get(SCHEMA_HEADER));
        }
    }
}
