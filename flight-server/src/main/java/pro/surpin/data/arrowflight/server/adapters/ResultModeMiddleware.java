package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * Carries the result mode a client asks for with the {@code x-flight-result-mode} header:
 * {@code complete} (one endpoint with the whole answer) or {@code partial} (one endpoint per node
 * with that node's part, merged by the client).
 */
public final class ResultModeMiddleware implements FlightServerMiddleware {

    /** Request header naming the result mode. */
    public static final String HEADER = "x-flight-result-mode";
    /** Key under which the middleware is registered. */
    public static final FlightServerMiddleware.Key<ResultModeMiddleware> KEY =
            FlightServerMiddleware.Key.of("flight-result-mode");

    private final String requested;

    /**
     * Remembers the requested mode of one call.
     *
     * @param requested header value, or null
     */
    private ResultModeMiddleware(String requested) {
        this.requested = requested;
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
            return new ResultModeMiddleware(incomingHeaders.get(HEADER));
        }
    }
}
