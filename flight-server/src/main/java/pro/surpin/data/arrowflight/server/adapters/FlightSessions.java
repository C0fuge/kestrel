package pro.surpin.data.arrowflight.server.adapters;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.SessionOptionValue;

/**
 * Flight SQL sessions of one node: the {@code arrow_flight_session_id} cookie of the Flight SQL
 * session actions names a set of session options. Unlike Arrow's own session middleware, a
 * cookie naming an unknown or expired session (after a restart, on another cluster node, or
 * of another user) never fails a call; the call just runs without session options. Sessions
 * expire after an idle time and their number is capped, the least recently used going first.
 */
public final class FlightSessions {

    /** Cookie carrying the session id, as in the Flight SQL specification. */
    public static final String COOKIE = "arrow_flight_session_id";
    /** Key under which the middleware is registered. */
    public static final FlightServerMiddleware.Key<Call> KEY =
            FlightServerMiddleware.Key.of("arrow-flight-session");

    private static final SecureRandom RANDOM = new SecureRandom();

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final long idleMillis;
    private final int maxSessions;
    private final LongSupplier clock;

    /**
     * Creates a session store.
     *
     * @param idleSeconds a session unused this long is dropped
     * @param maxSessions most sessions kept at once
     * @throws IllegalArgumentException if a value is not positive
     */
    public FlightSessions(long idleSeconds, int maxSessions) {
        this(idleSeconds, maxSessions, System::currentTimeMillis);
    }

    /**
     * Creates a session store with a given clock.
     *
     * @param idleSeconds a session unused this long is dropped
     * @param maxSessions most sessions kept at once
     * @param clock current time in milliseconds
     * @throws IllegalArgumentException if a value is not positive
     */
    FlightSessions(long idleSeconds, int maxSessions, LongSupplier clock) {
        if (idleSeconds <= 0) {
            throw new IllegalArgumentException("flightSessionIdleSeconds must be positive: " + idleSeconds);
        }
        if (maxSessions <= 0) {
            throw new IllegalArgumentException("flightSessionMax must be positive: " + maxSessions);
        }
        this.idleMillis = idleSeconds * 1000L;
        this.maxSessions = maxSessions;
        this.clock = clock;
    }

    /**
     * Returns the number of live sessions.
     *
     * @return session count
     */
    public int size() {
        return sessions.size();
    }

    /**
     * Returns the middleware factory.
     *
     * @return factory reading the session cookie of every call
     */
    public FlightServerMiddleware.Factory<Call> factory() {
        return (CallInfo info, CallHeaders headers, RequestContext context) -> new Call(cookie(headers));
    }

    /**
     * Reads the session id out of the call's cookie headers.
     *
     * @param headers incoming headers
     * @return session id, or null
     */
    static String cookie(CallHeaders headers) {
        Iterable<String> values = headers.getAll("cookie");
        if (values == null) {
            return null;
        }
        for (String value : values) {
            for (String part : value.split(";")) {
                String cookie = part.trim();
                int eq = cookie.indexOf('=');
                if (eq > 0 && COOKIE.equals(cookie.substring(0, eq).trim())) {
                    String id = cookie.substring(eq + 1).trim();
                    if (!id.isEmpty()) {
                        return id;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Looks a session up and marks it used.
     *
     * @param id session id
     * @param peer identity of the caller
     * @return the session, or null when it is unknown, expired or of another identity
     */
    private Session find(String id, String peer) {
        if (id == null) {
            return null;
        }
        Session session = sessions.get(id);
        if (session == null) {
            return null;
        }
        long now = clock.getAsLong();
        if (now - session.lastUsed > idleMillis) {
            sessions.remove(id, session);
            return null;
        }
        if (!session.peer.equals(peer == null ? "" : peer)) {
            return null;
        }
        session.lastUsed = now;
        return session;
    }

    /**
     * Creates a session, first dropping expired ones and, at the cap, the least recently used.
     *
     * @param peer identity of the caller
     * @return new session
     */
    private Session create(String peer) {
        long now = clock.getAsLong();
        sessions.values().removeIf(s -> now - s.lastUsed > idleMillis);
        while (sessions.size() >= maxSessions) {
            Session oldest = null;
            for (Session s : sessions.values()) {
                if (oldest == null || s.lastUsed < oldest.lastUsed) {
                    oldest = s;
                }
            }
            if (oldest == null) {
                break;
            }
            sessions.remove(oldest.id, oldest);
        }
        byte[] bytes = new byte[18];
        RANDOM.nextBytes(bytes);
        Session session = new Session(Base64.getUrlEncoder().withoutPadding().encodeToString(bytes),
                peer == null ? "" : peer, now);
        sessions.put(session.id, session);
        return session;
    }

    /** Options of one session; only the creating identity may use them. */
    public static final class Session {

        private final String id;
        private final String peer;
        private final Map<String, SessionOptionValue> options = new ConcurrentHashMap<>();
        private volatile long lastUsed;

        /**
         * Creates an empty session.
         *
         * @param id session id
         * @param peer identity of the creator
         * @param now creation time in milliseconds
         */
        private Session(String id, String peer, long now) {
            this.id = id;
            this.peer = peer;
            this.lastUsed = now;
        }

        /**
         * Returns one option.
         *
         * @param name option name
         * @return value, or null when unset
         */
        public SessionOptionValue option(String name) {
            return options.get(name);
        }

        /**
         * Returns a copy of all options.
         *
         * @return options by name
         */
        public Map<String, SessionOptionValue> options() {
            return Collections.unmodifiableMap(new HashMap<>(options));
        }

        /**
         * Sets or, for null, removes an option.
         *
         * @param name option name
         * @param value value, or null to remove it
         */
        public void set(String name, SessionOptionValue value) {
            if (value == null) {
                options.remove(name);
            } else {
                options.put(name, value);
            }
        }
    }

    /** The session state of one call: the cookie it sent and what the call created or closed. */
    public final class Call implements FlightServerMiddleware {

        private final String cookieId;
        private Session created;
        private String closed;

        /**
         * Remembers the call's cookie.
         *
         * @param cookieId session id from the cookie, or null
         */
        private Call(String cookieId) {
            this.cookieId = cookieId;
        }

        /**
         * Returns the caller's session.
         *
         * @param peer identity of the caller
         * @return the live session named by the cookie (or created by this call), or null
         */
        public synchronized Session session(String peer) {
            if (created != null) {
                return created;
            }
            return closed != null ? null : find(cookieId, peer);
        }

        /**
         * Returns the caller's session, creating one (and its cookie) when there is none.
         *
         * @param peer identity of the caller
         * @return session
         */
        public synchronized Session sessionOrCreate(String peer) {
            Session session = session(peer);
            if (session == null) {
                created = create(peer);
                session = created;
            }
            return session;
        }

        /**
         * Closes the caller's session and expires its cookie.
         *
         * @param peer identity of the caller
         * @throws org.apache.arrow.flight.FlightRuntimeException NOT_FOUND when there is no session
         */
        public synchronized void close(String peer) {
            Session session = session(peer);
            if (session == null) {
                throw CallStatus.NOT_FOUND.withDescription("No Flight SQL session for this call")
                        .toRuntimeException();
            }
            sessions.remove(session.id, session);
            closed = session.id;
            created = null;
        }

        @Override
        public synchronized void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
            if (created != null) {
                outgoingHeaders.insert("set-cookie", COOKIE + "=" + created.id);
            }
            if (closed != null) {
                outgoingHeaders.insert("set-cookie", COOKIE + "=" + closed + "; Max-Age=0");
            }
        }

        @Override
        public void onCallCompleted(CallStatus status) {
            // Nothing to release.
        }

        @Override
        public void onCallErrored(Throwable err) {
            // Nothing to release.
        }
    }
}
