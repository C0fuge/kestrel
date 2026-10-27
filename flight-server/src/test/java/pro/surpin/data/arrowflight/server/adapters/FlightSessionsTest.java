package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the node-local Flight SQL session store: cookie parsing, soft handling of unknown
 * cookies, identity binding, idle expiry, the size cap and closing.
 */
@Tag("unit")
class FlightSessionsTest {

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final FlightSessions sessions = new FlightSessions(60, 3, now::get);

    /**
     * Verifies the session id is found among other cookies and empty or foreign cookies are ignored.
     */
    @Test
    void readsTheSessionCookie() {
        assertNull(FlightSessions.cookie(headers()));
        assertNull(FlightSessions.cookie(headers("other=1")));
        assertNull(FlightSessions.cookie(headers("arrow_flight_session_id=")));
        assertEquals("abc", FlightSessions.cookie(headers("a=1; arrow_flight_session_id=abc; b=2")));
        assertEquals("xyz", FlightSessions.cookie(headers("a=1", "arrow_flight_session_id = xyz")));
    }

    /**
     * Verifies a created session gets a cookie and the next call with that cookie finds it.
     */
    @Test
    void aCreatedSessionIsFoundByItsCookie() {
        FlightSessions.Call first = call(null);
        assertNull(first.session("alice"));
        FlightSessions.Session session = first.sessionOrCreate("alice");
        session.set("arrow-compression", SessionOptionValueFactory.makeSessionOptionValue("zstd"));
        String id = setCookie(first);
        assertNotNull(id);

        FlightSessions.Call second = call(id);
        assertSame(session, second.session("alice"));
        assertSame(session, second.sessionOrCreate("alice"));
        assertNull(setCookieHeader(second), "an existing session sends no new cookie");
        assertEquals(1, sessions.size());
    }

    /**
     * Verifies an unknown cookie or another identity's cookie gives no session instead of an error.
     */
    @Test
    void unknownOrForeignCookiesGiveNoSession() {
        assertNull(call("no-such-session").session("alice"));
        FlightSessions.Call first = call(null);
        first.sessionOrCreate("alice");
        String id = setCookie(first);
        assertNull(call(id).session("bob"));
        FlightSessions.Call bob = call(id);
        assertTrue(bob.sessionOrCreate("bob") != null);
        assertEquals(2, sessions.size());
        FlightSessions.Call anonymous = call(null);
        anonymous.sessionOrCreate(null);
        assertNotNull(call(setCookie(anonymous)).session(null), "no identity is the empty identity");
    }

    /**
     * Verifies a session unused for longer than the idle time is gone.
     */
    @Test
    void idleSessionsExpire() {
        FlightSessions.Call first = call(null);
        first.sessionOrCreate("alice");
        String id = setCookie(first);
        now.addAndGet(59_000);
        assertNotNull(call(id).session("alice"), "use renews the session");
        now.addAndGet(59_000);
        assertNotNull(call(id).session("alice"));
        now.addAndGet(61_000);
        assertNull(call(id).session("alice"));
        assertEquals(0, sessions.size());
    }

    /**
     * Verifies the store keeps at most the cap and drops the least recently used session first.
     */
    @Test
    void theCapDropsTheLeastRecentlyUsedSession() {
        String[] ids = new String[3];
        for (int i = 0; i < 3; i++) {
            FlightSessions.Call c = call(null);
            c.sessionOrCreate("alice");
            ids[i] = setCookie(c);
            now.addAndGet(1000);
        }
        assertNotNull(call(ids[0]).session("alice"));
        now.addAndGet(1000);
        FlightSessions.Call fourth = call(null);
        fourth.sessionOrCreate("alice");
        assertEquals(3, sessions.size());
        assertNotNull(call(ids[0]).session("alice"));
        assertNull(call(ids[1]).session("alice"), "least recently used goes first");
        assertNotNull(call(ids[2]).session("alice"));
    }

    /**
     * Verifies closing removes the session and expires its cookie, and closing without one is NOT_FOUND.
     */
    @Test
    void closeRemovesTheSessionAndExpiresTheCookie() {
        FlightSessions.Call first = call(null);
        first.sessionOrCreate("alice");
        String id = setCookie(first);
        FlightSessions.Call closing = call(id);
        closing.close("alice");
        assertNull(closing.session("alice"));
        assertEquals("arrow_flight_session_id=" + id + "; Max-Age=0", setCookieHeader(closing));
        assertEquals(0, sessions.size());
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class, () -> call(id).close("alice"));
        assertEquals(CallStatus.NOT_FOUND.code(), e.status().code());
    }

    /**
     * Verifies settings that are not positive are refused.
     */
    @Test
    void refusesSettingsThatAreNotPositive() {
        assertThrows(IllegalArgumentException.class, () -> new FlightSessions(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new FlightSessions(1, 0));
    }

    /**
     * Starts a call carrying a session cookie.
     *
     * @param id session id, or null for no cookie
     * @return call middleware
     */
    private FlightSessions.Call call(String id) {
        CallHeaders headers = id == null ? headers() : headers(FlightSessions.COOKIE + "=" + id);
        return sessions.factory().onCallStarted(null, headers, null);
    }

    /**
     * Returns the session id a call's set-cookie header carries.
     *
     * @param call call middleware
     * @return session id, or null without the header
     */
    private static String setCookie(FlightSessions.Call call) {
        String header = setCookieHeader(call);
        return header == null ? null : header.substring(FlightSessions.COOKIE.length() + 1);
    }

    /**
     * Returns the set-cookie header a call sends.
     *
     * @param call call middleware
     * @return header value, or null
     */
    private static String setCookieHeader(FlightSessions.Call call) {
        FlightCallHeaders out = new FlightCallHeaders();
        call.onBeforeSendingHeaders(out);
        return out.get("set-cookie");
    }

    /**
     * Builds request headers with cookie values.
     *
     * @param cookies cookie header values
     * @return headers
     */
    private static CallHeaders headers(String... cookies) {
        FlightCallHeaders headers = new FlightCallHeaders();
        for (String cookie : cookies) {
            headers.insert("cookie", cookie);
        }
        return headers;
    }
}
