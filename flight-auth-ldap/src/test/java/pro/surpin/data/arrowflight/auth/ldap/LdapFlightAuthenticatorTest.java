package pro.surpin.data.arrowflight.auth.ldap;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the Basic login and session handling of the LDAP authenticator. */
@Tag("unit")
class LdapFlightAuthenticatorTest {
    private final LdapSessionToken sessions = new LdapSessionToken(new byte[32], 60_000, Clock.systemUTC());

    /**
     * Builds an authenticator without a group search.
     *
     * @param directory credential check
     * @return authenticator
     */
    private LdapFlightAuthenticator authenticator(LdapFlightAuthenticator.Directory directory) {
        return new LdapFlightAuthenticator(directory, sessions,
                new LdapGroupCache(directory, false, 60_000, Clock.systemUTC()));
    }

    /**
     * Builds headers with one Authorization value.
     *
     * @param authorization header value
     * @return headers
     */
    private static FlightCallHeaders headers(String authorization) {
        FlightCallHeaders headers = new FlightCallHeaders();
        if (authorization != null) {
            headers.insert("Authorization", authorization);
        }
        return headers;
    }

    /**
     * Encodes a Basic credential.
     *
     * @param credential user, colon, password
     * @return header value
     */
    private static String basic(String credential) {
        return "Basic " + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Asserts the status of a refused call.
     *
     * @param auth authenticator
     * @param authorization header value
     * @param code expected status
     */
    private static void refused(LdapFlightAuthenticator auth, String authorization, FlightStatusCode code) {
        assertEquals(code, assertThrows(FlightRuntimeException.class,
                () -> auth.authenticate(headers(authorization))).status().code());
    }

    /** The identity is what the directory returns; the session token carries it to later calls. */
    @Test
    void issuesSessionForDirectoryIdentity() {
        LdapFlightAuthenticator auth = authenticator((user, password) -> user.equals("ALICE")
                && password.equals("p:w") ? new LdapFlightAuthenticator.Login("alice", Set.of()) : null);
        CallHeaderAuthenticator.AuthResult result = auth.authenticate(headers(basic("ALICE:p:w")));
        assertEquals("alice", result.getPeerIdentity());
        FlightCallHeaders outgoing = new FlightCallHeaders();
        result.appendToOutgoingHeaders(outgoing);
        String bearer = outgoing.get("Authorization");
        assertEquals("alice", auth.authenticate(headers(bearer)).getPeerIdentity());
        refused(auth, basic("ALICE:wrong"), FlightStatusCode.UNAUTHENTICATED);
    }

    /** Malformed and missing credentials never reach the directory. */
    @Test
    void refusesMalformedCredentials() {
        AtomicInteger calls = new AtomicInteger();
        LdapFlightAuthenticator auth = authenticator((user, password) -> {
            calls.incrementAndGet();
            return new LdapFlightAuthenticator.Login(user, Set.of());
        });
        for (String header : new String[] {null, "", "Negotiate abc", "Basic !!!", basic("nocolon"),
                basic(":pw"), "Basic " + "A".repeat(9000), "Bearer junk"}) {
            refused(auth, header, FlightStatusCode.UNAUTHENTICATED);
        }
        assertEquals(0, calls.get());
    }

    /** A directory outage is UNAVAILABLE, not a wrong password. */
    @Test
    void reportsOutagesAsUnavailable() {
        LdapFlightAuthenticator auth = authenticator((user, password) -> {
            throw new LdapDirectory.DirectoryUnavailableException("down", null);
        });
        refused(auth, basic("alice:pw"), FlightStatusCode.UNAVAILABLE);
    }

    /**
     * A login fills the group cache, so session calls on this node do not ask the directory; a session
     * whose groups cannot be read (another node, directory down) fails as UNAVAILABLE.
     */
    @Test
    void sessionCallsNeedTheirGroups() {
        AtomicInteger lookups = new AtomicInteger();
        boolean[] down = {false};
        LdapFlightAuthenticator.Directory directory = new LdapFlightAuthenticator.Directory() {
            @Override
            public LdapFlightAuthenticator.Login login(String user, String password) {
                return new LdapFlightAuthenticator.Login(user, Set.of("analysts"));
            }

            @Override
            public Set<String> groups(String identity) throws LdapDirectory.DirectoryUnavailableException {
                lookups.incrementAndGet();
                if (down[0]) {
                    throw new LdapDirectory.DirectoryUnavailableException("down", null);
                }
                return Set.of("analysts");
            }
        };
        LdapFlightAuthenticator auth = new LdapFlightAuthenticator(directory, sessions,
                new LdapGroupCache(directory, true, 60_000, Clock.systemUTC()));
        FlightCallHeaders outgoing = new FlightCallHeaders();
        auth.authenticate(headers(basic("alice:pw"))).appendToOutgoingHeaders(outgoing);
        String bearer = outgoing.get("Authorization");
        assertEquals("alice", auth.authenticate(headers(bearer)).getPeerIdentity());
        assertEquals(0, lookups.get());

        LdapFlightAuthenticator otherNode = new LdapFlightAuthenticator(directory, sessions,
                new LdapGroupCache(directory, true, 60_000, Clock.systemUTC()));
        assertEquals("alice", otherNode.authenticate(headers(bearer)).getPeerIdentity());
        assertEquals(1, lookups.get());
        down[0] = true;
        LdapFlightAuthenticator thirdNode = new LdapFlightAuthenticator(directory, sessions,
                new LdapGroupCache(directory, true, 60_000, Clock.systemUTC()));
        refused(thirdNode, bearer, FlightStatusCode.UNAVAILABLE);
        assertEquals("alice", otherNode.authenticate(headers(bearer)).getPeerIdentity());
    }
}
