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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the Basic login and session handling of the LDAP authenticator. */
@Tag("unit")
class LdapFlightAuthenticatorTest {
    private final LdapSessionToken sessions = new LdapSessionToken(new byte[32], 60_000, Clock.systemUTC());

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
        LdapFlightAuthenticator auth = new LdapFlightAuthenticator(
                (user, password) -> user.equals("ALICE") && password.equals("p:w") ? "alice" : null, sessions);
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
        LdapFlightAuthenticator auth = new LdapFlightAuthenticator((user, password) -> {
            calls.incrementAndGet();
            return user;
        }, sessions);
        for (String header : new String[] {null, "", "Negotiate abc", "Basic !!!", basic("nocolon"),
                basic(":pw"), "Basic " + "A".repeat(9000), "Bearer junk"}) {
            refused(auth, header, FlightStatusCode.UNAUTHENTICATED);
        }
        assertEquals(0, calls.get());
    }

    /** A directory outage is UNAVAILABLE, not a wrong password. */
    @Test
    void reportsOutagesAsUnavailable() {
        LdapFlightAuthenticator auth = new LdapFlightAuthenticator((user, password) -> {
            throw new LdapDirectory.DirectoryUnavailableException("down", null);
        }, sessions);
        refused(auth, basic("alice:pw"), FlightStatusCode.UNAVAILABLE);
    }
}
