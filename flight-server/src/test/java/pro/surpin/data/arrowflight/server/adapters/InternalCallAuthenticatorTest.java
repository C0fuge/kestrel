package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.services.InternalCallIdentity;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the authenticator that admits signed node-to-node calls. */
@Tag("unit")
class InternalCallAuthenticatorTest {

    private final byte[] secret = new byte[32];
    private final InternalCallIdentity identity = new InternalCallIdentity(() -> secret);
    private final InternalCallAuthenticator authenticator =
            new InternalCallAuthenticator(new TokenAuthenticator("s3cret"), identity);

    /** Verifies a signed call is admitted as the signed user without client credentials. */
    @Test
    void signedCallIsAdmittedAsSignedUser() {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(InternalCallIdentity.HEADER, identity.sign("alice", System.currentTimeMillis()));
        assertEquals("alice", authenticator.authenticate(headers).getPeerIdentity());
    }

    /** Verifies a badly signed call is refused even with valid client credentials. */
    @Test
    void badlySignedCallIsRefused() {
        Arrays.fill(secret, (byte) 0);
        String signed = new InternalCallIdentity(() -> new byte[] {1, 2, 3})
                .sign("alice", System.currentTimeMillis());
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(InternalCallIdentity.HEADER, signed);
        headers.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + "s3cret");
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                () -> authenticator.authenticate(headers));
        assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
    }

    /** Verifies calls without the header go to the configured authenticator. */
    @Test
    void otherCallsUseConfiguredAuthenticator() {
        FlightCallHeaders good = new FlightCallHeaders();
        good.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + "s3cret");
        assertEquals("token", authenticator.authenticate(good).getPeerIdentity());
        assertThrows(FlightRuntimeException.class,
                () -> authenticator.authenticate(new FlightCallHeaders()));
    }

    /** Verifies only addresses of live servers' hosts count as members. */
    @Test
    void memberAddressesMatchServerHosts() {
        List<String> servers = List.of("grpc+tcp://127.0.0.1:47470", "grpc+tls://localhost:47471");
        assertTrue(InternalCallAuthenticator.fromMember(new InetSocketAddress("127.0.0.1", 5555), servers));
        assertFalse(InternalCallAuthenticator.fromMember(new InetSocketAddress("10.255.255.1", 5555),
                servers));
        assertFalse(InternalCallAuthenticator.fromMember(new InetSocketAddress("127.0.0.1", 5555),
                List.of("grpc+tcp://10.255.255.1:47470", "not a uri")));
        assertFalse(InternalCallAuthenticator.fromMember(null, servers));
    }
}
