package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.services.InternalCallIdentity;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    /**
     * Item 55 (a): with a remember time, an admitted member address skips the live-server read
     * of its next calls; a refused address is checked on every call; 0 checks every call.
     */
    @Test
    @SuppressWarnings("unchecked")
    void admittedMembersAreRememberedRefusedOnesAreNot() {
        List<String> servers = List.of("grpc+tcp://127.0.0.1:47470");
        AtomicInteger reads = new AtomicInteger();
        Supplier<Collection<String>> live = () -> {
            reads.incrementAndGet();
            return servers;
        };
        ServerInterceptor remembering = InternalCallAuthenticator.memberAddressCheck(live, 60_000);
        ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);
        when(next.startCall(any(), any())).thenReturn(new ServerCall.Listener<>() { });

        ServerCall<Object, Object> member = call(new InetSocketAddress("127.0.0.1", 5555));
        remembering.interceptCall(member, signedHeaders(), next);
        remembering.interceptCall(member, signedHeaders(), next);
        assertEquals(1, reads.get());
        verify(next, times(2)).startCall(eq(member), any());

        ServerCall<Object, Object> stranger = call(new InetSocketAddress("10.255.255.1", 5555));
        remembering.interceptCall(stranger, signedHeaders(), next);
        remembering.interceptCall(stranger, signedHeaders(), next);
        assertEquals(3, reads.get());
        verify(stranger, times(2)).close(any(), any());
        verify(next, never()).startCall(eq(stranger), any());

        ServerInterceptor everyCall = InternalCallAuthenticator.memberAddressCheck(live, 0);
        everyCall.interceptCall(member, signedHeaders(), next);
        everyCall.interceptCall(member, signedHeaders(), next);
        assertEquals(5, reads.get());
        assertThrows(IllegalArgumentException.class,
                () -> InternalCallAuthenticator.memberAddressCheck(live, -1));
    }

    @SuppressWarnings("unchecked")
    private static ServerCall<Object, Object> call(InetSocketAddress remote) {
        ServerCall<Object, Object> call = mock(ServerCall.class);
        when(call.getAttributes()).thenReturn(
                Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, remote).build());
        return call;
    }

    private Metadata signedHeaders() {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of(InternalCallIdentity.HEADER, Metadata.ASCII_STRING_MARSHALLER),
                identity.sign("alice", System.currentTimeMillis()));
        return headers;
    }
}
