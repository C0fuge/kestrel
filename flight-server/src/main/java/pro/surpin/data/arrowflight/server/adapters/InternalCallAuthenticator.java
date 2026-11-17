package pro.surpin.data.arrowflight.server.adapters;

import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.services.InternalCallIdentity;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Admits node-to-node calls of distributed complete mode as the user their signed identity
 * header names ({@link InternalCallIdentity}); every other call goes to the configured
 * authenticator. Installed only with {@code distributedComplete=true}, together with
 * {@link #memberAddressCheck}, which refuses the header from addresses that are not live members.
 */
public final class InternalCallAuthenticator implements CallHeaderAuthenticator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InternalCallAuthenticator.class);
    /** Bound of the remembered member addresses; a cluster has far fewer. */
    private static final int MAX_ADMITTED = 1024;

    private final CallHeaderAuthenticator delegate;
    private final InternalCallIdentity identity;

    /**
     * Wraps the configured authenticator.
     *
     * @param delegate authenticator of client calls
     * @param identity checker of signed identities
     */
    public InternalCallAuthenticator(CallHeaderAuthenticator delegate, InternalCallIdentity identity) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.identity = Objects.requireNonNull(identity, "identity");
    }

    @Override
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        String signed = incomingHeaders.get(InternalCallIdentity.HEADER);
        if (signed == null) {
            return delegate.authenticate(incomingHeaders);
        }
        try {
            String user = identity.verify(signed, System.currentTimeMillis());
            return () -> user;
        } catch (IllegalArgumentException e) {
            LOGGER.warn("node={} internalCall=refused reason='{}'", LogUtil.node(), e.getMessage());
            throw CallStatus.UNAUTHENTICATED.withDescription(e.getMessage()).toRuntimeException();
        }
    }

    /**
     * Creates the gRPC interceptor that refuses calls carrying a signed identity unless they come
     * from the address of a live cluster member, reading the live servers for every such call.
     *
     * @param liveServerUris returns the URIs of the live servers
     * @return server interceptor
     */
    public static ServerInterceptor memberAddressCheck(Supplier<Collection<String>> liveServerUris) {
        return memberAddressCheck(liveServerUris, 0);
    }

    /**
     * Creates the gRPC interceptor that refuses calls carrying a signed identity unless they come
     * from the address of a live cluster member. An address found among the live members may be
     * admitted again for {@code admittedMillis} without reading the live servers, which costs two
     * cluster map reads per peer call (item 55 (a)); a refused address is checked every time.
     *
     * @param liveServerUris returns the URIs of the live servers
     * @param admittedMillis how long an admitted address is remembered, 0 to check every call
     * @return server interceptor
     * @throws IllegalArgumentException for a negative time
     */
    public static ServerInterceptor memberAddressCheck(Supplier<Collection<String>> liveServerUris,
            long admittedMillis) {
        if (admittedMillis < 0) {
            throw new IllegalArgumentException("admittedMillis must be 0 or more: " + admittedMillis);
        }
        Metadata.Key<String> key = Metadata.Key.of(InternalCallIdentity.HEADER,
                Metadata.ASCII_STRING_MARSHALLER);
        long admittedNanos = TimeUnit.MILLISECONDS.toNanos(admittedMillis);
        Map<InetAddress, Long> admitted = new ConcurrentHashMap<>();
        return new ServerInterceptor() {
            @Override
            public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call,
                    Metadata headers, ServerCallHandler<Q, R> next) {
                if (headers.containsKey(key)) {
                    SocketAddress remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                    if (!admittedRecently(remote) && !checkMember(remote, liveServerUris.get())) {
                        LOGGER.warn("node={} internalCall=refused reason='not a live member' remote={}",
                                LogUtil.node(), remote);
                        call.close(Status.UNAUTHENTICATED.withDescription(
                                "Internal calls are accepted from live cluster members only"),
                                new Metadata());
                        return new ServerCall.Listener<>() { };
                    }
                }
                return next.startCall(call, headers);
            }

            private boolean admittedRecently(SocketAddress remote) {
                if (admittedNanos == 0 || !(remote instanceof InetSocketAddress socket)
                        || socket.getAddress() == null) {
                    return false;
                }
                long now = System.nanoTime();
                Long since = admitted.get(socket.getAddress());
                return since != null && now - since < admittedNanos;
            }

            private boolean checkMember(SocketAddress remote, Collection<String> serverUris) {
                boolean member = InternalCallAuthenticator.fromMember(remote, serverUris);
                if (member && admittedNanos > 0 && remote instanceof InetSocketAddress socket
                        && socket.getAddress() != null) {
                    if (admitted.size() > MAX_ADMITTED) {
                        admitted.clear();
                    }
                    admitted.put(socket.getAddress(), System.nanoTime());
                }
                return member;
            }
        };
    }

    /**
     * Tells whether a remote address is the address of one of the given servers' hosts.
     *
     * @param remote remote socket address of a call
     * @param serverUris URIs of live servers
     * @return true when the address matches an address of a server's host
     */
    static boolean fromMember(SocketAddress remote, Collection<String> serverUris) {
        if (!(remote instanceof InetSocketAddress socket) || socket.getAddress() == null) {
            return false;
        }
        InetAddress address = socket.getAddress();
        for (String uri : serverUris) {
            String host;
            try {
                host = URI.create(uri).getHost();
            } catch (IllegalArgumentException e) {
                continue;
            }
            if (host == null) {
                continue;
            }
            try {
                for (InetAddress candidate : InetAddress.getAllByName(host)) {
                    if (candidate.equals(address)) {
                        return true;
                    }
                }
            } catch (UnknownHostException e) {
                LOGGER.debug("node={} internalCall=unresolved host={}", LogUtil.node(), host);
            }
        }
        return false;
    }
}
