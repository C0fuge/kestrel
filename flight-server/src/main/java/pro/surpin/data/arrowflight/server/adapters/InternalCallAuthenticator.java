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
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Admits node-to-node calls of distributed complete mode as the user their signed identity
 * header names ({@link InternalCallIdentity}); every other call goes to the configured
 * authenticator. Installed only with {@code distributedComplete=true}, together with
 * {@link #memberAddressCheck}, which refuses the header from addresses that are not live members.
 */
public final class InternalCallAuthenticator implements CallHeaderAuthenticator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InternalCallAuthenticator.class);

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
     * from the address of a live cluster member.
     *
     * @param liveServerUris returns the URIs of the live servers
     * @return server interceptor
     */
    public static ServerInterceptor memberAddressCheck(Supplier<Collection<String>> liveServerUris) {
        Metadata.Key<String> key = Metadata.Key.of(InternalCallIdentity.HEADER,
                Metadata.ASCII_STRING_MARSHALLER);
        return new ServerInterceptor() {
            @Override
            public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call,
                    Metadata headers, ServerCallHandler<Q, R> next) {
                if (headers.containsKey(key)) {
                    SocketAddress remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                    if (!fromMember(remote, liveServerUris.get())) {
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
