package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.CallStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks which failures of an endpoint answer after the client's first batch count as a lost peer
 * (the client gets the retryable {@code UNAVAILABLE}) and which keep the merge's error.
 */
@Tag("unit")
class DistributedCompletePeerLostTest {

    /** A peer stream ended with UNAVAILABLE, or by a transport error, is a lost peer. */
    @Test
    void unavailableAndTransportErrorsAreALostPeer() {
        assertTrue(DistributedComplete.peerLost(
                CallStatus.UNAVAILABLE.withDescription("Connection reset").toRuntimeException()));
        assertTrue(DistributedComplete.peerLost(CallStatus.INTERNAL.withDescription("stream closed")
                .withCause(new SocketException("Connection reset")).toRuntimeException()));
        assertTrue(DistributedComplete.peerLost(new UncheckedIOException(new IOException("broken pipe"))));
    }

    /** Other failures, and no endpoint failure at all (the merge itself failed), stay as they are. */
    @Test
    void otherFailuresAreNotALostPeer() {
        assertFalse(DistributedComplete.peerLost(null));
        assertFalse(DistributedComplete.peerLost(
                CallStatus.INTERNAL.withDescription("Out of memory").toRuntimeException()));
        assertFalse(DistributedComplete.peerLost(
                CallStatus.UNAUTHENTICATED.withDescription("expired").toRuntimeException()));
        assertFalse(DistributedComplete.peerLost(
                CallStatus.INVALID_ARGUMENT.withDescription("bad").toRuntimeException()));
        assertFalse(DistributedComplete.peerLost(new IllegalStateException("schema changed")));
    }
}
