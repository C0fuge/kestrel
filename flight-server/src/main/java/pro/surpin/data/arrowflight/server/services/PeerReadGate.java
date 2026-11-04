package pro.surpin.data.arrowflight.server.services;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractStub;
import org.apache.arrow.flight.CallOptions.GrpcCallOption;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds back the messages of one endpoint answer stream until its reader gets to it (item 31):
 * the distributed complete path opens every endpoint stream at once so that all peers compute in
 * parallel, but reads them one after another. A Flight client stream asks for five messages ahead,
 * so every stream not yet read held up to five batches in direct memory (twelve streams of
 * lineitem batches exhausted 512 MiB). Until {@link #activate()} the gate lets the stream receive
 * only its first {@link #IDLE_MESSAGES} message (the schema, needed for fail-over and the common
 * schema); further requests are held and passed on when the reader activates the stream, after
 * which the stream reads ahead as usual. A peer's answer waits in its own bounded outbound queue.
 * Cancelling the call activates the gate, so that a cancelled stream can be drained and closed.
 * One gate serves one call; use it as the call option of one {@code getStream}.
 */
final class PeerReadGate implements ClientInterceptor, GrpcCallOption {

    /** Messages an idle stream may receive: its schema. */
    static final int IDLE_MESSAGES = 1;

    private final AtomicLong delivered = new AtomicLong();
    private ClientCall<?, ?> call;
    private long forwarded;
    private int held;
    private boolean active;

    @Override
    public <T extends AbstractStub<T>> T wrapStub(T stub) {
        return stub.withInterceptors(this);
    }

    @Override
    public <R, S> ClientCall<R, S> interceptCall(MethodDescriptor<R, S> method, CallOptions callOptions,
            Channel next) {
        ClientCall<R, S> gated = new ForwardingClientCall.SimpleForwardingClientCall<>(
                next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<S> listener, Metadata headers) {
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                    @Override
                    public void onMessage(S message) {
                        delivered.incrementAndGet();
                        super.onMessage(message);
                    }
                }, headers);
            }

            @Override
            public void cancel(String message, Throwable cause) {
                // gRPC delivers the messages it holds before a call's final status: a held-back
                // stream would never end, and FlightStream.close() drains until it does.
                activate();
                super.cancel(message, cause);
            }

            @Override
            public void request(int numMessages) {
                int pass = admit(numMessages);
                if (pass > 0) {
                    super.request(pass);
                }
            }
        };
        synchronized (this) {
            call = gated;
        }
        return gated;
    }

    /**
     * Records a request of the stream's reader and returns how many messages may be asked for now.
     *
     * @param numMessages messages the reader asked for
     * @return messages to pass on to the call
     */
    synchronized int admit(int numMessages) {
        if (numMessages <= 0) {
            return numMessages;
        }
        if (active) {
            return numMessages;
        }
        int pass = (int) Math.min(numMessages, Math.max(0, IDLE_MESSAGES - forwarded));
        forwarded += pass;
        held += numMessages - pass;
        return pass;
    }

    /**
     * Lets the stream read ahead as its reader asks: passes on the requests held so far. Called by
     * the reader when it gets to this stream; later calls do nothing.
     */
    void activate() {
        int pass;
        ClientCall<?, ?> target;
        synchronized (this) {
            if (active) {
                return;
            }
            active = true;
            pass = held;
            held = 0;
            target = call;
        }
        if (pass > 0 && target != null) {
            target.request(pass);
        }
    }

    /**
     * Returns the requests held back while the stream was idle.
     *
     * @return held messages
     */
    synchronized int held() {
        return held;
    }

    /**
     * Returns how many messages the stream has received so far.
     *
     * @return delivered messages
     */
    long delivered() {
        return delivered.get();
    }
}
