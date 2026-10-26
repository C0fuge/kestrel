package pro.surpin.data.arrowflight.server.adapters;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shares one budget of queued outbound bytes among all open Flight streams of the server: each
 * stream may queue at most budget / open streams before it waits for its client, never less than
 * {@link #MIN_STREAM_BYTES} and never more than the per-stream threshold Flight asks for. Many
 * concurrent streams to slow clients thus wait instead of exhausting direct memory.
 */
public final class OutboundBudget implements ServerInterceptor {

    /** Smallest share of a stream: one batch in flight still keeps a stream moving. */
    public static final int MIN_STREAM_BYTES = 1 << 20;
    /** Share of the JVM's direct memory the default budget takes. */
    private static final int DEFAULT_DIRECT_MEMORY_DIVISOR = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(OutboundBudget.class);

    private final long budgetBytes;
    private final Set<BudgetedCall<?, ?>> streams = ConcurrentHashMap.newKeySet();

    /**
     * Creates a budget.
     *
     * @param budgetBytes queued outbound bytes all streams may hold together
     * @throws IllegalArgumentException if the budget is not positive
     */
    public OutboundBudget(long budgetBytes) {
        if (budgetBytes <= 0) {
            throw new IllegalArgumentException("Outbound budget must be positive: " + budgetBytes);
        }
        this.budgetBytes = budgetBytes;
    }

    /**
     * Returns the default budget: a quarter of the JVM's direct memory limit.
     *
     * @return budget in bytes, at least {@link #MIN_STREAM_BYTES}
     */
    public static long defaultBudgetBytes() {
        return Math.max(MIN_STREAM_BYTES, maxDirectMemory() / DEFAULT_DIRECT_MEMORY_DIVISOR);
    }

    /**
     * Returns the JVM's direct memory limit ({@code -XX:MaxDirectMemorySize}, else the heap limit).
     *
     * @return limit in bytes
     */
    static long maxDirectMemory() {
        try {
            com.sun.management.HotSpotDiagnosticMXBean hotspot = ManagementFactory
                    .getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class);
            long configured = Long.parseLong(hotspot.getVMOption("MaxDirectMemorySize").getValue());
            if (configured > 0) {
                return configured;
            }
        } catch (RuntimeException e) {
            LOGGER.debug("MaxDirectMemorySize not readable, using the heap limit", e);
        }
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * Returns the budget all streams share.
     *
     * @return budget in bytes
     */
    public long budgetBytes() {
        return budgetBytes;
    }

    /**
     * Returns the number of streams that hold a share now.
     *
     * @return open streams
     */
    public int openStreams() {
        return streams.size();
    }

    /**
     * Returns the bytes one stream may queue with the current number of open streams.
     *
     * @return share in bytes
     */
    public int streamShare() {
        long share = budgetBytes / Math.max(1, streams.size());
        return (int) Math.max(MIN_STREAM_BYTES, Math.min(Integer.MAX_VALUE, share));
    }

    @Override
    public <R, S> ServerCall.Listener<R> interceptCall(ServerCall<R, S> call, Metadata headers,
            ServerCallHandler<R, S> next) {
        if (call.getMethodDescriptor().getType().serverSendsOneMessage()) {
            return next.startCall(call, headers);
        }
        BudgetedCall<R, S> budgeted = new BudgetedCall<>(call);
        ServerCall.Listener<R> listener;
        try {
            listener = next.startCall(budgeted, headers);
        } catch (RuntimeException | Error e) {
            leave(budgeted);
            throw e;
        }
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onComplete() {
                try {
                    super.onComplete();
                } finally {
                    leave(budgeted);
                }
            }

            @Override
            public void onCancel() {
                try {
                    super.onCancel();
                } finally {
                    leave(budgeted);
                }
            }
        };
    }

    /**
     * Gives a stream that asked for a backpressure threshold its share and shrinks the others.
     *
     * @param call stream that asked
     */
    private void join(BudgetedCall<?, ?> call) {
        streams.add(call);
        rebalance();
    }

    /**
     * Returns the share of a finished stream to the others.
     *
     * @param call finished stream
     */
    private void leave(BudgetedCall<?, ?> call) {
        if (streams.remove(call)) {
            rebalance();
        }
    }

    /**
     * Applies the current share to every open stream.
     */
    private synchronized void rebalance() {
        int share = streamShare();
        for (BudgetedCall<?, ?> call : streams) {
            call.apply(share);
        }
    }

    /**
     * A streaming call whose backpressure threshold is capped by its share of the budget.
     *
     * @param <R> request type
     * @param <S> response type
     */
    private final class BudgetedCall<R, S>
            extends ForwardingServerCall.SimpleForwardingServerCall<R, S> {
        /** Threshold Flight asked for; 0 until it asks. */
        private int requested;
        /** Threshold the stream has now; 0 until set. */
        private int applied;

        /**
         * Wraps a call.
         *
         * @param delegate gRPC call
         */
        private BudgetedCall(ServerCall<R, S> delegate) {
            super(delegate);
        }

        @Override
        public void setOnReadyThreshold(int numBytes) {
            synchronized (this) {
                requested = numBytes;
            }
            join(this);
        }

        /**
         * Sets the threshold to the smaller of the requested one and the share.
         *
         * @param share bytes this stream may queue
         */
        private synchronized void apply(int share) {
            int threshold = Math.min(requested, share);
            if (threshold > 0 && threshold != applied) {
                applied = threshold;
                // gRPC reads the threshold on every readiness check, also after the first message.
                delegate().setOnReadyThreshold(threshold);
            }
        }
    }
}
