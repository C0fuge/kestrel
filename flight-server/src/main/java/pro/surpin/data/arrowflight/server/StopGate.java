package pro.surpin.data.arrowflight.server;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Lets a stopping server finish its running calls while it refuses new queries (item 37 (b)):
 * once {@link #close() closed}, a call that starts a query (GetFlightInfo, PollFlightInfo,
 * GetSchema, DoPut, DoExchange) is answered {@code UNAVAILABLE 'The server is stopping; retry the
 * query'}, while DoGet calls of queries already planned here and other calls still run. The gate
 * counts the calls that run, so the drain knows when the last one has ended. gRPC ends every call
 * with exactly one of {@code onComplete} or {@code onCancel}, so the count cannot leak.
 */
public final class StopGate implements ServerInterceptor {

    /** What a client is told when it starts a query on a stopping server. */
    public static final String STOPPING_MESSAGE = "The server is stopping; retry the query";

    /** Flight methods that start a query, refused while the server stops. */
    private static final Set<String> QUERY_METHODS = Set.of("GetFlightInfo", "PollFlightInfo",
            "GetSchema", "DoPut", "DoExchange");

    private final AtomicInteger running = new AtomicInteger();
    /** Method of every running call, by the call's end flag. */
    private final java.util.Map<AtomicBoolean, String> methods = new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicLong refused = new AtomicLong();
    private volatile boolean closed;

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
            Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (closed && QUERY_METHODS.contains(call.getMethodDescriptor().getBareMethodName())) {
            refused.incrementAndGet();
            call.close(Status.UNAVAILABLE.withDescription(STOPPING_MESSAGE), new Metadata());
            return new ServerCall.Listener<>() {
            };
        }
        running.incrementAndGet();
        AtomicBoolean ended = new AtomicBoolean();
        methods.put(ended, String.valueOf(call.getMethodDescriptor().getBareMethodName()));
        Runnable end = () -> {
            if (ended.compareAndSet(false, true)) {
                methods.remove(ended);
                running.decrementAndGet();
            }
        };
        ServerCall.Listener<ReqT> delegate;
        try {
            delegate = next.startCall(call, headers);
        } catch (RuntimeException e) {
            end.run();
            throw e;
        }
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onComplete() {
                try {
                    super.onComplete();
                } finally {
                    end.run();
                }
            }

            @Override
            public void onCancel() {
                try {
                    super.onCancel();
                } finally {
                    end.run();
                }
            }
        };
    }

    /** Refuses new queries from now on; running calls go on. */
    public void close() {
        closed = true;
    }

    /**
     * Tells whether the gate refuses new queries.
     *
     * @return true once closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Returns the number of calls that run now.
     *
     * @return running calls
     */
    public int running() {
        return running.get();
    }

    /**
     * Names the methods of the calls that run now, for the drain's log line.
     *
     * @return method names with their counts, e.g. {@code DoGet=2}, in name order
     */
    public String runningMethods() {
        java.util.Map<String, Integer> counts = new java.util.TreeMap<>();
        for (String method : methods.values()) {
            counts.merge(method, 1, Integer::sum);
        }
        return counts.toString();
    }

    /**
     * Returns the number of queries refused since the gate closed.
     *
     * @return refused calls
     */
    public long refused() {
        return refused.get();
    }

    /**
     * Waits until no call runs any more.
     *
     * @param timeout longest wait
     * @param unit    unit of the timeout
     * @return true when no call runs, false when the time ran out first
     * @throws InterruptedException when the waiting thread is interrupted
     */
    public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (running.get() > 0) {
            long left = deadline - System.nanoTime();
            if (left <= 0) {
                return false;
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(left, TimeUnit.MILLISECONDS.toNanos(50)));
        }
        return true;
    }
}
