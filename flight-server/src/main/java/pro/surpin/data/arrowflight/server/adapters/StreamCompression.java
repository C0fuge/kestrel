package pro.surpin.data.arrowflight.server.adapters;

import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * Carries the Arrow IPC body compression of one call's streams: the codec a client asks for with
 * the {@code arrow-compression} header ({@code zstd}, {@code lz4} or {@code none}), else the
 * {@code arrow-compression} option of its Flight SQL session, else the server's
 * {@code flightCompression} setting (default none). Clients that neither ask nor run
 * against a server set to compress get plain batches, as before.
 */
public final class StreamCompression implements FlightServerMiddleware {

    /** Request header naming the codec a client can decode. */
    public static final String HEADER = "arrow-compression";
    /** Key under which the middleware is registered. */
    public static final FlightServerMiddleware.Key<StreamCompression> KEY =
            FlightServerMiddleware.Key.of("arrow-compression");

    /** Codecs a stream may use. */
    public enum Codec {
        /** Plain batches. */
        NONE,
        /** Zstandard. */
        ZSTD,
        /** LZ4 frames (Arrow's LZ4_FRAME). */
        LZ4
    }

    /**
     * Server settings of stream compression.
     *
     * @param defaultCodec codec for calls without the header
     * @param zstdLevel Zstandard level (LZ4 has none)
     * @param minBatchBytes batches with fewer body bytes are sent plain
     * @param threads threads that compress the buffers of one batch side by side (0 = one per core
     *     up to 8, 1 = the stream's own thread only)
     */
    public record Settings(Codec defaultCodec, int zstdLevel, long minBatchBytes, int threads) {

        /** Most threads the automatic thread count picks. */
        static final int MAX_AUTO_THREADS = 8;

        /** Settings that never compress unless a client asks. */
        public static final Settings DEFAULT = new Settings(Codec.NONE, 1, 64 * 1024L);

        /**
         * Creates settings with the automatic thread count.
         *
         * @param defaultCodec codec for calls without the header
         * @param zstdLevel Zstandard level
         * @param minBatchBytes batches with fewer body bytes are sent plain
         */
        public Settings(Codec defaultCodec, int zstdLevel, long minBatchBytes) {
            this(defaultCodec, zstdLevel, minBatchBytes, 0);
        }

        /**
         * Checks the settings.
         *
         * @throws IllegalArgumentException if the level is outside 1..22 or the size or thread
         *     count is negative
         */
        public Settings {
            if (defaultCodec == null) {
                throw new IllegalArgumentException("flightCompression must be set");
            }
            if (zstdLevel < 1 || zstdLevel > 22) {
                throw new IllegalArgumentException("flightCompressionLevel must be 1..22: " + zstdLevel);
            }
            if (minBatchBytes < 0) {
                throw new IllegalArgumentException(
                        "flightCompressionMinBytes must not be negative: " + minBatchBytes);
            }
            if (threads < 0) {
                throw new IllegalArgumentException("flightCompressionThreads must not be negative: " + threads);
            }
        }

        /**
         * Resolves the thread count.
         *
         * @param processors cores the JVM sees
         * @return threads per batch, at least 1
         */
        public int threadsFor(int processors) {
            return threads > 0 ? threads : Math.max(1, Math.min(processors, MAX_AUTO_THREADS));
        }
    }

    private final Settings settings;
    private final ParallelWork parallel;
    private final Codec codec;
    private final String refusal;
    private final boolean fromHeader;

    /**
     * Remembers the codec of one call.
     *
     * @param settings server settings
     * @param parallel helpers that compress the buffers of a batch side by side
     * @param codec resolved codec
     * @param refusal why the header names no usable codec, or null
     * @param fromHeader whether the call sent the header
     */
    private StreamCompression(Settings settings, ParallelWork parallel, Codec codec, String refusal,
            boolean fromHeader) {
        this.settings = settings;
        this.parallel = parallel;
        this.codec = codec;
        this.refusal = refusal;
        this.fromHeader = fromHeader;
    }

    /**
     * Reads a codec name.
     *
     * @param value name, case-insensitive: none, zstd or lz4 (also lz4_frame, lz4-frame)
     * @return codec
     * @throws IllegalArgumentException if the name is unknown, or lz4 while lz4-java is not on
     *     the class path
     */
    public static Codec parse(String value) {
        String name = value.trim().toLowerCase(Locale.ROOT);
        return switch (name) {
            case "", "none" -> Codec.NONE;
            case "zstd" -> Codec.ZSTD;
            case "lz4", "lz4_frame", "lz4-frame" -> {
                if (!Lz4FrameCodec.available()) {
                    throw new IllegalArgumentException("Unsupported " + HEADER + ": " + value
                            + " (lz4 needs lz4-java of the runtime libs set on the class path, use zstd)");
                }
                yield Codec.LZ4;
            }
            default -> throw new IllegalArgumentException("Unknown " + HEADER + ": " + value
                    + " (expected none, zstd or lz4)");
        };
    }

    /**
     * Returns the codec of this call's streams.
     *
     * @return codec
     */
    public Codec codec() {
        return codec;
    }

    /**
     * Wraps a stream listener so its batches are compressed as this call asks.
     *
     * @param listener Flight stream listener
     * @return the listener itself when no compression applies, else a compressing one
     * @throws org.apache.arrow.flight.FlightRuntimeException INVALID_ARGUMENT for an unknown codec
     */
    public FlightProducer.ServerStreamListener wrap(FlightProducer.ServerStreamListener listener) {
        return wrap(listener, null);
    }

    /**
     * Wraps a stream listener so its batches are compressed as this call asks; the call header
     * goes before the session's codec, which goes before the server default.
     *
     * @param listener Flight stream listener
     * @param sessionCodec codec of the call's Flight SQL session, or null when it sets none
     * @return the listener itself when no compression applies, else a compressing one
     * @throws org.apache.arrow.flight.FlightRuntimeException INVALID_ARGUMENT for an unknown codec
     */
    public FlightProducer.ServerStreamListener wrap(FlightProducer.ServerStreamListener listener,
            Codec sessionCodec) {
        if (refusal != null) {
            throw CallStatus.INVALID_ARGUMENT.withDescription(refusal).toRuntimeException();
        }
        Codec use = fromHeader || sessionCodec == null ? codec : sessionCodec;
        if (use == Codec.NONE) {
            return listener;
        }
        return new CompressingStreamListener(listener, use, settings, parallel);
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        // Nothing to add.
    }

    @Override
    public void onCallCompleted(CallStatus status) {
        // Nothing to release.
    }

    @Override
    public void onCallErrored(Throwable err) {
        // Nothing to release.
    }

    /** Creates the middleware for every call. */
    public static final class Factory implements FlightServerMiddleware.Factory<StreamCompression> {

        private final Settings settings;
        private final ParallelWork parallel;

        /**
         * Creates the factory; its compression threads start on first use and end when idle.
         *
         * @param settings server settings
         */
        public Factory(Settings settings) {
            this.settings = settings;
            this.parallel = ParallelWork.create(settings.threadsFor(Runtime.getRuntime().availableProcessors()));
        }

        @Override
        public StreamCompression onCallStarted(CallInfo info, CallHeaders incomingHeaders,
                RequestContext context) {
            String requested = incomingHeaders.get(HEADER);
            if (requested == null) {
                return new StreamCompression(settings, parallel, settings.defaultCodec(), null, false);
            }
            try {
                return new StreamCompression(settings, parallel, parse(requested), null, true);
            } catch (IllegalArgumentException e) {
                return new StreamCompression(settings, parallel, Codec.NONE, e.getMessage(), true);
            }
        }
    }

    /**
     * Helper threads of a server's compressing streams.
     *
     * @param executor runs helpers, or null when batches are compressed by the stream's thread only
     * @param helpers most helpers one batch asks for
     */
    record ParallelWork(Executor executor, int helpers) {

        /** No helpers: each stream compresses on its own thread. */
        static final ParallelWork SERIAL = new ParallelWork(null, 0);

        /**
         * Creates the helpers of a thread count: the stream's thread plus threads - 1 pool threads
         * shared by all streams, which end after 30 s idle.
         *
         * @param threads threads per batch
         * @return helpers
         */
        static ParallelWork create(int threads) {
            if (threads <= 1) {
                return SERIAL;
            }
            AtomicInteger counter = new AtomicInteger();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(threads - 1, threads - 1, 30, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(), runnable -> {
                        Thread thread = new Thread(runnable, "arrow-compress-" + counter.incrementAndGet());
                        thread.setDaemon(true);
                        return thread;
                    });
            pool.allowCoreThreadTimeOut(true);
            return new ParallelWork(pool, threads - 1);
        }
    }
}
