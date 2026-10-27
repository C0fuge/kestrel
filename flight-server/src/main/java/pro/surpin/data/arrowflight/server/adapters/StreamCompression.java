package pro.surpin.data.arrowflight.server.adapters;

import java.util.Locale;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * Carries the Arrow IPC body compression of one call's streams: the codec a client asks for with
 * the {@code arrow-compression} header ({@code zstd}, {@code lz4} or {@code none}), else the
 * server's {@code flightCompression} setting (default none). Clients that neither ask nor run
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
     */
    public record Settings(Codec defaultCodec, int zstdLevel, long minBatchBytes) {

        /** Settings that never compress unless a client asks. */
        public static final Settings DEFAULT = new Settings(Codec.NONE, 1, 64 * 1024L);

        /**
         * Checks the settings.
         *
         * @throws IllegalArgumentException if the level is outside 1..22 or the size is negative
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
        }
    }

    private final Settings settings;
    private final Codec codec;
    private final String refusal;

    /**
     * Remembers the codec of one call.
     *
     * @param settings server settings
     * @param codec resolved codec
     * @param refusal why the header names no usable codec, or null
     */
    private StreamCompression(Settings settings, Codec codec, String refusal) {
        this.settings = settings;
        this.codec = codec;
        this.refusal = refusal;
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
        if (refusal != null) {
            throw CallStatus.INVALID_ARGUMENT.withDescription(refusal).toRuntimeException();
        }
        if (codec == Codec.NONE) {
            return listener;
        }
        return new CompressingStreamListener(listener, codec, settings);
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

        /**
         * Creates the factory.
         *
         * @param settings server settings
         */
        public Factory(Settings settings) {
            this.settings = settings;
        }

        @Override
        public StreamCompression onCallStarted(CallInfo info, CallHeaders incomingHeaders,
                RequestContext context) {
            String requested = incomingHeaders.get(HEADER);
            if (requested == null) {
                return new StreamCompression(settings, settings.defaultCodec(), null);
            }
            try {
                return new StreamCompression(settings, parse(requested), null);
            } catch (IllegalArgumentException e) {
                return new StreamCompression(settings, Codec.NONE, e.getMessage());
            }
        }
    }
}
