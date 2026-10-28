package pro.surpin.data.arrowflight.server.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts the batches the server sent compressed and the bytes before and after, for the
 * Prometheus endpoint.
 */
public final class CompressionMetrics {

    private static final AtomicLong COMPRESSED_BATCHES = new AtomicLong();
    private static final AtomicLong PLAIN_BATCHES = new AtomicLong();
    private static final AtomicLong INPUT_BYTES = new AtomicLong();
    private static final AtomicLong OUTPUT_BYTES = new AtomicLong();
    private static final AtomicLong GIVEN_UP_STREAMS = new AtomicLong();

    /**
     * Prevents instantiation.
     */
    private CompressionMetrics() {
    }

    /**
     * Records a batch sent compressed.
     *
     * @param inputBytes body bytes before compression
     * @param outputBytes body bytes sent
     */
    public static void compressed(long inputBytes, long outputBytes) {
        COMPRESSED_BATCHES.incrementAndGet();
        INPUT_BYTES.addAndGet(inputBytes);
        OUTPUT_BYTES.addAndGet(outputBytes);
    }

    /**
     * Records a batch of a compressing stream sent plain (small, or the stream gave up).
     */
    public static void plain() {
        PLAIN_BATCHES.incrementAndGet();
    }

    /**
     * Records a stream that stopped compressing because its batches did not shrink.
     */
    public static void givenUp() {
        GIVEN_UP_STREAMS.incrementAndGet();
    }

    /**
     * Returns how many batches were sent compressed.
     *
     * @return count since start
     */
    public static long compressedBatches() {
        return COMPRESSED_BATCHES.get();
    }

    /**
     * Returns how many batches of compressing streams were sent plain.
     *
     * @return count since start
     */
    public static long plainBatches() {
        return PLAIN_BATCHES.get();
    }

    /**
     * Returns the body bytes of compressed batches before compression.
     *
     * @return bytes since start
     */
    public static long inputBytes() {
        return INPUT_BYTES.get();
    }

    /**
     * Returns the body bytes of compressed batches as sent.
     *
     * @return bytes since start
     */
    public static long outputBytes() {
        return OUTPUT_BYTES.get();
    }

    /**
     * Returns how many streams stopped compressing.
     *
     * @return count since start
     */
    public static long givenUpStreams() {
        return GIVEN_UP_STREAMS.get();
    }

    /**
     * Appends the compression counters in Prometheus text format.
     *
     * @param metrics destination payload
     */
    static void append(StringBuilder metrics) {
        counter(metrics, "arrowflight_flight_compressed_batches_total",
                "Batches sent with Arrow IPC body compression", COMPRESSED_BATCHES.get());
        counter(metrics, "arrowflight_flight_compression_plain_batches_total",
                "Batches of compressing streams sent plain (small or poor ratio)", PLAIN_BATCHES.get());
        counter(metrics, "arrowflight_flight_compression_input_bytes_total",
                "Body bytes of compressed batches before compression", INPUT_BYTES.get());
        counter(metrics, "arrowflight_flight_compression_output_bytes_total",
                "Body bytes of compressed batches as sent", OUTPUT_BYTES.get());
        counter(metrics, "arrowflight_flight_compression_given_up_streams_total",
                "Streams that stopped compressing because batches did not shrink",
                GIVEN_UP_STREAMS.get());
    }

    /**
     * Appends one counter.
     *
     * @param metrics destination payload
     * @param name metric name
     * @param help help text
     * @param value value
     */
    private static void counter(StringBuilder metrics, String name, String help, long value) {
        metrics.append("# HELP ").append(name).append(' ').append(help).append('\n')
                .append("# TYPE ").append(name).append(" counter\n")
                .append(name).append(' ').append(value).append('\n');
    }
}
