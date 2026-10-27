package pro.surpin.data.arrowflight.server.adapters;

import java.lang.reflect.Field;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.server.metrics.CompressionMetrics;

/**
 * Stream listener that sends batches with Arrow IPC body compression. Flight's listener builds
 * its unloader without a codec, so after it started the stream this one puts in an unloader that
 * compresses: batches below the size setting go plain, and a stream whose first batches do not
 * shrink by a tenth sends the rest plain. Each batch says itself whether it is compressed.
 */
final class CompressingStreamListener implements FlightProducer.ServerStreamListener {

    /** Compressed batches after which a stream checks its ratio. */
    static final int RATIO_CHECK_BATCHES = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(CompressingStreamListener.class);
    private static final Field UNLOADER = unloaderField();

    private final FlightProducer.ServerStreamListener delegate;
    private final StreamCompression.Codec codec;
    private final StreamCompression.Settings settings;

    /**
     * Creates the listener.
     *
     * @param delegate Flight's stream listener
     * @param codec codec to use, not NONE
     * @param settings server settings
     */
    CompressingStreamListener(FlightProducer.ServerStreamListener delegate,
            StreamCompression.Codec codec, StreamCompression.Settings settings) {
        this.delegate = delegate;
        this.codec = codec;
        this.settings = settings;
    }

    /**
     * Finds the unloader field of Flight's (package-private) stream listener.
     *
     * @return accessible field, or null when this Flight version has none
     */
    private static Field unloaderField() {
        try {
            Field field = Class.forName("org.apache.arrow.flight.OutboundStreamListenerImpl")
                    .getDeclaredField("unloader");
            field.setAccessible(true);
            return field;
        } catch (ReflectiveOperationException | RuntimeException e) {
            LOGGER.warn("Flight stream compression is not available: {}", e.toString());
            return null;
        }
    }

    /**
     * Creates the Arrow codec of a setting.
     *
     * @param codec codec
     * @param zstdLevel Zstandard level, unused for LZ4
     * @return Arrow codec
     */
    static CompressionCodec arrowCodec(StreamCompression.Codec codec, int zstdLevel) {
        return switch (codec) {
            case ZSTD -> CommonsCompressionFactory.INSTANCE.createCodec(
                    CompressionUtil.CodecType.ZSTD, zstdLevel);
            case LZ4 -> new Lz4FrameCodec();
            case NONE -> throw new IllegalArgumentException("No codec for NONE");
        };
    }

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        delegate.start(root, dictionaries, option);
        if (UNLOADER == null || !UNLOADER.getDeclaringClass().isInstance(delegate)) {
            return;
        }
        try {
            UNLOADER.set(delegate, new Unloader(root, arrowCodec(codec, settings.zstdLevel()),
                    settings.minBatchBytes()));
        } catch (IllegalAccessException e) {
            LOGGER.warn("Flight stream sent plain, compression failed to start: {}", e.toString());
        }
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {
        delegate.setOnCancelHandler(handler);
    }

    @Override
    public boolean isReady() {
        return delegate.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {
        delegate.setOnReadyHandler(handler);
    }

    @Override
    public void putNext() {
        delegate.putNext();
    }

    @Override
    public void putNext(ArrowBuf metadata) {
        delegate.putNext(metadata);
    }

    @Override
    public void putMetadata(ArrowBuf metadata) {
        delegate.putMetadata(metadata);
    }

    @Override
    public void error(Throwable ex) {
        delegate.error(ex);
    }

    @Override
    public void completed() {
        delegate.completed();
    }

    @Override
    public void setUseZeroCopy(boolean enabled) {
        delegate.setUseZeroCopy(enabled);
    }

    /** Unloader that compresses large batches while they shrink and sends the others plain. */
    static final class Unloader extends VectorUnloader {

        private final VectorSchemaRoot root;
        private final VectorUnloader compressing;
        private final long minBatchBytes;
        private boolean givenUp;
        private int compressedBatches;
        private long inputBytes;
        private long outputBytes;

        /**
         * Creates the unloader.
         *
         * @param root root of the stream
         * @param codec Arrow codec
         * @param minBatchBytes batches with fewer body bytes go plain
         */
        Unloader(VectorSchemaRoot root, CompressionCodec codec, long minBatchBytes) {
            super(root, true, true);
            this.root = root;
            this.compressing = new VectorUnloader(root, true, codec, true);
            this.minBatchBytes = minBatchBytes;
        }

        @Override
        public ArrowRecordBatch getRecordBatch() {
            long bodyBytes = bodyBytes(root);
            if (givenUp || bodyBytes < minBatchBytes || bodyBytes == 0) {
                CompressionMetrics.plain();
                return super.getRecordBatch();
            }
            ArrowRecordBatch batch = compressing.getRecordBatch();
            long sent = batch.computeBodyLength();
            CompressionMetrics.compressed(bodyBytes, sent);
            compressedBatches++;
            inputBytes += bodyBytes;
            outputBytes += sent;
            if (compressedBatches == RATIO_CHECK_BATCHES && outputBytes * 10 > inputBytes * 9) {
                givenUp = true;
                CompressionMetrics.givenUp();
            }
            return batch;
        }

        /**
         * Tells whether the stream stopped compressing.
         *
         * @return true after a poor ratio
         */
        boolean givenUp() {
            return givenUp;
        }

        /**
         * Sums the buffer bytes of a batch.
         *
         * @param root batch
         * @return bytes of all vectors, children included
         */
        static long bodyBytes(VectorSchemaRoot root) {
            long total = 0;
            for (FieldVector vector : root.getFieldVectors()) {
                total += vector.getBufferSize();
            }
            return total;
        }
    }
}
