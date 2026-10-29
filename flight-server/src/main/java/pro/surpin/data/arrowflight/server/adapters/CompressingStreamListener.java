package pro.surpin.data.arrowflight.server.adapters;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
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
import pro.surpin.data.arrowflight.codec.Lz4FrameCodec;
import pro.surpin.data.arrowflight.server.metrics.CompressionMetrics;

/**
 * Stream listener that sends batches with Arrow IPC body compression. Flight's listener builds
 * its unloader without a codec, so after it started the stream this one puts in an unloader that
 * compresses: batches below the size setting go plain, and a stream whose first batches do not
 * shrink by a tenth sends the rest plain. Each batch says itself whether it is compressed. The
 * buffers of a large batch are compressed side by side by the stream's thread and the server's
 * compression helpers; the batch is sent when all are done, so no more batches are held than before.
 */
final class CompressingStreamListener implements FlightProducer.ServerStreamListener {

    /** Compressed batches after which a stream checks its ratio. */
    static final int RATIO_CHECK_BATCHES = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(CompressingStreamListener.class);
    private static final Field UNLOADER = unloaderField();

    private final FlightProducer.ServerStreamListener delegate;
    private final StreamCompression.Codec codec;
    private final StreamCompression.Settings settings;
    private final StreamCompression.ParallelWork parallel;

    /**
     * Creates the listener.
     *
     * @param delegate Flight's stream listener
     * @param codec codec to use, not NONE
     * @param settings server settings
     * @param parallel helpers that compress the buffers of a batch side by side
     */
    CompressingStreamListener(FlightProducer.ServerStreamListener delegate,
            StreamCompression.Codec codec, StreamCompression.Settings settings,
            StreamCompression.ParallelWork parallel) {
        this.delegate = delegate;
        this.codec = codec;
        this.settings = settings;
        this.parallel = parallel;
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
                    settings.minBatchBytes(), parallel));
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
        private final CompressionCodec codec;
        private final VectorUnloader compressing;
        private final long minBatchBytes;
        private final StreamCompression.ParallelWork parallel;
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
            this(root, codec, minBatchBytes, StreamCompression.ParallelWork.SERIAL);
        }

        /**
         * Creates the unloader.
         *
         * @param root root of the stream
         * @param codec Arrow codec, safe to use from several threads at once
         * @param minBatchBytes batches with fewer body bytes go plain
         * @param parallel helpers that compress the buffers of a batch side by side
         */
        Unloader(VectorSchemaRoot root, CompressionCodec codec, long minBatchBytes,
                StreamCompression.ParallelWork parallel) {
            super(root, true, true);
            this.root = root;
            this.codec = codec;
            this.compressing = new VectorUnloader(root, true, codec, true);
            this.minBatchBytes = minBatchBytes;
            this.parallel = parallel;
        }

        @Override
        public ArrowRecordBatch getRecordBatch() {
            long bodyBytes = bodyBytes(root);
            if (givenUp || bodyBytes < minBatchBytes || bodyBytes == 0) {
                CompressionMetrics.plain();
                return super.getRecordBatch();
            }
            ArrowRecordBatch batch = parallel.helpers() == 0 ? compressing.getRecordBatch() : compressInParallel();
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
         * Compresses the buffers of the current batch side by side; the result is the batch the
         * serial unloader builds.
         *
         * @return compressed batch
         */
        private ArrowRecordBatch compressInParallel() {
            List<BufferAllocator> allocators = new ArrayList<>();
            for (FieldVector vector : root.getFieldVectors()) {
                addAllocators(vector, allocators);
            }
            // The plain batch holds one reference per buffer; compress() hands each one on.
            ArrowRecordBatch plain = super.getRecordBatch();
            List<ArrowBuf> inputs = plain.getBuffers();
            if (inputs.size() != allocators.size()) {
                plain.close();
                return compressing.getRecordBatch();
            }
            List<ArrowBuf> outputs = ParallelBuffers.compressAll(codec, inputs, allocators, parallel);
            return new ArrowRecordBatch(plain.getLength(), plain.getNodes(), outputs,
                    CompressionUtil.createBodyCompression(codec), plain.getVariadicBufferCounts(), true, false);
        }

        /**
         * Lists the allocator of every buffer of a vector in the order the unloader lists the buffers.
         *
         * @param vector vector
         * @param allocators destination
         */
        private static void addAllocators(FieldVector vector, List<BufferAllocator> allocators) {
            for (int i = 0; i < vector.getFieldBuffers().size(); i++) {
                allocators.add(vector.getAllocator());
            }
            for (FieldVector child : vector.getChildrenFromFields()) {
                addAllocators(child, allocators);
            }
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

    /** Compresses the buffers of one batch side by side. */
    static final class ParallelBuffers {

        /** Buffers smaller than this are not worth a helper of their own. */
        static final long PARALLEL_MIN_BUFFER_BYTES = 128 * 1024L;

        /**
         * Prevents instantiation.
         */
        private ParallelBuffers() {
        }

        /**
         * Compresses buffers on the calling thread and, when at least two are large, on helper
         * threads too, largest first. Each input reference is consumed; on failure every output is
         * released and the first error is thrown once all threads are done with the buffers.
         *
         * @param codec Arrow codec
         * @param inputs buffers, one reference each owned by this call
         * @param allocators allocator of each output
         * @param parallel helpers
         * @return compressed buffers in input order
         */
        static List<ArrowBuf> compressAll(CompressionCodec codec, List<ArrowBuf> inputs,
                List<BufferAllocator> allocators, StreamCompression.ParallelWork parallel) {
            int count = inputs.size();
            Integer[] order = new Integer[count];
            int large = 0;
            for (int i = 0; i < count; i++) {
                order[i] = i;
                if (inputs.get(i).writerIndex() >= PARALLEL_MIN_BUFFER_BYTES) {
                    large++;
                }
            }
            Arrays.sort(order, Comparator.comparingLong((Integer i) -> inputs.get(i).writerIndex()).reversed());
            ArrowBuf[] outputs = new ArrowBuf[count];
            AtomicInteger next = new AtomicInteger();
            AtomicReference<RuntimeException> failure = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(count);
            Runnable work = () -> {
                for (int i = next.getAndIncrement(); i < count; i = next.getAndIncrement()) {
                    int index = order[i];
                    try {
                        if (failure.get() == null) {
                            outputs[index] = codec.compress(allocators.get(index), inputs.get(index));
                        } else {
                            inputs.get(index).close();
                        }
                    } catch (RuntimeException e) {
                        failure.compareAndSet(null, e);
                        inputs.get(index).close();
                    } finally {
                        done.countDown();
                    }
                }
            };
            int helpers = parallel.executor() == null ? 0 : Math.min(parallel.helpers(), large - 1);
            for (int h = 0; h < helpers; h++) {
                try {
                    parallel.executor().execute(work);
                } catch (RejectedExecutionException e) {
                    break;
                }
            }
            work.run();
            awaitAll(done);
            RuntimeException error = failure.get();
            if (error != null) {
                for (ArrowBuf output : outputs) {
                    if (output != null) {
                        output.close();
                    }
                }
                throw error;
            }
            return Arrays.asList(outputs);
        }

        /**
         * Waits until helpers finished the buffers they took, even when interrupted (they still
         * use the buffers), and keeps the interrupt.
         *
         * @param done latch counted down per buffer
         */
        private static void awaitAll(CountDownLatch done) {
            boolean interrupted = false;
            while (true) {
                try {
                    done.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
