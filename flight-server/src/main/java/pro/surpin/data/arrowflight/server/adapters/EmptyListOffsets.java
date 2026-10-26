package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;

/**
 * Gives every empty list vector of a batch its own offset buffer before the batch is sent.
 *
 * <p>An unallocated vector holds the allocation manager's empty buffer, one object for the whole
 * process. Arrow's ListVector and LargeListVector set that buffer's writer index to 4 (8) when
 * they are written to IPC, while every other empty vector sets it to 0, so concurrent streams
 * serialize buffers of the wrong length: the client either gets a cancelled stream or decodes a
 * shifted body (e.g. ADBC read "transactions supported" from SqlInfo). Every SqlInfo answer has
 * empty list and map children, and metadata answers are built from unallocated vectors.
 */
final class EmptyListOffsets {

    private EmptyListOffsets() {
    }

    /**
     * Allocates own buffers for the empty list vectors of a root, at any depth.
     *
     * @param root batch about to be sent
     */
    static void own(VectorSchemaRoot root) {
        for (FieldVector vector : root.getFieldVectors()) {
            own(vector);
        }
    }

    /**
     * Allocates own buffers for an empty list vector on the shared empty buffer, then its children.
     *
     * @param vector vector to check
     */
    private static void own(FieldVector vector) {
        if (vector.getValueCount() == 0
                && (vector instanceof ListVector || vector instanceof LargeListVector)
                && sharesEmpty(vector.getOffsetBuffer())) {
            vector.allocateNew();
            vector.setValueCount(0);
        }
        for (FieldVector child : vector.getChildrenFromFields()) {
            own(child);
        }
    }

    /**
     * Tells whether a buffer is an empty one, which may be the process-wide empty buffer.
     *
     * @param buffer offset buffer
     * @return true when it has no capacity
     */
    private static boolean sharesEmpty(ArrowBuf buffer) {
        return buffer.capacity() == 0;
    }

    /**
     * Wraps a stream listener so every batch it sends has own offset buffers.
     *
     * @param listener Flight stream listener
     * @return listener that fixes each batch before passing it on
     */
    static FlightProducer.ServerStreamListener guard(FlightProducer.ServerStreamListener listener) {
        return new Guard(listener);
    }

    /** Stream listener that calls {@link #own(VectorSchemaRoot)} before each batch. */
    private static final class Guard implements FlightProducer.ServerStreamListener {

        private final FlightProducer.ServerStreamListener delegate;
        private VectorSchemaRoot root;

        /**
         * Creates the guard.
         *
         * @param delegate listener that sends the batches
         */
        Guard(FlightProducer.ServerStreamListener delegate) {
            this.delegate = delegate;
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
        public void start(VectorSchemaRoot batch, DictionaryProvider dictionaries, IpcOption option) {
            this.root = batch;
            own(batch);
            delegate.start(batch, dictionaries, option);
        }

        @Override
        public void putNext() {
            ownCurrent();
            delegate.putNext();
        }

        @Override
        public void putNext(ArrowBuf metadata) {
            ownCurrent();
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

        /** Fixes the started root, if any, before a batch of it is sent. */
        private void ownCurrent() {
            if (root != null) {
                own(root);
            }
        }
    }
}
