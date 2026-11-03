package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Sends the answers of several DuckDB readers as one client stream (item 30, a ticket read in
 * batches of local copies): the client's stream is started once with a root this listener owns,
 * and every batch of the reader currently started is moved into it before it is sent. Each
 * reader must answer with the same columns; dictionary-encoded columns are refused, because
 * each reader has its own dictionaries.
 */
final class ConcatenatingStreamListener implements FlightProducer.ServerStreamListener,
        AutoCloseable {

    private final FlightProducer.ServerStreamListener delegate;
    private final BufferAllocator allocator;
    private VectorSchemaRoot owned;
    private VectorSchemaRoot source;

    /**
     * Wraps the client's stream.
     *
     * @param delegate the client's stream, not started yet
     * @param allocator allocator of the owned root
     */
    ConcatenatingStreamListener(FlightProducer.ServerStreamListener delegate,
            BufferAllocator allocator) {
        this.delegate = delegate;
        this.allocator = allocator;
    }

    /**
     * Tells whether the client's stream was started.
     *
     * @return true after the first reader started
     */
    boolean started() {
        return owned != null;
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
    public void setUseZeroCopy(boolean enabled) {
        // Batches are moved into the owned root: its buffers are shared, not copied, either way.
        delegate.setUseZeroCopy(enabled);
    }

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        for (Field field : root.getSchema().getFields()) {
            if (field.getDictionary() != null) {
                throw new IllegalStateException("The column '" + field.getName()
                        + "' is dictionary-encoded: its batches cannot be sent as one stream");
            }
        }
        if (owned == null) {
            owned = VectorSchemaRoot.create(root.getSchema(), allocator);
            delegate.start(owned, dictionaries, option);
        } else if (!DistributedComplete.DeferredStart.sameColumns(owned.getSchema().getFields(),
                root.getSchema().getFields())) {
            throw new IllegalStateException("A batch of files answers with the columns "
                    + root.getSchema() + ", the first with " + owned.getSchema());
        }
        source = root;
    }

    @Override
    public void putNext() {
        moveBatch();
        delegate.putNext();
    }

    @Override
    public void putNext(ArrowBuf metadata) {
        moveBatch();
        delegate.putNext(metadata);
    }

    @Override
    public void putMetadata(ArrowBuf metadata) {
        delegate.putMetadata(metadata);
    }

    @Override
    public void error(Throwable cause) {
        delegate.error(cause);
    }

    @Override
    public void completed() {
        delegate.completed();
    }

    /** Moves the current batch of the started reader into the owned root. */
    private void moveBatch() {
        if (source == null) {
            throw new IllegalStateException("A batch was sent before its reader started");
        }
        try (ArrowRecordBatch batch = new VectorUnloader(source).getRecordBatch()) {
            new VectorLoader(owned).load(batch);
        }
    }

    /** Frees the owned root; call it after the stream has been completed. */
    @Override
    public void close() {
        if (owned != null) {
            owned.close();
        }
    }
}
