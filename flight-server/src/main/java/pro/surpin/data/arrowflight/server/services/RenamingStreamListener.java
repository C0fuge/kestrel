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
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Objects;

/**
 * Sends a stream under another schema of the same column types: the columns keep their data
 * and take the names of the given schema (a complete answer computed on the partial path, whose
 * columns DuckDB names otherwise, e.g. {@code count(*)} vs {@code count_star()}; item 33 (A)).
 * Batches are moved into an owned root, so their buffers are shared, not copied.
 */
final class RenamingStreamListener implements FlightProducer.ServerStreamListener, AutoCloseable {

    private final FlightProducer.ServerStreamListener delegate;
    private final BufferAllocator allocator;
    private final Schema schema;
    private VectorSchemaRoot owned;
    private VectorSchemaRoot source;

    /**
     * Creates the listener.
     *
     * @param delegate listener of the client
     * @param allocator allocator of the owned root
     * @param schema schema the client gets, with the same column types as the stream's
     */
    RenamingStreamListener(FlightProducer.ServerStreamListener delegate, BufferAllocator allocator,
            Schema schema) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.schema = Objects.requireNonNull(schema, "schema");
    }

    /**
     * Tells whether two schemas have the same columns apart from their names.
     *
     * @param a first schema
     * @param b second schema
     * @return true when the numbers of columns, their types and children agree
     */
    static boolean sameTypes(Schema a, Schema b) {
        List<Field> x = a.getFields();
        List<Field> y = b.getFields();
        if (x.size() != y.size()) {
            return false;
        }
        for (int i = 0; i < x.size(); i++) {
            if (!x.get(i).getType().equals(y.get(i).getType())
                    || !DistributedComplete.DeferredStart.sameColumns(x.get(i).getChildren(),
                            y.get(i).getChildren())) {
                return false;
            }
        }
        return true;
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
        delegate.setUseZeroCopy(enabled);
    }

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        for (Field field : root.getSchema().getFields()) {
            if (field.getDictionary() != null) {
                throw new IllegalStateException("The column '" + field.getName()
                        + "' is dictionary-encoded: it cannot be renamed");
            }
        }
        if (!sameTypes(schema, root.getSchema())) {
            throw new IllegalStateException("The answer has the columns " + root.getSchema()
                    + ", expected the types of " + schema);
        }
        if (owned == null) {
            owned = VectorSchemaRoot.create(schema, allocator);
            delegate.start(owned, dictionaries, option);
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

    /**
     * Moves the current batch of the stream into the owned root.
     */
    private void moveBatch() {
        if (source == null) {
            throw new IllegalStateException("A batch was sent before its reader started");
        }
        try (ArrowRecordBatch batch = new VectorUnloader(source).getRecordBatch()) {
            new VectorLoader(owned).load(batch);
        }
    }

    @Override
    public void close() {
        if (owned != null) {
            owned.close();
        }
    }
}
