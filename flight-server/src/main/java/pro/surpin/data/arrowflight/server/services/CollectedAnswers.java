package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps the answers of several DuckDB readers in memory so that they can be merged into one
 * answer afterwards (item 30, an aggregate whose local copies are read in batches): every batch
 * a reader sends is kept, and {@link #reader} reads all kept batches back as one Arrow stream.
 * Each reader must answer with the same columns; dictionary-encoded columns are refused.
 */
final class CollectedAnswers implements FlightProducer.ServerStreamListener, AutoCloseable {

    private final BufferAllocator allocator;
    private final FlightProducer.ServerStreamListener client;
    private final List<ArrowRecordBatch> batches = new ArrayList<>();
    private Schema schema;
    private VectorSchemaRoot source;
    private long bytes;

    /**
     * Creates an empty collection.
     *
     * @param allocator allocator of the reader over the kept batches
     * @param client the client's stream, asked only whether it was cancelled
     */
    CollectedAnswers(BufferAllocator allocator, FlightProducer.ServerStreamListener client) {
        this.allocator = allocator;
        this.client = client;
    }

    /**
     * Returns the columns of the kept answers.
     *
     * @return schema, or null when no reader started
     */
    Schema schema() {
        return schema;
    }

    /**
     * Counts the Arrow body bytes kept.
     *
     * @return bytes
     */
    long bytes() {
        return bytes;
    }

    /**
     * Counts the batches kept.
     *
     * @return batches
     */
    int batchCount() {
        return batches.size();
    }

    /**
     * Opens a reader over every kept batch in the order they were sent; the batches stay owned
     * by this collection.
     *
     * @return reader with the kept answers' schema
     * @throws IllegalStateException when no reader started
     */
    ArrowReader reader() {
        if (schema == null) {
            throw new IllegalStateException("No batch of files answered");
        }
        return new Reader(allocator, schema, batches);
    }

    @Override
    public boolean isCancelled() {
        return client.isCancelled();
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {
        client.setOnCancelHandler(handler);
    }

    @Override
    public boolean isReady() {
        // Batches are kept in memory: nothing waits for the client.
        return true;
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {
        // Always ready.
    }

    @Override
    public void setUseZeroCopy(boolean enabled) {
        // Batches are kept, not sent.
    }

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        for (Field field : root.getSchema().getFields()) {
            if (field.getDictionary() != null) {
                throw new IllegalStateException("The column '" + field.getName()
                        + "' is dictionary-encoded: its batches cannot be merged");
            }
        }
        if (schema == null) {
            schema = root.getSchema();
        } else if (!DistributedComplete.DeferredStart.sameColumns(schema.getFields(),
                root.getSchema().getFields())) {
            throw new IllegalStateException("A batch of files answers with the columns "
                    + root.getSchema() + ", the first with " + schema);
        }
        source = root;
    }

    @Override
    public void putNext() {
        keepBatch();
    }

    @Override
    public void putNext(ArrowBuf metadata) {
        keepBatch();
    }

    @Override
    public void putMetadata(ArrowBuf metadata) {
        // Metadata of a part answer is not merged.
    }

    @Override
    public void error(Throwable cause) {
        throw new IllegalStateException("A batch of files failed: " + cause.getMessage(), cause);
    }

    @Override
    public void completed() {
        // The merge completes the client's stream.
    }

    /** Keeps the current batch of the started reader; its buffers are retained. */
    private void keepBatch() {
        if (source == null) {
            throw new IllegalStateException("A batch was sent before its reader started");
        }
        ArrowRecordBatch batch = new VectorUnloader(source).getRecordBatch();
        bytes += batch.computeBodyLength();
        batches.add(batch);
    }

    /** Releases every kept batch. */
    @Override
    public void close() {
        for (ArrowRecordBatch batch : batches) {
            batch.close();
        }
        batches.clear();
    }

    /** Reads kept batches back in order. */
    private static final class Reader extends ArrowReader {

        private final Schema schema;
        private final List<ArrowRecordBatch> batches;
        private int next;

        /**
         * Creates the reader.
         *
         * @param allocator allocator of the reader's root
         * @param schema columns of the batches
         * @param batches kept batches, owned by the collection
         */
        Reader(BufferAllocator allocator, Schema schema, List<ArrowRecordBatch> batches) {
            super(allocator);
            this.schema = schema;
            this.batches = batches;
        }

        @Override
        public boolean loadNextBatch() throws java.io.IOException {
            prepareLoadNextBatch();
            if (next >= batches.size()) {
                return false;
            }
            loadRecordBatch(batches.get(next++));
            return true;
        }

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        protected void closeReadSource() {
            // The batches belong to the collection.
        }

        @Override
        protected Schema readSchema() {
            return schema;
        }
    }
}
