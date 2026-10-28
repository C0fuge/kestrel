package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.TypeDescription;

import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * Reads the ORC files of one table scan one after the other as one Arrow stream: the requested
 * columns in the requested order, data columns as {@link OrcArrowReader} reads them and partition
 * columns as text, a constant per file from its partition values (NULL when missing). Up to
 * {@code readAhead} files are decoded at once on the executor's threads, each a few batches ahead,
 * and their batches are handed over in file order, so the stream is the same as a file-by-file
 * read. The stream's callbacks run on DuckDB's threads and the decoding on the executor's, so
 * both set the plugin's context class loader, which Hadoop and ORC load classes from.
 */
final class OrcTableReader extends ArrowReader {

    /**
     * Opens a file of the scan through the server's stores.
     */
    @FunctionalInterface
    interface Opener {

        /**
         * Opens a file.
         *
         * @param file table file
         * @return readable file
         * @throws IOException when the file cannot be opened
         */
        ReadableFile open(TableFile file) throws IOException;
    }

    /** Decoded batches a file may hold ahead of the stream. */
    private static final int BATCHES_AHEAD = 2;
    private static final long OFFER_MILLIS = 100L;
    /** Marks the end of a file's batches. */
    private static final Object END = new Object();

    private final List<TableFile> files;
    private final Opener opener;
    private final List<OrcArrowReader.Column> dataColumns;
    private final List<String> columns;
    private final List<String> partitionColumns;
    private final List<String> projection = new ArrayList<>();
    private final SearchArgument searchArgument;
    private final int batchSize;
    private final ClassLoader loader;
    private final Executor executor;
    private final int readAhead;
    private final ArrayDeque<FileRead> reads = new ArrayDeque<>();
    private int next;

    /**
     * Creates the reader; no file is opened yet.
     *
     * @param allocator allocator of the batches
     * @param files files to read, in order
     * @param opener opens a file
     * @param dataColumns the table's data columns with their Hive types
     * @param partitionColumns the table's partition columns
     * @param columns requested columns, in output order
     * @param searchArgument stripe and row group filter, or null
     * @param batchSize rows per batch
     * @param executor runs the decoding of files
     * @param readAhead files decoded at once, at least 1
     * @throws IllegalArgumentException when a requested column is not a column of the table
     */
    OrcTableReader(BufferAllocator allocator, List<TableFile> files, Opener opener,
            List<OrcArrowReader.Column> dataColumns, List<String> partitionColumns,
            List<String> columns, SearchArgument searchArgument, int batchSize,
            Executor executor, int readAhead) {
        super(allocator);
        this.files = List.copyOf(files);
        this.opener = opener;
        this.dataColumns = List.copyOf(dataColumns);
        this.partitionColumns = List.copyOf(partitionColumns);
        this.columns = List.copyOf(columns);
        for (String column : columns) {
            if (dataColumn(column) != null) {
                projection.add(column);
            } else if (!partitionColumns.contains(column)) {
                throw new IllegalArgumentException("Column " + column + " is not a column of the table");
            }
        }
        this.searchArgument = searchArgument;
        this.batchSize = batchSize;
        this.loader = OrcTableReader.class.getClassLoader();
        this.executor = executor;
        this.readAhead = Math.max(1, readAhead);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            prepareLoadNextBatch();
            while (true) {
                while (reads.size() < readAhead && next < files.size()) {
                    FileRead read = new FileRead(files.get(next++));
                    // Queued only once accepted: close waits for every queued read to end.
                    executor.execute(read);
                    reads.add(read);
                }
                FileRead head = reads.peek();
                if (head == null) {
                    return false;
                }
                Object item;
                try {
                    item = head.batches.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("Interrupted reading " + head.file.location());
                }
                if (item == END) {
                    reads.poll();
                    continue;
                }
                if (item instanceof IOException e) {
                    reads.poll();
                    throw e;
                }
                try (VectorSchemaRoot batch = (VectorSchemaRoot) item) {
                    fill(batch, head.file);
                }
                return true;
            }
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    @Override
    public long bytesRead() {
        return 0L;
    }

    @Override
    protected void closeReadSource() throws IOException {
        IOException failure = null;
        for (FileRead read : reads) {
            try {
                read.cancel();
            } catch (InterruptedIOException e) {
                failure = e;
            }
        }
        reads.clear();
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    protected Schema readSchema() {
        List<Field> fields = new ArrayList<>(columns.size());
        for (String column : columns) {
            OrcArrowReader.Column data = dataColumn(column);
            fields.add(data == null ? Field.nullable(column, ArrowType.Utf8.INSTANCE)
                    : OrcArrowReader.field(column, TypeDescription.fromString(data.hiveType())));
        }
        return new Schema(fields);
    }

    /**
     * Opens one file.
     *
     * @param file table file
     * @return its reader
     * @throws IOException when it cannot be opened
     */
    private OrcArrowReader open(TableFile file) throws IOException {
        try {
            return new OrcArrowReader(allocator, opener.open(file), file.location(), dataColumns,
                    projection, searchArgument, batchSize);
        } catch (IOException | RuntimeException e) {
            throw failure(file, e);
        }
    }

    /**
     * Moves a file's batch into the stream's batch and adds the partition values.
     *
     * @param batch the file reader's batch
     * @param file its file
     * @throws IOException when the stream's batch cannot be made
     */
    private void fill(VectorSchemaRoot batch, TableFile file) throws IOException {
        VectorSchemaRoot root = getVectorSchemaRoot();
        int rows = batch.getRowCount();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            FieldVector target = root.getVector(i);
            int data = projection.indexOf(column);
            if (data >= 0) {
                batch.getVector(data).makeTransferPair(target).transfer();
                continue;
            }
            VarCharVector text = (VarCharVector) target;
            text.allocateNew(rows);
            String value = file.partitionValues().get(column);
            byte[] bytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
            for (int row = 0; row < rows; row++) {
                if (bytes == null) {
                    text.setNull(row);
                } else {
                    text.setSafe(row, bytes);
                }
            }
            text.setValueCount(rows);
        }
        root.setRowCount(rows);
    }

    /**
     * Decodes one file on the executor into a queue of batches, ended by {@link #END} or by the
     * file's failure as an {@link IOException}.
     */
    private final class FileRead implements Runnable {

        private final TableFile file;
        private final BlockingQueue<Object> batches = new ArrayBlockingQueue<>(BATCHES_AHEAD + 1);
        private final CountDownLatch finished = new CountDownLatch(1);
        private volatile boolean cancelled;

        /**
         * Creates the read of a file.
         *
         * @param file table file
         */
        FileRead(TableFile file) {
            this.file = file;
        }

        @Override
        public void run() {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                Object last;
                try (OrcArrowReader reader = open(file)) {
                    VectorSchemaRoot source = reader.getVectorSchemaRoot();
                    while (!cancelled && loadNext(reader)) {
                        VectorSchemaRoot batch = detach(source);
                        boolean queued = false;
                        try {
                            queued = put(batch);
                        } finally {
                            if (!queued) {
                                batch.close();
                            }
                        }
                        if (!queued) {
                            return;
                        }
                    }
                    last = END;
                } catch (IOException e) {
                    last = e;
                } catch (RuntimeException e) {
                    last = failure(file, e);
                }
                put(last);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
                thread.setContextClassLoader(previous);
            }
        }

        /**
         * Loads the file's next batch.
         *
         * @param reader the file's reader
         * @return false at the end of the file
         * @throws IOException naming the file
         */
        private boolean loadNext(OrcArrowReader reader) throws IOException {
            try {
                return reader.loadNextBatch();
            } catch (IOException | RuntimeException e) {
                throw failure(file, e);
            }
        }

        /**
         * Moves a batch out of the file reader's root, which the next batch reuses.
         *
         * @param source the file reader's root
         * @return a batch of its own
         */
        private VectorSchemaRoot detach(VectorSchemaRoot source) {
            List<FieldVector> vectors = new ArrayList<>(source.getFieldVectors().size());
            for (FieldVector vector : source.getFieldVectors()) {
                TransferPair pair = vector.getTransferPair(allocator);
                pair.transfer();
                vectors.add((FieldVector) pair.getTo());
            }
            return new VectorSchemaRoot(source.getSchema().getFields(), vectors,
                    source.getRowCount());
        }

        /**
         * Queues an item unless the read is cancelled.
         *
         * @param item batch, {@link #END} or failure
         * @return false when cancelled (the item was not queued)
         * @throws InterruptedException when the thread is interrupted
         */
        private boolean put(Object item) throws InterruptedException {
            while (!cancelled) {
                if (batches.offer(item, OFFER_MILLIS, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Stops the read, waits for it and frees its queued batches.
         *
         * @throws InterruptedIOException when interrupted while waiting
         */
        void cancel() throws InterruptedIOException {
            cancelled = true;
            try {
                discard();
                finished.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("Interrupted closing " + file.location());
            } finally {
                discard();
            }
        }

        private void discard() {
            for (Object item = batches.poll(); item != null; item = batches.poll()) {
                if (item instanceof VectorSchemaRoot batch) {
                    batch.close();
                }
            }
        }
    }

    private OrcArrowReader.Column dataColumn(String name) {
        for (OrcArrowReader.Column column : dataColumns) {
            if (column.name().equals(name)) {
                return column;
            }
        }
        return null;
    }

    private static IOException failure(TableFile file, Exception e) {
        return new IOException("Cannot read ORC file " + file.location() + ": " + e.getMessage(), e);
    }
}
