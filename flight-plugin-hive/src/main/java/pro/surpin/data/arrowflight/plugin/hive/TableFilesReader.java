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
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * Reads the files of one table scan (ORC or text) one after the other as one Arrow stream: the
 * requested columns in the requested order, data columns as the format's reader
 * ({@link OrcArrowReader}, {@link TextArrowReader}) reads them and partition
 * columns as text, a constant per file from its partition values (NULL when missing). An ORC file
 * larger than a quarter of {@code rangeBytes} is read as consecutive byte ranges, each a group of whole stripes
 * from the file's footer of at least its size over {@code readAhead}, at most about
 * {@code rangeBytes} (ORC reads the stripes that start in a range;
 * the first range starts at 0 and the last is open-ended, so the ranges cover the file whatever
 * its footer or listed size say). Up to {@code readAhead} ranges are decoded at once on the executor's threads, each a few
 * batches ahead, and their batches are handed over in file and range order, so the stream is the
 * same as a file-by-file read; a text file is one range. The stream's callbacks run on DuckDB's threads and the decoding on the executor's, so
 * both set the plugin's context class loader, which Hadoop and ORC load classes from.
 */
final class TableFilesReader extends ArrowReader {

    /**
     * Reads the files of one format.
     */
    interface Format {

        /**
         * Names the format for messages.
         *
         * @return name, for example ORC
         */
        String name();

        /**
         * Tells whether large files are read as several ranges.
         *
         * @return true when {@link #rangeStarts} plans ranges
         */
        boolean splits();

        /**
         * Plans the ranges of a file.
         *
         * @param file the file
         * @param location its location
         * @param target least bytes of a range
         * @return the starts of the ranges after the first, ascending
         * @throws IOException when the file cannot be read
         */
        List<Long> rangeStarts(ReadableFile file, String location, long target) throws IOException;

        /**
         * Opens a byte range of a file.
         *
         * @param allocator allocator of the batches
         * @param file the file
         * @param location its location
         * @param projection data columns to read, in output order
         * @param offset first byte of the range
         * @param length bytes in the range
         * @return its reader
         * @throws IOException when it cannot be opened
         */
        ArrowReader open(BufferAllocator allocator, ReadableFile file, String location,
                List<String> projection, long offset, long length) throws IOException;
    }

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
    private final Format format;
    private final ClassLoader loader;
    private final Executor executor;
    private final int readAhead;
    private final long rangeBytes;
    private final ArrayDeque<FileRead> reads = new ArrayDeque<>();
    /** Next file to read. */
    private int next;
    /** Next byte of file {@code next} to read. */
    private long nextOffset;
    /** Whether the ranges of file {@code next} are planned. */
    private boolean planned;
    /** Starts of the ranges of file {@code next} after the one at {@code nextOffset}. */
    private final ArrayDeque<Long> rangeStarts = new ArrayDeque<>();

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
     * @param readAhead ranges decoded at once, at least 1
     * @param rangeBytes most bytes of a file read as one range (about; whole stripes), at least 1;
     *     files of more than a quarter of it are split
     * @throws IllegalArgumentException when a requested column is not a column of the table
     */
    TableFilesReader(BufferAllocator allocator, List<TableFile> files, Opener opener,
            List<OrcArrowReader.Column> dataColumns, List<String> partitionColumns,
            List<String> columns, SearchArgument searchArgument, int batchSize,
            Executor executor, int readAhead, long rangeBytes) {
        this(allocator, files, opener, dataColumns, partitionColumns, columns,
                orc(dataColumns, searchArgument, batchSize), executor, readAhead, rangeBytes);
    }

    /**
     * Creates the reader of files of any format; no file is opened yet.
     *
     * @param allocator allocator of the batches
     * @param files files to read, in order
     * @param opener opens a file
     * @param dataColumns the table's data columns with their Hive types
     * @param partitionColumns the table's partition columns
     * @param columns requested columns, in output order
     * @param format reads the files
     * @param executor runs the decoding of files
     * @param readAhead ranges decoded at once, at least 1
     * @param rangeBytes most bytes of a file read as one range, at least 1
     * @throws IllegalArgumentException when a requested column is not a column of the table
     */
    TableFilesReader(BufferAllocator allocator, List<TableFile> files, Opener opener,
            List<OrcArrowReader.Column> dataColumns, List<String> partitionColumns,
            List<String> columns, Format format, Executor executor, int readAhead,
            long rangeBytes) {
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
        this.format = format;
        this.loader = TableFilesReader.class.getClassLoader();
        this.executor = executor;
        this.readAhead = Math.max(1, readAhead);
        this.rangeBytes = Math.max(1L, rangeBytes);
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
                    FileRead read = nextRange();
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
     * Takes the next range to read: the next group of stripes of the current file, or the rest of
     * it (open-ended).
     *
     * @return the range's read, not started
     */
    private FileRead nextRange() {
        TableFile file = files.get(next);
        if (!planned) {
            planned = true;
            nextOffset = 0L;
            rangeStarts.clear();
            if (format.splits() && file.size() > rangeBytes / 4) {
                rangeStarts.addAll(plan(file));
            }
        }
        long offset = nextOffset;
        Long end = rangeStarts.poll();
        if (end != null) {
            nextOffset = end;
            return new FileRead(file, offset, end - offset);
        }
        next++;
        planned = false;
        return new FileRead(file, offset, Long.MAX_VALUE - offset);
    }

    /**
     * Groups a file's stripes into ranges of at least {@code rangeBytes}, or of the file's size over
     * {@code readAhead} when that is less (but the last).
     *
     * @param file table file
     * @return the starts of the ranges after the first, ascending; empty when the footer cannot
     *     be read (the file is then read as one range, whose read reports the failure)
     */
    private List<Long> plan(TableFile file) {
        long target = Math.min(rangeBytes, Math.max(1L, file.size() / readAhead));
        try {
            return format.rangeStarts(opener.open(file), file.location(), target);
        } catch (IOException | RuntimeException e) {
            return List.of();
        }
    }

    /**
     * Reads ORC files: stripe groups as ranges, a search argument skipping stripes and row groups.
     *
     * @param dataColumns the table's data columns
     * @param searchArgument stripe and row group filter, or null
     * @param batchSize rows per batch
     * @return the format
     */
    static Format orc(List<OrcArrowReader.Column> dataColumns, SearchArgument searchArgument,
            int batchSize) {
        return new Format() {
            @Override
            public String name() {
                return "ORC";
            }

            @Override
            public boolean splits() {
                return true;
            }

            @Override
            public List<Long> rangeStarts(ReadableFile file, String location, long target)
                    throws IOException {
                List<Long> starts = new ArrayList<>();
                long group = 0L;
                for (StripeInformation stripe : OrcArrowReader.stripes(file, location)) {
                    if (group >= target) {
                        starts.add(stripe.getOffset());
                        group = 0L;
                    }
                    group += stripe.getLength();
                }
                return starts;
            }

            @Override
            public ArrowReader open(BufferAllocator allocator, ReadableFile file, String location,
                    List<String> projection, long offset, long length) throws IOException {
                return new OrcArrowReader(allocator, file, location, dataColumns, projection,
                        searchArgument, batchSize, offset, length);
            }
        };
    }

    /**
     * Reads Hive text files, each as one range.
     *
     * @param dataColumns the table's data columns
     * @param textFormat how the files are laid out
     * @param batchSize rows per batch
     * @return the format
     */
    static Format text(List<OrcArrowReader.Column> dataColumns, TextFormat textFormat,
            int batchSize) {
        return new Format() {
            @Override
            public String name() {
                return "text";
            }

            @Override
            public boolean splits() {
                return false;
            }

            @Override
            public List<Long> rangeStarts(ReadableFile file, String location, long target) {
                return List.of();
            }

            @Override
            public ArrowReader open(BufferAllocator allocator, ReadableFile file, String location,
                    List<String> projection, long offset, long length) throws IOException {
                return new TextArrowReader(allocator, file, location, dataColumns, projection,
                        textFormat, batchSize);
            }
        };
    }

    /**
     * Opens a byte range of one file.
     *
     * @param file table file
     * @param offset first byte of the range
     * @param length bytes in the range
     * @return its reader
     * @throws IOException when it cannot be opened
     */
    private ArrowReader open(TableFile file, long offset, long length) throws IOException {
        try {
            return format.open(allocator, opener.open(file), file.location(), projection, offset,
                    length);
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
     * Decodes one byte range of a file on the executor into a queue of batches, ended by
     * {@link #END} or by the file's failure as an {@link IOException}.
     */
    private final class FileRead implements Runnable {

        private final TableFile file;
        private final long offset;
        private final long length;
        private final BlockingQueue<Object> batches = new ArrayBlockingQueue<>(BATCHES_AHEAD + 1);
        private final CountDownLatch finished = new CountDownLatch(1);
        private volatile boolean cancelled;

        /**
         * Creates the read of a byte range of a file.
         *
         * @param file table file
         * @param offset first byte of the range
         * @param length bytes in the range
         */
        FileRead(TableFile file, long offset, long length) {
            this.file = file;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public void run() {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                Object last;
                try (ArrowReader reader = open(file, offset, length)) {
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
        private boolean loadNext(ArrowReader reader) throws IOException {
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

    private IOException failure(TableFile file, Exception e) {
        return new IOException("Cannot read " + format.name() + " file " + file.location() + ": "
                + e.getMessage(), e);
    }
}
