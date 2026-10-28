package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
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
 * columns as text, a constant per file from its partition values (NULL when missing). Each file
 * is opened only when the previous one is done. The stream's callbacks run on DuckDB's threads,
 * so every read sets the plugin's context class loader, which Hadoop and ORC load classes from.
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

    private final List<TableFile> files;
    private final Opener opener;
    private final List<OrcArrowReader.Column> dataColumns;
    private final List<String> columns;
    private final List<String> partitionColumns;
    private final List<String> projection = new ArrayList<>();
    private final SearchArgument searchArgument;
    private final int batchSize;
    private final ClassLoader loader;
    private int next;
    private OrcArrowReader current;
    private TableFile currentFile;

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
     * @throws IllegalArgumentException when a requested column is not a column of the table
     */
    OrcTableReader(BufferAllocator allocator, List<TableFile> files, Opener opener,
            List<OrcArrowReader.Column> dataColumns, List<String> partitionColumns,
            List<String> columns, SearchArgument searchArgument, int batchSize) {
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
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            prepareLoadNextBatch();
            while (true) {
                if (current == null) {
                    if (next >= files.size()) {
                        return false;
                    }
                    currentFile = files.get(next++);
                    current = open(currentFile);
                }
                boolean loaded;
                try {
                    loaded = current.loadNextBatch();
                } catch (IOException | RuntimeException e) {
                    throw failure(currentFile, e);
                }
                if (loaded) {
                    fill(current.getVectorSchemaRoot(), currentFile);
                    return true;
                }
                OrcArrowReader done = current;
                current = null;
                done.close();
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
        if (current != null) {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                current.close();
            } finally {
                current = null;
                thread.setContextClassLoader(previous);
            }
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
