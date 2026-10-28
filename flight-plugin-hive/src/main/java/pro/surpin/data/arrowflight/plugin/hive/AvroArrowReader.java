package pro.surpin.data.arrowflight.plugin.hive;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.orc.TypeDescription;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * Reads one Avro data file of a Hive Avro table ({@link AvroFormat}) as Arrow batches, as Hive
 * 3.1.3's {@code AvroGenericRecordReader} and {@code AvroDeserializer} read it: every record is
 * resolved from the file's schema to the table's reader schema (Avro's promotions, defaults
 * for fields the file lacks, fields it has beyond them skipped; a file the schema cannot
 * resolve fails), fields are the columns by position. Strings and enums are text; bytes and
 * fixed binary; decimals the unscaled bytes at the scale of the file's own schema, NULL when
 * they do not fit the column; char and varchar cut to the file schema's maxLength and the
 * column's length (char without trailing spaces); dates days since 1970; timestamps
 * milliseconds since 1970 UTC shown in the file's {@code writer.time.zone} (a file without
 * it fails on its first timestamp: Hive shows those in the time zone of the server reading
 * them). A decimal, char or varchar the file's schema lacks fails, as in Hive. An empty file
 * has no rows; a file cut short fails. A file may be read as byte ranges, as Hadoop's Avro
 * record reader splits it: a range reads the blocks whose preceding sync marker starts in it,
 * so the ranges of a file read every block once.
 */
final class AvroArrowReader extends ArrowReader {

    /** File metadata key of the writer's time zone (Hive 3.1's {@code AvroSerDe}). */
    static final String WRITER_TIME_ZONE = "writer.time.zone";
    private static final int WINDOW_BYTES = 1 << 20;

    private final String location;
    private final int batchSize;
    private final List<String> projection;
    private final int[] positions;
    private final TypeDescription[] types;
    private final Schema[] readerFields;
    private final Schema[] writerFields;
    private final DataFileReader<GenericRecord> reader;
    private final ZoneId writerZone;
    private final long fileLength;
    /** First byte of the range read. */
    private final long offset;
    /** End of the range: blocks whose sync marker starts at or after it belong to the next range. */
    private final long end;
    private GenericRecord record;
    private long rowNumber;

    /**
     * Opens a file.
     *
     * @param allocator allocator of the batches
     * @param file the file
     * @param location its location, for messages
     * @param columns the table's data columns, in the reader schema's order
     * @param projection data columns to read, in output order
     * @param format the table's reader schema
     * @param batchSize rows per batch
     * @throws IOException when the file is not an Avro data file or names a bad time zone
     */
    AvroArrowReader(BufferAllocator allocator, ReadableFile file, String location,
            List<OrcArrowReader.Column> columns, List<String> projection, AvroFormat format,
            int batchSize) throws IOException {
        this(allocator, file, location, columns, projection, format, batchSize, 0L,
                Long.MAX_VALUE);
    }

    /**
     * Opens a byte range of a file: the blocks whose preceding sync marker starts in it.
     *
     * @param allocator allocator of the batches
     * @param file the file
     * @param location its location, for messages
     * @param columns the table's data columns, in the reader schema's order
     * @param projection data columns to read, in output order
     * @param format the table's reader schema
     * @param batchSize rows per batch
     * @param offset first byte of the range
     * @param length bytes in the range
     * @throws IOException when the file is not an Avro data file or names a bad time zone
     */
    AvroArrowReader(BufferAllocator allocator, ReadableFile file, String location,
            List<OrcArrowReader.Column> columns, List<String> projection, AvroFormat format,
            int batchSize, long offset, long length) throws IOException {
        super(allocator);
        this.offset = offset;
        this.end = length > Long.MAX_VALUE - offset ? Long.MAX_VALUE : offset + length;
        this.location = location;
        this.batchSize = batchSize;
        this.projection = List.copyOf(projection);
        Schema readerSchema = format.schema();
        if (readerSchema.getFields().size() != columns.size()) {
            throw new IllegalArgumentException("The reader schema has "
                    + readerSchema.getFields().size() + " fields for " + columns.size() + " columns");
        }
        this.positions = new int[projection.size()];
        this.types = new TypeDescription[projection.size()];
        this.readerFields = new Schema[projection.size()];
        this.writerFields = new Schema[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            int position = -1;
            for (int c = 0; c < columns.size(); c++) {
                if (columns.get(c).name().equals(projection.get(i))) {
                    position = c;
                }
            }
            if (position < 0) {
                throw new IllegalArgumentException("Column " + projection.get(i)
                        + " is not a data column of the table");
            }
            positions[i] = position;
            types[i] = TypeDescription.fromString(columns.get(position).hiveType());
            readerFields[i] = readerSchema.getFields().get(position).schema();
        }
        this.fileLength = file.length();
        if (fileLength == 0) {
            this.reader = null;
            this.writerZone = null;
            return;
        }
        GenericDatumReader<GenericRecord> datums = new GenericDatumReader<>();
        datums.setExpected(readerSchema);
        try {
            this.reader = new DataFileReader<>(new Input(file), datums);
        } catch (IOException | AvroRuntimeException e) {
            throw new IOException(location + " is not an Avro data file: " + e.getMessage(), e);
        }
        try {
            Schema writer = AvroFormat.unwrapped(reader.getSchema());
            for (int i = 0; i < positions.length; i++) {
                Schema.Field field = writer.getType() != Schema.Type.RECORD ? null
                        : writer.getField(readerSchema.getFields().get(positions[i]).name());
                writerFields[i] = field == null ? null : field.schema();
            }
            byte[] zone = reader.getMeta(WRITER_TIME_ZONE);
            this.writerZone = zone == null ? null
                    : ZoneId.of(new String(zone, StandardCharsets.UTF_8));
            if (offset > 0) {
                reader.sync(offset);
            }
        } catch (IOException | AvroRuntimeException e) {
            reader.close();
            throw new IOException("Cannot read " + location + " from byte " + offset + ": "
                    + e.getMessage(), e);
        } catch (DateTimeException e) {
            reader.close();
            throw new IOException("Can't parse writer time zone stored in file metadata of "
                    + location, e);
        }
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.allocateNew();
        int rows = 0;
        while (reader != null && rows < batchSize && hasNext()) {
            rowNumber++;
            try {
                record = reader.next(record);
                for (int i = 0; i < positions.length; i++) {
                    JsonRows.write(root.getVector(i), types[i], rows, value(record.get(positions[i]),
                            writerFields[i], readerFields[i], types[i]));
                }
            } catch (IOException | RuntimeException e) {
                throw new IOException("Record " + rowNumber
                        + (offset == 0 ? "" : " after byte " + offset) + " of " + location
                        + " is not read as Hive reads it: " + e.getMessage(), e);
            }
            rows++;
        }
        for (FieldVector vector : root.getFieldVectors()) {
            vector.setValueCount(rows);
        }
        root.setRowCount(rows);
        return rows > 0;
    }

    /**
     * Tells whether the range has another record; at the file's end, checks that the last block
     * ended at the end of the file (Avro's reader takes a file cut inside a block's header for
     * its end).
     *
     * @return true when a record follows
     * @throws IOException when the file cannot be read or is cut short
     */
    private boolean hasNext() throws IOException {
        try {
            // Between blocks the sync point is the next block's marker; at the file's end its length.
            if (end != Long.MAX_VALUE && reader.pastSync(end)) {
                return false;
            }
            if (reader.hasNext()) {
                return true;
            }
            long end = reader.previousSync();
            if (end != fileLength) {
                throw new IOException("the file is cut short or has bytes after its last block"
                        + " (last block ends at " + end + " of " + fileLength + " bytes)");
            }
            return false;
        } catch (IOException | AvroRuntimeException e) {
            throw new IOException("Cannot read " + location + ": " + e.getMessage(), e);
        }
    }

    /**
     * Converts a resolved datum to the value {@link JsonRows#write} takes, as Hive's
     * {@code AvroDeserializer.worker}.
     *
     * @param datum the datum, or null
     * @param writer its schema in the file, or null when the file lacks it
     * @param reader its schema in the reader schema
     * @param type its column type
     * @return the value, or null
     * @throws IOException where Hive's deserializer fails
     */
    private Object value(Object datum, Schema writer, Schema reader, TypeDescription type)
            throws IOException {
        if (datum == null) {
            return null;
        }
        Schema read = AvroFormat.unwrapped(reader);
        Schema written = AvroFormat.unwrapped(writer);
        switch (type.getCategory()) {
            case STRUCT -> {
                GenericRecord nested = (GenericRecord) datum;
                List<Schema.Field> fields = read.getFields();
                Object[] values = new Object[fields.size()];
                for (int f = 0; f < fields.size(); f++) {
                    Schema.Field own = written == null || written.getType() != Schema.Type.RECORD
                            ? null : written.getField(fields.get(f).name());
                    values[f] = value(nested.get(f), own == null ? null : own.schema(),
                            fields.get(f).schema(), type.getChildren().get(f));
                }
                return values;
            }
            case LIST -> {
                List<?> elements = (List<?>) datum;
                Schema element = written == null || written.getType() != Schema.Type.ARRAY ? null
                        : written.getElementType();
                List<Object> values = new ArrayList<>(elements.size());
                for (Object e : elements) {
                    values.add(value(e, element, read.getElementType(), type.getChildren().get(0)));
                }
                return values;
            }
            case MAP -> {
                Map<?, ?> entries = (Map<?, ?>) datum;
                Schema valueSchema = written == null || written.getType() != Schema.Type.MAP ? null
                        : written.getValueType();
                Map<String, Object> values = new HashMap<>();
                for (Map.Entry<?, ?> entry : entries.entrySet()) {
                    values.put(entry.getKey().toString(), value(entry.getValue(), valueSchema,
                            read.getValueType(), type.getChildren().get(1)));
                }
                return values;
            }
            case STRING -> {
                return datum.toString();
            }
            case CHAR, VARCHAR -> {
                if (written == null) {
                    throw new IOException("File schema is missing for " + type + " field");
                }
                String text = JsonRows.cut(JsonRows.cut(datum.toString(),
                        AvroFormat.intProp(written, "maxLength")), type.getMaxLength());
                return type.getCategory() == TypeDescription.Category.CHAR
                        ? JsonRows.stripEnd(text) : text;
            }
            case BINARY -> {
                if (read.getType() == Schema.Type.FIXED) {
                    return ((GenericFixed) datum).bytes().clone();
                }
                if (read.getType() == Schema.Type.BYTES) {
                    return bytes((ByteBuffer) datum);
                }
                throw new IOException("Unexpected Avro schema for Binary TypeInfo: " + read.getType());
            }
            case DECIMAL -> {
                if (written == null) {
                    throw new IOException("File schema is missing for decimal field");
                }
                return HiveDecimal.create(new BigInteger(bytes((ByteBuffer) datum)),
                        AvroFormat.intProp(written, "scale"));
            }
            case DATE -> {
                return LocalDate.ofEpochDay((Integer) datum);
            }
            case TIMESTAMP -> {
                if (writerZone == null) {
                    throw new IOException("the file has no " + WRITER_TIME_ZONE + " metadata:"
                            + " Hive 3.1.3 shows its timestamps in the time zone of the server"
                            + " reading them, not read");
                }
                return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) datum), writerZone);
            }
            default -> {
                return datum;
            }
        }
    }

    /**
     * Copies a buffer's bytes as Hive's {@code getBytesFromByteBuffer}: from its start to its
     * limit.
     *
     * @param buffer the buffer
     * @return its bytes
     */
    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        copy.rewind();
        byte[] result = new byte[copy.limit()];
        copy.get(result);
        return result;
    }

    @Override
    public long bytesRead() {
        return 0L;
    }

    @Override
    protected void closeReadSource() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected org.apache.arrow.vector.types.pojo.Schema readSchema() {
        List<Field> fields = new ArrayList<>(projection.size());
        for (int i = 0; i < projection.size(); i++) {
            fields.add(OrcArrowReader.field(projection.get(i), types[i]));
        }
        return new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    /** A {@link ReadableFile} as Avro's seekable input, read through a window of bytes. */
    private static final class Input implements SeekableInput {

        private final ReadableFile file;
        private final long length;
        private final byte[] window;
        private long windowStart;
        private int windowLength;
        private long position;

        Input(ReadableFile file) {
            this.file = file;
            this.length = file.length();
            this.window = new byte[(int) Math.min(WINDOW_BYTES, Math.max(1L, length))];
        }

        @Override
        public void seek(long p) throws IOException {
            if (p < 0 || p > length) {
                throw new EOFException("Seek to " + p + " outside the file's " + length + " bytes");
            }
            position = p;
        }

        @Override
        public long tell() {
            return position;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public int read(byte[] buffer, int offset, int count) throws IOException {
            if (count == 0) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }
            if (position < windowStart || position >= windowStart + windowLength) {
                windowStart = position;
                windowLength = (int) Math.min(window.length, length - position);
                windowLength = Math.max(windowLength, 0);
                try {
                    file.readFully(windowStart, window, 0, windowLength);
                } catch (EOFException e) {
                    windowLength = 0;
                    throw new IOException("File ended before its listed length " + length, e);
                }
            }
            int n = (int) Math.min(count, windowStart + windowLength - position);
            System.arraycopy(window, (int) (position - windowStart), buffer, offset, n);
            position += n;
            return n;
        }

        @Override
        public void close() {
        }
    }
}
