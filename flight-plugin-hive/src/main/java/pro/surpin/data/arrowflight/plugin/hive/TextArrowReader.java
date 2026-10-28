package pro.surpin.data.arrowflight.plugin.hive;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.orc.TypeDescription;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * Reads one file of a Hive text table ({@code LazySimpleSerDe}) as Arrow batches, as Hive reads
 * it: one row per line, fields split at the format's delimiter (not at an escaped one) and
 * matched to the table's data columns by position; a missing field is NULL, extra fields are
 * ignored (or kept in the last column, see {@link TextFormat}), a field equal to the NULL text is
 * NULL, and a field that does not read as its column's type is NULL ({@link TextValues}). Text
 * columns are unescaped; VARCHAR and CHAR values are cut to their length, CHAR values lose their
 * trailing spaces. Files ending in {@code .gz}, {@code .deflate} or {@code .bz2} are
 * decompressed as Hadoop's codecs write them; other compressed files ({@code .snappy},
 * {@code .lz4}, {@code .zst}, {@code .lzo}, {@code .br}) are refused. The Arrow types are the
 * ones of {@link OrcArrowReader#field}, which DuckDB reads as the declared types.
 */
final class TextArrowReader extends ArrowReader {

    /** Longest line read (Hadoop has no limit; this keeps a broken file from taking the heap). */
    static final int MAX_LINE_BYTES = 64 << 20;
    private static final int BUFFER_BYTES = 1 << 16;

    private final String location;
    private final TextFormat format;
    private final int batchSize;
    private final List<String> projection;
    /** Table position of each projected column. */
    private final int[] positions;
    private final TypeDescription[] types;
    /** Fields of a row needed: the last projected position + 1. */
    private final int fieldsNeeded;
    /** Data columns of the table. */
    private final int columnCount;
    private final InputStream in;
    private final byte[] buffer = new byte[BUFFER_BYTES];
    private int bufferPosition;
    private int bufferLimit;
    private byte[] line = new byte[1024];
    private int lineLength;
    private boolean headerSkipped;
    private boolean pendingCarriageReturn;
    private final int[] fieldStarts;
    private final int[] fieldEnds;

    /**
     * Opens a file.
     *
     * @param allocator allocator of the batches
     * @param file the file, opened through the server's store
     * @param location the file's location, for messages and its compression suffix
     * @param columns the table's data columns as {@code name -> Hive type}, in table order
     * @param projection names of the columns to read, in output order
     * @param format how the table's files are laid out
     * @param batchSize rows per batch
     * @throws IOException when the file's compression is not read
     * @throws IllegalArgumentException when a projected column is unknown or of a type not read
     */
    TextArrowReader(BufferAllocator allocator, ReadableFile file, String location,
            List<OrcArrowReader.Column> columns, List<String> projection, TextFormat format,
            int batchSize) throws IOException {
        super(allocator);
        this.location = location;
        this.format = format;
        this.batchSize = batchSize;
        this.projection = List.copyOf(projection);
        this.columnCount = columns.size();
        this.positions = new int[projection.size()];
        this.types = new TypeDescription[projection.size()];
        int needed = 0;
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
            checkType(projection.get(i), types[i]);
            needed = Math.max(needed, position + 1);
        }
        this.fieldsNeeded = needed;
        this.fieldStarts = new int[columnCount];
        this.fieldEnds = new int[columnCount];
        this.in = decompressed(new BufferedInputStream(new FileStream(file), BUFFER_BYTES), location);
    }

    /**
     * Checks that a column's type is read from text.
     *
     * @param name column name
     * @param type its type
     * @throws IllegalArgumentException for nested types and time zone timestamps
     */
    static void checkType(String name, TypeDescription type) {
        switch (type.getCategory()) {
            case BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, VARCHAR, CHAR, BINARY,
                    DECIMAL, DATE, TIMESTAMP -> {
            }
            default -> throw new IllegalArgumentException("column " + name + " has type " + type
                    + " that the text reader does not read yet");
        }
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();
        if (!headerSkipped) {
            headerSkipped = true;
            for (int i = 0; i < format.headerLines(); i++) {
                if (!readLine()) {
                    return false;
                }
            }
        }
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.allocateNew();
        int rows = 0;
        while (rows < batchSize && readLine()) {
            split();
            for (int i = 0; i < positions.length; i++) {
                write(root.getVector(i), types[i], rows, positions[i]);
            }
            rows++;
        }
        for (FieldVector vector : root.getFieldVectors()) {
            vector.setValueCount(rows);
        }
        root.setRowCount(rows);
        return rows > 0;
    }

    @Override
    public long bytesRead() {
        return 0L;
    }

    @Override
    protected void closeReadSource() throws IOException {
        in.close();
    }

    @Override
    protected Schema readSchema() {
        List<Field> fields = new ArrayList<>(projection.size());
        for (int i = 0; i < projection.size(); i++) {
            fields.add(OrcArrowReader.field(projection.get(i), types[i]));
        }
        return new Schema(fields);
    }

    /**
     * Reads the next line into {@link #line}, without its end: a line feed, a carriage return,
     * or both.
     *
     * @return false at the end of the file
     * @throws IOException when the file cannot be read or a line is too long
     */
    private boolean readLine() throws IOException {
        lineLength = 0;
        if (pendingCarriageReturn) {
            pendingCarriageReturn = false;
            if (fill() && buffer[bufferPosition] == '\n') {
                bufferPosition++;
            }
        }
        if (!fill()) {
            return false;
        }
        while (fill()) {
            int from = bufferPosition;
            int i = from;
            while (i < bufferLimit && buffer[i] != '\n' && buffer[i] != '\r') {
                i++;
            }
            append(from, i);
            if (i < bufferLimit) {
                pendingCarriageReturn = buffer[i] == '\r';
                bufferPosition = i + 1;
                return true;
            }
            bufferPosition = i;
        }
        return true;
    }

    /**
     * Makes sure the buffer holds a byte to read.
     *
     * @return false at the end of the file
     * @throws IOException when the file cannot be read
     */
    private boolean fill() throws IOException {
        while (bufferPosition == bufferLimit) {
            int n = in.read(buffer, 0, buffer.length);
            if (n < 0) {
                return false;
            }
            bufferPosition = 0;
            bufferLimit = n;
        }
        return true;
    }

    /**
     * Adds buffered bytes to the line.
     *
     * @param from first byte
     * @param to end
     * @throws IOException when the line gets too long
     */
    private void append(int from, int to) throws IOException {
        int count = to - from;
        if (lineLength + count > line.length) {
            if (lineLength + count > MAX_LINE_BYTES) {
                throw new IOException("Line longer than " + MAX_LINE_BYTES + " bytes in " + location);
            }
            line = Arrays.copyOf(line, Math.min(MAX_LINE_BYTES,
                    Math.max(lineLength + count, line.length * 2)));
        }
        System.arraycopy(buffer, from, line, lineLength, count);
        lineLength += count;
    }

    /** Splits {@link #line} into fields; missing fields get a start of -1. */
    private void split() {
        int field = 0;
        int start = 0;
        int i = 0;
        int last = columnCount - 1;
        byte delimiter = format.fieldDelimiter();
        int escape = format.escape();
        while (field < fieldsNeeded) {
            if (field == last && format.lastColumnTakesRest()) {
                i = lineLength;
            }
            while (i < lineLength && line[i] != delimiter) {
                i += escape >= 0 && line[i] == (byte) escape && i + 1 < lineLength ? 2 : 1;
            }
            fieldStarts[field] = start;
            fieldEnds[field] = i;
            field++;
            if (i >= lineLength) {
                break;
            }
            i++;
            start = i;
        }
        for (; field < fieldsNeeded; field++) {
            fieldStarts[field] = -1;
        }
    }

    /**
     * Writes one field of the current line into a vector.
     *
     * @param vector target vector
     * @param type the column's type
     * @param row target row
     * @param position the column's position in the table
     */
    private void write(FieldVector vector, TypeDescription type, int row, int position) {
        int start = fieldStarts[position];
        int end = fieldEnds[position];
        if (start < 0 || isNullSequence(start, end)) {
            vector.setNull(row);
            return;
        }
        switch (type.getCategory()) {
            case BOOLEAN -> {
                int value = TextValues.bool(line, start, end);
                if (value < 0) {
                    vector.setNull(row);
                } else {
                    ((BitVector) vector).setSafe(row, value);
                }
            }
            case BYTE -> {
                Long value = TextValues.integer(line, start, end, Byte.MIN_VALUE, Byte.MAX_VALUE);
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((TinyIntVector) vector).setSafe(row, value.byteValue());
                }
            }
            case SHORT -> {
                Long value = TextValues.integer(line, start, end, Short.MIN_VALUE, Short.MAX_VALUE);
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((SmallIntVector) vector).setSafe(row, value.shortValue());
                }
            }
            case INT -> {
                Long value = TextValues.integer(line, start, end, Integer.MIN_VALUE,
                        Integer.MAX_VALUE);
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((IntVector) vector).setSafe(row, value.intValue());
                }
            }
            case LONG -> {
                Long value = TextValues.integer(line, start, end, Long.MIN_VALUE, Long.MAX_VALUE);
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((BigIntVector) vector).setSafe(row, value);
                }
            }
            case FLOAT -> {
                Float value = TextValues.single(line, start, end);
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((Float4Vector) vector).setSafe(row, value);
                }
            }
            case DOUBLE -> {
                Double value = TextValues.floating(line, start, end);
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((Float8Vector) vector).setSafe(row, value);
                }
            }
            case DECIMAL -> {
                BigDecimal value = TextValues.decimal(line, start, end, type.getPrecision(),
                        type.getScale());
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((DecimalVector) vector).setSafe(row, value);
                }
            }
            case DATE -> {
                Integer value = TextValues.date(line, start, end);
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((DateDayVector) vector).setSafe(row, value);
                }
            }
            case TIMESTAMP -> {
                Long value = TextValues.timestamp(line, start, end);
                if (value == null) {
                    vector.setNull(row);
                } else {
                    ((TimeStampVector) vector).setSafe(row, value);
                }
            }
            case BINARY -> ((VarBinaryVector) vector).setSafe(row,
                    TextValues.binary(line, start, end));
            case STRING, VARCHAR, CHAR -> {
                byte[] text = unescaped(start, end);
                text = TextValues.utf8(text, 0, text.length);
                if (type.getCategory() != TypeDescription.Category.STRING) {
                    text = TextValues.truncate(text, type.getMaxLength());
                }
                int length = text.length;
                if (type.getCategory() == TypeDescription.Category.CHAR) {
                    while (length > 0 && text[length - 1] == ' ') {
                        length--;
                    }
                }
                ((VarCharVector) vector).setSafe(row, text, 0, length);
            }
            default -> throw new IllegalArgumentException("type " + type + " is not read from text");
        }
    }

    private boolean isNullSequence(int start, int end) {
        byte[] nulls = format.nullSequence();
        if (end - start != nulls.length) {
            return false;
        }
        for (int i = 0; i < nulls.length; i++) {
            if (line[start + i] != nulls[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Copies a text field without its escapes.
     *
     * @param start first byte
     * @param end end of the field
     * @return the field's bytes
     */
    private byte[] unescaped(int start, int end) {
        int escape = format.escape();
        byte[] result = new byte[end - start];
        int length = 0;
        for (int i = start; i < end; i++) {
            byte b = line[i];
            if (escape >= 0 && b == (byte) escape && i + 1 < end) {
                byte next = line[++i];
                if (format.escapeCrlf() && next == 'r') {
                    next = '\r';
                } else if (format.escapeCrlf() && next == 'n') {
                    next = '\n';
                }
                result[length++] = next;
            } else {
                result[length++] = b;
            }
        }
        return length == result.length ? result : Arrays.copyOf(result, length);
    }

    /**
     * Wraps a file's bytes in the decompression its name asks for, as Hadoop's codec factory picks
     * it by suffix.
     *
     * @param raw the file's bytes
     * @param location its location
     * @return the text
     * @throws IOException when the compression is not read or the stream cannot be opened
     */
    static InputStream decompressed(InputStream raw, String location) throws IOException {
        String name = location.substring(location.lastIndexOf('/') + 1).toLowerCase(Locale.ROOT);
        try {
            if (name.endsWith(".gz")) {
                return new GZIPInputStream(raw, BUFFER_BYTES);
            }
            if (name.endsWith(".deflate")) {
                return new InflaterInputStream(raw);
            }
            if (name.endsWith(".bz2")) {
                BZip2Codec codec = new BZip2Codec();
                codec.setConf(new Configuration(false));
                return codec.createInputStream(raw);
            }
        } catch (IOException | RuntimeException e) {
            raw.close();
            throw e;
        }
        for (String suffix : new String[] {".snappy", ".lz4", ".zst", ".lzo", ".lzo_deflate", ".br"}) {
            if (name.endsWith(suffix)) {
                raw.close();
                throw new IOException("Compression " + suffix.substring(1) + " of " + location
                        + " is not read yet");
            }
        }
        return raw;
    }

    /** Reads a {@link ReadableFile} from its start. */
    private static final class FileStream extends InputStream {

        private final ReadableFile file;
        private final long length;
        private long position;

        FileStream(ReadableFile file) {
            this.file = file;
            this.length = file.length();
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int count) throws IOException {
            if (count == 0) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }
            int n = (int) Math.min(count, length - position);
            try {
                file.readFully(position, buffer, offset, n);
            } catch (EOFException e) {
                throw new IOException("File ended before its listed length " + length, e);
            }
            position += n;
            return n;
        }
    }
}
