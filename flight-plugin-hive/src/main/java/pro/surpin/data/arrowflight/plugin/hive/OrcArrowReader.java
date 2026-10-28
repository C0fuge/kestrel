package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * Reads one ORC file of a Hive table as Arrow batches, with the table's declared columns: the
 * file's columns are matched to them by name (case-insensitive), or by position when the file
 * has Hive 1.x names ({@code _col0}, {@code _col1}, ...); a column the file lacks reads NULL and
 * a column of another type is converted by ORC's schema evolution. Only the projected columns
 * are read (and the columns of the search argument, which ORC needs to evaluate it), and a
 * search argument skips stripes and row groups whose statistics rule it out
 * (rows are not filtered: the query applies its filters again). The Arrow types are the ones
 * DuckDB reads for the same Hive types from Parquet ({@link HiveTypes}); CHAR values lose their
 * trailing spaces, as Hive reads them. The file is read through the server's store.
 */
final class OrcArrowReader extends ArrowReader {

    /** Hive 1.x ORC files name their columns by position. */
    private static final Pattern POSITIONAL = Pattern.compile("_col\\d+");
    private static final long MICROS_PER_SECOND = 1_000_000L;

    private final TypeDescription readerSchema;
    private final List<Integer> projected;
    private final RecordReader rows;
    private final VectorizedRowBatch batch;

    /**
     * Opens a file.
     *
     * @param allocator allocator of the batches
     * @param file the file, opened through the server's store
     * @param location the file's location, for messages
     * @param columns the table's data columns as {@code name -> Hive type}, in table order
     * @param projection names of the columns to read, in output order
     * @param searchArgument stripe and row group filter, or null
     * @param batchSize rows per batch
     * @throws IOException when the file cannot be read or a column is unknown
     */
    OrcArrowReader(BufferAllocator allocator, ReadableFile file, String location,
            List<Column> columns, List<String> projection, SearchArgument searchArgument,
            int batchSize) throws IOException {
        super(allocator);
        this.readerSchema = TypeDescription.createStruct();
        for (Column column : columns) {
            try {
                readerSchema.addField(column.name(), TypeDescription.fromString(column.hiveType()));
            } catch (IllegalArgumentException e) {
                throw new IOException("Column " + column.name() + " of " + location
                        + " has type " + column.hiveType() + " that ORC cannot read", e);
            }
        }
        this.projected = new ArrayList<>(projection.size());
        boolean[] include = new boolean[readerSchema.getMaximumId() + 1];
        include[0] = true;
        for (String name : projection) {
            int index = indexOf(columns, name);
            if (index < 0) {
                throw new IOException("Column " + name + " is not a data column of " + location);
            }
            projected.add(index);
            include(include, readerSchema.getChildren().get(index));
        }
        if (searchArgument != null) {
            // ORC evaluates a predicate only on columns it reads.
            for (PredicateLeaf leaf : searchArgument.getLeaves()) {
                int index = indexOf(columns, leaf.getColumnName());
                if (index >= 0) {
                    include(include, readerSchema.getChildren().get(index));
                }
            }
        }
        Configuration conf = new Configuration(false);
        OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, false);
        Path path = new Path("readable:/" + Integer.toHexString(location.hashCode()) + ".orc");
        ReadableFileSystem fs = new ReadableFileSystem(file, path);
        fs.setConf(conf);
        Reader reader = OrcFile.createReader(path,
                // Timestamps as their wall clock in UTC, whatever the JVM's zone.
                OrcFile.readerOptions(conf).filesystem(fs).maxLength(file.length())
                        .useUTCTimestamp(true));
        boolean positional = !reader.getSchema().getFieldNames().isEmpty();
        for (String name : reader.getSchema().getFieldNames()) {
            positional &= POSITIONAL.matcher(name).matches();
        }
        Reader.Options options = reader.options().schema(readerSchema).include(include)
                .forcePositionalEvolution(positional).useSelected(false).allowSARGToFilter(false);
        if (searchArgument != null) {
            options.searchArgument(searchArgument, new String[0]);
        }
        try {
            this.rows = reader.rows(options);
        } catch (IllegalArgumentException e) {
            throw new IOException("Cannot read " + location + " as " + readerSchema + ": "
                    + e.getMessage(), e);
        }
        this.batch = readerSchema.createRowBatch(batchSize);
        reader.close();
    }

    /**
     * A data column of the table.
     *
     * @param name column name
     * @param hiveType type as the metastore writes it
     */
    record Column(String name, String hiveType) {
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();
        if (!rows.nextBatch(batch)) {
            return false;
        }
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.allocateNew();
        for (int i = 0; i < projected.size(); i++) {
            int index = projected.get(i);
            copy(batch.cols[index], readerSchema.getChildren().get(index),
                    root.getVector(i), 0, 0, batch.size);
        }
        root.setRowCount(batch.size);
        return true;
    }

    @Override
    public long bytesRead() {
        return 0L;
    }

    @Override
    protected void closeReadSource() throws IOException {
        rows.close();
    }

    @Override
    protected Schema readSchema() {
        List<Field> fields = new ArrayList<>(projected.size());
        for (int index : projected) {
            fields.add(field(readerSchema.getFieldNames().get(index),
                    readerSchema.getChildren().get(index)));
        }
        return new Schema(fields);
    }

    /**
     * Maps an ORC type to the Arrow field DuckDB reads for the same Hive type from Parquet.
     *
     * @param name field name
     * @param type ORC type
     * @return nullable field
     */
    static Field field(String name, TypeDescription type) {
        List<Field> children = new ArrayList<>();
        ArrowType arrow;
        switch (type.getCategory()) {
            case BOOLEAN -> arrow = ArrowType.Bool.INSTANCE;
            case BYTE -> arrow = new ArrowType.Int(8, true);
            case SHORT -> arrow = new ArrowType.Int(16, true);
            case INT -> arrow = new ArrowType.Int(32, true);
            case LONG -> arrow = new ArrowType.Int(64, true);
            case FLOAT -> arrow = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE -> arrow = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case STRING, VARCHAR, CHAR -> arrow = ArrowType.Utf8.INSTANCE;
            case BINARY -> arrow = ArrowType.Binary.INSTANCE;
            case DECIMAL -> arrow = new ArrowType.Decimal(type.getPrecision(), type.getScale(), 128);
            case DATE -> arrow = new ArrowType.Date(DateUnit.DAY);
            case TIMESTAMP -> arrow = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            case TIMESTAMP_INSTANT -> arrow = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
            case LIST -> {
                arrow = ArrowType.List.INSTANCE;
                children.add(field("element", type.getChildren().get(0)));
            }
            case MAP -> {
                arrow = new ArrowType.Map(false);
                Field key = field("key", type.getChildren().get(0));
                List<Field> entry = List.of(
                        new Field("key", FieldType.notNullable(key.getType()), key.getChildren()),
                        field("value", type.getChildren().get(1)));
                children.add(new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                        entry));
            }
            case STRUCT -> {
                arrow = ArrowType.Struct.INSTANCE;
                for (int i = 0; i < type.getChildren().size(); i++) {
                    children.add(field(type.getFieldNames().get(i), type.getChildren().get(i)));
                }
            }
            default -> throw new IllegalArgumentException("ORC type " + type + " has no Arrow type");
        }
        return new Field(name, FieldType.nullable(arrow), children);
    }

    /**
     * Copies rows of an ORC column into an Arrow vector.
     *
     * @param source ORC column
     * @param type its ORC type
     * @param target Arrow vector of {@link #field} for the type
     * @param from first source row
     * @param to first target row
     * @param count rows to copy
     */
    static void copy(ColumnVector source, TypeDescription type, FieldVector target, int from,
            int to, int count) {
        for (int i = 0; i < count; i++) {
            int row = source.isRepeating ? 0 : from + i;
            int out = to + i;
            if (!source.noNulls && source.isNull[row]) {
                setNull(target, out);
                continue;
            }
            switch (type.getCategory()) {
                case BOOLEAN -> ((BitVector) target).setSafe(out,
                        ((LongColumnVector) source).vector[row] == 0 ? 0 : 1);
                case BYTE -> ((TinyIntVector) target).setSafe(out,
                        (byte) ((LongColumnVector) source).vector[row]);
                case SHORT -> ((SmallIntVector) target).setSafe(out,
                        (short) ((LongColumnVector) source).vector[row]);
                case INT -> ((IntVector) target).setSafe(out,
                        (int) ((LongColumnVector) source).vector[row]);
                case LONG -> ((BigIntVector) target).setSafe(out,
                        ((LongColumnVector) source).vector[row]);
                case DATE -> ((DateDayVector) target).setSafe(out,
                        (int) ((LongColumnVector) source).vector[row]);
                case FLOAT -> ((Float4Vector) target).setSafe(out,
                        (float) ((DoubleColumnVector) source).vector[row]);
                case DOUBLE -> ((Float8Vector) target).setSafe(out,
                        ((DoubleColumnVector) source).vector[row]);
                case STRING, VARCHAR, BINARY -> {
                    BytesColumnVector bytes = (BytesColumnVector) source;
                    if (target instanceof VarCharVector text) {
                        text.setSafe(out, bytes.vector[row], bytes.start[row], bytes.length[row]);
                    } else {
                        ((VarBinaryVector) target).setSafe(out, bytes.vector[row],
                                bytes.start[row], bytes.length[row]);
                    }
                }
                case CHAR -> {
                    BytesColumnVector bytes = (BytesColumnVector) source;
                    int length = bytes.length[row];
                    while (length > 0 && bytes.vector[row][bytes.start[row] + length - 1] == ' ') {
                        length--;
                    }
                    ((VarCharVector) target).setSafe(out, bytes.vector[row], bytes.start[row],
                            length);
                }
                case DECIMAL -> {
                    BigDecimal value = ((DecimalColumnVector) source).vector[row].getHiveDecimal()
                            .bigDecimalValue().setScale(type.getScale(), RoundingMode.UNNECESSARY);
                    ((DecimalVector) target).setSafe(out, value);
                }
                case TIMESTAMP, TIMESTAMP_INSTANT -> {
                    TimestampColumnVector times = (TimestampColumnVector) source;
                    long micros = Math.floorDiv(times.time[row], 1000L) * MICROS_PER_SECOND
                            + times.nanos[row] / 1000;
                    ((TimeStampVector) target).setSafe(out, micros);
                }
                case LIST -> {
                    ListColumnVector list = (ListColumnVector) source;
                    ListVector vector = (ListVector) target;
                    int start = vector.startNewValue(out);
                    int length = (int) list.lengths[row];
                    copy(list.child, type.getChildren().get(0), (FieldVector) vector.getDataVector(),
                            (int) list.offsets[row], start, length);
                    vector.endValue(out, length);
                }
                case MAP -> {
                    MapColumnVector map = (MapColumnVector) source;
                    MapVector vector = (MapVector) target;
                    StructVector entries = (StructVector) vector.getDataVector();
                    int start = vector.startNewValue(out);
                    int length = (int) map.lengths[row];
                    int offset = (int) map.offsets[row];
                    copy(map.keys, type.getChildren().get(0),
                            (FieldVector) entries.getChildByOrdinal(0), offset, start, length);
                    copy(map.values, type.getChildren().get(1),
                            (FieldVector) entries.getChildByOrdinal(1), offset, start, length);
                    for (int j = 0; j < length; j++) {
                        entries.setIndexDefined(start + j);
                    }
                    vector.endValue(out, length);
                }
                case STRUCT -> {
                    StructColumnVector struct = (StructColumnVector) source;
                    StructVector vector = (StructVector) target;
                    vector.setIndexDefined(out);
                    for (int f = 0; f < struct.fields.length; f++) {
                        copy(struct.fields[f], type.getChildren().get(f),
                                (FieldVector) vector.getChildByOrdinal(f), row, out, 1);
                    }
                }
                default -> throw new IllegalArgumentException("ORC type " + type
                        + " has no Arrow type");
            }
        }
    }

    /**
     * Sets a NULL, also for the children of a struct (their slots must exist).
     *
     * @param target vector
     * @param index row
     */
    private static void setNull(FieldVector target, int index) {
        if (target instanceof StructVector struct) {
            struct.setNull(index);
            for (int f = 0; f < struct.size(); f++) {
                setNull((FieldVector) struct.getChildByOrdinal(f), index);
            }
        } else if (target instanceof ListVector list) {
            list.setNull(index);
        } else {
            while (index >= target.getValueCapacity()) {
                target.reAlloc();
            }
            target.setNull(index);
        }
    }

    /**
     * Marks a column and its children as read.
     *
     * @param include read flags by column ID
     * @param type column type
     */
    private static void include(boolean[] include, TypeDescription type) {
        for (int id = type.getId(); id <= type.getMaximumId(); id++) {
            include[id] = true;
        }
    }

    private static int indexOf(List<Column> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }
}
