package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Join runtime filters a Flight client sends with a ticket (protobuf field 15 of the ticket's
 * {@code Any}): the keys a join's build side holds, so that a scan sends only the rows the
 * client's join can match.
 *
 * <p>The block is little-endian. {@code "ARF1"}: magic, filter count, then per filter the
 * output column name (length, UTF-8 bytes) and its keys (count, int64 each). {@code "ARF2"}:
 * the same IN-filters, then bloom filters (count, per filter the column name, the partition
 * count, per partition the log2 of its bucket count as int32, the directory length as uint64
 * and the directory, then the witness count and per witness a key as int64 and the client's
 * answer as uint8). The bloom filters are checked and skipped: a filter only ever removes rows
 * the client drops anyway, so leaving one out costs speed, never rows.
 *
 * @param in the IN-filters, in block order
 * @param skippedBlooms how many bloom filters the block carried (not applied)
 */
public record RuntimeFilters(List<InFilter> in, int skippedBlooms) {

    /** Protobuf field of the ticket's {@code Any} that carries the block. */
    public static final int TICKET_FIELD = 15;
    /** Block marker "ARF1": IN-filters only. */
    static final int MAGIC_IN = 0x41524631;
    /** Block marker "ARF2": IN-filters, then bloom filters. */
    static final int MAGIC_BLOOM = 0x41524632;
    /** Most filters of each kind one block may carry. */
    static final int MAX_FILTERS = 16;
    /** Longest column name accepted, in UTF-8 bytes. */
    static final int MAX_NAME_BYTES = 256;
    /** Most keys accepted across all IN-filters of one block. */
    static final int MAX_VALUES = 2_000_000;
    /**
     * Most keys written into an SQL IN list; a longer set is applied as its range only, because
     * DuckDB needs seconds to plan an IN list of a few hundred thousand literals.
     */
    static final int MAX_IN_VALUES = 4096;
    /** Most partitions one bloom filter may have. */
    static final int MAX_PARTITIONS = 1024;
    /** Most directory bytes accepted across all bloom filters of one block. */
    static final long MAX_BLOOM_BYTES = 64L << 20;
    /** Most witnesses one bloom filter may carry. */
    static final int MAX_WITNESSES = 256;
    private static final int BLOOM_BUCKET_BYTES = 32;
    private static final int BLOOM_MAX_LOG_BUCKETS = 26;

    /** No filters. */
    public static final RuntimeFilters NONE = new RuntimeFilters(List.of(), 0);

    /**
     * Creates filters.
     *
     * @param in the IN-filters
     * @param skippedBlooms bloom filters the block carried
     */
    public RuntimeFilters {
        in = List.copyOf(in);
    }

    /**
     * One {@code column IN (keys)} filter on an output column of the scan.
     *
     * @param column output column name, as the client names it
     * @param values the keys the client's join can match
     */
    public record InFilter(String column, long[] values) {

        /**
         * Creates a filter; the keys are copied.
         *
         * @param column output column name
         * @param values the keys
         */
        public InFilter {
            values = values.clone();
        }

        @Override
        public long[] values() {
            return values.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof InFilter that && column.equals(that.column)
                    && Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return 31 * column.hashCode() + Arrays.hashCode(values);
        }

        @Override
        public String toString() {
            return "InFilter[" + column + ", " + values.length + " keys]";
        }
    }

    /**
     * Tells whether there is no IN-filter to apply.
     *
     * @return true without IN-filters
     */
    public boolean isEmpty() {
        return in.isEmpty();
    }

    /**
     * Counts the keys of all IN-filters.
     *
     * @return key count
     */
    public long keyCount() {
        long keys = 0;
        for (InFilter filter : in) {
            keys += filter.values.length;
        }
        return keys;
    }

    /**
     * Serialises IN-filters into an {@code "ARF1"} block, as a client writes it.
     *
     * @param filters filters to send
     * @return block bytes
     */
    public static byte[] encode(List<InFilter> filters) {
        int size = Integer.BYTES * 2;
        for (InFilter filter : filters) {
            size += Integer.BYTES + filter.column().getBytes(StandardCharsets.UTF_8).length
                    + Integer.BYTES + Long.BYTES * filter.values.length;
        }
        ByteBuffer block = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        block.putInt(MAGIC_IN).putInt(filters.size());
        for (InFilter filter : filters) {
            byte[] name = filter.column().getBytes(StandardCharsets.UTF_8);
            block.putInt(name.length).put(name).putInt(filter.values.length);
            for (long value : filter.values) {
                block.putLong(value);
            }
        }
        return block.array();
    }

    /**
     * Parses a block, refusing anything malformed or beyond the limits.
     *
     * @param bytes block bytes
     * @return the filters
     * @throws IllegalArgumentException when the block is not valid
     */
    public static RuntimeFilters decode(byte[] bytes) {
        ByteBuffer block = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        try {
            int magic = block.getInt();
            if (magic != MAGIC_IN && magic != MAGIC_BLOOM) {
                throw new IllegalArgumentException("Unsupported runtime filter block");
            }
            List<InFilter> in = readInFilters(block);
            int blooms = magic == MAGIC_BLOOM ? skipBlooms(block) : 0;
            if (block.hasRemaining()) {
                throw new IllegalArgumentException("Unexpected data after the runtime filters");
            }
            return new RuntimeFilters(in, blooms);
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("Truncated runtime filter block", e);
        }
    }

    private static List<InFilter> readInFilters(ByteBuffer block) {
        int count = block.getInt();
        if (count < 0 || count > MAX_FILTERS) {
            throw new IllegalArgumentException("Invalid runtime filter count " + count);
        }
        List<InFilter> filters = new ArrayList<>(count);
        long total = 0;
        for (int i = 0; i < count; i++) {
            String column = readName(block);
            int valueCount = block.getInt();
            total += valueCount;
            if (valueCount < 0 || total > MAX_VALUES
                    || (long) valueCount * Long.BYTES > block.remaining()) {
                throw new IllegalArgumentException("Invalid runtime filter key count " + valueCount);
            }
            long[] values = new long[valueCount];
            block.asLongBuffer().get(values);
            block.position(block.position() + valueCount * Long.BYTES);
            filters.add(new InFilter(column, values));
        }
        return filters;
    }

    private static String readName(ByteBuffer block) {
        int length = block.getInt();
        if (length <= 0 || length > MAX_NAME_BYTES || length > block.remaining()) {
            throw new IllegalArgumentException("Invalid runtime filter column name");
        }
        byte[] name = new byte[length];
        block.get(name);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(name)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid runtime filter column name", e);
        }
    }

    private static int skipBlooms(ByteBuffer block) {
        int count = block.getInt();
        if (count < 0 || count > MAX_FILTERS) {
            throw new IllegalArgumentException("Invalid bloom filter count " + count);
        }
        long totalBytes = 0;
        for (int i = 0; i < count; i++) {
            readName(block);
            int partitions = block.getInt();
            if (partitions < 1 || partitions > MAX_PARTITIONS) {
                throw new IllegalArgumentException("Invalid bloom filter partition count");
            }
            for (int p = 0; p < partitions; p++) {
                int logBuckets = block.getInt();
                long length = block.getLong();
                totalBytes += Math.max(0, length);
                if (logBuckets < 1 || logBuckets > BLOOM_MAX_LOG_BUCKETS
                        || length != (long) BLOOM_BUCKET_BYTES << logBuckets
                        || totalBytes > MAX_BLOOM_BYTES || length > block.remaining()) {
                    throw new IllegalArgumentException("Invalid bloom filter directory");
                }
                block.position(block.position() + (int) length);
            }
            int witnesses = block.getInt();
            if (witnesses < 0 || witnesses > MAX_WITNESSES
                    || (long) witnesses * (Long.BYTES + 1) > block.remaining()) {
                throw new IllegalArgumentException("Invalid bloom filter witness count");
            }
            block.position(block.position() + witnesses * (Long.BYTES + 1));
        }
        return count;
    }

    /**
     * Writes the condition that keeps the rows the IN-filters accept, over the scan's source
     * columns. A filter applies to an integer output column that is a plain column of the scan
     * (named once, case-insensitively); filters on other columns are left out. Keys out of the
     * column type's range are dropped (no row can hold them); a filter left without keys keeps
     * no row. Up to {@link #MAX_IN_VALUES} keys become {@code BETWEEN min AND max AND IN (...)},
     * more keys their range only, which DuckDB pushes into the Parquet scan either way.
     *
     * @param outputSchema the scan's result schema
     * @param sourceColumn maps an output column name to its quoted source column, or null
     * @return the condition, or null when no filter applies
     */
    public String condition(Schema outputSchema, java.util.function.Function<String, String> sourceColumn) {
        StringJoiner conditions = new StringJoiner(" AND ");
        for (InFilter filter : in) {
            Field field = outputField(outputSchema, filter.column());
            if (field == null || !(field.getType() instanceof ArrowType.Int type)) {
                continue;
            }
            String column = sourceColumn.apply(field.getName());
            if (column == null) {
                continue;
            }
            long[] keys = inRange(filter.values, type);
            if (keys.length == 0) {
                return "FALSE";
            }
            conditions.add(keyCondition(column, keys));
        }
        return conditions.length() == 0 ? null : conditions.toString();
    }

    private static String keyCondition(String column, long[] keys) {
        long min = keys[0];
        long max = keys[0];
        for (long key : keys) {
            min = Math.min(min, key);
            max = Math.max(max, key);
        }
        if (min == max) {
            return column + " = " + min;
        }
        String range = column + " BETWEEN " + min + " AND " + max;
        if (keys.length > MAX_IN_VALUES) {
            return range;
        }
        long[] sorted = keys.clone();
        Arrays.sort(sorted);
        StringJoiner list = new StringJoiner(",", column + " IN (", ")");
        long previous = 0;
        for (int i = 0; i < sorted.length; i++) {
            if (i == 0 || sorted[i] != previous) {
                list.add(Long.toString(sorted[i]));
            }
            previous = sorted[i];
        }
        return "(" + range + " AND " + list + ")";
    }

    private static long[] inRange(long[] keys, ArrowType.Int type) {
        int bits = type.getBitWidth();
        long min;
        long max;
        if (type.getIsSigned()) {
            min = bits >= Long.SIZE ? Long.MIN_VALUE : -(1L << (bits - 1));
            max = bits >= Long.SIZE ? Long.MAX_VALUE : (1L << (bits - 1)) - 1;
        } else {
            // A UBIGINT above Long.MAX_VALUE cannot be sent as an int64 key.
            min = 0;
            max = bits >= Long.SIZE ? Long.MAX_VALUE : (1L << bits) - 1;
        }
        long[] kept = new long[keys.length];
        int n = 0;
        for (long key : keys) {
            if (key >= min && key <= max) {
                kept[n++] = key;
            }
        }
        return n == keys.length ? keys : Arrays.copyOf(kept, n);
    }

    private static Field outputField(Schema schema, String wanted) {
        Field found = null;
        String lower = wanted.toLowerCase(Locale.ROOT);
        for (Field field : schema.getFields()) {
            if (field.getName().toLowerCase(Locale.ROOT).equals(lower)) {
                if (found != null) {
                    return null;
                }
                found = field;
            }
        }
        return found;
    }
}
