package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.ByteString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Values of {@code ?} parameters of prepared statements. A Flight SQL client sends them in a
 * batch after preparing; the server answers with a new statement handle that carries the SQL and
 * the values together, signed by the cluster. DuckDB binds the values; they never become SQL text.
 */
public final class QueryParameters {

    /** Starts a handle with values; a plain handle is the SQL text, which cannot start with 0xFF. */
    private static final byte[] BOUND_MAGIC = {(byte) 0xFF, 'A', 'F', 'P', '2'};
    private static final String MAC_ALGORITHM = "HmacSHA256";
    private static final int MAC_BYTES = 32;
    /** Starts the handle of one prepared statement: an id, then the SQL text. */
    private static final byte[] STATEMENT_MAGIC = {(byte) 0xFF, 'A', 'F', 'S', '1'};
    private static final int STATEMENT_ID_BYTES = 16;
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();
    private static final int MAX_PARAMETER_BYTES = 16 * 1024 * 1024;
    private static final Pattern DECIMAL = Pattern.compile("DECIMAL\\((\\d+),\\s*(\\d+)\\)");

    /**
     * A statement handle: the SQL and, once the client has sent them, the parameter values.
     *
     * @param sql SQL text
     * @param parameters one-row Arrow IPC stream with the values, or null
     * @param statementId id of the prepared statement, or null for a handle without one
     */
    public record Handle(String sql, byte[] parameters, String statementId) {
    }

    /**
     * Makes the handle of a new prepared statement. Clients that do not take the handle the
     * server returns for parameter values keep using this one; its id finds their values.
     *
     * @param sql SQL text
     * @return statement handle
     */
    public static ByteString statementHandle(String sql) {
        byte[] id = new byte[STATEMENT_ID_BYTES];
        RANDOM.nextBytes(id);
        byte[] text = sql.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(STATEMENT_MAGIC.length + id.length + text.length);
        buffer.put(STATEMENT_MAGIC).put(id).put(text);
        return ByteString.copyFrom(buffer.array());
    }

    /**
     * Key that signs the values in handles. The values are read as Arrow data, and reading
     * damaged Arrow data can crash the JVM, so only handles this cluster signed are read. A
     * cluster sets one key on every node ({@link #useSigningKey}); until then a key of this JVM.
     */
    private static volatile byte[] signingKey = randomKey();
    /** Key before the last {@link #useSigningKey}: handles signed with it still verify. */
    private static volatile byte[] previousKey;

    private QueryParameters() {
    }

    /**
     * Sets the key that signs handles with values; every node of a cluster must use the same one.
     *
     * @param secret cluster secret, at least 32 bytes
     */
    public static synchronized void useSigningKey(byte[] secret) {
        // A key of its own, so that a handle signature can never pass for a ticket's.
        byte[] key = mac(secret, "prepared-statement-values".getBytes(StandardCharsets.UTF_8), 0,
                "prepared-statement-values".length());
        if (!java.security.MessageDigest.isEqual(key, signingKey)) {
            // A node that adopts the cluster's secret later keeps accepting what it signed.
            previousKey = signingKey;
            signingKey = key;
        }
    }

    private static boolean signedHere(byte[] bytes, int signed) {
        byte[] signature = Arrays.copyOfRange(bytes, signed, bytes.length);
        if (java.security.MessageDigest.isEqual(mac(signingKey, bytes, 0, signed), signature)) {
            return true;
        }
        byte[] previous = previousKey;
        return previous != null
                && java.security.MessageDigest.isEqual(mac(previous, bytes, 0, signed), signature);
    }

    private static byte[] randomKey() {
        byte[] key = new byte[MAC_BYTES];
        RANDOM.nextBytes(key);
        return key;
    }

    private static byte[] mac(byte[] key, byte[] data, int offset, int length) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance(MAC_ALGORITHM);
            mac.init(new javax.crypto.spec.SecretKeySpec(key, MAC_ALGORITHM));
            mac.update(data, offset, length);
            return mac.doFinal();
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(MAC_ALGORITHM + " is not available", e);
        }
    }

    /**
     * Makes the handle of a statement with its parameter values.
     *
     * @param sql SQL text
     * @param parameters one-row Arrow IPC stream with the values
     * @return statement handle
     */
    public static ByteString boundHandle(String sql, byte[] parameters) {
        byte[] text = sql.getBytes(StandardCharsets.UTF_8);
        int signed = BOUND_MAGIC.length + Integer.BYTES + text.length + parameters.length;
        ByteBuffer buffer = ByteBuffer.allocate(signed + MAC_BYTES);
        buffer.put(BOUND_MAGIC).putInt(text.length).put(text).put(parameters);
        buffer.put(mac(signingKey, buffer.array(), 0, signed));
        return ByteString.copyFrom(buffer.array());
    }

    /**
     * Reads a statement handle made by this server: SQL text, or SQL with values.
     *
     * @param handle statement handle
     * @return SQL and values
     * @throws IllegalArgumentException when a handle with values is malformed
     */
    public static Handle parseHandle(ByteString handle) {
        byte[] bytes = handle.toByteArray();
        if (startsWith(bytes, STATEMENT_MAGIC)) {
            int textStart = STATEMENT_MAGIC.length + STATEMENT_ID_BYTES;
            if (bytes.length < textStart) {
                throw new IllegalArgumentException("Malformed prepared statement handle");
            }
            String id = java.util.HexFormat.of().formatHex(bytes, STATEMENT_MAGIC.length, textStart);
            return new Handle(new String(bytes, textStart, bytes.length - textStart, StandardCharsets.UTF_8),
                    null, id);
        }
        if (!startsWith(bytes, BOUND_MAGIC)) {
            return new Handle(handle.toStringUtf8(), null, null);
        }
        int signed = bytes.length - MAC_BYTES;
        if (signed < BOUND_MAGIC.length || !signedHere(bytes, signed)) {
            // Damaged, forged, or signed by another cluster (or before a restart of this node).
            throw new IllegalArgumentException("Malformed prepared statement handle");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes, BOUND_MAGIC.length, signed - BOUND_MAGIC.length);
        if (buffer.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException("Malformed prepared statement handle");
        }
        int textLength = buffer.getInt();
        if (textLength < 0 || textLength > buffer.remaining()) {
            throw new IllegalArgumentException("Malformed prepared statement handle");
        }
        String sql = new String(bytes, buffer.position(), textLength, StandardCharsets.UTF_8);
        byte[] parameters = Arrays.copyOfRange(bytes, buffer.position() + textLength, signed);
        return new Handle(sql, parameters, null);
    }

    /**
     * Tells whether bytes start with a marker.
     *
     * @param bytes handle bytes
     * @param magic marker
     * @return true when they do
     */
    private static boolean startsWith(byte[] bytes, byte[] magic) {
        return bytes.length >= magic.length
                && Arrays.equals(bytes, 0, magic.length, magic, 0, magic.length);
    }

    /**
     * Writes the loaded batch of a parameter stream as a one-batch Arrow IPC stream.
     *
     * @param root batch with one row of values
     * @return IPC stream bytes
     * @throws IOException when writing fails
     */
    public static byte[] serialize(VectorSchemaRoot root) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        if (out.size() > MAX_PARAMETER_BYTES) {
            throw new IllegalArgumentException("Parameter values exceed " + MAX_PARAMETER_BYTES + " bytes");
        }
        return out.toByteArray();
    }

    /**
     * Reads the values of a one-row parameter stream as JDBC values, in parameter order.
     *
     * @param allocator Arrow allocator
     * @param parameters IPC stream bytes
     * @return values; null for SQL NULL
     * @throws IOException when the stream is malformed
     */
    public static List<Object> values(BufferAllocator allocator, byte[] parameters) throws IOException {
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(parameters), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (!reader.loadNextBatch() || root.getRowCount() != 1) {
                throw new IllegalArgumentException("Exactly one set of parameter values is supported");
            }
            List<Object> values = new ArrayList<>(root.getFieldVectors().size());
            for (FieldVector vector : root.getFieldVectors()) {
                values.add(jdbcValue(vector));
            }
            return values;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            // The values travel inside the client's handle: damaged bytes are the client's fault.
            throw new IllegalArgumentException("Malformed prepared statement handle", e);
        }
    }

    /**
     * Converts the first value of a vector to the Java type DuckDB binds.
     *
     * @param vector Arrow vector
     * @return value, or null
     */
    static Object jdbcValue(FieldVector vector) {
        if (vector.isNull(0)) {
            return null;
        }
        Object value = vector.getObject(0);
        if (value instanceof Text text) {
            return text.toString();
        }
        if (vector instanceof DateDayVector days) {
            return LocalDate.ofEpochDay(days.get(0));
        }
        if (vector instanceof DateMilliVector) {
            return ((LocalDateTime) value).toLocalDate();
        }
        if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
            return value instanceof LocalDateTime time ? time.toLocalTime() : time(vector);
        }
        if (vector instanceof TimeStampVector stamps
                && vector.getField().getType() instanceof ArrowType.Timestamp type
                && type.getTimezone() != null) {
            // Zoned timestamps come as a count since the epoch in UTC.
            return Instant.EPOCH.plus(stamps.get(0), unit(type.getUnit())).atOffset(ZoneOffset.UTC);
        }
        return value;
    }

    /**
     * Reads a time-of-day value stored as a count of units since midnight.
     *
     * @param vector time vector
     * @return time of day
     */
    private static LocalTime time(FieldVector vector) {
        ArrowType.Time type = (ArrowType.Time) vector.getField().getType();
        long count = ((Number) vector.getObject(0)).longValue();
        return LocalTime.MIDNIGHT.plus(count, unit(type.getUnit()));
    }

    /**
     * Maps an Arrow time unit to a Java one.
     *
     * @param unit Arrow unit
     * @return Java unit
     */
    private static java.time.temporal.ChronoUnit unit(TimeUnit unit) {
        return switch (unit) {
            case SECOND -> java.time.temporal.ChronoUnit.SECONDS;
            case MILLISECOND -> java.time.temporal.ChronoUnit.MILLIS;
            case MICROSECOND -> java.time.temporal.ChronoUnit.MICROS;
            case NANOSECOND -> java.time.temporal.ChronoUnit.NANOS;
        };
    }

    /**
     * Describes the parameters of a statement as an Arrow schema, which the client uses to send
     * values. A parameter of a type DuckDB cannot tell yet is sent as text and cast by DuckDB.
     *
     * @param metaData DuckDB parameter metadata
     * @return one nullable field per parameter
     * @throws SQLException when DuckDB cannot describe the parameters
     */
    static Schema schema(ParameterMetaData metaData) throws SQLException {
        List<Field> fields = new ArrayList<>();
        for (int i = 1; i <= metaData.getParameterCount(); i++) {
            fields.add(Field.nullable("parameter_" + i,
                    arrowType(metaData.getParameterType(i), metaData.getParameterTypeName(i))));
        }
        return new Schema(fields);
    }

    /**
     * Maps a JDBC parameter type to the Arrow type a client sends it in.
     *
     * @param jdbcType JDBC type
     * @param typeName DuckDB type name
     * @return Arrow type
     */
    static ArrowType arrowType(int jdbcType, String typeName) {
        return switch (jdbcType) {
            case Types.BOOLEAN, Types.BIT -> ArrowType.Bool.INSTANCE;
            case Types.TINYINT -> new ArrowType.Int(8, true);
            case Types.SMALLINT -> new ArrowType.Int(16, true);
            case Types.INTEGER -> new ArrowType.Int(32, true);
            case Types.BIGINT -> new ArrowType.Int(64, true);
            case Types.FLOAT, Types.REAL -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case Types.DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case Types.DECIMAL, Types.NUMERIC -> decimal(typeName);
            case Types.DATE -> new ArrowType.Date(DateUnit.DAY);
            case Types.TIME -> new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            case Types.TIMESTAMP -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            case Types.TIMESTAMP_WITH_TIMEZONE -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
            case Types.BLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> ArrowType.Binary.INSTANCE;
            default -> ArrowType.Utf8.INSTANCE;
        };
    }

    /**
     * Reads precision and scale from a DuckDB decimal type name.
     *
     * @param typeName e.g. DECIMAL(10,2)
     * @return Arrow decimal type, or text when the name has no precision
     */
    private static ArrowType decimal(String typeName) {
        Matcher m = typeName == null ? null : DECIMAL.matcher(typeName);
        if (m == null || !m.find()) {
            return ArrowType.Utf8.INSTANCE;
        }
        return new ArrowType.Decimal(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), 128);
    }
}
