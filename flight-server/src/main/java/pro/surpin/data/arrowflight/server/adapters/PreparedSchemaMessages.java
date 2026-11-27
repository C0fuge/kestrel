package pro.surpin.data.arrowflight.server.adapters;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.apache.arrow.vector.types.pojo.Schema;

/** Keeps bounded immutable IPC encodings of resolved prepared-statement schemas. */
final class PreparedSchemaMessages {
    /** Largest encoding retained individually. */
    private static final int MAX_SCHEMA_BYTES = 64 * 1024;
    private final int maxEntries;
    private final int maxBytes;
    private final Map<Schema, ByteString> messages = new LinkedHashMap<>(16, 0.75f, true);
    private long bytes;

    /** Creates the producer-local cache with at most sixteen encodings and 256 KiB of bytes. */
    PreparedSchemaMessages() {
        this(16, 256 * 1024);
    }

    /**
     * Creates a cache with explicit retention bounds.
     *
     * @param maxEntries maximum retained schemas
     * @param maxBytes maximum retained encoding bytes
     * @throws IllegalArgumentException when either bound is not positive
     */
    PreparedSchemaMessages(int maxEntries, int maxBytes) {
        if (maxEntries <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("Schema cache bounds must be positive");
        }
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
    }

    /**
     * Encodes a currently resolved schema, reusing equal schemas including all their metadata.
     *
     * @param schema resolved result or parameter schema
     * @return immutable schema IPC message
     */
    ByteString encode(Schema schema) {
        synchronized (this) {
            ByteString cached = messages.get(schema);
            if (cached != null) {
                return cached;
            }
        }
        // Serialization stays outside the cache lock; duplicate concurrent misses are harmless.
        ByteString encoded = ByteString.copyFrom(schema.serializeAsMessage());
        if (encoded.size() > MAX_SCHEMA_BYTES || encoded.size() > maxBytes) {
            return encoded;
        }
        synchronized (this) {
            ByteString cached = messages.get(schema);
            if (cached != null) {
                return cached;
            }
            messages.put(schema, encoded);
            bytes += encoded.size();
            var oldest = messages.entrySet().iterator();
            while (messages.size() > maxEntries || bytes > maxBytes) {
                bytes -= oldest.next().getValue().size();
                oldest.remove();
            }
        }
        return encoded;
    }

    /** Drops producer-local schema encodings when the producer closes. */
    synchronized void clear() {
        messages.clear();
        bytes = 0;
    }
}
