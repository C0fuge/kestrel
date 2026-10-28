package pro.surpin.data.arrowflight.plugin.hive;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/**
 * Writes Avro data files from plain Java values, for the server's tests (which call it in the
 * metastore's class loader over target/fixture-libs, where Avro is): integers, longs, doubles,
 * booleans and strings as they are, BigDecimal as the unscaled bytes at the schema's scale,
 * lists as arrays, maps as maps and {@code Object[]} as records (fields by position).
 */
public final class AvroTestFiles {

    private AvroTestFiles() {
    }

    /**
     * Writes a file with the writer time zone UTC.
     *
     * @param file file to write
     * @param schemaJson the file's schema
     * @param codec null, deflate or zstandard
     * @param rows records, each the values of the schema's fields
     * @throws IOException when it cannot be written
     */
    public static void write(String file, String schemaJson, String codec, List<Object[]> rows)
            throws IOException {
        Schema schema = new Schema.Parser().parse(schemaJson);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                new GenericDatumWriter<>(schema))) {
            writer.setCodec(switch (codec) {
                case "deflate" -> CodecFactory.deflateCodec(6);
                case "zstandard" -> CodecFactory.zstandardCodec(3);
                default -> CodecFactory.nullCodec();
            });
            writer.setMeta("writer.time.zone", "UTC");
            writer.setSyncInterval(16 * 1024);
            writer.create(schema, new File(file));
            for (Object[] row : rows) {
                writer.append((GenericRecord) datum(schema, row));
            }
        }
    }

    private static Object datum(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        switch (schema.getType()) {
            case UNION -> {
                for (Schema type : schema.getTypes()) {
                    if (type.getType() != Schema.Type.NULL) {
                        return datum(type, value);
                    }
                }
                throw new IllegalArgumentException("union without a type: " + schema);
            }
            case INT -> {
                return ((Number) value).intValue();
            }
            case LONG -> {
                return ((Number) value).longValue();
            }
            case DOUBLE -> {
                return ((Number) value).doubleValue();
            }
            case FLOAT -> {
                return ((Number) value).floatValue();
            }
            case STRING -> {
                return value.toString();
            }
            case BYTES -> {
                if (value instanceof BigDecimal decimal) {
                    int scale = (Integer) schema.getObjectProp("scale");
                    return ByteBuffer.wrap(decimal.setScale(scale).unscaledValue().toByteArray());
                }
                return ByteBuffer.wrap((byte[]) value);
            }
            case ARRAY -> {
                List<Object> elements = new ArrayList<>();
                for (Object element : (List<?>) value) {
                    elements.add(datum(schema.getElementType(), element));
                }
                return new GenericData.Array<>(schema, elements);
            }
            case MAP -> {
                Map<String, Object> entries = new HashMap<>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    entries.put(entry.getKey().toString(), datum(schema.getValueType(),
                            entry.getValue()));
                }
                return entries;
            }
            case RECORD -> {
                Object[] fields = (Object[]) value;
                GenericRecord record = new GenericData.Record(schema);
                for (int i = 0; i < fields.length; i++) {
                    record.put(i, datum(schema.getFields().get(i).schema(), fields[i]));
                }
                return record;
            }
            default -> {
                return value;
            }
        }
    }
}
